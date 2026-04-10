#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.42",
#   "typer>=0.12",
#   "rich>=13",
# ]
# ///
"""Open an SSM port-forwarding tunnel to a private RDS instance or ECS service via a bastion EC2."""

from __future__ import annotations

import atexit
import json
import signal
import subprocess
import sys
from pathlib import Path

import boto3
import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

app = typer.Typer(
    help="Open an SSM port-forwarding tunnel to a private resource.",
    no_args_is_help=True,
)
console = Console()

AWS_REGION = "eu-west-3"
BASTION_NAME = "opk-opac-shared-bastion"
CLUSTER_NAME_PREFIX = "opk-opac-"
CLUSTER_NAME_SUFFIX = "-ecs-cluster"
ALB_NAME = "opac-nonprod-alb"
API_HOST_PATTERN = "api.opac.{stage}.onepark.dev"


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def _get_bastion() -> str:
    """Return the instance ID of the running bastion."""
    ec2_client = boto3.client("ec2", region_name=AWS_REGION)
    resp = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [BASTION_NAME]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    instances = [i for r in resp["Reservations"] for i in r["Instances"]]
    if not instances:
        console.print(f"[red]No running instance found with Name tag = {BASTION_NAME!r}[/red]")
        raise typer.Exit(1)
    return instances[0]["InstanceId"]


def _get_alb_dns() -> str:
    """Return the internal DNS name of the shared ALB."""
    elb_client = boto3.client("elbv2", region_name=AWS_REGION)
    resp = elb_client.describe_load_balancers(Names=[ALB_NAME])
    load_balancers = resp["LoadBalancers"]
    if not load_balancers:
        console.print(f"[red]No ALB found with name {ALB_NAME!r}[/red]")
        raise typer.Exit(1)
    return load_balancers[0]["DNSName"]


def _select_rds_instance() -> str:
    """List RDS instances interactively and return the chosen hostname."""
    console.print("\nFetching RDS instances…")
    rds_client = boto3.client("rds", region_name=AWS_REGION)
    paginator = rds_client.get_paginator("describe_db_instances")
    instances = [inst for page in paginator.paginate() for inst in page["DBInstances"]]

    if not instances:
        console.print("[red]No RDS instances found.[/red]")
        raise typer.Exit(1)

    table = Table(show_header=True, header_style="bold", title="Available RDS Instances")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Instance ID", style="cyan", no_wrap=True)
    table.add_column("Class")
    table.add_column("Engine")
    table.add_column("Status")
    table.add_column("Host", style="dim")

    for i, inst in enumerate(instances, 1):
        status = inst["DBInstanceStatus"]
        color = "green" if status == "available" else "yellow"
        host = inst.get("Endpoint", {}).get("Address", "—")
        table.add_row(
            str(i),
            inst["DBInstanceIdentifier"],
            inst["DBInstanceClass"],
            f"{inst['Engine']} {inst['EngineVersion']}",
            f"[{color}]{status}[/{color}]",
            host,
        )

    console.print(table)
    choice = Prompt.ask("Select RDS instance", choices=[str(i) for i in range(1, len(instances) + 1)])
    return instances[int(choice) - 1]["Endpoint"]["Address"]


def _select_ecs_stage() -> str:
    """List ECS clusters interactively and return the chosen stage name."""
    console.print("\nFetching ECS clusters…")
    ecs_client = boto3.client("ecs", region_name=AWS_REGION)
    paginator = ecs_client.get_paginator("list_clusters")
    all_arns = [arn for page in paginator.paginate() for arn in page["clusterArns"]]

    stages = []
    for arn in all_arns:
        name = arn.split("/")[-1]
        if name.startswith(CLUSTER_NAME_PREFIX) and name.endswith(CLUSTER_NAME_SUFFIX):
            stage = name[len(CLUSTER_NAME_PREFIX):-len(CLUSTER_NAME_SUFFIX)]
            stages.append((stage, name))

    if not stages:
        console.print("[red]No matching ECS clusters found.[/red]")
        raise typer.Exit(1)

    cluster_names = [name for _, name in stages]
    details = {
        c["clusterName"]: c
        for c in ecs_client.describe_clusters(clusters=cluster_names)["clusters"]
    }

    table = Table(show_header=True, header_style="bold", title="Available ECS Clusters")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Stage", style="cyan")
    table.add_column("Cluster")
    table.add_column("Status")
    table.add_column("Running tasks", justify="right")

    for i, (stage, name) in enumerate(stages, 1):
        info = details.get(name, {})
        status = info.get("status", "—")
        color = "green" if status == "ACTIVE" else "yellow"
        running = str(info.get("runningTasksCount", "—"))
        table.add_row(str(i), stage, name, f"[{color}]{status}[/{color}]", running)

    console.print(table)
    choice = Prompt.ask("Select ECS cluster", choices=[str(i) for i in range(1, len(stages) + 1)])
    return stages[int(choice) - 1][0]


# ---------------------------------------------------------------------------
# /etc/hosts management
# ---------------------------------------------------------------------------


def _hosts_add(hostname: str) -> None:
    subprocess.run(
        ["sudo", "sh", "-c", f"echo '127.0.0.1 {hostname}  # ssm-tunnel' >> /etc/hosts"],
        check=True,
    )


def _hosts_remove(hostname: str) -> None:
    hosts_file = Path("/etc/hosts")
    lines = hosts_file.read_text().splitlines(keepends=True)
    new_content = "".join(line for line in lines if hostname not in line)
    subprocess.run(["sudo", "tee", str(hosts_file)], input=new_content.encode(), check=True, capture_output=True)


def _register_cleanup(hostname: str) -> None:
    cleaned = False

    def _cleanup() -> None:
        nonlocal cleaned
        if cleaned:
            return
        cleaned = True
        console.print(f"\nRemoving [dim]{hostname}[/dim] from /etc/hosts…")
        try:
            _hosts_remove(hostname)
            console.print("[green]✓[/green] Hosts entry removed.")
        except Exception as exc:
            console.print(f"[yellow]Could not clean /etc/hosts:[/yellow] {exc}")
            console.print(f"[dim]Remove manually:[/dim] sudo sed -i '/{hostname}/d' /etc/hosts")

    atexit.register(_cleanup)
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))


# ---------------------------------------------------------------------------
# Tunnel
# ---------------------------------------------------------------------------


def _open_tunnel(instance_id: str, remote_host: str, remote_port: int, local_port: int) -> None:
    """Start an SSM port-forwarding session through the bastion."""
    subprocess.run([
        "aws", "ssm", "start-session",
        "--target", instance_id,
        "--document-name", "AWS-StartPortForwardingSessionToRemoteHost",
        "--parameters", json.dumps({
            "host": [remote_host],
            "portNumber": [str(remote_port)],
            "localPortNumber": [str(local_port)],
        }),
        "--region", AWS_REGION,
    ])


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


RDS_PORT = 5432


@app.command()
def rds(
    local_port: int = typer.Option(RDS_PORT, "--local-port", "-p", help="Local port to bind"),
) -> None:
    """Open an SSM port-forwarding tunnel to a private RDS instance."""
    console.rule("[bold blue]RDS SSM Tunnel[/bold blue]")

    console.print(f"\nLooking up bastion [cyan]{BASTION_NAME}[/cyan]…")
    instance_id = _get_bastion()
    console.print(f"Bastion: [cyan]{instance_id}[/cyan]")

    rds_host = _select_rds_instance()
    console.print(f"RDS:     [cyan]{rds_host}[/cyan]")

    console.print(f"\nAdding [dim]127.0.0.1 {rds_host}[/dim] to /etc/hosts (requires sudo)…")
    _hosts_add(rds_host)
    console.print("[green]✓[/green] Hosts entry added.")
    _register_cleanup(rds_host)

    console.print(f"\n[bold]Tunnel open[/bold]  [cyan]{rds_host}:{local_port}[/cyan] → RDS :{RDS_PORT}")
    console.print(f"Connect:  [dim]psql -h {rds_host} -p {local_port} -U <user> -d <db>[/dim]")
    console.print("[dim]Ctrl+C to stop and remove the hosts entry[/dim]\n")

    _open_tunnel(instance_id, rds_host, RDS_PORT, local_port)


@app.command()
def ecs(
    local_port: int = typer.Option(8443, "--local-port", "-p", help="Local port to bind"),
) -> None:
    """Open an SSM port-forwarding tunnel to an ECS API service via the internal ALB."""
    console.rule("[bold blue]ECS SSM Tunnel[/bold blue]")

    console.print(f"\nLooking up bastion [cyan]{BASTION_NAME}[/cyan]…")
    instance_id = _get_bastion()
    console.print(f"Bastion: [cyan]{instance_id}[/cyan]")

    stage = _select_ecs_stage()

    console.print(f"\nResolving ALB [cyan]{ALB_NAME}[/cyan]…")
    alb_dns = _get_alb_dns()
    api_host = API_HOST_PATTERN.format(stage=stage)
    console.print(f"ALB:     [cyan]{alb_dns}[/cyan]")
    console.print(f"Host:    [cyan]{api_host}[/cyan]")

    console.print(f"\n[bold]Tunnel open[/bold]  localhost:{local_port} → ALB :443 → ECS ({stage})")
    console.print(f"Host header: [dim]{api_host}[/dim]")
    console.print(f"Request: [dim]https --verify=no --json POST https://localhost:{local_port}/api/cpm/graphql \"Host:{api_host}\"[/dim]")
    console.print("[dim]Ctrl+C to stop[/dim]\n")

    _open_tunnel(instance_id, alb_dns, 443, local_port)


if __name__ == "__main__":
    app()
