#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.42",
#   "typer>=0.12",
#   "rich>=13",
# ]
# ///
"""Open an SSM port-forwarding tunnel to a private RDS instance via a bastion EC2."""

from __future__ import annotations

import atexit
import signal
import subprocess
import sys
from pathlib import Path
from typing import Optional

import boto3
import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

app = typer.Typer(
    help="Open an SSM port-forwarding tunnel to a private RDS instance.",
    no_args_is_help=False,
)
console = Console()

AWS_REGION = "eu-west-3"
HOSTS_FILE = Path("/etc/hosts")


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def _find_bastion(ec2_client, name_tag: str) -> str:
    resp = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [name_tag]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    instances = [i for r in resp["Reservations"] for i in r["Instances"]]
    if not instances:
        console.print(
            f"[red]No running instance found with Name tag = {name_tag!r}[/red]"
        )
        raise typer.Exit(1)
    return instances[0]["InstanceId"]


def _select_rds_instance(rds_client) -> str:
    """List RDS instances and return the chosen hostname."""
    console.print("\nFetching RDS instances…")
    paginator = rds_client.get_paginator("describe_db_instances")
    instances = [inst for page in paginator.paginate() for inst in page["DBInstances"]]

    if not instances:
        console.print("[yellow]No RDS instances found.[/yellow]")
        return Prompt.ask("RDS hostname")

    table = Table(
        show_header=True, header_style="bold", title="Available RDS Instances"
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Instance ID", style="cyan", no_wrap=True)
    table.add_column("Class")
    table.add_column("Engine")
    table.add_column("Status")
    table.add_column("Host", style="dim")

    for i, inst in enumerate(instances, 1):
        status = inst["DBInstanceStatus"]
        style = "green" if status == "available" else "yellow"
        host = inst.get("Endpoint", {}).get("Address", "—")
        table.add_row(
            str(i),
            inst["DBInstanceIdentifier"],
            inst["DBInstanceClass"],
            f"{inst['Engine']} {inst['EngineVersion']}",
            f"[{style}]{status}[/{style}]",
            host,
        )

    console.print(table)
    choice = Prompt.ask(
        "Select RDS instance",
        choices=[str(i) for i in range(1, len(instances) + 1)],
    )
    return instances[int(choice) - 1]["Endpoint"]["Address"]


# ---------------------------------------------------------------------------
# /etc/hosts management
# ---------------------------------------------------------------------------


def _hosts_add(hostname: str) -> None:
    subprocess.run(
        [
            "sudo",
            "sh",
            "-c",
            f"echo '127.0.0.1 {hostname}  # ssm-tunnel' >> /etc/hosts",
        ],
        check=True,
    )


def _hosts_remove(hostname: str) -> None:
    lines = HOSTS_FILE.read_text().splitlines(keepends=True)
    new_content = "".join(line for line in lines if hostname not in line)
    subprocess.run(
        ["sudo", "tee", str(HOSTS_FILE)],
        input=new_content.encode(),
        check=True,
        capture_output=True,
    )


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


@app.command()
def main(
    bastion: str = typer.Option(
        "opk-opac-shared-bastion",
        "--bastion",
        "-b",
        help="Bastion EC2 instance ID (i-xxx) or Name tag",
    ),
    rds_instance: Optional[str] = typer.Option(
        None,
        "--rds",
        "-r",
        help="RDS instance ID — skips interactive listing if provided",
        show_default=False,
    ),
    local_port: int = typer.Option(
        5432, "--local-port", "-p", help="Local port to bind"
    ),
    rds_port: int = typer.Option(5432, "--rds-port", help="Remote RDS port"),
) -> None:
    console.rule("[bold blue]RDS SSM Tunnel[/bold blue]")

    ec2_client = boto3.client("ec2", region_name=AWS_REGION)
    rds_client = boto3.client("rds", region_name=AWS_REGION)

    # --- Bastion ---
    if bastion.startswith("i-"):
        instance_id = bastion
    else:
        console.print(f"\nLooking up bastion [cyan]{bastion}[/cyan]…")
        instance_id = _find_bastion(ec2_client, bastion)

    console.print(f"Bastion: [cyan]{instance_id}[/cyan]")

    # --- RDS host ---
    if rds_instance:
        resp = rds_client.describe_db_instances(DBInstanceIdentifier=rds_instance)
        rds_host = resp["DBInstances"][0]["Endpoint"]["Address"]
    else:
        rds_host = _select_rds_instance(rds_client)

    console.print(f"RDS:     [cyan]{rds_host}[/cyan]")

    # --- /etc/hosts ---
    # Required for SSL hostname verification: psql connects to the real RDS hostname
    # which resolves to 127.0.0.1 (the tunnel) instead of the private VPC address.
    console.print(
        f"\nAdding [dim]127.0.0.1 {rds_host}[/dim] to /etc/hosts (requires sudo)…"
    )
    _hosts_add(rds_host)
    console.print("[green]✓[/green] Hosts entry added.")

    hosts_cleaned = False

    def _cleanup() -> None:
        nonlocal hosts_cleaned
        if hosts_cleaned:
            return
        hosts_cleaned = True
        console.print(f"\nRemoving [dim]{rds_host}[/dim] from /etc/hosts…")
        try:
            _hosts_remove(rds_host)
            console.print("[green]✓[/green] Hosts entry removed.")
        except Exception as exc:
            console.print(f"[yellow]Could not clean /etc/hosts:[/yellow] {exc}")
            console.print(
                f"[dim]Remove manually:[/dim] sudo sed -i '' '/{rds_host}/d' /etc/hosts"
            )

    atexit.register(_cleanup)
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    # --- Start tunnel ---
    console.print(
        f"\n[bold]Tunnel open[/bold]  "
        f"[cyan]{rds_host}:{local_port}[/cyan] → RDS :{rds_port}"
    )
    console.print(
        f"Connect:  [dim]psql -h {rds_host} -p {local_port} -U <user> -d <db>[/dim]"
    )
    console.print("[dim]Ctrl+C to stop and remove the hosts entry[/dim]\n")

    subprocess.run(
        [
            "aws",
            "ssm",
            "start-session",
            "--target",
            instance_id,
            "--document-name",
            "AWS-StartPortForwardingSessionToRemoteHost",
            "--parameters",
            (
                f'{{"host":["{rds_host}"],'
                f'"portNumber":["{rds_port}"],'
                f'"localPortNumber":["{local_port}"]}}'
            ),
            "--region",
            AWS_REGION,
        ]
    )


if __name__ == "__main__":
    app()
