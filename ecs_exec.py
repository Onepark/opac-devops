#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.42",
#   "typer>=0.12",
#   "rich>=13",
# ]
# ///
"""Exec into a running ECS container via SSM."""

from __future__ import annotations

import subprocess
from typing import Optional

import boto3
import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

app = typer.Typer(
    help="Exec into a running ECS container via SSM.",
    no_args_is_help=False,
)
console = Console()

AWS_REGION = "eu-west-3"


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def _select_cluster(ecs_client) -> str:
    console.print("\nFetching ECS clusters…")
    paginator = ecs_client.get_paginator("list_clusters")
    arns = [arn for page in paginator.paginate() for arn in page["clusterArns"]]

    if not arns:
        console.print("[yellow]No ECS clusters found.[/yellow]")
        raise typer.Exit(1)

    clusters = ecs_client.describe_clusters(clusters=arns)["clusters"]

    table = Table(show_header=True, header_style="bold", title="Available ECS Clusters")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Status")
    table.add_column("Running tasks", justify="right")

    for i, cluster in enumerate(clusters, 1):
        status = cluster.get("status", "—")
        style = "green" if status == "ACTIVE" else "yellow"
        table.add_row(
            str(i),
            cluster["clusterName"],
            f"[{style}]{status}[/{style}]",
            str(cluster.get("runningTasksCount", 0)),
        )

    console.print(table)
    choice = Prompt.ask(
        "Select cluster",
        choices=[str(i) for i in range(1, len(clusters) + 1)],
    )
    return clusters[int(choice) - 1]["clusterArn"]


def _select_task(ecs_client, cluster: str, service: str) -> str:
    console.print(f"\nFetching tasks…")
    resp = ecs_client.list_tasks(cluster=cluster, serviceName=service)
    task_arns = resp.get("taskArns", [])

    if not task_arns:
        console.print(f"[red]No running tasks found for service {service!r}.[/red]")
        raise typer.Exit(1)

    if len(task_arns) == 1:
        return task_arns[0]

    tasks = ecs_client.describe_tasks(cluster=cluster, tasks=task_arns)["tasks"]

    table = Table(show_header=True, header_style="bold", title="Running Tasks")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Task ID", style="cyan", no_wrap=True)
    table.add_column("Status")
    table.add_column("Started At", style="dim")

    for i, task in enumerate(tasks, 1):
        task_id = task["taskArn"].split("/")[-1]
        status = task.get("lastStatus", "—")
        style = "green" if status == "RUNNING" else "yellow"
        table.add_row(
            str(i),
            task_id,
            f"[{style}]{status}[/{style}]",
            str(task.get("startedAt", "—")),
        )

    console.print(table)
    choice = Prompt.ask(
        "Select task",
        choices=[str(i) for i in range(1, len(tasks) + 1)],
    )
    return tasks[int(choice) - 1]["taskArn"]


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


@app.command()
def main(
    cluster: Optional[str] = typer.Option(
        None,
        "--cluster",
        "-c",
        help="ECS cluster ARN or name — skips interactive listing if provided",
        show_default=False,
    ),
    task: Optional[str] = typer.Option(
        None,
        "--task",
        "-t",
        help="ECS task ID — skips interactive listing if provided",
        show_default=False,
    ),
    container: str = typer.Option(
        "api",
        "--container",
        help="Container name",
    ),
    command: str = typer.Option(
        "/bin/sh",
        "--command",
        help="Command to run inside the container",
    ),
) -> None:
    console.rule("[bold blue]ECS Exec[/bold blue]")

    ecs_client = boto3.client("ecs", region_name=AWS_REGION)

    # --- Cluster ---
    if not cluster:
        cluster = _select_cluster(ecs_client)

    cluster_name = cluster.split("/")[-1]
    console.print(f"Cluster: [cyan]{cluster_name}[/cyan]")

    # --- Service (derive from cluster name) ---
    service = cluster_name.replace("-ecs-cluster", "-ecs-service")

    # --- Task ---
    if not task:
        task_arn = _select_task(ecs_client, cluster, service)
    else:
        task_arn = task

    task_id = task_arn.split("/")[-1]
    console.print(f"Task:      [cyan]{task_id}[/cyan]")
    console.print(f"Container: [cyan]{container}[/cyan]")
    console.print(f"\n[dim]Ctrl+C to exit the session[/dim]\n")

    subprocess.run(
        [
            "aws",
            "ecs",
            "execute-command",
            "--cluster", cluster,
            "--task", task_id,
            "--container", container,
            "--command", command,
            "--interactive",
            "--region", AWS_REGION,
        ]
    )


if __name__ == "__main__":
    app()
