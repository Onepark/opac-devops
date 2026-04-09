#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.42",
#   "typer>=0.12",
#   "rich>=13",
# ]
# ///
"""CLI to trigger the OPAC data step function (drifting / anonymisation)."""

from __future__ import annotations

import json
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import typer
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

app = typer.Typer(
    help="Trigger the OPAC data step function (date drifting / anonymisation).",
    no_args_is_help=False,
)
console = Console()

AWS_REGION = "eu-west-3"
STATE_MACHINE_ARN = (
    "arn:aws:states:eu-west-3:418484240945:stateMachine:"
    "drift-anonymisation-state-machine"
)
ECS_CLUSTER_ARN = "arn:aws:ecs:eu-west-3:418484240945:cluster/opk-opac-int-ecs-cluster"
DOPPLER_PROJECT = "opac-data-step-function"
SSM_CONTEXT_PARAM = "/opac/int/step_function/context"


# ---------------------------------------------------------------------------
# Doppler
# ---------------------------------------------------------------------------


def _fetch_doppler_secrets(config: str) -> dict[str, str]:
    """Download secrets from Doppler for the given config (int | prod)."""
    try:
        result = subprocess.run(
            [
                "doppler",
                "secrets",
                "download",
                "--format",
                "json",
                "--no-file",
                "--project",
                DOPPLER_PROJECT,
                "--config",
                config,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        console.print(
            "[bold red]Error:[/bold red] doppler CLI not found.\n"
            "Install it with: [cyan]brew install dopplerhq/cli/doppler[/cyan]"
        )
        raise typer.Exit(1)
    except subprocess.CalledProcessError as exc:
        console.print(f"[bold red]Doppler error:[/bold red] {exc.stderr.strip()}")
        raise typer.Exit(1)

    return json.loads(result.stdout)


# ---------------------------------------------------------------------------
# RDS instance picker
# ---------------------------------------------------------------------------


def _select_rds_instance() -> str:
    """List available RDS instances and let the user pick one."""
    console.print("\nFetching RDS instances…")
    rds = boto3.client("rds", region_name=AWS_REGION)

    try:
        paginator = rds.get_paginator("describe_db_instances")
        instances = [
            inst
            for page in paginator.paginate()
            for inst in page["DBInstances"]
            if any(k in inst["DBInstanceIdentifier"] for k in ("test", "stg"))
        ]
    except Exception as exc:
        console.print(f"[bold red]Error fetching RDS instances:[/bold red] {exc}")
        raise typer.Exit(1)

    if not instances:
        console.print("[yellow]No RDS instances found.[/yellow]")
        return Prompt.ask("Target RDS instance ID")

    table = Table(show_header=True, header_style="bold", title="Available RDS Instances")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Instance ID", style="cyan", no_wrap=True)
    table.add_column("Class")
    table.add_column("Engine")
    table.add_column("Status")

    for i, inst in enumerate(instances, 1):
        status = inst["DBInstanceStatus"]
        status_style = "green" if status == "available" else "yellow"
        table.add_row(
            str(i),
            inst["DBInstanceIdentifier"],
            inst["DBInstanceClass"],
            f"{inst['Engine']} {inst['EngineVersion']}",
            f"[{status_style}]{status}[/{status_style}]",
        )

    console.print(table)

    choice = Prompt.ask(
        "Select target instance",
        choices=[str(i) for i in range(1, len(instances) + 1)],
    )
    return instances[int(choice) - 1]["DBInstanceIdentifier"]


# ---------------------------------------------------------------------------
# Debug: CloudWatch log tailing
# ---------------------------------------------------------------------------


def _get_ecs_log_streams(ecs_client, task_arn: str) -> list[dict]:
    """Return CloudWatch log configs for each container in the ECS task."""
    try:
        tasks = ecs_client.describe_tasks(cluster=ECS_CLUSTER_ARN, tasks=[task_arn])["tasks"]
        if not tasks:
            return []
        task_id = task_arn.split("/")[-1]
        task_def = ecs_client.describe_task_definition(
            taskDefinition=tasks[0]["taskDefinitionArn"]
        )["taskDefinition"]
        result = []
        for c in task_def["containerDefinitions"]:
            log_cfg = c.get("logConfiguration", {})
            if log_cfg.get("logDriver") == "awslogs":
                opts = log_cfg.get("options", {})
                prefix = opts.get("awslogs-stream-prefix", "ecs")
                result.append({
                    "container": c["name"],
                    "log_group": opts.get("awslogs-group", ""),
                    "log_stream": f"{prefix}/{c['name']}/{task_id}",
                })
        return result
    except Exception as exc:
        console.print(f"[dim]  Could not resolve log streams: {exc}[/dim]")
        return []


def _tail_log_stream(
    logs_client,
    log_group: str,
    log_stream: str,
    container_name: str,
    stop_event: threading.Event,
) -> None:
    """Tail a CloudWatch log stream in a background thread until stop_event is set."""
    start_ms = int(time.time() * 1000)
    next_token: str | None = None

    while not stop_event.wait(timeout=2):
        if next_token:
            kwargs: dict = {
                "logGroupName": log_group,
                "logStreamName": log_stream,
                "nextToken": next_token,
            }
        else:
            kwargs = {
                "logGroupName": log_group,
                "logStreamName": log_stream,
                "startFromHead": True,
                "startTime": start_ms,
            }
        try:
            resp = logs_client.get_log_events(**kwargs)
            for ev in resp.get("events", []):
                ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
                msg = ev["message"].rstrip()
                console.print(f"  [dim][{ts}][/dim] [cyan]{container_name}[/cyan] [dim]│[/dim] {msg}")
            next_token = resp.get("nextForwardToken")
        except Exception:
            pass

    # Drain any remaining events after the state exits
    time.sleep(3)
    if next_token:
        try:
            resp = logs_client.get_log_events(
                logGroupName=log_group, logStreamName=log_stream, nextToken=next_token
            )
            for ev in resp.get("events", []):
                ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
                msg = ev["message"].rstrip()
                console.print(f"  [dim][{ts}][/dim] [cyan]{container_name}[/cyan] [dim]│[/dim] {msg}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Execution watcher
# ---------------------------------------------------------------------------

# Events we care about and how to render them
_EVENT_RENDERERS: dict[str, tuple[str, str]] = {
    "ExecutionStarted":   ("dim",         "Execution started"),
    "TaskStateEntered":   ("bold cyan",   "▶  {name} — started"),
    "TaskStateExited":    ("bold green",  "✓  {name} — completed"),
    "ExecutionSucceeded": ("bold green",  "Execution SUCCEEDED"),
    "ExecutionFailed":    ("bold red",    "Execution FAILED"),
    "ExecutionTimedOut":  ("bold red",    "Execution TIMED OUT"),
    "ExecutionAborted":   ("yellow",      "Execution ABORTED"),
}

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}


def _event_label(event: dict) -> tuple[str, str] | None:
    """Return (style, message) for a notable event, or None to skip it."""
    etype = event["type"]
    if etype not in _EVENT_RENDERERS:
        return None
    style, template = _EVENT_RENDERERS[etype]
    # Extract the state name for enter/exit events
    name = ""
    if etype == "TaskStateEntered":
        name = event.get("stateEnteredEventDetails", {}).get("name", "")
    elif etype == "TaskStateExited":
        name = event.get("stateExitedEventDetails", {}).get("name", "")
    message = template.format(name=name)
    return style, message


def _try_cleanup_ssm(ssm_client) -> None:
    """Offer to delete the stale SSM context parameter after a failed execution."""
    if not Confirm.ask(
        f"\nClean up stale SSM context [dim]({SSM_CONTEXT_PARAM})[/dim] "
        "to allow future executions?",
        default=True,
    ):
        console.print(
            f"[dim]To clean up manually:[/dim]\n"
            f"  aws ssm delete-parameter --name \"{SSM_CONTEXT_PARAM}\" --region {AWS_REGION}"
        )
        return
    try:
        ssm_client.delete_parameter(Name=SSM_CONTEXT_PARAM)
        console.print("[green]✓ SSM context cleared.[/green]")
    except ssm_client.exceptions.ParameterNotFound:
        console.print("[dim]SSM context was already absent.[/dim]")
    except Exception as exc:
        console.print(f"[red]Could not delete SSM parameter:[/red] {exc}")
        console.print(
            f"[dim]Run manually:[/dim]\n"
            f"  aws ssm delete-parameter --name \"{SSM_CONTEXT_PARAM}\" --region {AWS_REGION}"
        )


def _watch_execution(
    sf_client,
    ssm_client,
    execution_arn: str,
    poll_interval: int = 30,
    debug: bool = False,
) -> None:
    """Poll execution history and print events until a terminal state is reached."""
    seen_ids: set[int] = set()
    start_time = time.monotonic()
    detached = False
    current_state: str | None = None
    # state_name -> list of (thread, stop_event)
    active_tailers: dict[str, list[tuple[threading.Thread, threading.Event]]] = {}

    if debug:
        ecs_client = boto3.client("ecs", region_name=AWS_REGION)
        logs_client = boto3.client("logs", region_name=AWS_REGION)
        poll_interval = min(poll_interval, 5)
        console.print("[dim]Debug mode: streaming CloudWatch logs (SF poll every 5 s).[/dim]\n")

    def _detach(sig, frame):  # noqa: ANN001
        nonlocal detached
        detached = True

    signal.signal(signal.SIGINT, _detach)

    console.print(
        "\n[bold]Watching execution[/bold] "
        "[dim](Ctrl+C to detach — execution keeps running in AWS)[/dim]\n"
    )

    try:
        while not detached:
            # Collect all unseen events via pagination
            kwargs: dict = {"executionArn": execution_arn, "includeExecutionData": debug}
            new_events: list[dict] = []
            while True:
                resp = sf_client.get_execution_history(**kwargs)
                for ev in resp["events"]:
                    if ev["id"] not in seen_ids:
                        seen_ids.add(ev["id"])
                        new_events.append(ev)
                if next_token := resp.get("nextToken"):
                    kwargs["nextToken"] = next_token
                else:
                    break

            for ev in new_events:
                etype = ev["type"]

                # Track current state name for log tailer association
                if etype == "TaskStateEntered":
                    current_state = ev.get("stateEnteredEventDetails", {}).get("name", "")

                # Debug: attach CW log tailer when ECS task is submitted
                if debug and etype == "TaskSubmitted" and current_state:
                    details = ev.get("taskSubmittedEventDetails", {})
                    try:
                        output = json.loads(details.get("output", "{}"))
                        tasks_list = output.get("Tasks", [])
                        task_arn = tasks_list[0].get("TaskArn", "") if tasks_list else ""
                        if task_arn:
                            short_id = task_arn.split("/")[-1][:8]
                            console.print(f"[dim]  ↳ Attaching logs for {current_state} (task {short_id}…)[/dim]")
                            log_streams = _get_ecs_log_streams(ecs_client, task_arn)
                            new_tailers: list[tuple[threading.Thread, threading.Event]] = []
                            for ls in log_streams:
                                stop_ev = threading.Event()
                                t = threading.Thread(
                                    target=_tail_log_stream,
                                    args=(logs_client, ls["log_group"], ls["log_stream"], ls["container"], stop_ev),
                                    daemon=True,
                                )
                                t.start()
                                new_tailers.append((t, stop_ev))
                            if new_tailers:
                                active_tailers[current_state] = new_tailers
                    except Exception as exc:
                        console.print(f"[dim]  Could not attach log tailer: {exc}[/dim]")

                # Stop tailers when their state exits (after a drain window)
                if etype == "TaskStateExited":
                    state_name = ev.get("stateExitedEventDetails", {}).get("name", "")
                    if state_name in active_tailers:
                        for _, stop_ev in active_tailers[state_name]:
                            stop_ev.set()
                        for t, _ in active_tailers[state_name]:
                            t.join(timeout=10)
                        del active_tailers[state_name]

                # Render notable events
                rendered = _event_label(ev)
                if rendered is None:
                    continue
                style, message = rendered
                elapsed = int(time.monotonic() - start_time)
                timestamp = f"[dim][{elapsed // 60:02d}:{elapsed % 60:02d}][/dim]"
                console.print(f"{timestamp}  [{style}]{message}[/{style}]")

            # Check terminal status
            status = sf_client.describe_execution(
                executionArn=execution_arn
            )["status"]

            if status in _TERMINAL_STATUSES:
                break

            # Show a waiting line and sleep
            elapsed = int(time.monotonic() - start_time)
            console.print(
                f"[dim][{elapsed // 60:02d}:{elapsed % 60:02d}]  "
                f"Still running… next poll in {poll_interval}s[/dim]"
            )
            time.sleep(poll_interval)

    finally:
        # Signal all active tailers to stop and wait briefly for final drain
        for tailer_list in active_tailers.values():
            for _, stop_ev in tailer_list:
                stop_ev.set()
        for tailer_list in active_tailers.values():
            for t, _ in tailer_list:
                t.join(timeout=8)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    if detached:
        console.print("\n[yellow]Detached.[/yellow] Execution continues in AWS.")
        console.print(f"[dim]ARN: {execution_arn}[/dim]")
        console.print(
            f"\n[dim]If the execution fails, clean up the SSM context with:[/dim]\n"
            f"  aws ssm delete-parameter --name \"{SSM_CONTEXT_PARAM}\" --region {AWS_REGION}"
        )
    else:
        final = sf_client.describe_execution(executionArn=execution_arn)
        status = final["status"]
        elapsed = int(time.monotonic() - start_time)
        elapsed_str = f"{elapsed // 60}m {elapsed % 60}s"
        if status == "SUCCEEDED":
            console.print(f"\n[bold green]✓ SUCCEEDED[/bold green] in {elapsed_str}")
        else:
            console.print(f"\n[bold red]✗ {status}[/bold red] after {elapsed_str}")
            if cause := final.get("cause"):
                console.print(f"[red]Cause:[/red] {cause}")
            _try_cleanup_ssm(ssm_client)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ask(value: Optional[str], prompt: str) -> str:
    if value is not None:
        return value
    return Prompt.ask(prompt)


def _ask_bool(value: Optional[bool], prompt: str) -> bool:
    if value is not None:
        return value
    return Confirm.ask(prompt)


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


@app.command()
def main(
    mode: Optional[str] = typer.Option(
        None,
        "--mode",
        "-m",
        help="Environment: int or prod",
        show_default=False,
    ),
    snapshot_arn: Optional[str] = typer.Option(
        None,
        "--snapshot-arn",
        "-s",
        help="ARN of the RDS snapshot to restore from",
        show_default=False,
    ),
    target_rds_instance_id: Optional[str] = typer.Option(
        None,
        "--target-rds-instance-id",
        "-t",
        help="Target RDS instance ID (skips interactive listing if provided)",
        show_default=False,
    ),
    anonymisation: Optional[bool] = typer.Option(
        None,
        "--anonymisation/--no-anonymisation",
        help="Enable or disable data anonymisation",
        show_default=False,
    ),
    drifting: Optional[bool] = typer.Option(
        None,
        "--drifting/--no-drifting",
        help="Enable or disable date drifting",
        show_default=False,
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the payload without triggering the step function",
    ),
    watch: bool = typer.Option(
        True,
        "--watch/--no-watch",
        help="Poll and stream execution progress after triggering",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Stream ECS task CloudWatch logs in real time (implies --watch)",
    ),
) -> None:
    console.rule("[bold blue]OPAC Data Step Function[/bold blue]")

    # --- Mode ---------------------------------------------------------------
    if mode is None:
        mode = Prompt.ask("\nEnvironment", choices=["int", "prod"])
    elif mode not in ("int", "prod"):
        console.print(
            f"[bold red]Invalid mode '[/bold red][cyan]{mode}[/cyan]"
            "[bold red]'. Choose int or prod.[/bold red]"
        )
        raise typer.Exit(1)

    # --- Doppler credentials ------------------------------------------------
    console.print(
        f"\nFetching DB credentials from Doppler "
        f"([cyan]{DOPPLER_PROJECT}[/cyan] / [cyan]{mode}[/cyan])…"
    )
    secrets = _fetch_doppler_secrets(mode)

    # --- Snapshot ARN -------------------------------------------------------
    console.print()
    snapshot_arn = _ask(snapshot_arn, "Snapshot ARN")

    # --- Target RDS instance (listing or manual) ----------------------------
    if target_rds_instance_id is None:
        target_rds_instance_id = _select_rds_instance()

    # --- Flags --------------------------------------------------------------
    console.print()
    anonymisation = _ask_bool(anonymisation, "Enable anonymisation")
    drifting = _ask_bool(drifting, "Enable date drifting")

    # --- Build payload ------------------------------------------------------
    payload: dict = {
        "comment": "Triggered via CLI",
        "snapshotArn": snapshot_arn,
        "snapshotDbHost": secrets["DB_HOST"],
        "snapshotDbName": secrets["DB_NAME"],
        "snapshotDbUsername": secrets["DB_USER"],
        "snapshotDbPassword": secrets["DB_PASSWORD"],
        "snapshotDbPort": int(secrets["DB_PORT"]),
        "targetRdsInstanceId": target_rds_instance_id,
        "anonymisation": anonymisation,
        "drifting": drifting,
    }

    # --- Summary table ------------------------------------------------------
    table = Table(show_header=True, header_style="bold", title="\nStep Function Input")
    table.add_column("Parameter", style="cyan", no_wrap=True)
    table.add_column("Value")
    for k, v in payload.items():
        display = "●●●●●●●●" if k == "snapshotDbPassword" else str(v)
        table.add_row(k, display)
    console.print(table)

    if dry_run:
        console.print("\n[yellow]--dry-run:[/yellow] step function not triggered.")
        return

    # --- Confirmation -------------------------------------------------------
    if not Confirm.ask("\nTrigger the step function?"):
        console.print("Aborted.")
        raise typer.Exit(0)

    # --- Execute ------------------------------------------------------------
    console.print("\nStarting execution…")
    sf_client = boto3.client("stepfunctions", region_name=AWS_REGION)
    ssm_client = boto3.client("ssm", region_name=AWS_REGION)
    response = sf_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(payload),
    )

    execution_arn = response["executionArn"]
    console.print("\n[bold green]✓ Execution started[/bold green]")
    console.print(f"ARN: [dim]{execution_arn}[/dim]")

    if watch or debug:
        _watch_execution(sf_client, ssm_client, execution_arn, debug=debug)
    else:
        console.print(
            f"\n[dim]If the execution fails, clean up the SSM context with:[/dim]\n"
            f"  aws ssm delete-parameter --name \"{SSM_CONTEXT_PARAM}\" --region {AWS_REGION}"
        )


if __name__ == "__main__":
    app()
