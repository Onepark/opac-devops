#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.42",
#   "typer>=0.12",
#   "rich>=13",
# ]
# ///
"""Switch the staging DB cutover CNAME to the latest ready production-derived slot."""

from __future__ import annotations

import datetime as dt
import os
import re
from typing import Optional

import boto3
import typer
from botocore.config import Config
from botocore.exceptions import ClientError
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

app = typer.Typer(
    help="Switch the staging DB cutover CNAME to the latest ready production-derived slot.",
    no_args_is_help=False,
)
console = Console()

AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")
HOSTED_ZONE_NAME = "onepark.dev."
CUTOVER_RECORD_NAME = "db.stg.onepark.dev"
CUTOVER_RECORD_FQDN = (
    f"{CUTOVER_RECORD_NAME}."
    if not CUTOVER_RECORD_NAME.endswith(".")
    else CUTOVER_RECORD_NAME
)
CUTOVER_TTL = 30
STATE_TABLE_NAME = os.environ.get(
    "OPAC_STG_DB_TABLE", "opk-opac-stg-prod-restore-state"
)
BLUE_DB_IDENTIFIER = os.environ.get(
    "OPAC_STG_DB_BLUE", "opk-opac-stg-prod-restore-blue"
)
GREEN_DB_IDENTIFIER = os.environ.get(
    "OPAC_STG_DB_GREEN", "opk-opac-stg-prod-restore-green"
)
ECS_CLUSTER_NAME = os.environ.get(
    "OPAC_STG_DB_ECS_CLUSTER", "opk-opac-stg-ecs-cluster"
)
ECS_SERVICE_NAME = os.environ.get(
    "OPAC_STG_DB_ECS_SERVICE", "opk-opac-stg-ecs-service"
)

SLOT_IDENTIFIERS = {"blue": BLUE_DB_IDENTIFIER, "green": GREEN_DB_IDENTIFIER}

RETRY_CONFIG = Config(retries={"mode": "standard", "max_attempts": 6})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_endpoint(value: str) -> str:
    return (value or "").lower().rstrip(".")


def _parse_iso_week(week_str: str) -> tuple[int, int]:
    """Parse 'YYYYwWW' into (iso_year, iso_week). Returns (-1, -1) if invalid."""
    if not week_str:
        return (-1, -1)
    match = re.match(r"^(\d{4})w(\d{2})$", week_str)
    if not match:
        return (-1, -1)
    return int(match.group(1)), int(match.group(2))


def _client_error_message(exc: ClientError) -> str:
    err = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
    code = err.get("Code", "Unknown")
    message = err.get("Message", str(exc))
    return f"{code} — {message}"


def _discover_hosted_zone_id(route53_client) -> str:
    paginator = route53_client.get_paginator("list_hosted_zones")
    matches = []
    for page in paginator.paginate():
        for zone in page.get("HostedZones", []):
            if (
                zone.get("Config", {}).get("PrivateZone") is True
                and zone.get("Name") == HOSTED_ZONE_NAME
            ):
                matches.append(zone["Id"])
    if not matches:
        console.print(
            f"[red]No private hosted zone found with name {HOSTED_ZONE_NAME!r}[/red]"
        )
        raise typer.Exit(1)
    if len(matches) > 1:
        console.print(
            f"[red]Multiple private hosted zones found with name {HOSTED_ZONE_NAME!r}[/red]"
        )
        raise typer.Exit(1)
    return matches[0]


def _read_cutover_target(route53_client, zone_id: str) -> str:
    response = route53_client.list_resource_record_sets(
        HostedZoneId=zone_id,
        StartRecordName=CUTOVER_RECORD_NAME,
        StartRecordType="CNAME",
        MaxItems="10",
    )
    target_name = _normalise_endpoint(CUTOVER_RECORD_NAME)
    for record in response.get("ResourceRecordSets", []):
        record_name = _normalise_endpoint(record.get("Name", ""))
        if record_name == target_name and record.get("Type") == "CNAME":
            values = record.get("ResourceRecords", [])
            if values:
                return _normalise_endpoint(values[0]["Value"])
    return ""


def _read_slot(dynamodb_resource, slot_name: str) -> Optional[dict]:
    table = dynamodb_resource.Table(STATE_TABLE_NAME)
    response = table.get_item(Key={"pk": f"SLOT#{slot_name}"})
    return response.get("Item")


def _slot_is_ready(doc: Optional[dict]) -> bool:
    return bool(doc) and bool(doc.get("readyForQa"))


def _pick_ready_slot(blue_doc: Optional[dict], green_doc: Optional[dict]) -> dict:
    candidates = []
    for name, doc in (("blue", blue_doc), ("green", green_doc)):
        if not _slot_is_ready(doc):
            continue
        iso_year, iso_week = _parse_iso_week(doc.get("sourceSnapshotWeek", ""))
        # Tie-break on sourceSnapshotCreateTime if present, else the slot's
        # own createdAt — both are ISO 8601 strings and sort lexicographically.
        tie_break = doc.get("sourceSnapshotCreateTime") or doc.get("createdAt") or ""
        candidates.append(
            {
                "slot": name,
                "doc": doc,
                "db_identifier": SLOT_IDENTIFIERS[name],
                "iso_year": iso_year,
                "iso_week": iso_week,
                "tie_break": tie_break,
            }
        )
    if not candidates:
        console.print("[red]no slot is ready; trigger or wait for a reconcile[/red]")
        raise typer.Exit(1)
    candidates.sort(
        key=lambda c: (c["iso_year"], c["iso_week"], c["tie_break"]),
        reverse=True,
    )
    return candidates[0]


def _resolve_slot_endpoint(rds_client, db_identifier: str) -> str:
    """Look up the live endpoint of a slot's RDS instance by its identifier."""
    try:
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "DBInstanceNotFound":
            console.print(
                f"[red]✗[/red] Slot's RDS instance {db_identifier!r} not found "
                f"in AWS even though DynamoDB marks its slot as readyForQa. "
                f"The state table and AWS are out of sync — investigate."
            )
            raise typer.Exit(1)
        raise

    instances = response.get("DBInstances", [])
    if not instances:
        console.print(
            f"[red]✗[/red] Slot's RDS instance {db_identifier!r} returned no "
            f"instance. The state table and AWS are out of sync — investigate."
        )
        raise typer.Exit(1)

    status = instances[0].get("DBInstanceStatus")
    endpoint = _normalise_endpoint(instances[0].get("Endpoint", {}).get("Address", ""))
    if not endpoint:
        console.print(
            f"[red]✗[/red] Slot's RDS instance {db_identifier!r} has status "
            f"{status!r} and no endpoint yet. Aborting."
        )
        raise typer.Exit(1)

    if status != "available":
        console.print(
            f"[yellow]![/yellow] Slot {db_identifier!r} status is {status!r} "
            f"(not 'available'); proceeding anyway."
        )

    return endpoint


def _update_cutover(route53_client, zone_id: str, new_target: str) -> None:
    response = route53_client.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Comment": f"stg-db switch: cutover to {new_target}",
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": CUTOVER_RECORD_FQDN,
                        "Type": "CNAME",
                        "TTL": CUTOVER_TTL,
                        "ResourceRecords": [{"Value": new_target}],
                    },
                }
            ],
        },
    )
    change_id = response.get("ChangeInfo", {}).get("Id", "")
    console.print(f"[dim]Route53 change submitted: {change_id}[/dim]")


def _promote_slot(dynamodb_resource, slot_name: str) -> None:
    table = dynamodb_resource.Table(STATE_TABLE_NAME)
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    table.update_item(
        Key={"pk": f"SLOT#{slot_name}"},
        UpdateExpression=("SET #role = :role, promotedAt = :now, updatedAt = :now"),
        ExpressionAttributeNames={"#role": "role"},
        ExpressionAttributeValues={
            ":role": "active",
            ":now": now_iso,
        },
    )


def _demote_slot(dynamodb_resource, slot_name: str) -> None:
    table = dynamodb_resource.Table(STATE_TABLE_NAME)
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    table.update_item(
        Key={"pk": f"SLOT#{slot_name}"},
        UpdateExpression="SET #role = :role, updatedAt = :now",
        ExpressionAttributeNames={"#role": "role"},
        ExpressionAttributeValues={":role": "", ":now": now_iso},
    )


def _destroy_rds(rds_client, db_identifier: str) -> None:
    try:
        rds_client.delete_db_instance(
            DBInstanceIdentifier=db_identifier,
            SkipFinalSnapshot=True,
        )
        console.print(
            f"[green]✓[/green] Destroy submitted for {db_identifier} (fire-and-forget)"
        )
    except ClientError as exc:
        console.print(
            f"[yellow]![/yellow] Could not destroy {db_identifier}: "
            f"{_client_error_message(exc)}"
        )
        console.print(
            "[dim]hint[/dim] Orphan stays; DynamoDB record still references the endpoint."
        )


def _print_restart_hint(cluster_name: str, service_name: str) -> None:
    console.print(
        f"[dim]hint[/dim] Cutover succeeded; restart the staging API tasks "
        f"manually: `aws ecs update-service --cluster {cluster_name} "
        f"--service {service_name} --force-new-deployment`."
    )


def _restart_ecs_tasks(ecs_client, cluster_name: str, service_name: str) -> None:
    """Force a new deployment of the staging ECS API service (soft failure)."""
    try:
        desc = ecs_client.describe_services(
            cluster=cluster_name, services=[service_name]
        )
    except ClientError as exc:
        console.print(
            f"[yellow]![/yellow] Could not describe ECS service "
            f"{service_name!r} on cluster {cluster_name!r}: "
            f"{_client_error_message(exc)}"
        )
        _print_restart_hint(cluster_name, service_name)
        return

    services = desc.get("services") or []
    if not services:
        console.print(
            f"[yellow]![/yellow] ECS service {service_name!r} not found on "
            f"cluster {cluster_name!r}. Set OPAC_STG_DB_ECS_CLUSTER / "
            f"OPAC_STG_DB_ECS_SERVICE to the correct names."
        )
        _print_restart_hint(cluster_name, service_name)
        return

    status = services[0].get("status")
    if status != "ACTIVE":
        console.print(
            f"[yellow]![/yellow] ECS service {service_name!r} status is "
            f"{status!r} (not ACTIVE); skipping forced redeployment."
        )
        _print_restart_hint(cluster_name, service_name)
        return

    try:
        ecs_client.update_service(
            cluster=cluster_name,
            service=service_name,
            forceNewDeployment=True,
        )
        console.print(
            f"[green]✓[/green] Forced new deployment of ECS service "
            f"{service_name!r} on cluster {cluster_name!r} "
            f"(tasks recycle against the new RDS)."
        )
    except ClientError as exc:
        console.print(
            f"[yellow]![/yellow] ECS update-service failed: "
            f"{_client_error_message(exc)}"
        )
        _print_restart_hint(cluster_name, service_name)


def _write_audit(dynamodb_resource, audit: dict) -> bool:
    table = dynamodb_resource.Table(STATE_TABLE_NAME)
    try:
        table.put_item(Item=audit)
        return True
    except ClientError as exc:
        console.print(
            f"[red]✗[/red] Audit record write failed: {_client_error_message(exc)}"
        )
        return False


def _build_plan(
    zone_id: str,
    current_target: str,
    chosen: dict,
    blue_doc: Optional[dict],
    green_doc: Optional[dict],
    restart_tasks: bool,
) -> dict:
    chosen_slot = chosen["slot"]
    other_doc = green_doc if chosen_slot == "blue" else blue_doc
    previous_active_slot = None
    if other_doc and other_doc.get("role") == "active":
        previous_active_slot = "green" if chosen_slot == "blue" else "blue"

    return {
        "zone_id": zone_id,
        "current_target": current_target,
        "chosen_slot": chosen_slot,
        "chosen_endpoint": chosen["endpoint"],
        "snapshot_week": chosen["doc"].get("sourceSnapshotWeek", ""),
        "snapshot_arn": chosen["doc"].get("sourceSnapshotArn", ""),
        "previous_active_slot": previous_active_slot,
        "previous_db_identifier": (
            SLOT_IDENTIFIERS[previous_active_slot] if previous_active_slot else None
        ),
        "restart_tasks": restart_tasks,
    }


def _print_plan(plan: dict) -> None:
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("key", style="dim")
    table.add_column("value", style="cyan")
    table.add_row("Zone", plan["zone_id"])
    table.add_row(
        "Current cutover target",
        plan["current_target"] or "[dim](empty)[/dim]",
    )
    table.add_row("Chosen slot", plan["chosen_slot"])
    table.add_row("Chosen endpoint", plan["chosen_endpoint"])
    table.add_row("Source snapshot week", plan["snapshot_week"])
    table.add_row("Source snapshot ARN", plan["snapshot_arn"])
    table.add_row(
        "Previous active slot",
        plan["previous_active_slot"] or "[dim](none)[/dim]",
    )
    if plan["previous_db_identifier"]:
        table.add_row("RDS to destroy", plan["previous_db_identifier"])
    table.add_row(
        "Post-switch task restart",
        "yes (force new deployment)"
        if plan["restart_tasks"]
        else "[dim]skipped (--no-restart-tasks)[/dim]",
    )
    console.print(table)


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


@app.command()
def switch(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the plan and exit without mutating anything.",
    ),
    requester: str = typer.Option(
        os.environ.get("USER", "unknown"),
        "--requester",
        help="Audit metadata: who is promoting.",
    ),
    reason: str = typer.Option(
        "",
        "--reason",
        help="Audit metadata: why are you promoting?",
    ),
    restart_tasks: bool = typer.Option(
        True,
        "--restart-tasks/--no-restart-tasks",
        help=(
            "Force a new deployment of the staging ECS API service after the "
            "cutover so tasks reconnect to the new RDS (default: on)."
        ),
    ),
) -> None:
    console.rule("[bold blue]Staging DB Switch[/bold blue]")

    route53_client = boto3.client(
        "route53", region_name=AWS_REGION, config=RETRY_CONFIG
    )
    dynamodb_resource = boto3.resource(
        "dynamodb", region_name=AWS_REGION, config=RETRY_CONFIG
    )
    rds_client = boto3.client("rds", region_name=AWS_REGION, config=RETRY_CONFIG)
    ecs_client = boto3.client("ecs", region_name=AWS_REGION, config=RETRY_CONFIG)

    # --- Step 1: current Route53 cutover target ---
    try:
        zone_id = _discover_hosted_zone_id(route53_client)
        current_target = _read_cutover_target(route53_client, zone_id)
    except ClientError as exc:
        console.print(f"[red]✗[/red] Route53 read failed: {_client_error_message(exc)}")
        raise typer.Exit(1)

    # --- Step 2: read both slot documents ---
    try:
        blue_doc = _read_slot(dynamodb_resource, "blue")
        green_doc = _read_slot(dynamodb_resource, "green")
    except ClientError as exc:
        console.print(
            f"[red]✗[/red] DynamoDB read failed: {_client_error_message(exc)}"
        )
        raise typer.Exit(1)

    # --- Step 3: pick the ready slot ---
    chosen = _pick_ready_slot(blue_doc, green_doc)

    # --- Resolve the chosen slot's endpoint from RDS ---
    # (DynamoDB doesn't store the live endpoint; RDS is authoritative.)
    try:
        chosen["endpoint"] = _resolve_slot_endpoint(rds_client, chosen["db_identifier"])
    except ClientError as exc:
        console.print(
            f"[red]✗[/red] RDS describe for chosen slot failed: "
            f"{_client_error_message(exc)}"
        )
        raise typer.Exit(1)

    # --- Step 5: no-op if current cutover already points at the chosen slot ---
    if current_target == chosen["endpoint"]:
        console.print(
            f"[green]✓[/green] Cutover already points at {chosen['slot']} "
            f"({current_target}). Nothing to do."
        )
        raise typer.Exit(0)

    plan = _build_plan(zone_id, current_target, chosen, blue_doc, green_doc, restart_tasks)

    console.print("\n[bold]Plan[/bold]")
    _print_plan(plan)

    if dry_run:
        console.print("\n[yellow]--dry-run[/yellow] No changes made.")
        raise typer.Exit(0)

    if not Confirm.ask("\nProceed with the promotion?", default=False):
        console.print("[yellow]Aborted.[/yellow]")
        raise typer.Exit(0)

    # --- Step 6.1: update Route53 CNAME ---
    try:
        _update_cutover(route53_client, zone_id, chosen["endpoint"])
    except ClientError as exc:
        console.print(
            f"[red]✗[/red] Route53 change failed: {_client_error_message(exc)}"
        )
        console.print(
            "[red]Aborting — no slot state was modified, no RDS was destroyed.[/red]"
        )
        raise typer.Exit(1)

    # --- Step 6.2: promote chosen slot ---
    try:
        _promote_slot(dynamodb_resource, plan["chosen_slot"])
    except ClientError as exc:
        console.print(
            f"[red]✗[/red] Promote slot state failed: {_client_error_message(exc)}"
        )
        console.print(
            "[red]CRITICAL: Route53 cutover already moved. "
            "Slot state is inconsistent — investigate before running again.[/red]"
        )
        raise typer.Exit(1)

    destroyed_slot = ""
    if plan["previous_active_slot"]:
        previous_slot = plan["previous_active_slot"]

        # --- Step 6.3: demote previous slot ---
        try:
            _demote_slot(dynamodb_resource, previous_slot)
        except ClientError as exc:
            console.print(
                f"[red]✗[/red] Demote previous slot ({previous_slot}) failed: "
                f"{_client_error_message(exc)}"
            )
            console.print(
                f"[red]CRITICAL: Route53 cutover moved; new slot is active; "
                f"previous slot {previous_slot} still flagged as active.[/red]"
            )
            raise typer.Exit(1)

        # --- Step 6.4: fire-and-forget destroy previous active RDS ---
        _destroy_rds(rds_client, plan["previous_db_identifier"])
        destroyed_slot = previous_slot

    # --- Step 6.5: audit record ---
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    audit = {
        "pk": f"PROMOTION#{plan['snapshot_week']}",
        "timestamp": now_iso,
        "requester": requester,
        "reason": reason,
        "snapshot_arn": plan["snapshot_arn"],
        "snapshot_week": plan["snapshot_week"],
        "slot": plan["chosen_slot"],
        "old_target": current_target,
        "new_target": plan["chosen_endpoint"],
        "destroyed_slot": destroyed_slot,
    }
    audit_ok = _write_audit(dynamodb_resource, audit)

    # --- Step 6.6: restart staging ECS tasks (default) ---
    if restart_tasks:
        _restart_ecs_tasks(ecs_client, ECS_CLUSTER_NAME, ECS_SERVICE_NAME)
    else:
        console.print(
            "[dim]hint[/dim] --no-restart-tasks: staging ECS tasks were not "
            "recycled. Run `aws ecs update-service --cluster "
            f"{ECS_CLUSTER_NAME} --service {ECS_SERVICE_NAME} "
            "--force-new-deployment` to establish fresh DB connections."
        )

    console.print(
        f"\n[green]✓[/green] Promoted [cyan]{plan['chosen_slot']}[/cyan] → "
        f"[cyan]{plan['chosen_endpoint']}[/cyan]"
    )
    if not audit_ok:
        console.print(
            "[yellow]![/yellow] Promote succeeded but audit record was not "
            "written. Check DynamoDB."
        )
    raise typer.Exit(0)


if __name__ == "__main__":
    app()
