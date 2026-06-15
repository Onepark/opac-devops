import logging
import os
import re
from datetime import date, datetime, timezone

from utils.aws import (
    rds_client,
    setup_logging,
    wait_for_available_instance,
    wait_for_deleted_instance,
)
from utils.db import get_ephemeral_conn_params, get_ephemeral_db_connection
from utils.drift import apply_drift
from utils.drift_policy import load_policy, run_preflight


def _extract_snapshot_creation_date(rds_client, snapshot_arn: str) -> date:
    desc = rds_client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_arn)
    snapshots = desc.get("DBSnapshots", [])
    if not snapshots:
        raise RuntimeError(f"Snapshot {snapshot_arn} not found")
    snapshot = snapshots[0]
    snapshot_name = snapshot["DBSnapshotIdentifier"]
    date_match = re.search(r"(\d{4})(\d{2})(\d{2})", snapshot_name)
    if date_match:
        return date(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))
    create_time = snapshot.get("SnapshotCreateTime")
    if create_time and isinstance(create_time, datetime):
        logging.info(
            "No date in snapshot name '%s' — using SnapshotCreateTime",
            snapshot_name,
        )
        return create_time.date()
    raise RuntimeError(f"Cannot determine creation date for snapshot '{snapshot_name}'")


def _ensure_ephemeral_ready(rds_client, ephemeral_id: str) -> bool:
    """Ensure ephemeral doesn't exist (or wait for deletion if in deleting state).
    Returns True if already exists (skip restore), False otherwise."""
    try:
        existing = rds_client.describe_db_instances(DBInstanceIdentifier=ephemeral_id)
        status = existing["DBInstances"][0]["DBInstanceStatus"]
        if status == "deleting":
            logging.info("Ephemeral %s is being deleted — waiting", ephemeral_id)
            wait_for_deleted_instance(rds_client, ephemeral_id)
            return False
        logging.info(
            "Ephemeral %s already exists (status=%s) — reusing",
            ephemeral_id,
            status,
        )
        return True
    except rds_client.exceptions.DBInstanceNotFoundFault:
        return False


def _build_restore_kwargs(ephemeral_id: str, snapshot_id: str, existing: dict, rds_client) -> dict:
    """Build restore_db_instance_from_db_snapshot kwargs that mirror the target instance.

    AllocatedStorage is intentionally omitted — it is determined by the snapshot and
    adjusted post-restore via modify_db_instance if the target is larger.
    IAMDatabaseAuthentication is copied from the target so Terraform sees no drift.
    DeletionProtection is forced to False so the ephemeral instance can be deleted.
    """
    tags = existing.get("TagList")
    if tags is None:
        tags = rds_client.list_tags_for_resource(ResourceName=existing["DBInstanceArn"]).get("TagList", [])

    kwargs: dict = {
        "DBInstanceIdentifier": ephemeral_id,
        "DBSnapshotIdentifier": snapshot_id,
        # Compute / network
        "DBInstanceClass": existing["DBInstanceClass"],
        "DBSubnetGroupName": existing["DBSubnetGroup"]["DBSubnetGroupName"],
        "VpcSecurityGroupIds": [sg["VpcSecurityGroupId"] for sg in existing["VpcSecurityGroups"]],
        "MultiAZ": False,  # Ephemeral is disposable — no HA needed
        "PubliclyAccessible": existing["PubliclyAccessible"],
        "NetworkType": existing.get("NetworkType", "IPV4"),
        # Parameter group
        "DBParameterGroupName": existing["DBParameterGroups"][0]["DBParameterGroupName"],
        # Storage type (AllocatedStorage comes from snapshot; adjusted post-restore if needed)
        "StorageType": existing["StorageType"],
        # Misc
        "AutoMinorVersionUpgrade": existing["AutoMinorVersionUpgrade"],
        "CopyTagsToSnapshot": existing.get("CopyTagsToSnapshot", False),
        "EnableIAMDatabaseAuthentication": existing["IAMDatabaseAuthenticationEnabled"],
        "DeletionProtection": False,
        "BackupRetentionPeriod": 0,  # Ephemeral is short-lived — no point in backups
        "Tags": tags,
    }

    return kwargs


def create_ephemeral_instance_from_snapshot(rds_client, target_id: str, snapshot_arn: str):
    ephemeral_id = f"ephemeral-transform-{target_id}"

    already_exists = _ensure_ephemeral_ready(rds_client, ephemeral_id)
    if already_exists:
        logging.info("Ephemeral %s already exists — skipping restore", ephemeral_id)
        return ephemeral_id

    snapshot_creation_date = _extract_snapshot_creation_date(rds_client, snapshot_arn)
    logging.info("Snapshot creation date: %s", snapshot_creation_date.isoformat())

    logging.info("Fetching config from target instance: %s", target_id)
    existing = rds_client.describe_db_instances(DBInstanceIdentifier=target_id)["DBInstances"][0]

    restore_kwargs = _build_restore_kwargs(ephemeral_id, snapshot_arn, existing, rds_client)

    logging.info(
        "Restoring ephemeral instance %s from snapshot %s",
        ephemeral_id,
        snapshot_arn,
    )
    rds_client.restore_db_instance_from_db_snapshot(**restore_kwargs)
    logging.info("Restore initiated for %s", ephemeral_id)

    return ephemeral_id


def apply_date_drifting(rds_client, ephemeral_id: str, snapshot_arn: str):
    policy = load_policy()
    conn = get_ephemeral_db_connection(rds_client, ephemeral_id)
    conn_params = get_ephemeral_conn_params(rds_client, ephemeral_id)

    try:
        with conn.cursor() as cursor:
            preflight_errors = run_preflight(cursor, policy)
        if preflight_errors:
            for err in preflight_errors:
                logging.error("Preflight: %s", err)
            raise RuntimeError(f"Drift preflight failed ({len(preflight_errors)} error(s))")
        logging.info(
            "Preflight passed: %d tables in policy v%d",
            len(policy.tables),
            policy.version,
        )

        snapshot_creation_date = _extract_snapshot_creation_date(rds_client, snapshot_arn)
        today = datetime.now(timezone.utc).date()
        delta_days = (today - snapshot_creation_date).days

        logging.info(
            "Date delta: +%d days (snapshot=%s, today=%s)",
            delta_days,
            snapshot_creation_date,
            today,
        )

        result = apply_drift(conn, conn_params, policy, delta_days)
        logging.info(
            "Date drifting complete (%d tables, %d rows)",
            result.tables_drifted,
            result.total_rows,
        )
    finally:
        conn.close()


def main():
    setup_logging()
    target_id = os.environ["TARGET_RDS_INSTANCE_ID"]
    snapshot_arn = os.environ["SNAPSHOT_ARN"]
    execution_name = os.environ.get("EXECUTION_NAME", "unknown")
    logging.info("=== Step: Drifting === execution=%s target=%s", execution_name, target_id)

    rds = rds_client()

    ephemeral_id = create_ephemeral_instance_from_snapshot(rds, target_id, snapshot_arn)

    wait_for_available_instance(rds, ephemeral_id)

    apply_date_drifting(rds, ephemeral_id, snapshot_arn)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Fatal error: %s", e)
        exit(1)
