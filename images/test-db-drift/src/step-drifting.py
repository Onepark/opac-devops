import logging
import os
import re
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import psycopg2

from utils.aws import (
    setup_logging,
    wait_for_available_instance,
    wait_for_deleted_instance,
)
from utils.db import get_ephemeral_db_connection, get_ephemeral_conn_params

# this dictionary list all the columns by table where apply date drifting
# if begin or end in the same table, because of check constraint, end is always updated first
date_drifting_table_column = {
    "allotments": ["end", "begin"],
    "connected_equipment_events": ["date"],
    "customers": ["inserted_at", "updated_at"],
    "devices": ["last_comm_date"],
    "entity_availabilities": ["end", "begin"],
    "installation_device_maps": ["end", "begin"],
    "installation_logs": ["date"],
    "invoices": ["date", "inserted_at", "updated_at"],
    "metrics": ["end", "begin"],
    "oban_jobs": ["scheduled_at"],
    "oban_peers": ["started_at", "expires_at"],
    "offers": ["end", "begin", "expires_at"],
    "parkings": ["end", "begin", "inserted_at", "updated_at", "finished_at"],
    "parking_categories": ["inserted_at", "updated_at"],
    "parking_comments": ["inserted_at", "updated_at"],
    "parking_prices": ["inserted_at", "updated_at"],
    "parking_states": ["date"],
    "payments": ["date", "paid_at", "cancelled_at", "refunded_at"],
    "payment_readers": ["inserted_at", "updated_at"],
    "rights": ["end", "begin"],
    "scenario_logs": ["date"],
    "terminals": ["inserted_at", "updated_at", "last_comm_date"],
    "validation_links": ["expiration"],
}

ALL_DRIFTED_DATE_COLUMNS = [
    "begin",
    "end",
    "created_at",
    "updated_at",
    "inserted_at",
    "starts_at",
    "ends_at",
    "scheduled_at",
    "attempted_at",
    "cancelled_at",
    "completed_at",
    "discarded_at",
    "confirmed_at",
    "delivered_at",
    "failed_at",
    "expires_at",
    "last_seen_at",
    "date",
    "paid_at",
    "refunded_at",
    "expiration",
    "last_comm_date",
    "finished_at",
]


def _get_rds_client():
    return boto3.client("rds", region_name=os.environ.get("AWS_REGION", "eu-west-3"))


def _extract_snapshot_creation_date(rds_client, snapshot_arn: str) -> date:
    desc = rds_client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_arn)
    snapshots = desc.get("DBSnapshots", [])
    if not snapshots:
        raise RuntimeError(f"Snapshot {snapshot_arn} not found")
    snapshot = snapshots[0]
    snapshot_name = snapshot["DBSnapshotIdentifier"]
    date_match = re.search(r"(\d{4})(\d{2})(\d{2})", snapshot_name)
    if date_match:
        return date(
            int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
        )
    create_time = snapshot.get("SnapshotCreateTime")
    if create_time and isinstance(create_time, datetime):
        logging.info(
            f"No date in snapshot name '{snapshot_name}' — using SnapshotCreateTime"
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
            logging.info(f"Ephemeral {ephemeral_id} is being deleted — waiting...")
            wait_for_deleted_instance(rds_client, ephemeral_id)
            return False
        logging.info(
            f"Ephemeral {ephemeral_id} already exists (status={status}) — reusing"
        )
        return True
    except rds_client.exceptions.DBInstanceNotFoundFault:
        return False


def _build_restore_kwargs(
    ephemeral_id: str, snapshot_id: str, existing: dict, rds_client
) -> dict:
    """Build restore_db_instance_from_db_snapshot kwargs that mirror the target instance.

    AllocatedStorage is intentionally omitted — it is determined by the snapshot and
    adjusted post-restore via modify_db_instance if the target is larger.
    IAMDatabaseAuthentication is copied from the target so Terraform sees no drift.
    DeletionProtection is forced to False so the ephemeral instance can be deleted.
    """
    tags = existing.get("TagList")
    if tags is None:
        tags = rds_client.list_tags_for_resource(
            ResourceName=existing["DBInstanceArn"]
        ).get("TagList", [])

    kwargs: dict = {
        "DBInstanceIdentifier": ephemeral_id,
        "DBSnapshotIdentifier": snapshot_id,
        # Compute / network
        "DBInstanceClass": existing["DBInstanceClass"],
        "DBSubnetGroupName": existing["DBSubnetGroup"]["DBSubnetGroupName"],
        "VpcSecurityGroupIds": [
            sg["VpcSecurityGroupId"] for sg in existing["VpcSecurityGroups"]
        ],
        "MultiAZ": False,  # Ephemeral is disposable — no HA needed
        "PubliclyAccessible": existing["PubliclyAccessible"],
        "NetworkType": existing.get("NetworkType", "IPV4"),
        # Parameter group
        "DBParameterGroupName": existing["DBParameterGroups"][0][
            "DBParameterGroupName"
        ],
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


def create_ephemeral_instance_from_snapshot(
    rds_client, target_id: str, snapshot_arn: str
):
    ephemeral_id = f"ephemeral-transform-{target_id}"

    already_exists = _ensure_ephemeral_ready(rds_client, ephemeral_id)
    if already_exists:
        logging.info(f"Ephemeral {ephemeral_id} already exists — skipping restore")
        return ephemeral_id

    snapshot_creation_date = _extract_snapshot_creation_date(rds_client, snapshot_arn)
    logging.info(f"Snapshot creation date: {snapshot_creation_date.isoformat()}")

    logging.info(f"Fetching config from target instance: {target_id}")
    existing = rds_client.describe_db_instances(DBInstanceIdentifier=target_id)[
        "DBInstances"
    ][0]

    restore_kwargs = _build_restore_kwargs(
        ephemeral_id, snapshot_arn, existing, rds_client
    )

    logging.info(
        f"Restoring ephemeral instance {ephemeral_id} from snapshot {snapshot_arn}"
    )
    rds_client.restore_db_instance_from_db_snapshot(**restore_kwargs)
    logging.info(f"Restore initiated for {ephemeral_id}")

    return ephemeral_id


def _drift_table(
    conn_params: dict, table: str, columns: list[str], delta_days: int
) -> tuple[str, int]:
    """Drift all date columns in one table with a single UPDATE. Each call uses its own connection."""
    set_clause = ", ".join(
        f'"{col}" = "{col}" + INTERVAL \'{delta_days} days\'' for col in columns
    )
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as c:
            c.execute(f'UPDATE "{table}" SET {set_clause}')
            row_count = c.rowcount
        conn.commit()
        return table, row_count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _find_disruptive_constraints(conn) -> list[dict]:
    """Find all CHECK and EXCLUDE constraints on tables being date-drifted."""
    with conn.cursor() as c:
        c.execute(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                pg_get_constraintdef(con.oid) AS constraint_def,
                con.contype AS constraint_type
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND con.contype IN ('c', 'x')
              AND EXISTS (
                  SELECT 1 FROM information_schema.columns col
                  WHERE col.table_schema = n.nspname
                    AND col.table_name = c.relname
                    AND col.column_name = ANY(%s)
              )
        """,
            (list(ALL_DRIFTED_DATE_COLUMNS),),
        )
        return [
            {
                "schema": row[0],
                "table": row[1],
                "name": row[2],
                "definition": row[3],
                "type": row[4],
            }
            for row in c.fetchall()
        ]


def _drop_disruptive_constraints(conn, constraints: list[dict]):
    for c_def in constraints:
        qualified = f'"{c_def["schema"]}"."{c_def["table"]}"'
        logging.info(
            f"Dropping constraint {c_def['name']} on {qualified} ({c_def['type']})"
        )
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {qualified} DROP CONSTRAINT "{c_def["name"]}"')
    conn.commit()


def _recreate_constraints(conn, constraints: list[dict]):
    for c_def in constraints:
        qualified = f'"{c_def["schema"]}"."{c_def["table"]}"'
        logging.info(f"Restoring constraint {c_def['name']} on {qualified}")
        with conn.cursor() as cur:
            cur.execute(
                f'ALTER TABLE {qualified} ADD CONSTRAINT "{c_def["name"]}" {c_def["definition"]}'
            )
    conn.commit()


def apply_date_drifting(rds_client, ephemeral_id: str, snapshot_arn: str):
    # Main connection used only for constraint management
    conn = get_ephemeral_db_connection(rds_client, ephemeral_id)
    # Separate params dict used to open one connection per parallel worker
    conn_params = get_ephemeral_conn_params(rds_client, ephemeral_id)

    snapshot_creation_date = _extract_snapshot_creation_date(rds_client, snapshot_arn)
    today = datetime.now(timezone.utc).date()
    delta_days = (today - snapshot_creation_date).days

    logging.info(
        f"Date delta: +{delta_days} days (snapshot={snapshot_creation_date}, today={today})"
    )

    # Find and drop disruptive constraints before drifting
    disruptive = _find_disruptive_constraints(conn)
    if disruptive:
        logging.info(f"Found {len(disruptive)} disruptive constraints to drop")
        _drop_disruptive_constraints(conn, disruptive)
    else:
        logging.info("No disruptive constraints found")

    table_count = len(date_drifting_table_column)
    max_workers = int(os.environ.get("DRIFT_MAX_WORKERS", "8"))
    batch_timeout = int(os.environ.get("DRIFT_BATCH_TIMEOUT_SECONDS", "3600"))

    logging.info(
        f"Drifting {table_count} tables in parallel (max_workers={max_workers})…"
    )

    errors = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _drift_table, conn_params, table, columns, delta_days
            ): table
            for table, columns in date_drifting_table_column.items()
        }
        for future in as_completed(futures, timeout=batch_timeout):
            table = futures[future]
            try:
                _, row_count = future.result()
                logging.info(f"Drifted {table}: {row_count} rows")
            except Exception as exc:
                logging.error(f"Error drifting {table}: {exc}")
                errors += 1

    # Recreate constraints — errors propagate (no swallowing)
    if disruptive:
        logging.info(f"Restoring {len(disruptive)} constraints")
        _recreate_constraints(conn, disruptive)

    conn.close()

    if errors:
        logging.warning(f"Date drifting completed with {errors} table error(s)")
    else:
        logging.info(f"Date drifting complete ({table_count} tables)")


def main():
    setup_logging()
    target_id = os.environ["TARGET_RDS_INSTANCE_ID"]
    snapshot_arn = os.environ["SNAPSHOT_ARN"]
    execution_name = os.environ.get("EXECUTION_NAME", "unknown")
    logging.info(
        "=== Step: Drifting ===",
        extra={"execution_name": execution_name, "target": target_id},
    )

    rds = _get_rds_client()

    ephemeral_id = create_ephemeral_instance_from_snapshot(rds, target_id, snapshot_arn)

    wait_for_available_instance(rds, ephemeral_id)

    apply_date_drifting(rds, ephemeral_id, snapshot_arn)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        exit(1)
