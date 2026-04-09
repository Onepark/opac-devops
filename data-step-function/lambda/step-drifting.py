import logging
import os
import re
import boto3
from datetime import date, datetime, timezone
import psycopg2

from utils.context import setup_logging, get_or_create_context_from_param_store, update_context_in_param_store
from utils.rds import wait_for_available_instance, get_ephemeral_db_connection

# prefix for creation of the ephemeral RDS instance
ephemeral_id_prefix = "ephemeral-transform"

# this dictionary list all the columns by table where apply date drifting
# if begin or end in the same table, because of check constraint, end is always updated first
date_drifting_table_column = {
    "allotments": [
        "end",
        "begin"],
    "connected_equipment_events": [
        "date"],
    "customers": [
        "inserted_at",
        "updated_at"],
    "devices": [
        "last_comm_date"],
    "entity_availabilities": [
        "end",
        "begin"],
    "installation_device_maps": [
        "end",
        "begin"],
    "installation_logs": [
        "date"],
    "invoices": [
        "date",
        "inserted_at",
        "updated_at"],
    "metrics": [
        "end",
        "begin"],
    "oban_jobs": [
        "scheduled_at"],
    "oban_peers": [
        "started_at",
        "expires_at"],
    "offers": [
        "end",
        "begin",
        "expires_at"],
    "parkings": [
        "end",
        "begin",
        "inserted_at",
        "updated_at",
        "finished_at"],
    "parking_categories": [
        "inserted_at",
        "updated_at"],
    "parking_comments": [
        "inserted_at",
        "updated_at"],
    "parking_prices": [
        "inserted_at",
        "updated_at"],
    "parking_states": [
        "date"],
    "payments": [
        "date",
        "paid_at",
        "cancelled_at",
        "refunded_at"],
    "payment_readers": [
        "inserted_at",
        "updated_at"],
    "rights": [
        "end",
        "begin"],
    "scenario_logs": [
        "date"],
    "terminals": [
        "inserted_at",
        "updated_at",
        "last_comm_date"],
    "validation_links": [
        "expiration"]
}

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")


def create_ephemeral_instance_from_snapshot(state_machine_context, create_rds_instance=True):
    target_rds_instance_id = state_machine_context["targetRdsInstanceId"]
    snapshot_arn = state_machine_context["snapshotArn"]

    logging.info(f"Describing snapshot: {snapshot_arn}")
    res_snapshot_desc = rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_arn)

    if res_snapshot_desc and res_snapshot_desc.get("DBSnapshots") and len(res_snapshot_desc["DBSnapshots"]):
        snapshot_name = res_snapshot_desc["DBSnapshots"][0]["DBSnapshotIdentifier"]
        snapshot_creation_date = datetime.strptime(re.search(r'\d{8}', snapshot_name).group(), '%Y%m%d').date()
        state_machine_context["snapshotCreationDate"] = snapshot_creation_date.isoformat()
        logging.info(f"Snapshot creation date: {snapshot_creation_date.isoformat()}")
    else:
        raise Exception("Can't retrieve snapshot creation date.")

    golden_snapshot_id = res_snapshot_desc["DBSnapshots"][0]["DBSnapshotIdentifier"]
    ephemeral_id = f"{ephemeral_id_prefix}-{target_rds_instance_id}"
    state_machine_context["ephemeralRdsInstanceId"] = ephemeral_id

    logging.info(f"Fetching config from target instance: {target_rds_instance_id}")
    existing = rds.describe_db_instances(DBInstanceIdentifier=target_rds_instance_id)["DBInstances"][0]

    creation_response = None

    if create_rds_instance:
        logging.info(f"Restoring ephemeral instance {ephemeral_id} from snapshot {golden_snapshot_id}")
        creation_response = rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=ephemeral_id,
            DBSnapshotIdentifier=golden_snapshot_id,
            DBInstanceClass=existing["DBInstanceClass"],
            DBSubnetGroupName=existing["DBSubnetGroup"]["DBSubnetGroupName"],
            VpcSecurityGroupIds=[
                sg["VpcSecurityGroupId"] for sg in existing["VpcSecurityGroups"]
            ],
            EnableIAMDatabaseAuthentication=True,
            DeletionProtection=False,
            Tags=[{"Key": "ephemeral", "Value": "true"}],
        )
        logging.info(f"Restore initiated for {ephemeral_id}")

    update_context_in_param_store(ssm, state_machine_context)

    if creation_response:
        return {"ephemeral_id": ephemeral_id, **creation_response}
    else:
        return {"ephemeral_id": ephemeral_id}


def _remove_overlapping_constraints(conn):
    try:
        logging.info("Removing overlapping constraints on entity_availabilities")
        with conn.cursor() as constraint_cursor:
            constraint_cursor.execute("ALTER TABLE entity_availabilities DROP CONSTRAINT entity_availabilities_overlap_constraint;")
        logging.info("Overlapping constraints removed")
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"Error removing overlapping constraints: {e}")


def _restore_overlapping_constraints(conn):
    try:
        logging.info("Restoring overlapping constraints on entity_availabilities")
        with conn.cursor() as constraint_cursor:
            constraint_cursor.execute("ALTER TABLE entity_availabilities ADD constraint entity_availabilities_overlap_constraint exclude using gist (entity_id with =, parking_category_id with =, type with =, tsrange(\"begin\", \"end\", '[)'::text) with &&);")
        logging.info("Overlapping constraints restored")
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"Error restoring overlapping constraints: {e}")


def apply_date_drifting(state_machine_context: dict):
    if not state_machine_context.get("drifting", False):
        logging.info("drifting=False — skipping date drifting")
        return

    conn = get_ephemeral_db_connection(rds, state_machine_context)

    today = datetime.now(timezone.utc).date()
    snapshot_creation_date = date.fromisoformat(state_machine_context['snapshotCreationDate'])
    delta = today - snapshot_creation_date

    logging.info(f"Date delta: +{delta.days} days (snapshot={snapshot_creation_date}, today={today})")

    _remove_overlapping_constraints(conn)
    conn.commit()

    for drift_elements in date_drifting_table_column.items():
        table = drift_elements[0]
        columns = drift_elements[1]

        for column in columns:
            sql_update = f"UPDATE \"{table}\" SET \"{column}\" = \"{column}\" + INTERVAL '{delta.days} days' WHERE \"{column}\" is not null"
            try:
                with conn.cursor() as c:
                    c.execute(sql_update)
                    updated_row_count = c.rowcount
                    logging.info(f"Drifted {table}.{column}: {updated_row_count} rows")
            except (Exception, psycopg2.DatabaseError) as e:
                logging.error(f"Error drifting {table}.{column}: {e}")

        conn.commit()

    _restore_overlapping_constraints(conn)
    conn.commit()
    logging.info("Date drifting complete")


if __name__ == '__main__':
    setup_logging()

    context = get_or_create_context_from_param_store(ssm, True)

    if context and "error" in context:
        logging.error("A stale or active drifting/anonymisation context exists in Parameter Store. "
                      "Delete /opac/int/step_function/context manually if the previous run is no longer active.")
        exit(1)

    logging.info("=== Step: Drifting ===")
    logging.info(f"Execution: {context.get('executionName', 'unknown') if context else 'unknown'}")

    create_ephemeral_instance_from_snapshot(
        state_machine_context=context,
        create_rds_instance=context.get("_debug_create_rds_instance", True)
    )

    context = wait_for_available_instance(rds, state_machine_context=context)

    apply_date_drifting(state_machine_context=context)
