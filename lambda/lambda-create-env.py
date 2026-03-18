import os
import boto3
from datetime import datetime, timezone
import psycopg2

# this dictionary list all the columns by table where apply date drifting
date_drifting_table_column = {
    "allotments": [
        "begin",
        "end" ],
    "connected_equipment_events": [
        "date" ],
    "customers": [
        "inserted_at",
        "updated_at" ],
    "devices": [ # Doc type : device -> devices
        "last_comm_date" ],
    # "entities": [
    #     "begin",
    #     "End" ], # columns does not exist
    "entity_availabilities": [ # typo : entities_availabilities => entity_availabilities
        "begin",
        "End" ],
    "installation_device_maps": [
        "begin",
        "end" ],
    "installation_logs": [
        "date" ],
    "invoices": [
        "date",
        "inserted_at",
        "updated_at" ],
    "metrics": [
        "begin",
        "end" ],
    "oban_jobs": [
        "scheduled_at" ],
    "oban_peers": [
        "started_at",
        "expires_at" ],
    "offers": [
        "begin",
        "end",
        "expires_at" ],
    "parkings": [
        "begin",
        "end",
        "inserted_at",
        "updated_at",
        "finished_at" ],
    "parking_categories": [
        "inserted_at",
        "updated_at" ],
    "parking_comments": [
        "inserted_at",
        "updated_at" ],
    "parking_prices": [
        "inserted_at",
        "updated_at" ],
    "parking_states": [
        "date" ],
    "payments": [
        "date",
        "paid_at",
        "cancelled_at",
        "refunded_at" ],
    "payment_readers": [
        "inserted_at",
        "updated_at" ],
    "rights": [
        "begin",
        "end" ],
    "scenario_logs": [
        "date" ],
    "terminals": [
        "inserted_at",
        "updated_at",
        "last_comm_date" ],
    "validation_links": [
        "expiration" ]
}

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")

waiter_available = rds.get_waiter("db_instance_available")
waiter_deleted = rds.get_waiter('db_instance_deleted')

def wait_for_available_instance(event, context):
    """Generic poller — reused for ephemeral and final instance readiness."""
    identifier = event.get("check_identifier") or event["ephemeral_id"]

    waiter_available.wait(
        DBInstanceIdentifier=identifier,
        WaiterConfig={
            "Delay": 10,
            "MaxAttempts": 60
        }
    )

    instance = rds.describe_db_instances(
        DBInstanceIdentifier=identifier
    )["DBInstances"][0]

    return {
        **event,
        "db_status": instance["DBInstanceStatus"],
        "db_host":   instance["Endpoint"]["Address"],
        "is_available": instance["DBInstanceStatus"] == "available",
    }

def create_ephemeral_instance_from_snapshot(event, context):
    env = event["environment"]
    golden_snapshot_id = "golden-snapshot-20260305"
    ephemeral_id = f"{event['ephemeral_id_prefix']}-{golden_snapshot_id}-{env}"

    existing = rds.describe_db_instances(DBInstanceIdentifier="opk-opac-int-rds", )["DBInstances"][0]

    creation_response = None

    # creation_response = rds.restore_db_instance_from_db_snapshot(
    #     DBInstanceIdentifier=ephemeral_id,
    #     DBSnapshotIdentifier=golden_snapshot_id,
    #     DBInstanceClass=existing["DBInstanceClass"],
    #     DBSubnetGroupName=existing["DBSubnetGroup"]["DBSubnetGroupName"],
    #     VpcSecurityGroupIds=[
    #         sg["VpcSecurityGroupId"] for sg in existing["VpcSecurityGroups"]
    #     ],
    #     EnableIAMDatabaseAuthentication=True,
    #     DeletionProtection=False,
    #     Tags=[{"Key": "ephemeral", "Value": "true"}],
    # )

    if creation_response:
        return {**event, "ephemeral_id": ephemeral_id, **creation_response}
    else:
        return {**event, "ephemeral_id": ephemeral_id}

def _get_ephemeral_db_connection(ephemeral_id: str):
    # password, username and database name are identical to int db (comes from snapshot) and
    # are available through env variables :

    # for instance:
    # SNAPSHOT_DB_PASSWORD=test
    # SNAPSHOT_DB_USERNAME=test
    # SNAPSHOT_DB_NAME=test
    # SNAPSHOT_DB_PORT=5432
    # SSLROOTCERTS=~/.aws/rds-certs/global-bundle.pem (when lambda is run directly from local machine)

    db_password = os.environ['SNAPSHOT_DB_PASSWORD']
    db_username = os.environ['SNAPSHOT_DB_USERNAME']
    db_name = os.environ['SNAPSHOT_DB_NAME']
    db_port = os.environ['SNAPSHOT_DB_PORT']
    db_sslrootcerts = os.environ['DB_SSLROOTCERTS']
    db_sslmode = os.environ['DB_SSLMODE']

    existing_ephemeral_db = rds.describe_db_instances(DBInstanceIdentifier=ephemeral_id)["DBInstances"][0]

    host = None

    # retrieve ephemeral instance host
    if existing_ephemeral_db.get("Endpoint") and existing_ephemeral_db["Endpoint"].get("Address"):
        host = existing_ephemeral_db["Endpoint"]["Address"]

    try:
        conn = psycopg2.connect(
            host=host,
            port=db_port,
            database=db_name,
            user=db_username,
            password=db_password,
            sslmode=db_sslmode,
            sslrootcert=db_sslrootcerts
        )
        cur = conn.cursor()
        cur.execute('SELECT version();')
        print(cur.fetchone()[0])
        cur.close()
    except Exception as e:
        print(f"Database error: {e}")
        raise

    return conn


def apply_date_drifting(event, context):
    conn = _get_ephemeral_db_connection(event["ephemeral_id"])

    # retrieve db snapshot creation time
    res_snapshot_desc = rds.describe_db_snapshots(DBInstanceIdentifier="opk-opac-int-rds",
                                                  DBSnapshotIdentifier="golden-snapshot-20260305")

    if res_snapshot_desc and res_snapshot_desc.get("DBSnapshots") and len(res_snapshot_desc["DBSnapshots"]):
        snapshot_creation_date = res_snapshot_desc["DBSnapshots"][0]["SnapshotCreateTime"]
    else:
        raise Exception("Can't retrieve snapshot creation date.")

    utc_now = datetime.now(timezone.utc)

    delta = utc_now - snapshot_creation_date

    for drift_elements in date_drifting_table_column.items():
        table = drift_elements[0]
        columns = drift_elements[1]

        for column in columns:
            print(f"updating {table} {column} to snapshot creation date + {delta.days} days.")
            sql_update = f"UPDATE \"{table}\" SET \"{column}\" = \"{column}\" + INTERVAL '{delta.days} days' WHERE \"{column}\" is not null"
            print(f"sql query => {sql_update}")

            try:
                with conn.cursor() as c:
                    c.execute(sql_update)
                    updated_row_count = c.rowcount
                    conn.commit()
                    print(f"Updated row count => {updated_row_count} for table {table} and column {column}")

            except (Exception, psycopg2.DatabaseError) as e:
                print(f"Error updating {table} {column}: {e}")

    pass


if __name__ == '__main__':
    create_event_to_send = { "environment": "test2",
                             "golden_snapshot_id": "golden-snapshot-20260305",
                             "ephemeral_id_prefix": f"ephemeral-transform" }

    res_create_ephemeral = create_ephemeral_instance_from_snapshot(create_event_to_send, None)

    res_wait = wait_for_available_instance(res_create_ephemeral, None)

    apply_date_drifting(res_create_ephemeral, None)

    pass