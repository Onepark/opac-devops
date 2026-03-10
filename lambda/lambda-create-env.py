import os
import boto3
from datetime import datetime, timezone

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
    "device": [
        "last_comm_date" ],
    "entities": [
        "begin",
        "End" ],
    "entities_availabilities": [
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

rds = boto3.client("rds")
REGION = os.environ['AWS_REGION']

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

    return {**event, "ephemeral_id": ephemeral_id, **creation_response}

def apply_date_drifting(event, context):
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
            print(f"update {table} {column} to {snapshot_creation_date + delta}")

    pass


if __name__ == '__main__':

    # import psycopg2
    # import boto3
    #
    # auth_token = boto3.client('rds', region_name='eu-west-3').generate_db_auth_token(DBHostname='ephemeral-transform-golden-snapshot-20260305-test2.c3k4uoc6kifg.eu-west-3.rds.amazonaws.com', Port=5432, DBUsername='dn3F7cBSgMSCZWub', Region='eu-west-3')
    #
    # conn = None
    # try:
    #     conn = psycopg2.connect(
    #         host='ephemeral-transform-golden-snapshot-20260305-test2.c3k4uoc6kifg.eu-west-3.rds.amazonaws.com',
    #         port=5432,
    #         database='opac',
    #         user='dn3F7cBSgMSCZWub',
    #         password=auth_token,
    #         sslmode='verify-full',
    #         sslrootcert='/certs/global-bundle.pem'
    #     )
    #     cur = conn.cursor()
    #     cur.execute('SELECT version();')
    #     print(cur.fetchone()[0])
    #     cur.close()
    # except Exception as e:
    #     print(f"Database error: {e}")
    #     raise
    # finally:
    #     if conn:
    #         conn.close()


    create_event_to_send = { "environment": "test2",
                             "golden_snapshot_id": "golden-snapshot-20260305",
                             "ephemeral_id_prefix": f"ephemeral-transform" }

    # res_create_ephemeral = create_ephemeral_instance_from_snapshot(create_event_to_send, None)

    # res_wait = wait_for_available_instance(res_create_ephemeral, None)

    apply_date_drifting(create_event_to_send, None)

    pass