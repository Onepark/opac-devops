import os
import boto3
from datetime import datetime, timezone
import psycopg2

# this dictionary list all the columns by table where apply anonymisation
anonymisation_table_columns = {
    "access_ways" : [
        "desc" # // Only when type = "license_plate" or "phone"
    ],
    "customers": [
        "email",
        "firstname",
        "lastname",
        "phone" ],
    "entities": [
        "contact_info" # (jsonb)
    ],
    "entity_settings": [
        "email",
        "phone" ],
    "installation_logs": [
        "desc",
        "read_desc" ],
    "invoices": [
        "customer_name" ]
    "parkings": [
        "email"
        "firstname"
        "lastname" ],
    "partners": [
        "email_for_commission_invoices",
        "iban",
        "name",
        "national_identifier"
    ],
    "payments": [
        "customer_name"
    ],
    "users": [
        "email",
        "firstname",
        "lastname"
    ]
}

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")

waiter_available = rds.get_waiter("db_instance_available")
waiter_deleted = rds.get_waiter('db_instance_deleted')

def _get_ephemeral_db_connection(ephemeral_id: str):
    # password are available through env variables :

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


def _remove_overlapping_constraints(conn):
    # because there are, for some tables, validation constraints, remove constraint for sql query execution.
    try:
        print("Remove overlapping constraints for updates ... ")
        with conn.cursor() as constraint_cursor:

            constraint_cursor.execute("ALTER TABLE entity_availabilities DROP CONSTRAINT entity_availabilities_overlap_constraint;")

            print("Overlapping constraints for updates removed !")
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error removing overlapping constraints for updates : {e}")


def _restore_overlapping_constraints(conn):
    # TODO this function should take the same list as _remove_overlapping_constraints function in order to restore the same list of non-overlapping constraints
    try:
        print("Remove overlapping constraints for updates ... ")
        with conn.cursor() as constraint_cursor:
            constraint_cursor.execute("ALTER TABLE entity_availabilities ADD constraint entity_availabilities_overlap_constraint exclude using gist (entity_id with =, parking_category_id with =, type with =, tsrange(\"begin\", \"end\", '[)'::text) with &&);")
            print("Overlapping constraints for updates removed !")
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error disabling constraints for updates : {e}")


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

    _remove_overlapping_constraints(conn)
    conn.commit()

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
                    print(f"Updated row count => {updated_row_count} for table {table} and column {column}")

            except (Exception, psycopg2.DatabaseError) as e:
                print(f"Error updating {table} {column}: {e}")

        conn.commit()

    _restore_overlapping_constraints(conn)
    conn.commit()
    pass


if __name__ == '__main__':
    create_event_to_send = { "target_env_name": "test2", "source_env_name": "int",
                             "golden_snapshot_id": "golden-snapshot-20260305",
                             "ephemeral_id_prefix": "ephemeral-transform" }

    res_create_ephemeral = create_ephemeral_instance_from_snapshot(event=create_event_to_send, context=None, create_rds_instance=False)

    res_wait = wait_for_available_instance(res_create_ephemeral, None)

    apply_date_drifting(res_create_ephemeral, None)

    pass