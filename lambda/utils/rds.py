import time

import psycopg2
import os
from botocore.exceptions import ClientError

def wait_for_deleted_instance(rds, db_instance_id: str):
    waiter_deleted = rds.get_waiter('db_instance_deleted')

    waiter_deleted.wait(
        DBInstanceIdentifier=db_instance_id,
        WaiterConfig={
            "Delay": 10,
            "MaxAttempts": 60
        }
    )


def wait_for_instance_to_exist(rds, db_instance_id: str):
    max_retries = 48
    for i in range(max_retries):
        try:
            # Because the instance maybe does not exist yet, a custom loop is required
            rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
            break
        except ClientError as e:
            if "DBInstanceNotFound" in str(e):
                print(f"Try {i+1}/{max_retries} : {db_instance_id} not ready yet ...")
                time.sleep(5)
            else:
                raise e
    else:
        raise Exception(f"Instance {db_instance_id} was never created.")

def wait_for_available_instance(rds, state_machine_context:dict|None=None,
                                db_instance_id:str|None=None):

    if db_instance_id:
        wait_for_instance_to_exist(rds, db_instance_id)

    waiter_available = rds.get_waiter("db_instance_available")

    if state_machine_context:
        identifier = state_machine_context.get("ephemeralRdsInstanceId", None)
    else:
        identifier = db_instance_id

    if identifier is None:
        raise RuntimeError("ephemeralRdsInstanceId or db_instance_id is not defined.")

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

    if state_machine_context:
        return {
            **state_machine_context,
            "db_status": instance["DBInstanceStatus"],
            "db_host":   instance["Endpoint"]["Address"],
            "is_available": instance["DBInstanceStatus"] == "available",
         }
    else:
        return {
            "db_status": instance["DBInstanceStatus"],
            "db_host":   instance["Endpoint"]["Address"],
            "is_available": instance["DBInstanceStatus"] == "available",
        }

def get_ephemeral_db_connection(rds, state_machine_context: dict):
    """
    password, username and database name are identical to snapshot db (comes from snapshot) and
    are available through state_machine_context (and env variables for local execution). DB_SSLROOTCERTS
    and DB_SSLMODE are only required when running from local machine through ssm session
    (with port forwarding cf README.md).

    for instance:
      snapshotDbName": "opac",
      snapshotDbUsername": "<snapshot db username>",
      snapshotDbPassword": "<snapshot db password>",
      snapshotDbPort: 5432,
      DB_SSLROOTCERTS=<certificate_path/global-bundle.pem (when lambda is run directly from local machine)
      DB_SSLMODE=verify-full

    ephmeral_id is passed through the state_machine_context

    :param state_machine_context:
    :return:
    """

    ephemeral_id = state_machine_context['ephemeralRdsInstanceId']

    db_password = state_machine_context['snapshotDbPassword']
    db_username = state_machine_context['snapshotDbUsername']
    db_name = state_machine_context['snapshotDbName']
    db_port = state_machine_context['snapshotDbPort']
    db_sslrootcerts = os.environ.get('DB_SSLROOTCERTS', None)
    db_sslmode = os.environ.get('DB_SSLMODE', None)

    # retrieve ephemeral instance description
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