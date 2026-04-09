import logging
import socket
import time

import botocore.waiter
import psycopg2
import os
from botocore.exceptions import ClientError

# Custom waiter: polls DescribeDBInstances until status == "available".
# Treats DBInstanceNotFound as "retry" so it covers the window between
# restore_db_instance_from_db_snapshot returning and the instance appearing.
_WAITER_MODEL = botocore.waiter.WaiterModel({
    "version": 2,
    "waiters": {
        "DBInstanceAvailable": {
            "operation": "DescribeDBInstances",
            "delay": 15,
            "maxAttempts": 80,   # 80 × 15 s = 20 min max
            "acceptors": [
                {
                    "state": "success",
                    "matcher": "path",
                    "argument": "DBInstances[0].DBInstanceStatus",
                    "expected": "available",
                },
                {
                    "state": "retry",
                    "matcher": "error",
                    "expected": "DBInstanceNotFound",
                },
                {
                    "state": "failure",
                    "matcher": "path",
                    "argument": "DBInstances[0].DBInstanceStatus",
                    "expected": "deleting",
                },
                {
                    "state": "failure",
                    "matcher": "path",
                    "argument": "DBInstances[0].DBInstanceStatus",
                    "expected": "failed",
                },
            ],
        }
    },
})


def wait_for_tcp_port(host: str, port: int, max_attempts: int = 30, delay: int = 10) -> None:
    """Poll TCP port until it accepts connections. Raises RuntimeError on timeout."""
    for attempt in range(1, max_attempts + 1):
        try:
            with socket.create_connection((host, port), timeout=5):
                logging.info(f"TCP {host}:{port} reachable.")
                return
        except OSError as exc:
            logging.info(f"TCP check {attempt}/{max_attempts}: {host}:{port} not reachable yet ({exc}). Retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"Could not reach {host}:{port} after {max_attempts} attempts ({max_attempts * delay}s).")


def wait_for_deleted_instance(rds, db_instance_id: str):
    waiter_deleted = rds.get_waiter('db_instance_deleted')
    waiter_deleted.wait(
        DBInstanceIdentifier=db_instance_id,
        WaiterConfig={"Delay": 10, "MaxAttempts": 60}
    )


def wait_for_available_instance(rds, state_machine_context: dict | None = None,
                                db_instance_id: str | None = None):
    if state_machine_context:
        identifier = state_machine_context.get("ephemeralRdsInstanceId")
    else:
        identifier = db_instance_id

    if identifier is None:
        raise RuntimeError("ephemeralRdsInstanceId or db_instance_id is not defined.")

    logging.info(f"Waiting for RDS instance: {identifier}")
    waiter = botocore.waiter.create_waiter_with_client("DBInstanceAvailable", _WAITER_MODEL, rds)
    waiter.wait(DBInstanceIdentifier=identifier)

    instance = rds.describe_db_instances(DBInstanceIdentifier=identifier)["DBInstances"][0]
    logging.info(f"RDS instance ready: {identifier} (status={instance['DBInstanceStatus']})")

    if state_machine_context:
        return {
            **state_machine_context,
            "db_status": instance["DBInstanceStatus"],
            "db_host": instance["Endpoint"]["Address"],
            "is_available": instance["DBInstanceStatus"] == "available",
        }
    else:
        return {
            "db_status": instance["DBInstanceStatus"],
            "db_host": instance["Endpoint"]["Address"],
            "is_available": instance["DBInstanceStatus"] == "available",
        }


def get_ephemeral_conn_params(rds_client, state_machine_context: dict) -> dict:
    """Return psycopg2 connect kwargs for the ephemeral RDS instance."""
    ephemeral_id = state_machine_context['ephemeralRdsInstanceId']
    existing = rds_client.describe_db_instances(DBInstanceIdentifier=ephemeral_id)["DBInstances"][0]
    return {
        "host": existing["Endpoint"]["Address"],
        "port": state_machine_context['snapshotDbPort'],
        "database": state_machine_context['snapshotDbName'],
        "user": state_machine_context['snapshotDbUsername'],
        "password": state_machine_context['snapshotDbPassword'],
        "sslmode": os.environ.get('DB_SSLMODE'),
        "sslrootcert": os.environ.get('DB_SSLROOTCERTS'),
        "connect_timeout": 30,
    }


def get_ephemeral_db_connection(rds_client, state_machine_context: dict):
    """
    password, username and database name are identical to snapshot db (comes from snapshot) and
    are available through state_machine_context (and env variables for local execution). DB_SSLROOTCERTS
    and DB_SSLMODE are only required when running from local machine through ssm session
    (with port forwarding cf README.md).
    """
    params = get_ephemeral_conn_params(rds_client, state_machine_context)

    wait_for_tcp_port(params["host"], params["port"])

    logging.info(f"Connecting to {params['host']}:{params['port']} database={params['database']} user={params['user']}")
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('SELECT version();')
        logging.info(f"Connected: {cur.fetchone()[0]}")
        cur.close()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

    return conn
