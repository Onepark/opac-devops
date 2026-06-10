import logging
import os
import socket
import sys
import time

import boto3
import botocore.waiter
from botocore.config import Config

CLIENT_CONFIG = Config(retries={"mode": "standard", "max_attempts": 6})


# Custom waiter: polls DescribeDBInstances until status == "available".
# Treats DBInstanceNotFound as "retry" so it covers the window between
# restore_db_instance_from_db_snapshot returning and the instance appearing.
_WAITER_MODEL = botocore.waiter.WaiterModel(
    {
        "version": 2,
        "waiters": {
            "DBInstanceAvailable": {
                "operation": "DescribeDBInstances",
                "delay": 15,
                "maxAttempts": 80,  # 80 × 15 s = 20 min max
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
    }
)


def rds_client() -> boto3.client:
    return boto3.client(
        "rds",
        region_name=os.environ.get("AWS_REGION", "eu-west-3"),
        config=CLIENT_CONFIG,
    )


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,
    )


def wait_for_tcp_port(
    host: str, port: int, max_attempts: int = 30, delay_seconds: int = 10
) -> None:
    """Poll TCP port until it accepts connections. Raises RuntimeError on timeout."""
    for attempt in range(1, max_attempts + 1):
        try:
            with socket.create_connection((host, port), timeout=5):
                logging.info("TCP %s:%s reachable", host, port)
                return
        except OSError as exc:
            logging.info(
                "TCP check %s/%s failed for %s:%s (%s); retrying in %ss",
                attempt,
                max_attempts,
                host,
                port,
                exc,
                delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError(
        f"Could not reach {host}:{port} after {max_attempts * delay_seconds}s"
    )


def wait_for_deleted_instance(rds_client, db_instance_id: str):
    waiter_deleted = rds_client.get_waiter("db_instance_deleted")
    waiter_deleted.wait(
        DBInstanceIdentifier=db_instance_id,
        WaiterConfig={"Delay": 10, "MaxAttempts": 60},
    )


def wait_for_available_instance(rds_client, db_instance_id: str) -> dict:
    if db_instance_id is None:
        raise RuntimeError("db_instance_id is not defined.")

    logging.info(f"Waiting for RDS instance: {db_instance_id}")
    waiter = botocore.waiter.create_waiter_with_client(
        "DBInstanceAvailable", _WAITER_MODEL, rds_client
    )
    waiter.wait(DBInstanceIdentifier=db_instance_id)

    instance = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)[
        "DBInstances"
    ][0]
    logging.info(
        f"RDS instance ready: {db_instance_id} (status={instance['DBInstanceStatus']})"
    )

    return {
        "db_status": instance["DBInstanceStatus"],
        "db_host": instance["Endpoint"]["Address"],
        "is_available": instance["DBInstanceStatus"] == "available",
    }
