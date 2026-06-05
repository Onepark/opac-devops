import logging
import os

import psycopg2

from utils.aws import wait_for_tcp_port


def get_ephemeral_conn_params(rds_client, ephemeral_id: str) -> dict:
    """Return psycopg2 connect kwargs for the ephemeral RDS instance."""
    existing = rds_client.describe_db_instances(DBInstanceIdentifier=ephemeral_id)[
        "DBInstances"
    ][0]
    return {
        "host": existing["Endpoint"]["Address"],
        "port": existing["Endpoint"]["Port"],
        "dbname": os.environ["SNAPSHOT_DB_NAME"],
        "user": os.environ["SNAPSHOT_DB_USERNAME"],
        "password": os.environ["SNAPSHOT_DB_PASSWORD"],
        "sslmode": os.environ.get("DB_SSLMODE"),
        "sslrootcert": os.environ.get("DB_SSLROOTCERTS"),
        "connect_timeout": 30,
    }


def get_ephemeral_db_connection(rds_client, ephemeral_id: str):
    """
    password, username and database name are identical to snapshot db (comes from snapshot) and
    are available through env vars. DB_SSLROOTCERTS and DB_SSLMODE are only required when running
    from local machine through ssm session (with port forwarding cf README.md).
    """
    params = get_ephemeral_conn_params(rds_client, ephemeral_id)

    wait_for_tcp_port(params["host"], params["port"])

    logging.info(
        f"Connecting to {params['host']}:{params['port']} database={params['dbname']} user={params['user']}"
    )
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        logging.info(f"Connected: {cur.fetchone()[0]}")
        cur.close()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

    return conn
