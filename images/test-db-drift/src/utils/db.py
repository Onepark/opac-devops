import logging
import os

import psycopg2

from utils.aws import wait_for_tcp_port


def get_ephemeral_conn_params(rds_client, ephemeral_id: str) -> dict:
    """Return psycopg2 connect kwargs for the ephemeral RDS instance."""
    existing = rds_client.describe_db_instances(DBInstanceIdentifier=ephemeral_id)["DBInstances"][0]
    kwargs = {
        "host": existing["Endpoint"]["Address"],
        "port": existing["Endpoint"]["Port"],
        "dbname": os.environ["SNAPSHOT_DB_NAME"],
        "user": os.environ["SNAPSHOT_DB_USERNAME"],
        "password": os.environ["SNAPSHOT_DB_PASSWORD"],
        "sslmode": os.environ.get("DB_SSLMODE", "require"),
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "30")),
        # TCP keepalives so a silently-dropped RDS connection cannot leave the
        # client blocked forever in recv().
        "keepalives": 1,
        "keepalives_idle": int(os.environ.get("DB_KEEPALIVES_IDLE_SECONDS", "30")),
        "keepalives_interval": int(os.environ.get("DB_KEEPALIVES_INTERVAL_SECONDS", "10")),
        "keepalives_count": int(os.environ.get("DB_KEEPALIVES_COUNT", "3")),
    }
    sslrootcert = os.environ.get("DB_SSLROOTCERTS", "").strip()
    if sslrootcert:
        kwargs["sslrootcert"] = sslrootcert
    return kwargs


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def configure_session(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            "SET statement_timeout = %s",
            (os.environ.get("DB_STATEMENT_TIMEOUT", "300000"),),
        )
        cursor.execute(
            "SET lock_timeout = %s",
            (os.environ.get("DB_LOCK_TIMEOUT", "30000"),),
        )
    conn.commit()


def get_ephemeral_db_connection(rds_client, ephemeral_id: str):
    """
    password, username and database name are identical to snapshot db (comes from snapshot) and
    are available through env vars. DB_SSLROOTCERTS and DB_SSLMODE are only required when running
    from local machine through ssm session (with port forwarding cf README.md).
    """
    params = get_ephemeral_conn_params(rds_client, ephemeral_id)

    wait_for_tcp_port(params["host"], params["port"])

    logging.info(f"Connecting to {params['host']}:{params['port']} database={params['dbname']} user={params['user']}")
    try:
        conn = psycopg2.connect(**params)
        configure_session(conn)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        logging.info(f"Connected: {cur.fetchone()[0]}")
        cur.close()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

    return conn
