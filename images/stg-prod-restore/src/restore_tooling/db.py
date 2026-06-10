import logging
import os
import socket
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2 import sql

from .common import AppSecret


def wait_for_tcp_port(
    host: str, port: int, max_attempts: int = 30, delay_seconds: int = 10
) -> None:
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


def wait_for_rds_available(rds, db_identifier: str) -> None:
    logging.info("Waiting for RDS instance %s to become available", db_identifier)
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(
        DBInstanceIdentifier=db_identifier,
        WaiterConfig={"Delay": 30, "MaxAttempts": 120},
    )


def describe_instance(rds, db_identifier: str) -> dict:
    response = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
    instances = response.get("DBInstances", [])
    if len(instances) != 1:
        raise RuntimeError(
            f"Expected exactly one DB instance for {db_identifier}, got {len(instances)}"
        )
    return instances[0]


@contextmanager
def connect(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    configure_session: bool = True,
) -> Iterator[psycopg2.extensions.connection]:
    sslmode = os.environ.get("DB_SSLMODE", "require")
    kwargs = {
        "host": host,
        "port": port,
        "database": database,
        "user": username,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "30")),
        # TCP keepalives so a silently-dropped RDS connection cannot leave the
        # client blocked forever in recv(). statement_timeout is enforced
        # server-side and is useless if the socket itself is dead.
        "keepalives": 1,
        "keepalives_idle": int(os.environ.get("DB_KEEPALIVES_IDLE_SECONDS", "30")),
        "keepalives_interval": int(
            os.environ.get("DB_KEEPALIVES_INTERVAL_SECONDS", "10")
        ),
        "keepalives_count": int(os.environ.get("DB_KEEPALIVES_COUNT", "3")),
    }
    sslrootcert = os.environ.get("DB_SSLROOTCERTS", "").strip()
    if sslrootcert:
        kwargs["sslrootcert"] = sslrootcert

    conn = psycopg2.connect(**kwargs)
    try:
        if configure_session:
            _configure_session(conn)
        yield conn
    finally:
        conn.close()


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _configure_session(conn: psycopg2.extensions.connection) -> None:
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

    # Optionally disable FK/user triggers for the bulk anonymization run. This
    # removes the per-row referential-integrity check overhead (the
    # `SELECT 1 FROM customers ... FOR KEY SHARE` that was timing out). It is
    # safe ONLY because the sanitizer never modifies key/FK columns and uses
    # deterministic hashing, so referential integrity and joins are preserved.
    # Requires the rds_superuser privilege; if the connecting role lacks it we
    # log a warning and continue with triggers enabled rather than aborting.
    if _truthy(os.environ.get("SANITIZER_DISABLE_TRIGGERS", "")):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET session_replication_role = 'replica'")
            conn.commit()
            logging.info(
                "session_replication_role set to 'replica' (triggers disabled "
                "for bulk anonymization)"
            )
        except psycopg2.Error as exc:
            conn.rollback()
            logging.warning(
                "Could not set session_replication_role=replica (%s); "
                "continuing with triggers enabled",
                exc,
            )


def postgres_major_from_server_version(server_version: int) -> str:
    return str(server_version // 10000)


def reset_master_password(rds, db_identifier: str, password: str) -> None:
    logging.info("Resetting master password on RDS instance %s", db_identifier)
    rds.modify_db_instance(
        DBInstanceIdentifier=db_identifier,
        MasterUserPassword=password,
        ApplyImmediately=True,
    )


def reconcile_app_role(conn, app_secret: AppSecret) -> None:
    role_name = app_secret.username
    database_name = app_secret.database
    password = app_secret.password

    with conn.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role_name,))
        role_exists = cursor.fetchone() is not None

        if role_exists:
            logging.info("Altering existing app role %s", role_name)
            cursor.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD %s").format(
                    sql.Identifier(role_name)
                ),
                (password,),
            )
        else:
            logging.info("Creating app role %s", role_name)
            cursor.execute(
                sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s").format(
                    sql.Identifier(role_name)
                ),
                (password,),
            )

        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database_name),
                sql.Identifier(role_name),
            )
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                sql.Identifier(role_name)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {}"
            ).format(sql.Identifier(role_name))
        )
        cursor.execute(
            sql.SQL(
                "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {}"
            ).format(sql.Identifier(role_name))
        )

    conn.commit()
