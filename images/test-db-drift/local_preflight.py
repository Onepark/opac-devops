#!/usr/bin/env python3
"""Local preflight script — runs drift schema preflight checks against a running DB.

Bypasses AWS dependencies (RDS) to allow testing against a local or
directly-accessible PostgreSQL instance.

Usage:
  DB_HOST=localhost DB_PORT=5432 DB_USER=opac DB_PASSWORD=pwd DB_NAME=opac \
    uv run python local_preflight.py

Optional env vars:
  DB_SSLMODE           — default "disable" (local DB); use "require" for remote
  DRIFT_POLICY_PATH    — override bundled policy YAML
"""

from __future__ import annotations

import logging
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from utils.drift_policy import load_policy, run_preflight


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,
    )

    host = os.environ.get("DB_HOST", "localhost")
    port = int(os.environ.get("DB_PORT", "5432"))
    user = os.environ.get("DB_USER", "opac")
    password = os.environ.get("DB_PASSWORD", "pwd")
    dbname = os.environ.get("DB_NAME", "opac")
    sslmode = os.environ.get("DB_SSLMODE", "disable")

    logging.info(
        "Connecting to %s:%s/%s as %s (sslmode=%s)",
        host,
        port,
        dbname,
        user,
        sslmode,
    )

    start = time.monotonic()

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=10,
        )
    except Exception as exc:
        logging.exception("Failed to connect to database: %s", exc)
        raise SystemExit(1)

    try:
        policy = load_policy()
        logging.info(
            "Loaded policy version %d with %d tables",
            policy.version,
            len(policy.tables),
        )

        with conn:
            with conn.cursor() as cursor:
                errors = run_preflight(cursor, policy)

        duration = time.monotonic() - start

        if errors:
            for err in errors:
                logging.error("[ERROR] %s", err)
            logging.error(
                "Preflight FAILED (%d error(s), %.2fs)",
                len(errors),
                duration,
            )
            raise SystemExit(1)
        else:
            logging.info(
                "Preflight PASSED (%d tables, %.2fs)",
                len(policy.tables),
                duration,
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
