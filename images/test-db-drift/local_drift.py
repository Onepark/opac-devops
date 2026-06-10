#!/usr/bin/env python3
"""Local drift script — runs date drifting against a running DB.

Bypasses AWS dependencies (RDS) to allow testing against a local or
directly-accessible PostgreSQL instance.

THIS MUTATES DATA. Run against a disposable/restore DB only.

Usage:
  DB_HOST=localhost DB_PORT=5432 DB_USER=opac DB_PASSWORD=pwd DB_NAME=opac \
    DRIFT_DELTA_DAYS=30 \
    uv run python local_drift.py

Optional env vars:
  DB_SSLMODE                  — default "disable" (local DB); use "require" for remote
  DRIFT_POLICY_PATH           — override bundled policy YAML
  DRIFT_DELTA_DAYS             — days to shift forward (default: 30)
  DRIFT_MAX_WORKERS            — parallel table workers (default: 8)
  DRIFT_BATCH_TIMEOUT_SECONDS  — per-batch timeout in seconds (default: 3600)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from utils.drift import apply_drift
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
    delta_days = int(os.environ.get("DRIFT_DELTA_DAYS", "30"))

    params = {
        "host": host,
        "port": port,
        "database": dbname,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": 10,
    }

    start = time.monotonic()

    # --- Preflight ---
    logging.info("=== PREFLIGHT ===")
    policy = load_policy()
    logging.info(
        "Loaded policy version %d with %d tables",
        policy.version,
        len(policy.tables),
    )

    try:
        conn = psycopg2.connect(**params)
    except Exception as exc:
        logging.exception("Failed to connect to database: %s", exc)
        raise SystemExit(1)

    try:
        with conn.cursor() as cursor:
            errors = run_preflight(cursor, policy)

        if errors:
            for err in errors:
                logging.error("[ERROR] %s", err)
            logging.error("Preflight FAILED — aborting drift")
            raise SystemExit(1)

        logging.info("Preflight PASSED")

        # --- Drift ---
        logging.info("=== DRIFT (delta_days=%d) ===", delta_days)
        result = apply_drift(conn, params, policy, delta_days)

        duration = time.monotonic() - start
        summary = {
            "mode": "drift",
            "policy_version": policy.version,
            "delta_days": delta_days,
            "tables_drifted": result.tables_drifted,
            "total_rows": result.total_rows,
            "constraints_dropped": result.constraints_dropped,
            "constraints_recreated": result.constraints_recreated,
            "duration_seconds": round(duration, 2),
        }
        logging.info("Drift summary:\n%s", json.dumps(summary, indent=2))
        logging.info("Drift PASSED")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
