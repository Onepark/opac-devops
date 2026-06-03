#!/usr/bin/env python3
"""Local preflight script — runs schema preflight checks against a running DB.

Bypasses AWS dependencies (Secrets Manager, DynamoDB) to allow testing
against a local or directly-accessible PostgreSQL instance.

Usage:
  DB_HOST=localhost DB_PORT=5432 DB_USER=opac DB_PASSWORD=pwd DB_NAME=opac \
    uv run python local_preflight.py

Optional env vars:
  DB_SSLMODE           — default "disable" (local DB); use "require" for remote
  SANITIZER_UNCOVERED_PII_MODE — "warn" (default) or "fail"
  SANITIZER_POLICY_PATH — override bundled policy YAML
  ANONYMISATION_SALT   — salt for collision checks (default: "local-test-salt")
  RUN_COLLISION_CHECKS — "true" to also install helpers + run collision checks
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import dataclasses

import psycopg2

# Ensure src/ is on the path so restore_tooling is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from restore_tooling.sanitizer_policy import load_policy
from restore_tooling.sanitizer_schema import SchemaIssue, run_preflight
from restore_tooling.sanitizer_sql import install_helpers_sql, verify_helpers_sql

# Collision checks need the execution module which imports from restore_tooling
from restore_tooling.sanitizer_execution import run_collision_checks


def _log_preflight_report(policy, report, duration_seconds: float) -> None:
    summary = {
        "mode": "preflight",
        "policy_version": policy.version,
        "tables_configured": len(policy.tables),
        "preflight_passed": report.passed,
        "preflight_issues": [
            {"severity": i.severity, "message": i.message} for i in report.issues
        ],
        "suspicious_uncovered": [
            {"table": t, "column": c} for t, c in report.suspicious_uncovered
        ],
        "duration_seconds": round(duration_seconds, 2),
    }
    logging.info("Preflight summary:\n%s", json.dumps(summary, indent=2))

    for issue in report.issues:
        level = logging.ERROR if issue.severity == "error" else logging.WARNING
        logging.log(level, "[%s] %s", issue.severity.upper(), issue.message)


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
    uncovered_mode = os.environ.get("SANITIZER_UNCOVERED_PII_MODE", "warn")
    salt = os.environ.get("ANONYMISATION_SALT", "local-test-salt")
    run_collisions = os.environ.get("RUN_COLLISION_CHECKS", "false").lower() == "true"

    logging.info("Connecting to %s:%s/%s as %s (sslmode=%s)", host, port, dbname, user, sslmode)

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
                report = run_preflight(cursor, policy, uncovered_pii_mode=uncovered_mode)

        duration = time.monotonic() - start
        _log_preflight_report(policy, report, duration)

        # Collision checks (optional — requires installing helper functions)
        if run_collisions and report.passed:
            logging.info("Installing helper functions for collision checks...")
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(install_helpers_sql())
                    cursor.execute(verify_helpers_sql())
                conn.commit()
            logging.info("Helper functions installed in schema restore_sanitizer")

            logging.info("Running collision checks...")
            with conn:
                with conn.cursor() as cursor:
                    collisions = run_collision_checks(
                        cursor, policy, salt, report.unique_columns
                    )

            if collisions:
                collision_issues = tuple(
                    SchemaIssue(
                        "error",
                        f"Generated collision for unique column '{c.table}.{c.column}' "
                        f"(value '{c.generated_value}' has {c.row_count} rows)",
                    )
                    for c in collisions
                )
                report = dataclasses.replace(
                    report,
                    passed=False,
                    issues=report.issues + collision_issues,
                )
                for c in collisions:
                    logging.error(
                        "Collision: %s.%s — value '%s' has %d rows",
                        c.table,
                        c.column,
                        c.generated_value,
                        c.row_count,
                    )
            else:
                logging.info("No collisions detected")

            # Re-log updated report
            duration = time.monotonic() - start
            _log_preflight_report(policy, report, duration)

        if not report.passed:
            logging.error("Preflight FAILED")
            raise SystemExit(1)
        else:
            logging.info("Preflight PASSED")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
