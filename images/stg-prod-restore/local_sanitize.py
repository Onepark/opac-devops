#!/usr/bin/env python3
"""Local sanitize script — runs full sanitization against a running DB.

Bypasses AWS dependencies (Secrets Manager, DynamoDB) to allow testing
against a local or directly-accessible PostgreSQL instance.

THIS MUTATES DATA. Run against a disposable/restore DB only.

Usage:
  DB_HOST=localhost DB_PORT=5432 DB_USER=opac DB_PASSWORD=pwd DB_NAME=opac \
    uv run python local_sanitize.py

Optional env vars:
  DB_SSLMODE           — default "disable" (local DB); use "require" for remote
  SANITIZER_UNCOVERED_PII_MODE — "warn" (default) or "fail"
  SANITIZER_POLICY_PATH — override bundled policy YAML
  ANONYMISATION_SALT   — salt for deterministic hashing (default: "local-test-salt")
  SANITIZER_MAX_WORKERS — parallel table workers (default: 4)
  SANITIZER_VERIFICATION_MODE — "fail" (default) or "warn"
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import dataclasses
from contextlib import closing

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from restore_tooling.sanitizer_policy import load_policy
from restore_tooling.sanitizer_schema import SchemaIssue, run_preflight
from restore_tooling.sanitizer_sql import install_helpers_sql, verify_helpers_sql
from restore_tooling.sanitizer_execution import run_collision_checks, run_execution
from restore_tooling.sanitizer_verification import run_verification


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
    max_workers = int(os.environ.get("SANITIZER_MAX_WORKERS", "4"))
    verification_mode = os.environ.get("SANITIZER_VERIFICATION_MODE", "fail")

    def conn_factory():
        return closing(
            psycopg2.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
                sslmode=sslmode,
                connect_timeout=10,
            )
        )

    start = time.monotonic()

    # --- Preflight ---
    logging.info("=== PREFLIGHT ===")
    policy = load_policy()
    logging.info(
        "Loaded policy version %d with %d tables", policy.version, len(policy.tables)
    )

    with conn_factory() as conn:
        with conn.cursor() as cursor:
            report = run_preflight(cursor, policy, uncovered_pii_mode=uncovered_mode)
            if report.passed:
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

    preflight_duration = time.monotonic() - start
    _log_preflight_report(policy, report, preflight_duration)

    if not report.passed:
        logging.error("Preflight FAILED — aborting sanitization")
        raise SystemExit(1)

    # --- Install helpers ---
    logging.info("=== INSTALL HELPERS ===")
    with conn_factory() as conn:
        with conn.cursor() as cursor:
            cursor.execute(install_helpers_sql())
            cursor.execute(verify_helpers_sql())
        conn.commit()
    logging.info("Helper functions installed in schema restore_sanitizer")

    # --- Collision checks ---
    logging.info("=== COLLISION CHECKS ===")
    with conn_factory() as conn:
        with conn.cursor() as cursor:
            collisions = run_collision_checks(
                cursor, policy, salt, report.unique_columns
            )
    if collisions:
        for c in collisions:
            logging.error(
                "Collision: %s.%s — value '%s' has %d rows",
                c.table,
                c.column,
                c.generated_value,
                c.row_count,
            )
        logging.error("Collision(s) detected — aborting")
        raise SystemExit(1)
    logging.info("No collisions detected")

    # --- Execute sanitization ---
    logging.info("=== SANITIZE ===")
    exec_report = run_execution(
        conn_factory=conn_factory,
        policy=policy,
        salt=salt,
        max_workers=max_workers,
        unique_columns=report.unique_columns,
    )

    # --- Verification ---
    logging.info("=== VERIFICATION ===")
    with conn_factory() as conn:
        with conn.cursor() as cursor:
            verify_report = run_verification(cursor, policy, mode=verification_mode)

    # --- Summary ---
    total_duration = time.monotonic() - start
    summary = {
        "mode": "sanitize",
        "policy_version": policy.version,
        "tables_configured": len(policy.tables),
        "preflight_passed": report.passed,
        "execution": {
            "max_workers": max_workers,
            "tables": [
                {
                    "table": t.table,
                    "batches": t.batches,
                    "updated_rows": t.updated_rows,
                    "duration_seconds": round(t.duration_seconds, 2),
                    "status": t.status,
                }
                for t in exec_report.tables
            ],
            "duration_seconds": round(exec_report.duration_seconds, 2),
        },
        "verification": {
            "mode": verification_mode,
            "passed": verify_report.passed,
            "results": [
                {
                    "target": r.target,
                    "checked": r.checked,
                    "failed": r.failed,
                }
                for r in verify_report.results
            ],
        },
        "duration_seconds": round(total_duration, 2),
        "status": "passed" if exec_report.passed and verify_report.passed else "failed",
    }
    logging.info("Sanitization summary:\n%s", json.dumps(summary, indent=2))

    if not exec_report.passed or not verify_report.passed:
        logging.error("Sanitization completed with failures")
        raise SystemExit(1)

    logging.info("Sanitization PASSED")


if __name__ == "__main__":
    main()
