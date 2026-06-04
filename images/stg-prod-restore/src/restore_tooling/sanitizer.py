import json
import logging
import os
import sys
import time
import dataclasses
from typing import Callable

from .common import (
    client,
    compact_error,
    get_app_secret,
    require_env,
    resource,
    setup_logging,
)
from .db import connect, wait_for_tcp_port
from .sanitizer_execution import run_collision_checks, run_execution
from .sanitizer_policy import load_policy
from .sanitizer_schema import SchemaIssue, run_preflight
from .sanitizer_verification import run_verification
from .state import mark_failed, mark_passed, mark_running

REQUIRED_ENV = [
    "AWS_REGION",
    "SLOT_NAME",
    "DB_IDENTIFIER",
    "DB_HOST",
    "DB_PORT",
    "STATE_TABLE_NAME",
    "SOURCE_SNAPSHOT_ARN",
    "SOURCE_SNAPSHOT_WEEK",
    "EXECUTION_ARN",
    "APP_SECRET_ARN",
    "ANONYMISATION_SALT",
]


def _validate_env(env: dict[str, str]) -> None:
    if env["SLOT_NAME"] not in {"blue", "green"}:
        raise RuntimeError(
            f"Invalid SLOT_NAME {env['SLOT_NAME']}; expected blue or green"
        )
    int(env["DB_PORT"])


def _conn_factory(env: dict[str, str], app_secret) -> Callable:
    """Return a zero-arg callable that opens a new DB connection."""
    def _open():
        return connect(
            env["DB_HOST"],
            int(env["DB_PORT"]),
            app_secret.database,
            app_secret.username,
            app_secret.password,
        )

    return _open


def _preflight(env: dict[str, str]) -> tuple:
    """Run non-mutating preflight checks. Returns (policy, report)."""
    policy = load_policy()
    logging.info(
        "Loaded policy version %d with %d tables", policy.version, len(policy.tables)
    )

    app_secret = get_app_secret(
        client("secretsmanager", env["AWS_REGION"]), env["APP_SECRET_ARN"]
    )
    wait_for_tcp_port(env["DB_HOST"], int(env["DB_PORT"]))

    uncovered_mode = os.environ.get("SANITIZER_UNCOVERED_PII_MODE", "warn")

    with connect(
        env["DB_HOST"],
        int(env["DB_PORT"]),
        app_secret.database,
        app_secret.username,
        app_secret.password,
    ) as conn:
        with conn.cursor() as cursor:
            report = run_preflight(cursor, policy, uncovered_pii_mode=uncovered_mode)
            if report.passed:
                collisions = run_collision_checks(
                    cursor,
                    policy,
                    env["ANONYMISATION_SALT"],
                    report.unique_columns,
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

    return policy, report, app_secret


def _log_preflight_report(policy, report, duration_seconds: float) -> None:
    """Log structured preflight summary."""
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
    logging.info("Preflight summary: %s", json.dumps(summary, indent=2))

    for issue in report.issues:
        level = logging.ERROR if issue.severity == "error" else logging.WARNING
        logging.log(level, "[%s] %s", issue.severity.upper(), issue.message)


def cmd_preflight() -> None:
    """Run preflight checks only; do not mutate DB or update DynamoDB."""
    start = time.monotonic()
    env = require_env(REQUIRED_ENV)
    _validate_env(env)

    try:
        policy, report, _ = _preflight(env)
        duration = time.monotonic() - start
        _log_preflight_report(policy, report, duration)

        if not report.passed:
            raise SystemExit(1)
    except Exception as exc:
        logging.exception("Preflight failed: %s", compact_error(exc))
        raise SystemExit(1)


def cmd_sanitize() -> None:
    """Run full sanitization: preflight, install helpers, mutate, verify, update state."""
    start = time.monotonic()
    env = require_env(REQUIRED_ENV)
    _validate_env(env)
    dynamodb = resource("dynamodb", env["AWS_REGION"])

    mark_running(
        dynamodb,
        env["STATE_TABLE_NAME"],
        env["SLOT_NAME"],
        "sanitizationStatus",
        "lastSanitizerExecutionArn",
        env["EXECUTION_ARN"],
    )

    try:
        policy, report, app_secret = _preflight(env)
        preflight_duration = time.monotonic() - start
        _log_preflight_report(policy, report, preflight_duration)

        if not report.passed:
            raise RuntimeError("Preflight checks failed; aborting sanitization")

        max_workers = int(os.environ.get("SANITIZER_MAX_WORKERS", "4"))
        verification_mode = os.environ.get("SANITIZER_VERIFICATION_MODE", "fail")
        conn_factory = _conn_factory(env, app_secret)

        exec_report = run_execution(
            conn_factory=conn_factory,
            policy=policy,
            salt=env["ANONYMISATION_SALT"],
            max_workers=max_workers,
            unique_columns=report.unique_columns,
        )

        # Verification
        with conn_factory() as conn:
            with conn.cursor() as cursor:
                verify_report = run_verification(cursor, policy, mode=verification_mode)

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
            "status": "passed"
            if exec_report.passed and verify_report.passed
            else "failed",
        }
        logging.info("Sanitization summary: %s", json.dumps(summary, indent=2))

        if not exec_report.passed or not verify_report.passed:
            raise RuntimeError("Sanitization completed with failures")

        mark_passed(
            dynamodb, env["STATE_TABLE_NAME"], env["SLOT_NAME"], "sanitizationStatus"
        )
    except Exception as exc:
        error = compact_error(exc)
        logging.exception("Sanitization failed: %s", error)
        mark_failed(
            dynamodb,
            env["STATE_TABLE_NAME"],
            env["SLOT_NAME"],
            "sanitizationStatus",
            "lastSanitizationError",
            error,
        )
        raise SystemExit(1)


def main() -> None:
    setup_logging()
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python -m restore_tooling.sanitizer <preflight|sanitize>"
        )

    mode = sys.argv[1]
    if mode == "preflight":
        cmd_preflight()
    elif mode == "sanitize":
        cmd_sanitize()
    else:
        raise SystemExit(f"Unknown mode: {mode}; expected 'preflight' or 'sanitize'")


if __name__ == "__main__":
    main()
