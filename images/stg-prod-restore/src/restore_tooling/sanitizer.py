import logging
import sys

from .common import (
    client,
    compact_error,
    get_app_secret,
    require_env,
    resource,
    setup_logging,
)
from .db import connect, wait_for_tcp_port
from .sanitization_rules import SANITIZATION_RULES, build_update
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


def verify_pgcrypto_extension(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'")
        if cursor.fetchone() is None:
            raise RuntimeError(
                "pgcrypto extension is missing; credential reconciliation should create it before sanitization"
            )
    logging.info("pgcrypto extension is available")


def apply_sanitization(conn, salt: str) -> None:
    verify_pgcrypto_extension(conn)

    total_errors = 0
    for table, set_clauses in SANITIZATION_RULES.items():
        table_rows = 0
        for set_clause in set_clauses:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(build_update(table, set_clause), {"salt": salt})
                    table_rows += cursor.rowcount
                conn.commit()
            except Exception:
                conn.rollback()
                total_errors += 1
                logging.exception("Error anonymising table %s", table)
        logging.info("Anonymised %s: %s rows", table, table_rows)

    if total_errors:
        raise RuntimeError(
            f"Sanitization failed with {total_errors} table/rule error(s)"
        )


def sanitize() -> None:
    env = require_env(REQUIRED_ENV)
    _validate_env(env)
    dynamodb = resource("dynamodb", env["AWS_REGION"])
    secretsmanager = client("secretsmanager", env["AWS_REGION"])

    mark_running(
        dynamodb,
        env["STATE_TABLE_NAME"],
        env["SLOT_NAME"],
        "sanitizationStatus",
        "lastSanitizerExecutionArn",
        env["EXECUTION_ARN"],
    )

    try:
        app_secret = get_app_secret(secretsmanager, env["APP_SECRET_ARN"])
        wait_for_tcp_port(env["DB_HOST"], int(env["DB_PORT"]))
        with connect(
            env["DB_HOST"],
            int(env["DB_PORT"]),
            app_secret.database,
            app_secret.username,
            app_secret.password,
        ) as conn:
            apply_sanitization(conn, env["ANONYMISATION_SALT"])

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
        raise


def main() -> None:
    setup_logging()
    if len(sys.argv) != 2:
        raise SystemExit("Usage: sanitizer.py <sanitize>")

    mode = sys.argv[1]
    if mode == "sanitize":
        sanitize()
    else:
        raise SystemExit(f"Unsupported sanitizer mode: {mode}")


if __name__ == "__main__":
    main()
