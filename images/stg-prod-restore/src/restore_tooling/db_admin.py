import logging
import sys

from .common import (
    client,
    compact_error,
    get_app_secret,
    get_secret_string,
    require_env,
    resource,
    setup_logging,
)
from .db import (
    connect,
    describe_instance,
    postgres_major_from_server_version,
    reconcile_app_role,
    reset_master_password,
    wait_for_rds_available,
    wait_for_tcp_port,
)
from .state import mark_failed, mark_passed, mark_running


COMMON_ENV = [
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
]


def _validate_slot(env: dict[str, str]) -> None:
    if env["SLOT_NAME"] not in {"blue", "green"}:
        raise RuntimeError(
            f"Invalid SLOT_NAME {env['SLOT_NAME']}; expected blue or green"
        )
    int(env["DB_PORT"])


def credential_reconcile() -> None:
    env = require_env([*COMMON_ENV, "ADMIN_SECRET_ARN"])
    _validate_slot(env)
    dynamodb = resource("dynamodb", env["AWS_REGION"])
    secretsmanager = client("secretsmanager", env["AWS_REGION"])
    rds = client("rds", env["AWS_REGION"])

    mark_running(
        dynamodb,
        env["STATE_TABLE_NAME"],
        env["SLOT_NAME"],
        "credentialStatus",
        "lastDbAdminExecutionArn",
        env["EXECUTION_ARN"],
    )

    try:
        app_secret = get_app_secret(secretsmanager, env["APP_SECRET_ARN"])
        admin_password = get_secret_string(
            secretsmanager, env["ADMIN_SECRET_ARN"]
        ).strip()
        if not admin_password:
            raise RuntimeError("Admin secret is empty")

        instance = describe_instance(rds, env["DB_IDENTIFIER"])
        master_username = instance.get("MasterUsername", "").strip()
        if not master_username:
            raise RuntimeError("RDS instance metadata did not include MasterUsername")

        reset_master_password(rds, env["DB_IDENTIFIER"], admin_password)
        wait_for_rds_available(rds, env["DB_IDENTIFIER"])
        wait_for_tcp_port(env["DB_HOST"], int(env["DB_PORT"]))

        with connect(
            env["DB_HOST"],
            int(env["DB_PORT"]),
            app_secret.database,
            master_username,
            admin_password,
        ) as conn:
            reconcile_app_role(conn, app_secret)

        mark_passed(
            dynamodb, env["STATE_TABLE_NAME"], env["SLOT_NAME"], "credentialStatus"
        )
    except Exception as exc:
        error = compact_error(exc)
        logging.exception("Credential reconciliation failed: %s", error)
        mark_failed(
            dynamodb,
            env["STATE_TABLE_NAME"],
            env["SLOT_NAME"],
            "credentialStatus",
            "lastCredentialError",
            error,
        )
        raise


def validate() -> None:
    env = require_env([*COMMON_ENV, "EXPECTED_POSTGRES_MAJOR_VERSION"])
    _validate_slot(env)
    dynamodb = resource("dynamodb", env["AWS_REGION"])
    secretsmanager = client("secretsmanager", env["AWS_REGION"])

    mark_running(
        dynamodb,
        env["STATE_TABLE_NAME"],
        env["SLOT_NAME"],
        "validationStatus",
        "lastDbAdminExecutionArn",
        env["EXECUTION_ARN"],
    )

    try:
        app_secret = get_app_secret(secretsmanager, env["APP_SECRET_ARN"])
        with connect(
            env["DB_HOST"],
            int(env["DB_PORT"]),
            app_secret.database,
            app_secret.username,
            app_secret.password,
            configure_session=False,
        ) as conn:
            actual_major = postgres_major_from_server_version(conn.server_version)

        expected_major = env["EXPECTED_POSTGRES_MAJOR_VERSION"]
        if actual_major != expected_major:
            raise RuntimeError(
                f"PostgreSQL major version mismatch: expected {expected_major}, got {actual_major}"
            )

        mark_passed(
            dynamodb, env["STATE_TABLE_NAME"], env["SLOT_NAME"], "validationStatus"
        )
    except Exception as exc:
        error = compact_error(exc)
        logging.exception("Validation failed: %s", error)
        mark_failed(
            dynamodb,
            env["STATE_TABLE_NAME"],
            env["SLOT_NAME"],
            "validationStatus",
            "lastValidationError",
            error,
        )
        raise


def main() -> None:
    setup_logging()
    if len(sys.argv) != 2:
        raise SystemExit("Usage: db_admin.py <credential-reconcile|validate>")

    mode = sys.argv[1]
    if mode == "credential-reconcile":
        credential_reconcile()
    elif mode == "validate":
        validate()
    else:
        raise SystemExit(f"Unsupported db-admin mode: {mode}")


if __name__ == "__main__":
    main()
