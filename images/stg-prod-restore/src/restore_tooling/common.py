import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Iterable

import boto3
from botocore.config import Config


CLIENT_CONFIG = Config(retries={"mode": "standard", "max_attempts": 6})


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,
    )


def require_env(names: Iterable[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    missing: list[str] = []
    for name in names:
        value = os.environ.get(name, "").strip()
        if not value:
            missing.append(name)
        else:
            values[name] = value

    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    return values


def client(service_name: str, region_name: str):
    return boto3.client(service_name, region_name=region_name, config=CLIENT_CONFIG)


def resource(service_name: str, region_name: str):
    return boto3.resource(service_name, region_name=region_name, config=CLIENT_CONFIG)


def get_secret_string(secretsmanager, secret_arn: str) -> str:
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    if "SecretString" not in response or not response["SecretString"]:
        raise RuntimeError(f"Secret {secret_arn} has no SecretString")
    return response["SecretString"]


@dataclass(frozen=True)
class AppSecret:
    username: str
    password: str
    database: str


def get_app_secret(secretsmanager, secret_arn: str) -> AppSecret:
    raw = get_secret_string(secretsmanager, secret_arn)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Application secret must be a JSON object") from exc

    required = {
        "DB_USERNAME": str(payload.get("DB_USERNAME", "")).strip(),
        "DB_PASSWORD": str(payload.get("DB_PASSWORD", "")).strip(),
        "DB_NAME": str(payload.get("DB_NAME", "")).strip(),
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            f"Application secret missing non-empty key(s): {', '.join(missing)}"
        )

    return AppSecret(
        username=required["DB_USERNAME"],
        password=required["DB_PASSWORD"],
        database=required["DB_NAME"],
    )


def compact_error(exc: BaseException, max_length: int = 900) -> str:
    message = f"{type(exc).__name__}: {exc}"
    if len(message) > max_length:
        return message[: max_length - 3] + "..."
    return message
