from __future__ import annotations

import dataclasses
import os
from typing import Any

import yaml

BUNDLED_POLICY = os.path.join(os.path.dirname(os.path.dirname(__file__)), "drift_policy.yaml")


@dataclasses.dataclass(frozen=True)
class DriftPolicy:
    version: int
    schema_name: str
    tables: tuple[tuple[str, tuple[str, ...]], ...]

    def all_drifted_columns(self) -> set[str]:
        cols: set[str] = set()
        for _, columns in self.tables:
            cols.update(columns)
        return cols

    def find_table(self, name: str) -> tuple[str, tuple[str, ...]] | None:
        for t, cols in self.tables:
            if t == name:
                return t, cols
        return None


def load_policy(path: str | None = None) -> DriftPolicy:
    resolved = path or os.environ.get("DRIFT_POLICY_PATH") or BUNDLED_POLICY
    with open(resolved) as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    version = int(raw.get("version", 1))
    schema_name = raw.get("schema", "public")

    tables_raw = raw.get("tables", {}) or {}
    if not isinstance(tables_raw, dict):
        raise ValueError("'tables' must be a mapping")

    tables: list[tuple[str, tuple[str, ...]]] = []
    for table_name, table_raw in tables_raw.items():
        if not isinstance(table_raw, dict):
            raise ValueError(f"Invalid table definition for '{table_name}'")
        columns_raw = table_raw.get("columns", []) or []
        if not isinstance(columns_raw, list):
            raise ValueError(f"'columns' for '{table_name}' must be a list")
        columns = tuple(str(c).strip() for c in columns_raw)
        if not columns:
            raise ValueError(f"'columns' for '{table_name}' is empty")
        tables.append((table_name, columns))

    return DriftPolicy(
        version=version,
        schema_name=schema_name,
        tables=tuple(tables),
    )


VALID_DATE_TYPES = frozenset(
    {
        "date",
        "timestamp without time zone",
        "timestamp with time zone",
    }
)


def run_preflight(cursor, policy: DriftPolicy) -> list[str]:
    """Validate that all tables and columns declared in the policy exist
    in the live database schema and are date/timestamp types.
    Returns a list of error messages (empty list = all good)."""
    errors: list[str] = []

    # Check tables exist
    for table_name, columns in policy.tables:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (policy.schema_name, table_name),
        )
        if cursor.fetchone() is None:
            errors.append(f"Table '{policy.schema_name}.{table_name}' not found in schema")
            continue

        # Check each column exists and is a date/timestamp type
        cursor.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (policy.schema_name, table_name),
        )
        existing = {row[0]: row[1] for row in cursor.fetchall()}
        for col in columns:
            if col not in existing:
                errors.append(f"Column '{policy.schema_name}.{table_name}.{col}' not found in schema")
            elif existing[col] not in VALID_DATE_TYPES:
                errors.append(
                    f"Column '{policy.schema_name}.{table_name}.{col}' "
                    f"has type '{existing[col]}', expected a date/timestamp type"
                )

    return errors
