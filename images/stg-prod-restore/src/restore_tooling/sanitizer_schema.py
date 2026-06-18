from __future__ import annotations

import dataclasses
import re
from typing import Any

from .sanitizer_policy import (
    SanitizationPolicy,
    known_strategies,
)

_SUSPICIOUS_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^email$",
        r"^e_mail$",
        r"^mail$",
        r"^phone",
        r"^mobile",
        r"^firstname",
        r"^first_name",
        r"^lastname",
        r"^last_name",
        r"^surname",
        r"^name$",
        r"^contact",
        r"^address",
        r"^iban$",
        r"^bic$",
        r"^siren$",
        r"^siret$",
        r"^national_identifier",
        r"^tax_identifier",
        r"^plate",
        r"^license",
        r"^birth",
        r"^birthday",
        r"^dob$",
    ]
]

_TECHNICAL_COLUMNS: set[str] = {
    "id",
    "created_at",
    "updated_at",
    "deleted_at",
    "uuid",
    "status",
    "type",
    "scope",
    "slug",
    "token",
    "reference",
    "position",
    "sort_order",
    "active",
    "enabled",
}


@dataclasses.dataclass(frozen=True)
class SchemaIssue:
    severity: str  # "error" or "warning"
    message: str


@dataclasses.dataclass(frozen=True)
class SchemaReport:
    passed: bool
    issues: tuple[SchemaIssue, ...]
    unique_columns: tuple[tuple[str, str], ...]
    suspicious_uncovered: tuple[tuple[str, str], ...]


def _resolve_table_columns(cursor: Any, policy: SanitizationPolicy) -> dict[str, dict[str, str]]:
    """Returns {table_name: {column_name: data_type}} for all configured tables.

    For ARRAY columns, data_type is reported as 'ARRAY' by information_schema.
    We also fetch the element type via the udt_name column (e.g. '_varchar' → 'varchar').
    """
    tables = [t.name for t in policy.tables]
    if not tables:
        return {}

    placeholders = ",".join("%s" for _ in tables)
    cursor.execute(
        f"""
        SELECT table_name, column_name, data_type,
               CASE WHEN data_type = 'ARRAY' THEN
                   ltrim(udt_name, '_')
               ELSE NULL END AS element_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = ANY (ARRAY[{placeholders}])
        """,
        [policy.schema_name] + tables,
    )
    result: dict[str, dict[str, str]] = {}
    for table_name, column_name, data_type, element_type in cursor.fetchall():
        # For array columns, store as "ARRAY" so callers can distinguish,
        # but also track element type for validation
        if data_type == "ARRAY" and element_type:
            result.setdefault(table_name, {})[column_name] = f"ARRAY:{element_type}"
        else:
            result.setdefault(table_name, {})[column_name] = data_type
    return result


def _resolve_unique_constraints(cursor: Any, policy: SanitizationPolicy) -> dict[str, set[str]]:
    """Returns {table_name: {column_name}} for single-column unique constraints."""
    tables = [t.name for t in policy.tables]
    if not tables:
        return {}

    placeholders = ",".join("%s" for _ in tables)
    cursor.execute(
        f"""
        SELECT
            t.relname AS table_name,
            a.attname AS column_name
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
        WHERE n.nspname = %s
          AND t.relname = ANY (ARRAY[{placeholders}])
          AND c.contype IN ('u', 'p')
          AND array_length(c.conkey, 1) = 1
        """,
        [policy.schema_name] + tables,
    )
    result: dict[str, set[str]] = {}
    for table_name, column_name in cursor.fetchall():
        result.setdefault(table_name, set()).add(column_name)
    return result


def _check_uncovered(
    schema_columns: dict[str, dict[str, str]],
    policy: SanitizationPolicy,
) -> list[tuple[str, str]]:
    """Find columns matching suspicious patterns that are not covered by policy."""
    covered: dict[str, set[str]] = {}
    for table in policy.tables:
        covered.setdefault(table.name, set())
        for col_name, rule in table.columns:
            covered[table.name].add(col_name)
            if rule.is_jsonb():
                covered[table.name].add(col_name)

    uncovered: list[tuple[str, str]] = []
    for table_name, columns in schema_columns.items():
        covered_cols = covered.get(table_name, set())
        for col_name in columns:
            if col_name in covered_cols:
                continue
            if col_name in _TECHNICAL_COLUMNS:
                continue
            if col_name.endswith("_id"):
                continue
            if any(p.search(col_name) for p in _SUSPICIOUS_PATTERNS):
                uncovered.append((table_name, col_name))
    return uncovered


def run_preflight(
    cursor: Any,
    policy: SanitizationPolicy,
    uncovered_pii_mode: str = "warn",
) -> SchemaReport:
    """Run schema-level preflight checks against a live DB cursor."""
    issues: list[SchemaIssue] = []

    schema_columns = _resolve_table_columns(cursor, policy)
    unique_constraints = _resolve_unique_constraints(cursor, policy)

    for table in policy.tables:
        table_cols = schema_columns.get(table.name)
        if table_cols is None:
            issues.append(SchemaIssue("error", f"Table '{table.name}' not found in schema"))
            continue

        for col_name, rule in table.columns:
            if rule.is_jsonb():
                actual_type = table_cols.get(col_name)
                if actual_type is None:
                    issues.append(SchemaIssue("error", f"Column '{table.name}.{col_name}' not found"))
                elif actual_type not in ("json", "jsonb"):
                    issues.append(
                        SchemaIssue(
                            "error",
                            f"Column '{table.name}.{col_name}' is type '{actual_type}', expected json/jsonb",
                        )
                    )
                for key, key_rule in rule.jsonb_keys:
                    if key_rule.strategy not in known_strategies():
                        issues.append(
                            SchemaIssue(
                                "error",
                                f"Unknown strategy '{key_rule.strategy}' for '{table.name}.{col_name}.{key}'",
                            )
                        )
            elif rule.is_conditional():
                actual_type = table_cols.get(col_name)
                if actual_type is None:
                    issues.append(SchemaIssue("error", f"Column '{table.name}.{col_name}' not found"))
                for cond in rule.strategy_by_condition:
                    if not cond.strategy:
                        issues.append(
                            SchemaIssue(
                                "error",
                                f"Empty strategy in condition for '{table.name}.{col_name}'",
                            )
                        )
                    elif cond.strategy not in known_strategies():
                        issues.append(
                            SchemaIssue(
                                "error",
                                f"Unknown strategy '{cond.strategy}' for '{table.name}.{col_name}'",
                            )
                        )
            elif rule.is_normal():
                actual_type = table_cols.get(col_name)
                if actual_type is None:
                    issues.append(SchemaIssue("error", f"Column '{table.name}.{col_name}' not found"))
                elif rule.array:
                    # Expect ARRAY with text-compatible element type
                    if not actual_type.startswith("ARRAY:"):
                        issues.append(
                            SchemaIssue(
                                "error",
                                f"Column '{table.name}.{col_name}' is type '{actual_type}', expected ARRAY type (array: true is set)",
                            )
                        )
                    else:
                        element_type = actual_type[len("ARRAY:") :]
                        if element_type not in (
                            "text",
                            "varchar",
                            "character varying",
                            "citext",
                            "character",
                        ):
                            issues.append(
                                SchemaIssue(
                                    "error",
                                    f"Column '{table.name}.{col_name}' has array element type '{element_type}', expected text-compatible element",
                                )
                            )
                elif actual_type not in (
                    "text",
                    "character varying",
                    "varchar",
                    "citext",
                    "character",
                ):
                    issues.append(
                        SchemaIssue(
                            "error",
                            f"Column '{table.name}.{col_name}' is type '{actual_type}', expected text type",
                        )
                    )
                if rule.strategy not in known_strategies():
                    issues.append(
                        SchemaIssue(
                            "error",
                            f"Unknown strategy '{rule.strategy}' for '{table.name}.{col_name}'",
                        )
                    )

        if table.batch.enabled:
            table_cols = schema_columns.get(table.name, {})
            actual_type = table_cols.get(table.batch.key)
            if actual_type is None:
                issues.append(
                    SchemaIssue(
                        "error",
                        f"Batch key '{table.batch.key}' not found in table '{table.name}'",
                    )
                )
            elif actual_type not in (
                "integer",
                "bigint",
                "smallint",
                "numeric",
                "serial",
                "bigserial",
            ):
                issues.append(
                    SchemaIssue(
                        "error",
                        f"Batch key '{table.name}.{table.batch.key}' is type '{actual_type}', expected numeric type",
                    )
                )

    # Check pgcrypto
    cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'")
    if cursor.fetchone() is None:
        issues.append(SchemaIssue("error", "pgcrypto extension is not installed"))

    suspicious = _check_uncovered(schema_columns, policy)
    if suspicious:
        msg = f"Suspicious uncovered PII columns: {', '.join(f'{t}.{c}' for t, c in suspicious)}"
        if uncovered_pii_mode == "fail":
            issues.append(SchemaIssue("error", msg))
        else:
            issues.append(SchemaIssue("warning", msg))

    for table_name in schema_columns:
        if not any(t.name == table_name for t in policy.tables):
            issues.append(
                SchemaIssue(
                    "warning",
                    f"Table '{table_name}' exists in schema but is not configured in policy",
                )
            )

    errors = [i for i in issues if i.severity == "error"]

    return SchemaReport(
        passed=len(errors) == 0,
        issues=tuple(issues),
        unique_columns=tuple((t, c) for t, cols in unique_constraints.items() for c in cols),
        suspicious_uncovered=tuple(suspicious),
    )
