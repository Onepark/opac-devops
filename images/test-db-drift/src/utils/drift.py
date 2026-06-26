from __future__ import annotations

import dataclasses
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2

from utils.db import configure_session
from utils.drift_policy import DriftPolicy


@dataclasses.dataclass(frozen=True)
class DriftResult:
    tables_drifted: int
    total_rows: int
    constraints_dropped: int
    constraints_recreated: int


def drift_table(
    conn,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    delta_days: int,
) -> int:
    """Drift all date columns in one table with a single UPDATE.

    Uses the provided connection (caller manages lifecycle).
    Returns the number of rows updated.
    """
    set_clause = ", ".join(f'"{col}" = "{col}" + INTERVAL \'{delta_days} days\'' for col in columns)
    with conn.cursor() as c:
        c.execute(f'UPDATE "{schema}"."{table}" SET {set_clause}')
        row_count = c.rowcount
    conn.commit()
    return row_count


def _definition_references_column(definition: str, column: str) -> bool:
    """True if *definition* references *column* as an identifier.

    Matches the column name on word boundaries (case-sensitively, since
    PostgreSQL stores unquoted identifiers in lower case and emits keywords in
    upper case). This catches a column used only inside an expression — e.g.
    tsrange("begin", "end") in an overlap exclusion constraint — while avoiding
    substring false positives such as "end" inside "weekend".
    """
    return re.search(rf"\b{re.escape(column)}\b", definition) is not None


def find_disruptive_constraints(
    conn,
    schema: str,
    table_columns: dict[str, tuple[str, ...]],
) -> list[dict]:
    """Find CHECK and EXCLUDE constraints that reference a drifted column.

    Detection is based on the full constraint definition text
    (pg_get_constraintdef), not pg_constraint.conkey. conkey lists only
    directly-referenced columns and omits any column used inside an expression,
    so an overlap exclusion constraint such as
    ``EXCLUDE USING gist (..., tsrange("begin", "end") WITH &&)`` has begin/end
    absent from conkey. A conkey-based match would miss it and the drift UPDATE
    would then violate the still-present constraint.
    """
    table_names = list(table_columns.keys())

    with conn.cursor() as c:
        c.execute(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                pg_get_constraintdef(con.oid) AS constraint_def,
                con.contype AS constraint_type
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND con.contype IN ('c', 'x')
              AND c.relname = ANY(%s)
            """,
            (schema, table_names),
        )
        rows = c.fetchall()

    constraints: list[dict] = []
    for schema_name, table_name, constraint_name, constraint_def, constraint_type in rows:
        drifted_columns = table_columns.get(table_name, ())
        if any(_definition_references_column(constraint_def, col) for col in drifted_columns):
            constraints.append(
                {
                    "schema": schema_name,
                    "table": table_name,
                    "name": constraint_name,
                    "definition": constraint_def,
                    "type": constraint_type,
                }
            )
    return constraints


def drop_constraints(conn, constraints: list[dict]) -> None:
    """Drop the given constraints. Commits after all drops."""
    for c_def in constraints:
        qualified = f'"{c_def["schema"]}"."{c_def["table"]}"'
        logging.info(
            "Dropping constraint %s on %s (%s)",
            c_def["name"],
            qualified,
            c_def["type"],
        )
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {qualified} DROP CONSTRAINT "{c_def["name"]}"')
    conn.commit()


def recreate_constraints(conn, constraints: list[dict]) -> None:
    """Recreate the given constraints. Single commit at end."""
    for c_def in constraints:
        qualified = f'"{c_def["schema"]}"."{c_def["table"]}"'
        logging.info("Restoring constraint %s on %s", c_def["name"], qualified)
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {qualified} ADD CONSTRAINT "{c_def["name"]}" {c_def["definition"]}')
    conn.commit()


def _drift_table_worker(
    conn_params: dict,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    delta_days: int,
) -> int:
    """Open a dedicated connection, configure session, drift one table, close."""
    conn = psycopg2.connect(**conn_params)
    try:
        configure_session(conn)
        return drift_table(conn, schema, table, columns, delta_days)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def apply_drift(
    conn,
    conn_params: dict,
    policy: DriftPolicy,
    delta_days: int,
    *,
    max_workers: int | None = None,
    batch_timeout: int | None = None,
) -> DriftResult:
    """Orchestrate: find/drop constraints → parallel drift → recreate.

    Raises RuntimeError if any table drift fails — constraints are NOT
    recreated on partial failure to avoid data-integrity issues.
    Raises ValueError if delta_days <= 0.
    """
    if delta_days <= 0:
        raise ValueError(f"delta_days must be positive, got {delta_days}")

    if max_workers is None:
        max_workers = int(os.environ.get("DRIFT_MAX_WORKERS", "8"))
    if batch_timeout is None:
        batch_timeout = int(os.environ.get("DRIFT_BATCH_TIMEOUT_SECONDS", "3600"))

    # Find and drop disruptive constraints
    table_columns = {table: columns for table, columns in policy.tables}
    disruptive = find_disruptive_constraints(conn, policy.schema_name, table_columns)
    if disruptive:
        logging.info("Found %d disruptive constraints to drop", len(disruptive))
        drop_constraints(conn, disruptive)
    else:
        logging.info("No disruptive constraints found")

    # Parallel drift
    total_rows = 0
    tables_drifted = 0
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _drift_table_worker,
                conn_params,
                policy.schema_name,
                table,
                columns,
                delta_days,
            ): table
            for table, columns in policy.tables
        }
        for future in as_completed(futures, timeout=batch_timeout):
            table = futures[future]
            try:
                row_count = future.result()
                total_rows += row_count
                tables_drifted += 1
                logging.info("Drifted %s: %d rows", table, row_count)
            except Exception as exc:
                logging.error("Error drifting %s: %s", table, exc)
                errors.append(table)

    if errors:
        raise RuntimeError(
            f"Drift failed for {len(errors)} table(s): {', '.join(errors)}; "
            f"{tables_drifted} succeeded. Constraints left dropped."
        )

    # Recreate constraints
    if disruptive:
        logging.info("Restoring %d constraints", len(disruptive))
        recreate_constraints(conn, disruptive)

    return DriftResult(
        tables_drifted=tables_drifted,
        total_rows=total_rows,
        constraints_dropped=len(disruptive),
        constraints_recreated=len(disruptive),
    )
