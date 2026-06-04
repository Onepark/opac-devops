from __future__ import annotations

import dataclasses
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .sanitizer_policy import SanitizationPolicy, TablePolicy
from .sanitizer_sql import (
    batch_bounds_query,
    collision_check_sql,
    generate_batched_update_sql,
    generate_update_sql,
    install_helpers_sql,
    verify_helpers_sql,
)


@dataclasses.dataclass(frozen=True)
class TableExecutionResult:
    table: str
    updated_rows: int
    batches: int
    duration_seconds: float
    status: str
    error: str | None = None


@dataclasses.dataclass(frozen=True)
class ExecutionReport:
    passed: bool
    tables: tuple[TableExecutionResult, ...]
    duration_seconds: float


@dataclasses.dataclass(frozen=True)
class CollisionCheckResult:
    table: str
    column: str
    generated_value: str
    row_count: int


def run_collision_checks(
    cursor: Any,
    policy: SanitizationPolicy,
    salt: str,
    unique_columns: tuple[tuple[str, str], ...] = (),
) -> tuple[CollisionCheckResult, ...]:
    """Detect generated-value collisions for configured or discovered unique columns."""
    discovered_unique = set(unique_columns)
    collisions: list[CollisionCheckResult] = []

    for table in policy.tables:
        for col_name, rule in table.columns:
            should_check = rule.unique or (table.name, col_name) in discovered_unique
            if not should_check:
                continue
            sql = collision_check_sql(policy, table, col_name, rule)
            if sql is None:
                continue
            cursor.execute(sql, {"salt": salt})
            collision = cursor.fetchone()
            if collision:
                collisions.append(
                    CollisionCheckResult(
                        table=table.name,
                        column=col_name,
                        generated_value=collision[0],
                        row_count=collision[1],
                    )
                )

    return tuple(collisions)


def _execute_table(
    conn_factory: Any,
    policy: SanitizationPolicy,
    table: TablePolicy,
    salt: str,
) -> TableExecutionResult:
    """Execute all sanitization rules for a single table."""
    start = time.monotonic()
    total_rows = 0
    total_batches = 0

    try:
        with conn_factory() as conn:
            with conn.cursor() as cursor:
                if table.batch.enabled:
                    # Get batch bounds
                    cursor.execute(batch_bounds_query(policy, table))
                    row = cursor.fetchone()
                    if row is None or row[0] is None or row[1] is None:
                        logging.info("Table %s is empty, skipping", table.name)
                        return _empty_result(table, start)
                    lo, hi = row[0], row[1]
                    batch_size = table.batch.size

                    current_lo = lo
                    while current_lo <= hi:
                        current_hi = min(current_lo + batch_size - 1, hi)
                        batch_start = time.monotonic()
                        statements = generate_batched_update_sql(
                            policy, table, current_lo, current_hi
                        )
                        try:
                            batch_rows = 0
                            for stmt in statements:
                                cursor.execute(
                                    stmt,
                                    {"salt": salt, "lo": current_lo, "hi": current_hi},
                                )
                                batch_rows += cursor.rowcount
                            total_rows += batch_rows
                            conn.commit()
                            total_batches += 1
                            logging.info(
                                "  Batch %d-%d for %s: %d rows (%ds)",
                                current_lo,
                                current_hi,
                                table.name,
                                batch_rows,
                                time.monotonic() - batch_start,
                            )
                        except Exception:
                            conn.rollback()
                            raise
                        current_lo = current_hi + 1
                else:
                    statements = generate_update_sql(policy, table)
                    for stmt in statements:
                        cursor.execute(stmt, {"salt": salt})
                        total_rows += cursor.rowcount
                    conn.commit()
                    total_batches = 1

        duration = time.monotonic() - start
        logging.info(
            "Table %s: %d rows in %d batches (%ds)",
            table.name,
            total_rows,
            total_batches,
            duration,
        )
        return TableExecutionResult(
            table=table.name,
            updated_rows=total_rows,
            batches=total_batches,
            duration_seconds=duration,
            status="passed",
        )
    except Exception as exc:
        duration = time.monotonic() - start
        logging.exception("Table %s execution failed", table.name)
        return TableExecutionResult(
            table=table.name,
            updated_rows=total_rows,
            batches=total_batches,
            duration_seconds=duration,
            status="failed",
            error=str(exc),
        )


def _empty_result(table: TablePolicy, start: float) -> TableExecutionResult:
    return TableExecutionResult(
        table=table.name,
        updated_rows=0,
        batches=0,
        duration_seconds=time.monotonic() - start,
        status="passed",
    )


def run_execution(
    conn_factory: Any,
    policy: SanitizationPolicy,
    salt: str,
    max_workers: int = 4,
    unique_columns: tuple[tuple[str, str], ...] = (),
) -> ExecutionReport:
    """Run sanitization for all tables in parallel."""
    start = time.monotonic()

    # Install helpers first (single connection)
    with conn_factory() as conn:
        with conn.cursor() as cursor:
            cursor.execute(install_helpers_sql())
            cursor.execute(verify_helpers_sql())
        conn.commit()
        logging.info("Helper functions installed in schema restore_sanitizer")

    # Collision checks
    failed_collision: list[str] = []
    logging.info("Running collision pre-checks...")
    with conn_factory() as conn:
        with conn.cursor() as cursor:
            collisions = run_collision_checks(cursor, policy, salt, unique_columns)
            for collision in collisions:
                failed_collision.append(f"{collision.table}.{collision.column}")
                logging.error(
                    "Collision detected for %s.%s: generated value '%s' has %d rows",
                    collision.table,
                    collision.column,
                    collision.generated_value,
                    collision.row_count,
                )
    if failed_collision:
        raise RuntimeError(
            f"Collision(s) detected for unique columns: {', '.join(failed_collision)}"
        )
    logging.info("Collision checks passed")

    # Run tables in parallel
    executor_max = max(1, max_workers)
    results: list[TableExecutionResult] = []
    executor = ThreadPoolExecutor(max_workers=executor_max)
    try:
        futures = {
            executor.submit(
                _execute_table, conn_factory, policy, table, salt
            ): table.name
            for table in policy.tables
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            # Fail-fast: one table failing previously left the remaining workers
            # grinding for hours while Step Functions polled a "RUNNING" task with
            # no logs. Abort the run as soon as any table fails.
            if result.status == "failed":
                logging.error(
                    "Table %s failed; cancelling remaining sanitization tasks",
                    result.table,
                )
                for pending in futures:
                    pending.cancel()
                break
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    duration = time.monotonic() - start
    failed = [r for r in results if r.status == "failed"]
    passed = len(failed) == 0

    return ExecutionReport(
        passed=passed,
        tables=tuple(sorted(results, key=lambda r: r.table)),
        duration_seconds=duration,
    )
