from __future__ import annotations

import dataclasses
import logging
from typing import Any

from .sanitizer_policy import SanitizationPolicy
from .sanitizer_sql import verification_sql


@dataclasses.dataclass(frozen=True)
class VerificationResult:
    target: str
    checked: int
    failed: int


@dataclasses.dataclass(frozen=True)
class VerificationReport:
    passed: bool
    results: tuple[VerificationResult, ...]


def run_verification(
    cursor: Any,
    policy: SanitizationPolicy,
    mode: str = "fail",
) -> VerificationReport:
    """Run post-sanitization verification for all configured columns."""
    if mode not in {"fail", "warn"}:
        raise ValueError("mode must be 'fail' or 'warn'")

    results: list[VerificationResult] = []

    for table in policy.tables:
        for col_name, rule in table.columns:
            sql_statements = verification_sql(policy, table, col_name, rule)
            for stmt in sql_statements:
                cursor.execute(stmt)
                row = cursor.fetchone()
                if row:
                    target = row[0]
                    checked = row[1] or 0
                    failed = row[2] or 0
                    results.append(
                        VerificationResult(
                            target=target, checked=checked, failed=failed
                        )
                    )

    failed_results = [r for r in results if r.failed > 0]
    if failed_results:
        for r in failed_results:
            log = logging.error if mode == "fail" else logging.warning
            log(
                "Verification failed for %s: %d/%d rows failed",
                r.target,
                r.failed,
                r.checked,
            )

    return VerificationReport(
        passed=mode == "warn" or len(failed_results) == 0,
        results=tuple(results),
    )
