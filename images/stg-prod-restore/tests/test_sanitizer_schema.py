import pytest
from restore_tooling.sanitizer_schema import (
    SchemaReport,
    SchemaIssue,
    run_preflight,
    _check_uncovered,
)


def test_schema_issue_dataclass():
    issue = SchemaIssue("error", "test message")
    assert issue.severity == "error"
    assert issue.message == "test message"


def test_schema_report_equality():
    r1 = SchemaReport(passed=True, issues=(), unique_columns=(), suspicious_uncovered=())
    r2 = SchemaReport(passed=True, issues=(), unique_columns=(), suspicious_uncovered=())
    assert r1 == r2


def test_schema_report_with_issues():
    issue = SchemaIssue("error", "oops")
    r = SchemaReport(
        passed=False,
        issues=(issue,),
        unique_columns=(),
        suspicious_uncovered=(),
    )
    assert not r.passed
    assert len(r.issues) == 1


# Mock cursor for unit-testing _check_uncovered without DB
class MockCursor:
    def __init__(self, schema_columns):
        self._schema_columns = schema_columns

    def execute(self, query, params=None):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class MockCursorWithUnique:
    def __init__(self, schema_columns, unique_constraints):
        self._schema_columns = schema_columns
        self._unique_constraints = unique_constraints
        self._call = 0

    def execute(self, query, params=None):
        self._call += 1

    def fetchone(self):
        return None

    def fetchall(self):
        if self._call == 1:
            # Return schema columns
            return [
                (t, c, dt)
                for t, cols in self._schema_columns.items()
                for c, dt in cols.items()
            ]
        # Return unique constraints
        return [
            (t, c)
            for t, cols in self._unique_constraints.items()
            for c in cols
        ]


def test_check_uncovered_simple():
    from restore_tooling.sanitizer_policy import load_policy

    policy = load_policy()
    schema = {"customers": {"email": "text", "phone": "text", "firstname": "text", "lastname": "text", "id": "integer"}}
    # All suspicious columns are covered by policy
    result = _check_uncovered(schema, policy)
    assert len(result) == 0


def test_check_uncovered_finds_email():
    from restore_tooling.sanitizer_policy import SanitizationPolicy, TablePolicy, ColumnRule, BatchConfig

    policy = SanitizationPolicy(
        version=1,
        schema_name="public",
        defaults_batch=BatchConfig(),
        tables=(
            TablePolicy(
                name="test_table",
                batch=BatchConfig(),
                columns=(
                    ("id", ColumnRule(strategy="text_token")),
                ),
            ),
        ),
    )
    schema = {
        "test_table": {
            "id": "integer",
            "email": "text",  # uncovered suspicious column
            "phone": "text",  # uncovered suspicious column
        }
    }
    result = _check_uncovered(schema, policy)
    found = {c for _, c in result}
    assert "email" in found
    assert "phone" in found


def test_check_uncovered_technical_columns_ignored():
    from restore_tooling.sanitizer_policy import SanitizationPolicy, TablePolicy, ColumnRule, BatchConfig

    policy = SanitizationPolicy(
        version=1,
        schema_name="public",
        defaults_batch=BatchConfig(),
        tables=(),
    )
    schema = {
        "t": {
            "id": "integer",
            "created_at": "timestamp",
            "updated_at": "timestamp",
            "deleted_at": "timestamp",
            "uuid": "uuid",
            "status": "text",
        }
    }
    result = _check_uncovered(schema, policy)
    assert len(result) == 0


def test_preflight_missing_pgcrypto():
    from restore_tooling.sanitizer_policy import load_policy
    policy = load_policy()
    cursor = MockCursor({
        "customers": {"email": "text"},
        "users": {"email": "text"},
    })
    report = run_preflight(cursor, policy, uncovered_pii_mode="warn")
    assert not report.passed
    # Should have pgcrypto error
    pgcrypto_errors = [i for i in report.issues if "pgcrypto" in i.message]
    assert len(pgcrypto_errors) > 0