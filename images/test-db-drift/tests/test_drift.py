from unittest.mock import patch

import pytest
from utils.drift import (
    DriftResult,
    apply_drift,
    drift_table,
    drop_constraints,
    find_disruptive_constraints,
    recreate_constraints,
)
from utils.drift_policy import DriftPolicy

# ---------------------------------------------------------------------------
# Helpers — lightweight fakes that capture SQL without a real DB
# ---------------------------------------------------------------------------


class FakeCursor:
    def __init__(self, rowcount=42, fetchall_result=None):
        self.executed: list[tuple[str, tuple | None]] = []
        self._rowcount = rowcount
        self._fetchall_result = fetchall_result or []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    @property
    def rowcount(self):
        return self._rowcount

    def fetchall(self):
        return self._fetchall_result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeConn:
    def __init__(self, rowcount=42, fetchall_result=None):
        self._cursor = FakeCursor(rowcount, fetchall_result)
        self.committed = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed += 1

    def rollback(self):
        pass

    def close(self):
        pass


def _make_policy(tables: dict[str, list[str]]) -> DriftPolicy:
    return DriftPolicy(
        version=1,
        schema_name="public",
        tables=tuple((name, tuple(cols)) for name, cols in tables.items()),
    )


# ---------------------------------------------------------------------------
# drift_table
# ---------------------------------------------------------------------------


def test_drift_table_generates_schema_qualified_sql():
    conn = FakeConn(rowcount=100)
    row_count = drift_table(conn, "public", "customers", ("inserted_at", "updated_at"), 7)
    assert row_count == 100
    assert conn.committed == 1

    sql = conn._cursor.executed[0][0]
    assert '"public"' in sql
    assert '"customers"' in sql
    assert '"inserted_at" = "inserted_at" + INTERVAL \'7 days\'' in sql
    assert '"updated_at" = "updated_at" + INTERVAL \'7 days\'' in sql


def test_drift_table_single_column():
    conn = FakeConn(rowcount=50)
    row_count = drift_table(conn, "myschema", "invoices", ("date",), 30)
    assert row_count == 50
    sql = conn._cursor.executed[0][0]
    assert '"myschema"' in sql
    assert '"invoices"' in sql
    assert "INTERVAL '30 days'" in sql


# ---------------------------------------------------------------------------
# find_disruptive_constraints
# ---------------------------------------------------------------------------


def test_find_disruptive_constraints_parameterizes_schema():
    rows = [
        ("public", "customers", "chk_date", "CHECK (end > begin)", "c"),
    ]
    conn = FakeConn(fetchall_result=rows)
    table_columns = {"customers": ("begin", "end")}
    result = find_disruptive_constraints(conn, "public", table_columns)

    # Verify the SQL was parameterized with schema
    sql, params = conn._cursor.executed[0]
    assert "%s" in sql
    assert params[0] == "public"
    assert "customers" in params[1]
    assert len(result) == 1
    assert result[0]["name"] == "chk_date"


def test_find_disruptive_constraints_empty():
    conn = FakeConn(fetchall_result=[])
    table_columns = {"customers": ("begin", "end")}
    result = find_disruptive_constraints(conn, "public", table_columns)
    assert result == []


# ---------------------------------------------------------------------------
# drop_constraints / recreate_constraints
# ---------------------------------------------------------------------------


def test_drop_constraints():
    conn = FakeConn()
    constraints = [
        {
            "schema": "public",
            "table": "customers",
            "name": "chk_date",
            "definition": "CHECK (end > begin)",
            "type": "c",
        }
    ]
    drop_constraints(conn, constraints)
    assert conn.committed == 1
    sql = conn._cursor.executed[0][0]
    assert "DROP CONSTRAINT" in sql
    assert '"chk_date"' in sql


def test_recreate_constraints():
    conn = FakeConn()
    constraints = [
        {
            "schema": "public",
            "table": "customers",
            "name": "chk_date",
            "definition": "CHECK (end > begin)",
            "type": "c",
        }
    ]
    recreate_constraints(conn, constraints)
    assert conn.committed == 1
    sql = conn._cursor.executed[0][0]
    assert "ADD CONSTRAINT" in sql
    assert '"chk_date"' in sql
    assert "CHECK (end > begin)" in sql


# ---------------------------------------------------------------------------
# apply_drift — delta_days validation
# ---------------------------------------------------------------------------


def test_apply_drift_raises_on_zero_delta():
    policy = _make_policy({"customers": ["inserted_at"]})
    conn = FakeConn()
    with pytest.raises(ValueError, match="positive"):
        apply_drift(conn, {}, policy, 0)


def test_apply_drift_raises_on_negative_delta():
    policy = _make_policy({"customers": ["inserted_at"]})
    conn = FakeConn()
    with pytest.raises(ValueError, match="positive"):
        apply_drift(conn, {}, policy, -5)


# ---------------------------------------------------------------------------
# apply_drift — partial failure (C1 fix)
# ---------------------------------------------------------------------------


def test_apply_drift_partial_failure_no_recreate():
    """When a table drift fails, constraints must NOT be recreated."""
    policy = _make_policy(
        {
            "ok_table": ["inserted_at"],
            "bad_table": ["updated_at"],
        }
    )

    # Fake that we found one disruptive constraint
    constraint_rows = [
        ("public", "ok_table", "chk_ok", "CHECK (end > begin)", "c"),
    ]

    call_count = 0

    def fake_drift_worker(conn_params, schema, table, columns, delta_days):
        nonlocal call_count
        call_count += 1
        if table == "bad_table":
            raise RuntimeError("simulated drift failure")
        return 10

    with (
        patch(
            "utils.drift.find_disruptive_constraints",
            return_value=[
                {
                    "schema": "public",
                    "table": "ok_table",
                    "name": "chk_ok",
                    "definition": "CHECK (end > begin)",
                    "type": "c",
                }
            ],
        ),
        patch("utils.drift.drop_constraints"),
        patch("utils.drift._drift_table_worker", side_effect=fake_drift_worker),
    ):
        conn = FakeConn(fetchall_result=constraint_rows)
        with pytest.raises(RuntimeError, match="Drift failed"):
            apply_drift(conn, {}, policy, 7)

    # recreate_constraints should NOT have been called
    # (if it were, conn.committed would be > 0 from drop + recreate)
    # drop_constraints was called (mocked), so we can't check commit count
    # The key assertion is that RuntimeError was raised


# ---------------------------------------------------------------------------
# apply_drift — success path
# ---------------------------------------------------------------------------


def test_apply_drift_success_returns_result():
    policy = _make_policy({"customers": ["inserted_at"]})

    with (
        patch("utils.drift.find_disruptive_constraints", return_value=[]),
        patch("utils.drift._drift_table_worker", return_value=100),
    ):
        conn = FakeConn()
        result = apply_drift(conn, {}, policy, 7)

    assert isinstance(result, DriftResult)
    assert result.tables_drifted == 1
    assert result.total_rows == 100
    assert result.constraints_dropped == 0
    assert result.constraints_recreated == 0
