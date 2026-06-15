from utils.drift_policy import DriftPolicy, run_preflight, VALID_DATE_TYPES


class MockCursor:
    """Simulates information_schema queries for preflight testing.

    Parameterizes schema_tables as {table_name: {column_name: data_type}}.
    """

    def __init__(self, schema_tables: dict[str, dict[str, str]]):
        self._schema_tables = schema_tables
        self._last_query = None
        self._last_params = None

    def execute(self, query, params=None):
        self._last_query = query
        self._last_params = params

    def fetchone(self):
        if "information_schema.tables" in self._last_query:
            table_name = self._last_params[1]
            if table_name in self._schema_tables:
                return (1,)
            return None
        return None

    def fetchall(self):
        if "information_schema.columns" in self._last_query:
            table_name = self._last_params[1]
            if table_name in self._schema_tables:
                return [
                    (col_name, col_type)
                    for col_name, col_type in self._schema_tables[table_name].items()
                ]
        return []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _make_policy(tables: dict[str, list[str]]) -> DriftPolicy:
    """Build a minimal DriftPolicy from {table_name: [columns]}."""
    return DriftPolicy(
        version=1,
        schema_name="public",
        tables=tuple((name, tuple(cols)) for name, cols in tables.items()),
    )


def test_preflight_all_ok():
    policy = _make_policy(
        {
            "customers": ["inserted_at", "updated_at"],
            "invoices": ["date"],
        }
    )
    schema = {
        "customers": {
            "inserted_at": "timestamp without time zone",
            "updated_at": "timestamp without time zone",
        },
        "invoices": {
            "date": "date",
        },
    }
    cursor = MockCursor(schema)
    errors = run_preflight(cursor, policy)
    assert errors == []


def test_preflight_missing_table():
    policy = _make_policy(
        {
            "customers": ["inserted_at"],
            "nonexistent": ["date"],
        }
    )
    schema = {
        "customers": {
            "inserted_at": "timestamp without time zone",
        },
    }
    cursor = MockCursor(schema)
    errors = run_preflight(cursor, policy)
    assert len(errors) == 1
    assert "nonexistent" in errors[0]
    assert "not found" in errors[0]


def test_preflight_missing_column():
    policy = _make_policy(
        {
            "customers": ["inserted_at", "missing_col"],
        }
    )
    schema = {
        "customers": {
            "inserted_at": "timestamp without time zone",
        },
    }
    cursor = MockCursor(schema)
    errors = run_preflight(cursor, policy)
    assert len(errors) == 1
    assert "missing_col" in errors[0]
    assert "not found" in errors[0]


def test_preflight_wrong_column_type():
    policy = _make_policy(
        {
            "customers": ["inserted_at", "status"],
        }
    )
    schema = {
        "customers": {
            "inserted_at": "timestamp without time zone",
            "status": "text",
        },
    }
    cursor = MockCursor(schema)
    errors = run_preflight(cursor, policy)
    assert len(errors) == 1
    assert "status" in errors[0]
    assert "text" in errors[0]
    assert "date/timestamp" in errors[0]


def test_preflight_multiple_errors():
    policy = _make_policy(
        {
            "missing_table": ["date"],
            "customers": ["inserted_at", "bad_col"],
        }
    )
    schema = {
        "customers": {
            "inserted_at": "timestamp without time zone",
        },
    }
    cursor = MockCursor(schema)
    errors = run_preflight(cursor, policy)
    assert len(errors) == 2


def test_valid_date_types():
    """Verify VALID_DATE_TYPES contains the expected PostgreSQL types."""
    assert "date" in VALID_DATE_TYPES
    assert "timestamp without time zone" in VALID_DATE_TYPES
    assert "timestamp with time zone" in VALID_DATE_TYPES
    assert "text" not in VALID_DATE_TYPES
    assert "integer" not in VALID_DATE_TYPES
