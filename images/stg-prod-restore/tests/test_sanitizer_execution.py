from restore_tooling.sanitizer_execution import run_collision_checks, run_execution
from restore_tooling.sanitizer_policy import BatchConfig, SanitizationPolicy, load_policy


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class FakeCursor:
    rowcount = 0

    def __init__(self, fetchone_values=None):
        self.statements = []
        self.fetchone_values = list(fetchone_values or [])

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        self.statements.append((statement, params))

    def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return None


def test_run_execution_empty_policy_returns_report():
    cursor = FakeCursor()
    policy = SanitizationPolicy(
        version=1,
        schema_name="public",
        defaults_batch=BatchConfig(),
        tables=(),
    )

    report = run_execution(lambda: FakeConnection(cursor), policy, "salt")

    assert report.passed is True
    assert report.tables == ()
    assert any("CREATE SCHEMA IF NOT EXISTS restore_sanitizer;" in s for s, _ in cursor.statements)


def test_run_collision_checks_uses_discovered_unique_columns():
    policy = load_policy()
    partners = policy.find_table("partners")
    assert partners is not None
    assert dict(partners.columns)["iban"].unique is False
    policy = SanitizationPolicy(
        version=policy.version,
        schema_name=policy.schema_name,
        defaults_batch=policy.defaults_batch,
        tables=(partners,),
    )

    cursor = FakeCursor(fetchone_values=[("FR0000000000000000000000000", 2)])

    collisions = run_collision_checks(
        cursor,
        policy,
        "salt",
        unique_columns=(("partners", "iban"),),
    )

    assert len(collisions) == 1
    assert collisions[0].table == "partners"
    assert collisions[0].column == "iban"
    assert cursor.statements
