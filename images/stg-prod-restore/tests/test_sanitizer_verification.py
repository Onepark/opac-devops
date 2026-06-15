from restore_tooling.sanitizer_policy import load_policy
from restore_tooling.sanitizer_verification import run_verification


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def execute(self, _statement):
        pass

    def fetchone(self):
        return self.row


def test_verification_warn_mode_does_not_fail_report():
    policy = load_policy()
    policy = type(policy)(
        version=policy.version,
        schema_name=policy.schema_name,
        defaults_batch=policy.defaults_batch,
        tables=(policy.find_table("customers"),),
    )

    report = run_verification(
        FakeCursor(("customers.email", 10, 1)), policy, mode="warn"
    )

    assert report.passed is True
    assert report.results[0].failed == 1


def test_verification_fail_mode_fails_report():
    policy = load_policy()
    policy = type(policy)(
        version=policy.version,
        schema_name=policy.schema_name,
        defaults_batch=policy.defaults_batch,
        tables=(policy.find_table("customers"),),
    )

    report = run_verification(
        FakeCursor(("customers.email", 10, 1)), policy, mode="fail"
    )

    assert report.passed is False
