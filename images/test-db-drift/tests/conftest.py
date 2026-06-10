import pytest


@pytest.fixture
def policy_path(tmp_path):
    """Return a temporary file path that can hold a custom policy YAML."""
    return tmp_path / "policy.yaml"
