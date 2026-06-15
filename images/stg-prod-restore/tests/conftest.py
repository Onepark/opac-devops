import pytest


@pytest.fixture
def policy_path(tmp_path):
    """Return a temporary directory path that can hold a custom policy file."""
    return tmp_path / "policy.yaml"
