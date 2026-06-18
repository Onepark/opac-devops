import pytest
from utils.drift_policy import DriftPolicy, load_policy


def test_bundled_policy_loads():
    policy = load_policy()
    assert isinstance(policy, DriftPolicy)
    assert policy.version == 1
    assert policy.schema_name == "public"
    assert len(policy.tables) == 23


def test_bundled_policy_has_known_tables():
    policy = load_policy()
    names = {t[0] for t in policy.tables}
    assert "customers" in names
    assert "parkings" in names
    assert "invoices" in names
    assert "payments" in names
    assert "devices" in names


def test_bundled_policy_all_drifted_columns():
    policy = load_policy()
    cols = policy.all_drifted_columns()
    assert "inserted_at" in cols
    assert "updated_at" in cols
    assert "date" in cols
    assert "begin" in cols
    assert "end" in cols
    assert len(cols) == 14


def test_bundled_policy_find_table():
    policy = load_policy()
    customers = policy.find_table("customers")
    assert customers is not None
    assert customers[0] == "customers"
    assert "inserted_at" in customers[1]
    assert "updated_at" in customers[1]


def test_bundled_policy_find_table_not_found():
    policy = load_policy()
    assert policy.find_table("nonexistent") is None


def test_custom_policy_loads(tmp_path):
    path = tmp_path / "test_policy.yaml"
    path.write_text("""
version: 2
schema: custom_schema
tables:
  my_table:
    columns:
      - created_at
      - updated_at
  other_table:
    columns:
      - date
""")
    policy = load_policy(str(path))
    assert policy.version == 2
    assert policy.schema_name == "custom_schema"
    assert len(policy.tables) == 2
    t = policy.find_table("my_table")
    assert t is not None
    assert t[1] == ("created_at", "updated_at")


def test_custom_policy_empty_columns_raises(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  my_table:
    columns: []
""")
    with pytest.raises(ValueError, match="empty"):
        load_policy(str(path))


def test_custom_policy_bad_tables_type_raises(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  - not_a_mapping
""")
    with pytest.raises(ValueError, match="mapping"):
        load_policy(str(path))


def test_custom_policy_bad_table_def_raises(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  my_table: "not_a_dict"
""")
    with pytest.raises(ValueError, match="Invalid table definition"):
        load_policy(str(path))


def test_custom_policy_columns_not_list_raises(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  my_table:
    columns: "not_a_list"
""")
    with pytest.raises(ValueError, match="must be a list"):
        load_policy(str(path))
