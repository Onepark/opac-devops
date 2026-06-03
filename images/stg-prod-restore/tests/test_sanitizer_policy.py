import os

from restore_tooling.sanitizer_policy import (
    ColumnRule,
    SanitizationPolicy,
    TablePolicy,
    load_policy,
    known_strategies,
)


def test_known_strategies():
    s = known_strategies()
    assert "email" in s
    assert "phone_fr" in s
    assert "first_name" in s
    assert "last_name" in s
    assert "company_name" in s
    assert "license_plate_fr" in s
    assert "iban_fr" in s
    assert "national_identifier_fr" in s
    assert "text_token" in s
    assert len(s) == 9


def test_bundled_policy_loads():
    policy = load_policy()
    assert isinstance(policy, SanitizationPolicy)
    assert policy.version == 1
    assert policy.schema_name == "public"
    assert len(policy.tables) >= 8


def test_bundled_policy_has_known_tables():
    policy = load_policy()
    names = {t.name for t in policy.tables}
    expected = {
        "access_ways", "customers", "entities", "entity_settings",
        "installation_logs", "invoices", "parkings", "partners", "users",
    }
    assert names == expected


def test_bundled_policy_customers_has_email_unique():
    policy = load_policy()
    customers = policy.find_table("customers")
    assert customers is not None
    cols = dict(customers.columns)
    assert cols["email"].strategy == "email"
    assert cols["email"].unique is True
    assert cols["firstname"].strategy == "first_name"
    assert cols["lastname"].strategy == "last_name"
    assert cols["phone"].strategy == "phone_fr"


def test_bundled_policy_access_ways_conditional():
    policy = load_policy()
    aw = policy.find_table("access_ways")
    assert aw is not None
    cols = dict(aw.columns)
    rule = cols["desc"]
    assert rule.is_conditional()
    assert len(rule.strategy_by_condition) == 2
    assert rule.strategy_by_condition[0].strategy == "license_plate_fr"
    assert rule.strategy_by_condition[1].strategy == "phone_fr"


def test_bundled_policy_entities_jsonb():
    policy = load_policy()
    entities = policy.find_table("entities")
    assert entities is not None
    cols = dict(entities.columns)
    rule = cols["contact_info"]
    assert rule.is_jsonb()
    keys = dict(rule.jsonb_keys)
    assert keys["email"].strategy == "email"
    assert keys["phone"].strategy == "phone_fr"


def test_bundled_policy_invoices_both_normal_and_jsonb():
    policy = load_policy()
    inv = policy.find_table("invoices")
    assert inv is not None
    cols = dict(inv.columns)
    assert cols["customer_email"].strategy == "email"
    assert cols["customer_firstname"].strategy == "first_name"
    assert cols["customer_lastname"].strategy == "last_name"
    assert cols["pdf_creation_data"].is_jsonb()
    pdf_keys = dict(cols["pdf_creation_data"].jsonb_keys)
    assert "customer_email" in pdf_keys
    assert "customer_firstname" in pdf_keys
    assert "customer_lastname" in pdf_keys


def test_bundled_policy_partners():
    policy = load_policy()
    partners = policy.find_table("partners")
    assert partners is not None
    cols = dict(partners.columns)
    assert cols["name"].strategy == "company_name"
    assert cols["iban"].strategy == "iban_fr"
    assert cols["national_identifier"].strategy == "national_identifier_fr"
    assert cols["email_for_commission_invoices"].strategy == "email"


def test_bundled_policy_users_email_unique():
    policy = load_policy()
    users = policy.find_table("users")
    assert users is not None
    assert dict(users.columns)["email"].unique is True


def test_bundled_policy_batching_on_large_tables():
    policy = load_policy()
    batched = {t.name: t.batch for t in policy.tables if t.batch.enabled}
    for name in ("access_ways", "customers", "entities", "installation_logs", "invoices", "users"):
        assert name in batched, f"{name} should have batching enabled"
        assert batched[name].key == "id"
        assert batched[name].size > 0
    for name in ("entity_settings", "parkings", "partners"):
        assert name not in batched or not batched[name].enabled, f"{name} should not have batching enabled"


def test_bundled_policy_defaults_batch_disabled():
    policy = load_policy()
    assert policy.defaults_batch.enabled is False
    assert policy.defaults_batch.size == 10000


def test_custom_policy_loads(tmp_path):
    path = tmp_path / "test_policy.yaml"
    path.write_text("""
version: 1
defaults:
  schema: custom_schema
  batch:
    enabled: true
    size: 500
tables:
  my_table:
    columns:
      col1:
        strategy: email
        unique: true
      col2:
        strategy: phone_fr
""")
    policy = load_policy(str(path))
    assert policy.schema_name == "custom_schema"
    assert policy.defaults_batch.enabled is True
    assert policy.defaults_batch.size == 500
    t = policy.find_table("my_table")
    assert t is not None
    cols = dict(t.columns)
    assert cols["col1"].strategy == "email"
    assert cols["col1"].unique is True
    assert cols["col2"].strategy == "phone_fr"


def test_custom_policy_jsonb(tmp_path):
    path = tmp_path / "test_policy.yaml"
    path.write_text("""
version: 1
tables:
  t:
    columns:
      data:
        type: jsonb
        keys:
          email: { strategy: email }
          phone: { strategy: phone_fr }
""")
    policy = load_policy(str(path))
    t = policy.find_table("t")
    rule = dict(t.columns)["data"]
    assert rule.is_jsonb()
    keys = dict(rule.jsonb_keys)
    assert keys["email"].strategy == "email"
    assert keys["phone"].strategy == "phone_fr"


def test_custom_policy_conditional(tmp_path):
    path = tmp_path / "test_policy.yaml"
    path.write_text("""
version: 1
tables:
  t:
    columns:
      col:
        strategy_by_condition:
          - where: "type = 'a'"
            strategy: email
          - where: "type = 'b'"
            strategy: phone_fr
""")
    policy = load_policy(str(path))
    t = policy.find_table("t")
    rule = dict(t.columns)["col"]
    assert rule.is_conditional()
    assert len(rule.strategy_by_condition) == 2


def test_invalid_column_no_strategy(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  t:
    columns:
      col: {}
""")
    import pytest
    with pytest.raises(ValueError, match="no strategy"):
        load_policy(str(path))


def test_invalid_column_unknown_strategy(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("""
version: 1
tables:
  t:
    columns:
      col:
        strategy: unknown_something
""")
    # This should load without error at the policy level;
    # strategy validation is the caller's responsibility.
    policy = load_policy(str(path))
    t = policy.find_table("t")
    assert dict(t.columns)["col"].strategy == "unknown_something"