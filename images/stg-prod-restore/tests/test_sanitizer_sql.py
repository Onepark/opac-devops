from restore_tooling.sanitizer_policy import load_policy
from restore_tooling.sanitizer_sql import (
    column_update_expr,
    generate_update_sql,
    install_helpers_sql,
    collision_check_sql,
    verify_helpers_sql,
)


def test_install_helpers_contains_all_functions():
    sql = install_helpers_sql()
    assert "anon_email" in sql
    assert "anon_phone_fr" in sql
    assert "anon_first_name" in sql
    assert "anon_last_name" in sql
    assert "anon_company_name" in sql
    assert "anon_license_plate_fr" in sql
    assert "anon_iban_fr" in sql
    assert "anon_national_identifier_fr" in sql
    assert "anon_text_token" in sql
    assert "CREATE SCHEMA IF NOT EXISTS restore_sanitizer" in sql
    assert (
        "CREATE SCHEMA IF NOT EXISTS restore_sanitizer;\nCREATE OR REPLACE FUNCTION"
        in sql
    )


def test_verify_helpers_contains_all():
    sql = verify_helpers_sql()
    assert "verify_email" in sql
    assert "verify_phone_fr" in sql
    assert "verify_first_name" in sql
    assert "verify_last_name" in sql
    assert "verify_company_name" in sql
    assert "verify_license_plate_fr" in sql
    assert "verify_iban_fr" in sql
    assert "verify_national_identifier_fr" in sql
    assert "verify_text_token" in sql


def test_column_update_expr_email():
    expr = column_update_expr("email", "email")
    assert "email" in expr
    assert "anon_email" in expr
    assert "%(salt)s" in expr


def test_column_update_expr_phone():
    expr = column_update_expr("phone", "phone_fr")
    assert "phone" in expr
    assert "anon_phone_fr" in expr


def test_column_update_uses_identifier_quoting():
    expr = column_update_expr("first_name", "first_name")
    assert '"first_name"' in expr
    assert "anon_first_name" in expr


def test_generate_update_sql_normal_column():
    policy = load_policy()
    customers = policy.find_table("customers")
    assert customers is not None
    updates = generate_update_sql(policy, customers)
    # Normal columns are collapsed into a single UPDATE statement
    assert len(updates) == 1
    stmt = updates[0]
    assert stmt.startswith("UPDATE")
    assert "public" in stmt or ".customers" in stmt
    assert "%(salt)s" in stmt
    for col in ('"email"', '"firstname"', '"lastname"', '"phone"'):
        assert col in stmt


def test_generate_update_sql_jsonb():
    policy = load_policy()
    entities = policy.find_table("entities")
    assert entities is not None
    updates = generate_update_sql(policy, entities)
    assert len(updates) == 2
    # First statement is the normal column (name)
    assert "anon_company_name" in updates[0]
    # Second statement is the JSONB column (contact_info)
    assert "contact_info" in updates[1]
    assert "jsonb_set" in updates[1]
    assert 'SET "contact_info" = jsonb_set' in updates[1]
    assert "SET jsonb_set" not in updates[1]
    assert "ARRAY['email']" in updates[1]
    assert "ARRAY['phone']" in updates[1]
    assert 'WHERE "contact_info" IS NOT NULL' in updates[1]


def test_generate_update_sql_conditional():
    policy = load_policy()
    aw = policy.find_table("access_ways")
    assert aw is not None
    updates = generate_update_sql(policy, aw)
    assert len(updates) == 2
    for stmt in updates:
        assert "WHERE" in stmt


def test_generate_update_sql_invoices_both():
    policy = load_policy()
    inv = policy.find_table("invoices")
    assert inv is not None
    updates = generate_update_sql(policy, inv)
    # Normal columns collapsed into one UPDATE + one JSONB UPDATE
    assert len(updates) == 2
    jsonb_updates = [u for u in updates if "pdf_creation_data" in u]
    assert len(jsonb_updates) == 1
    assert "jsonb_set" in jsonb_updates[0]
    normal_updates = [u for u in updates if "pdf_creation_data" not in u]
    assert len(normal_updates) == 1
    for col in ("customer_email", "customer_firstname", "customer_lastname"):
        assert col in normal_updates[0]


def test_generate_update_sql_quotes_table_identifier():
    policy = load_policy()
    customers = policy.find_table("customers")
    assert customers is not None
    updates = generate_update_sql(policy, customers)
    # Should use quoted identifier for table
    for stmt in updates:
        # The qualified table reference with public."customers"
        assert '".customers"' not in stmt  # this would mean no quoting
        assert '."customers"' in stmt or '"customers"' in stmt


def test_helper_functions_null_preserving():
    """Helper function SQL should have CASE WHEN value IS NULL THEN NULL."""
    sql = install_helpers_sql()
    # Each function should handle NULL preservation
    assert sql.count("CASE WHEN value IS NULL THEN NULL") >= 9


def test_helper_functions_include_strategy_in_seed():
    """Deterministic seed should include strategy name."""
    sql = install_helpers_sql()
    assert ":email'" in sql or "'email'" in sql or ":email" in sql
    assert ":phone_fr" in sql or "'phone_fr'" in sql


def test_helper_functions_use_bounded_format_chunks():
    sql = install_helpers_sql()
    assert "mod(abs" in sql
    assert ", 1000000000)" in sql  # phone keeps exactly 9 local digits
    assert ", 100000000000000)" in sql  # national_identifier keeps 14 digits
    assert "'FR' ||" in sql
    assert "bit(80)::bigint" not in sql
    assert "ORDER BY" not in sql  # name pools use array indexing, not constant sort


def test_verify_helpers_regex_patterns():
    sql = verify_helpers_sql()
    # Check email regex
    assert "anon_[0-9a-f]{20}@test" in sql
    assert r"^\+336[0-9]{9}$" in sql
    assert r"test\.local" in sql
    assert r"test\\.local" not in sql
    assert "FR[0-9]{25}" in sql
    assert "[A-Z]{2}-[0-9]{3}-[A-Z]{2}" in sql
    assert "Partner [0-9A-F]{8}" in sql
    assert "Anonymized [0-9A-F]{8}" in sql


def test_collision_check_sql_generated_inline_for_unique_column():
    policy = load_policy()
    customers = policy.find_table("customers")
    rule = dict(customers.columns)["email"]
    sql = collision_check_sql(policy, customers, "email", rule)
    assert sql is not None
    assert "anon_email" not in sql
    assert "GROUP BY generated HAVING COUNT(DISTINCT" in sql
    assert "%(salt)s" in sql
