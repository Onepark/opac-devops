from __future__ import annotations


from .sanitizer_names import first_names, last_names
from .sanitizer_policy import (
    ColumnRule,
    SanitizationPolicy,
    TablePolicy,
)

SCHEMA = "restore_sanitizer"


def _quote_ident(name: str) -> str:
    """Quote a PostgreSQL identifier, escaping embedded double quotes."""
    return f'"{name.replace('"', '""')}"'


def _quote_literal(value: str) -> str:
    """Quote a PostgreSQL literal string, escaping single quotes."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _seed_expr(value_expr: str, salt_expr: str, strategy: str) -> str:
    return f"COALESCE({value_expr} || ':' || {salt_expr} || ':{strategy}', 'null:' || {salt_expr} || ':{strategy}')"


def _digest_hex_expr(seed_expr: str, length: int) -> str:
    return f"left(encode(digest({seed_expr}, 'sha256'), 'hex'), {length})"


def _hash_int_expr(seed_expr: str, byte_count: int) -> str:
    if byte_count > 8:
        raise ValueError("byte_count must fit in a PostgreSQL bigint")
    hex_count = byte_count * 2
    return f"('x' || left(encode(digest({seed_expr}, 'sha256'), 'hex'), {hex_count}))::bit({byte_count * 8})::bigint"


def _positive_mod_expr(value_expr: str, modulus: int) -> str:
    return f"mod(abs({value_expr}), {modulus})"


def _digit_chunk_expr(seed_expr: str, discriminator: str, width: int) -> str:
    h = _hash_int_expr(f"{seed_expr} || {_quote_literal(discriminator)}", 8)
    modulus = 10**width
    return f"lpad(({_positive_mod_expr(h, modulus)})::text, {width}, '0')"


def strategy_value_expr(value_expr: str, salt_expr: str, strategy: str) -> str:
    """Return a SQL expression that anonymizes value_expr with the named strategy."""
    seed = _seed_expr(value_expr, salt_expr, strategy)

    if strategy == "email":
        hex_part = _digest_hex_expr(seed, 20)
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE 'anon_' || {hex_part} || '@test.local' END"

    if strategy == "phone_fr":
        h = _hash_int_expr(seed, 6)
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE '+336' || lpad(({_positive_mod_expr(h, 1_000_000_000)})::text, 9, '0') END"

    if strategy == "first_name":
        pool = first_names()
        h = _hash_int_expr(seed, 4)
        values_sql = ", ".join(_quote_literal(n) for n in pool)
        index = f"(({_positive_mod_expr(h, len(pool))}) + 1)::int"
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE (ARRAY[{values_sql}]::text[])[{index}] END"

    if strategy == "last_name":
        pool = last_names()
        h = _hash_int_expr(seed, 4)
        values_sql = ", ".join(_quote_literal(n) for n in pool)
        index = f"(({_positive_mod_expr(h, len(pool))}) + 1)::int"
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE (ARRAY[{values_sql}]::text[])[{index}] END"

    if strategy == "company_name":
        hex_part = _digest_hex_expr(seed, 8)
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE 'Partner ' || upper({hex_part}) END"

    if strategy == "license_plate_fr":
        h1 = _hash_int_expr(seed + " || '1'", 1)
        h2 = _hash_int_expr(seed + " || '2'", 1)
        num = _hash_int_expr(seed + " || '3'", 2)
        h4 = _hash_int_expr(seed + " || '4'", 1)
        h5 = _hash_int_expr(seed + " || '5'", 1)
        return (
            f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE "
            f"chr((65 + {_positive_mod_expr(h1, 26)})::int) || chr((65 + {_positive_mod_expr(h2, 26)})::int)"
            f" || '-' || lpad(({_positive_mod_expr(num, 1000)})::text, 3, '0')"
            f" || '-' || chr((65 + {_positive_mod_expr(h4, 26)})::int) || chr((65 + {_positive_mod_expr(h5, 26)})::int)"
            f" END"
        )

    if strategy == "iban_fr":
        digits = (
            _digit_chunk_expr(seed, "1", 12)
            + " || "
            + _digit_chunk_expr(seed, "2", 12)
            + " || "
            + _digit_chunk_expr(seed, "3", 1)
        )
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE 'FR' || {digits} END"

    if strategy == "national_identifier_fr":
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE {_digit_chunk_expr(seed, '1', 14)} END"

    if strategy == "text_token":
        hex_part = _digest_hex_expr(seed, 8)
        return f"CASE WHEN {value_expr} IS NULL THEN NULL ELSE 'Anonymized ' || upper({hex_part}) END"

    raise KeyError(f"Unknown strategy: {strategy}")


# --- Helper function generators ---


def fn_email() -> str:
    return _gen_fn("anon_email", strategy_value_expr("value", "salt", "email"))


def fn_phone_fr() -> str:
    return _gen_fn("anon_phone_fr", strategy_value_expr("value", "salt", "phone_fr"))


def fn_first_name() -> str:
    return _gen_fn(
        "anon_first_name", strategy_value_expr("value", "salt", "first_name")
    )


def fn_last_name() -> str:
    return _gen_fn("anon_last_name", strategy_value_expr("value", "salt", "last_name"))


def fn_company_name() -> str:
    return _gen_fn(
        "anon_company_name", strategy_value_expr("value", "salt", "company_name")
    )


def fn_license_plate_fr() -> str:
    return _gen_fn(
        "anon_license_plate_fr",
        strategy_value_expr("value", "salt", "license_plate_fr"),
    )


def fn_iban_fr() -> str:
    return _gen_fn("anon_iban_fr", strategy_value_expr("value", "salt", "iban_fr"))


def fn_national_identifier_fr() -> str:
    return _gen_fn(
        "anon_national_identifier_fr",
        strategy_value_expr("value", "salt", "national_identifier_fr"),
    )


def fn_text_token() -> str:
    return _gen_fn(
        "anon_text_token", strategy_value_expr("value", "salt", "text_token")
    )


def _gen_fn(fn_name: str, body_expr: str) -> str:
    return (
        f"CREATE OR REPLACE FUNCTION {SCHEMA}.{fn_name}(value text, salt text) RETURNS text\n"
        f"LANGUAGE sql IMMUTABLE PARALLEL SAFE\n"
        f"AS $$ SELECT {body_expr} $$;"
    )


# --- Install all helpers ---


def install_helpers_sql() -> str:
    parts = [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};",
        fn_email(),
        fn_phone_fr(),
        fn_first_name(),
        fn_last_name(),
        fn_company_name(),
        fn_license_plate_fr(),
        fn_iban_fr(),
        fn_national_identifier_fr(),
        fn_text_token(),
    ]
    return "\n".join(parts)


# --- Strategy -> function mapping ---

_FN_MAP: dict[str, str] = {
    "email": f"{SCHEMA}.anon_email",
    "phone_fr": f"{SCHEMA}.anon_phone_fr",
    "first_name": f"{SCHEMA}.anon_first_name",
    "last_name": f"{SCHEMA}.anon_last_name",
    "company_name": f"{SCHEMA}.anon_company_name",
    "license_plate_fr": f"{SCHEMA}.anon_license_plate_fr",
    "iban_fr": f"{SCHEMA}.anon_iban_fr",
    "national_identifier_fr": f"{SCHEMA}.anon_national_identifier_fr",
    "text_token": f"{SCHEMA}.anon_text_token",
}


def column_update_expr(column: str, strategy: str, is_array: bool = False) -> str:
    fn = _FN_MAP[strategy]
    quoted = _quote_ident(column)
    if is_array:
        # Apply strategy to each element: ARRAY(SELECT fn(elem, salt) FROM unnest(col))
        return f"{quoted} = ARRAY(SELECT {fn}(elem::text, %(salt)s) FROM unnest({quoted}) AS elem)"
    return f"{quoted} = {fn}({quoted}::text, %(salt)s)"


def jsonb_key_update_expr(jsonb_column: str, jsonb_key: str, strategy: str) -> str:
    fn = _FN_MAP[strategy]
    col_q = _quote_ident(jsonb_column)
    key_lit = _quote_literal(jsonb_key)
    return (
        f"jsonb_set({col_q}, "
        f"ARRAY[{key_lit}], "
        f"to_jsonb({fn}(({col_q} ->> {key_lit})::text, %(salt)s)), false)"
    )


def jsonb_column_update_expr(jsonb_column: str, rule: ColumnRule) -> str:
    col_q = _quote_ident(jsonb_column)
    expr = col_q
    for key, key_rule in rule.jsonb_keys:
        fn = _FN_MAP[key_rule.strategy]
        key_lit = _quote_literal(key)
        expr = (
            f"jsonb_set({expr}, ARRAY[{key_lit}], "
            f"to_jsonb({fn}(({col_q} ->> {key_lit})::text, %(salt)s)), false)"
        )
    return f"{col_q} = {expr}"


# --- UPDATE generation ---


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def generate_update_sql(
    policy: SanitizationPolicy,
    table: TablePolicy,
) -> list[str]:
    qualified = _qualified(policy.schema_name, table.name)

    # Collapse all plain ("normal") column rewrites into a SINGLE UPDATE so each
    # batch scans the table once instead of once per column. This cuts table
    # scans, WAL volume and per-row foreign-key trigger firings by Nx (e.g. 3x
    # for parkings) and was a primary cause of batches exceeding statement_timeout.
    normal_assignments: list[str] = []
    other_statements: list[str] = []

    for col_name, rule in table.columns:
        if rule.is_jsonb():
            if rule.jsonb_keys:
                col_q = _quote_ident(col_name)
                other_statements.append(
                    f"UPDATE {qualified} SET {jsonb_column_update_expr(col_name, rule)} WHERE {col_q} IS NOT NULL"
                )
        elif rule.is_conditional():
            for condition in rule.strategy_by_condition:
                col_q = _quote_ident(col_name)
                fn = _FN_MAP[condition.strategy]
                other_statements.append(
                    f"UPDATE {qualified} SET {col_q} = {fn}({col_q}::text, %(salt)s)"
                    f" WHERE {condition.where}"
                )
        elif rule.is_normal():
            normal_assignments.append(
                column_update_expr(col_name, rule.strategy, rule.array)
            )

    statements: list[str] = []
    if normal_assignments:
        statements.append(f"UPDATE {qualified} SET {', '.join(normal_assignments)}")
    statements.extend(other_statements)

    return statements


# --- Batch extraction ---


def batch_bounds_query(policy: SanitizationPolicy, table: TablePolicy) -> str:
    key = _quote_ident(table.batch.key)
    qualified = _qualified(policy.schema_name, table.name)
    return f"SELECT min({key}) AS lo, max({key}) AS hi FROM {qualified}"


def generate_batched_update_sql(
    policy: SanitizationPolicy,
    table: TablePolicy,
    lo: int,
    hi: int,
) -> list[str]:
    base = generate_update_sql(policy, table)
    key = _quote_ident(table.batch.key)
    result: list[str] = []
    for stmt in base:
        where_idx = stmt.find(" WHERE ")
        if where_idx >= 0:
            stmt = (
                stmt[:where_idx]
                + f" WHERE ({key} BETWEEN %(lo)s AND %(hi)s)"
                + " AND "
                + stmt[where_idx + 7 :]
            )
        else:
            stmt = stmt + f" WHERE ({key} BETWEEN %(lo)s AND %(hi)s)"
        result.append(stmt)
    return result


# --- Collision check SQL ---


def collision_check_sql(
    policy: SanitizationPolicy,
    table: TablePolicy,
    column: str,
    rule: ColumnRule,
) -> str | None:
    if not rule.is_normal():
        return None
    qualified = _qualified(policy.schema_name, table.name)
    col_q = _quote_ident(column)
    strategy = rule.strategy
    if not strategy:
        return None
    if rule.array:
        # Unnest array elements and check for collisions across all elements
        generated = strategy_value_expr("elem::text", "%(salt)s", strategy)
        return (
            f"SELECT {generated} AS generated, COUNT(DISTINCT elem) "
            f"FROM {qualified}, unnest({col_q}) AS elem "
            f"WHERE {col_q} IS NOT NULL "
            f"GROUP BY generated HAVING COUNT(DISTINCT elem) > 1 LIMIT 1"
        )
    generated = strategy_value_expr(f"{col_q}::text", "%(salt)s", strategy)
    return (
        f"SELECT {generated} AS generated, COUNT(DISTINCT {col_q}) "
        f"FROM {qualified} WHERE {col_q} IS NOT NULL "
        f"GROUP BY generated HAVING COUNT(DISTINCT {col_q}) > 1 LIMIT 1"
    )


# --- Verification SQL ---


def verification_sql(
    policy: SanitizationPolicy,
    table: TablePolicy,
    column: str,
    rule: ColumnRule,
) -> list[str]:
    qualified = _qualified(policy.schema_name, table.name)
    col_q = _quote_ident(column)

    if rule.is_jsonb():
        results: list[str] = []
        for key, key_rule in rule.jsonb_keys:
            key_lit = _quote_literal(key)
            results.append(
                f"SELECT {_quote_literal(f'{table.name}.{column}.{key}')} AS target, "
                f"COUNT(*) FILTER (WHERE {col_q} IS NOT NULL AND {col_q} ? {key_lit}) AS checked, "
                f"COALESCE(SUM(CASE WHEN {col_q} IS NOT NULL AND {col_q} ? {key_lit} "
                f"AND NOT {SCHEMA}.verify_{key_rule.strategy}(({col_q} ->> {key_lit})::text) THEN 1 ELSE 0 END), 0) AS failed "
                f"FROM {qualified}"
            )
        return results

    if rule.is_conditional():
        results = []
        for condition in rule.strategy_by_condition:
            results.append(
                f"SELECT {_quote_literal(f'{table.name}.{column} ({condition.strategy})')} AS target, "
                f"COUNT({col_q}) AS checked, "
                f"COALESCE(SUM(CASE WHEN {col_q} IS NOT NULL AND NOT {SCHEMA}.verify_{condition.strategy}({col_q}::text) THEN 1 ELSE 0 END), 0) AS failed "
                f"FROM {qualified} WHERE {condition.where}"
            )
        return results

    if rule.is_normal():
        if rule.array:
            # Verify each unnested array element
            return [
                f"SELECT {_quote_literal(f'{table.name}.{column}')} AS target, "
                f"COUNT(*) AS checked, "
                f"COALESCE(SUM(CASE WHEN NOT {SCHEMA}.verify_{rule.strategy}(elem::text) THEN 1 ELSE 0 END), 0) AS failed "
                f"FROM {qualified}, unnest({col_q}) AS elem "
                f"WHERE {col_q} IS NOT NULL AND elem IS NOT NULL"
            ]
        return [
            f"SELECT {_quote_literal(f'{table.name}.{column}')} AS target, "
            f"COUNT({col_q}) AS checked, "
            f"COALESCE(SUM(CASE WHEN {col_q} IS NOT NULL AND NOT {SCHEMA}.verify_{rule.strategy}({col_q}::text) THEN 1 ELSE 0 END), 0) AS failed "
            f"FROM {qualified}"
        ]

    return []


# --- Verify helper SQL ---


def verify_helpers_sql() -> str:
    def _verify_fn(name: str, pattern: str) -> str:
        return (
            f"CREATE OR REPLACE FUNCTION {SCHEMA}.verify_{name}(value text) RETURNS boolean "
            f"LANGUAGE sql IMMUTABLE PARALLEL SAFE "
            f"AS $$ SELECT value ~ {_quote_literal(pattern)} $$;"
        )

    pool_fn = ", ".join(_quote_literal(n) for n in first_names())
    pool_ln = ", ".join(_quote_literal(n) for n in last_names())

    parts = [
        _verify_fn("email", r"^anon_[0-9a-f]{20}@test\.local$"),
        _verify_fn("phone_fr", r"^\+336[0-9]{9}$"),
        f"CREATE OR REPLACE FUNCTION {SCHEMA}.verify_first_name(value text) RETURNS boolean "
        f"LANGUAGE sql IMMUTABLE PARALLEL SAFE "
        f"AS $$ SELECT value = ANY (ARRAY[{pool_fn}]::text[]) $$;",
        f"CREATE OR REPLACE FUNCTION {SCHEMA}.verify_last_name(value text) RETURNS boolean "
        f"LANGUAGE sql IMMUTABLE PARALLEL SAFE "
        f"AS $$ SELECT value = ANY (ARRAY[{pool_ln}]::text[]) $$;",
        _verify_fn("company_name", r"^Partner [0-9A-F]{8}$"),
        _verify_fn("license_plate_fr", r"^[A-Z]{2}-[0-9]{3}-[A-Z]{2}$"),
        _verify_fn("iban_fr", r"^FR[0-9]{25}$"),
        _verify_fn("national_identifier_fr", r"^[0-9]{14}$"),
        _verify_fn("text_token", r"^Anonymized [0-9A-F]{8}$"),
    ]
    return "\n".join(parts)
