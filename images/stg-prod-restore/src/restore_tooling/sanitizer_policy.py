import dataclasses
import os
from typing import Any, Optional

import yaml


BUNDLED_POLICY = os.path.join(os.path.dirname(__file__), "sanitization_policy.yaml")


@dataclasses.dataclass(frozen=True)
class StrategyCondition:
    where: str
    strategy: str


@dataclasses.dataclass(frozen=True)
class JsonbKeyRule:
    strategy: str
    unique: bool = False


@dataclasses.dataclass(frozen=True)
class ColumnRule:
    strategy: Optional[str] = None
    unique: bool = False
    array: bool = False
    strategy_by_condition: tuple[StrategyCondition, ...] = ()
    jsonb_keys: tuple[tuple[str, JsonbKeyRule], ...] = ()

    def is_jsonb(self) -> bool:
        return len(self.jsonb_keys) > 0

    def is_conditional(self) -> bool:
        return len(self.strategy_by_condition) > 0

    def is_normal(self) -> bool:
        return self.strategy is not None


@dataclasses.dataclass(frozen=True)
class BatchConfig:
    enabled: bool = False
    key: str = "id"
    size: int = 10000


@dataclasses.dataclass(frozen=True)
class TablePolicy:
    name: str
    batch: BatchConfig
    columns: tuple[tuple[str, ColumnRule], ...]


@dataclasses.dataclass(frozen=True)
class SanitizationPolicy:
    version: int
    schema_name: str
    defaults_batch: BatchConfig
    tables: tuple[TablePolicy, ...]

    def find_table(self, name: str) -> Optional[TablePolicy]:
        for t in self.tables:
            if t.name == name:
                return t
        return None


_STRATEGIES = frozenset(
    {
        "email",
        "phone_fr",
        "first_name",
        "last_name",
        "company_name",
        "license_plate_fr",
        "iban_fr",
        "national_identifier_fr",
        "text_token",
    }
)


def _parse_batch(raw: Any, fallback: BatchConfig) -> BatchConfig:
    if not raw or not isinstance(raw, dict):
        return fallback
    return BatchConfig(
        enabled=bool(raw.get("enabled", fallback.enabled)),
        key=str(raw.get("key", fallback.key)),
        size=int(raw.get("size", fallback.size)),
    )


def _parse_jsonb_keys(keys: Any) -> tuple[tuple[str, JsonbKeyRule], ...]:
    if not keys or not isinstance(keys, dict):
        return ()
    return tuple(
        (
            k,
            JsonbKeyRule(
                strategy=str(v.get("strategy", "")), unique=bool(v.get("unique", False))
            ),
        )
        for k, v in keys.items()
    )


def _parse_column(name: str, raw: Any) -> ColumnRule:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid column definition for {name}")
    if raw.get("type") == "jsonb":
        return ColumnRule(jsonb_keys=_parse_jsonb_keys(raw.get("keys")))
    conditions = raw.get("strategy_by_condition")
    if conditions is not None:
        if not isinstance(conditions, list) or len(conditions) == 0:
            raise ValueError(
                f"strategy_by_condition for {name} must be a non-empty list"
            )
        parsed = tuple(
            StrategyCondition(
                where=str(c.get("where", "")), strategy=str(c.get("strategy", ""))
            )
            for c in conditions
        )
        return ColumnRule(strategy_by_condition=parsed)
    strategy = raw.get("strategy", "")
    if not strategy:
        raise ValueError(
            f"Column {name} has no strategy, type, or strategy_by_condition"
        )
    return ColumnRule(
        strategy=strategy,
        unique=bool(raw.get("unique", False)),
        array=bool(raw.get("array", False)),
    )


def load_policy(path: Optional[str] = None) -> SanitizationPolicy:
    resolved = path or os.environ.get("SANITIZER_POLICY_PATH") or BUNDLED_POLICY
    with open(resolved) as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    version = int(raw.get("version", 1))
    defaults_raw = raw.get("defaults", {}) or {}
    schema_name = defaults_raw.get("schema", "public")
    defaults_batch = _parse_batch(defaults_raw.get("batch"), BatchConfig())

    tables_raw = raw.get("tables", {}) or {}
    if not isinstance(tables_raw, dict):
        raise ValueError("'tables' must be a mapping")

    tables: list[TablePolicy] = []
    for table_name, table_raw in tables_raw.items():
        if not isinstance(table_raw, dict):
            raise ValueError(f"Invalid table definition for {table_name}")
        table_batch = _parse_batch(table_raw.get("batch"), defaults_batch)
        columns_raw = table_raw.get("columns", {}) or {}
        columns = tuple(
            (col_name, _parse_column(col_name, col_raw))
            for col_name, col_raw in columns_raw.items()
        )
        tables.append(TablePolicy(name=table_name, batch=table_batch, columns=columns))

    return SanitizationPolicy(
        version=version,
        schema_name=schema_name,
        defaults_batch=defaults_batch,
        tables=tuple(tables),
    )


def known_strategies() -> frozenset:
    return _STRATEGIES
