"""Load and validate environment + table configuration (YAML).

Why validate so strictly? Table and column names from config are placed
inside SQL text. Allowing only plain Oracle identifiers here prevents a
typo (or a malicious edit) from turning into SQL injection.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
PREFIX = re.compile(r"^[a-z0-9][a-z0-9_\-/]*[a-z0-9]$")
TABLE_ID = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(Exception):
    """Raised when a config file is missing or invalid."""


@dataclass
class EnvConfig:
    name: str
    region: str
    bucket: str
    oracle_dsn: str
    oracle_user_env: str
    oracle_password_env: str
    control_prefix: str = "_control"


@dataclass
class TableConfig:
    table_id: str
    schema: str
    table: str
    load_type: str                      # "full" or "incremental"
    target_prefix: str
    primary_key: list[str] = field(default_factory=list)
    watermark_column: str | None = None
    lag_minutes: int = 2
    chunk_size: int = 50_000
    sum_columns: list[str] = field(default_factory=list)
    enabled: bool = True
    source_file: str = ""

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}"


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    return data


def _require(data: dict, dotted: str, path: Path):
    node = data
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node or node[part] in (None, ""):
            raise ConfigError(f"{path.name}: missing required key '{dotted}'")
        node = node[part]
    return node


def _identifier(value: str, what: str, path: Path) -> str:
    value = str(value).strip().upper()
    if not IDENTIFIER.match(value):
        raise ConfigError(f"{path.name}: invalid {what} '{value}' (letters, digits, _ $ # only)")
    return value


def load_env(config_dir: Path, env: str) -> EnvConfig:
    path = config_dir / "env" / f"{env}.yaml"
    data = _read_yaml(path)
    bucket = str(_require(data, "aws.bucket", path))
    if "<" in bucket or "change-me" in bucket:
        raise ConfigError(f"{path.name}: set aws.bucket to your real bucket name")
    return EnvConfig(
        name=env,
        region=str(_require(data, "aws.region", path)),
        bucket=bucket,
        oracle_dsn=str(_require(data, "oracle.dsn", path)),
        oracle_user_env=str(_require(data, "oracle.user_env", path)),
        oracle_password_env=str(_require(data, "oracle.password_env", path)),
        control_prefix=str(data.get("control_prefix", "_control")).strip("/"),
    )


def load_table(path: Path) -> TableConfig:
    data = _read_yaml(path)
    table_id = str(_require(data, "table_id", path))
    if not TABLE_ID.match(table_id):
        raise ConfigError(f"{path.name}: table_id must be lowercase letters, digits, underscores")
    if path.stem != table_id:
        raise ConfigError(f"{path.name}: file name must match table_id '{table_id}'")

    load_type = str(_require(data, "load.type", path)).lower()
    if load_type not in ("full", "incremental"):
        raise ConfigError(f"{path.name}: load.type must be 'full' or 'incremental'")

    load = data.get("load", {})
    watermark = load.get("watermark_column")
    if load_type == "incremental":
        if not watermark:
            raise ConfigError(f"{path.name}: incremental loads need load.watermark_column")
        watermark = _identifier(watermark, "watermark_column", path)
    elif watermark:
        raise ConfigError(f"{path.name}: watermark_column is only allowed for incremental loads")

    prefix = str(_require(data, "target.prefix", path)).strip("/")
    if not PREFIX.match(prefix):
        raise ConfigError(f"{path.name}: target.prefix must be lowercase, e.g. raw/oracle/northwind/orders")

    chunk_size = int(load.get("chunk_size", 50_000))
    if not 1_000 <= chunk_size <= 1_000_000:
        raise ConfigError(f"{path.name}: load.chunk_size must be between 1,000 and 1,000,000")

    validation = data.get("validation", {}) or {}
    return TableConfig(
        table_id=table_id,
        schema=_identifier(_require(data, "source.schema", path), "source.schema", path),
        table=_identifier(_require(data, "source.table", path), "source.table", path),
        primary_key=[_identifier(c, "primary_key", path) for c in data["source"].get("primary_key", [])],
        load_type=load_type,
        watermark_column=watermark,
        lag_minutes=int(load.get("lag_minutes", 2)),
        chunk_size=chunk_size,
        target_prefix=prefix,
        sum_columns=[_identifier(c, "sum_columns", path) for c in validation.get("sum_columns", [])],
        enabled=bool(data.get("enabled", True)),
        source_file=str(path),
    )


def load_all_tables(config_dir: Path) -> list[TableConfig]:
    tables_dir = config_dir / "tables"
    configs = [load_table(p) for p in sorted(tables_dir.glob("*.yaml"))]
    seen: dict[str, str] = {}
    for cfg in configs:
        if cfg.target_prefix in seen:
            raise ConfigError(
                f"{cfg.table_id} and {seen[cfg.target_prefix]} write to the same prefix '{cfg.target_prefix}'"
            )
        seen[cfg.target_prefix] = cfg.table_id
    return configs
