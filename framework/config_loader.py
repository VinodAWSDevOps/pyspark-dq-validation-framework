"""Pure config-parsing module. Depends only on pyyaml + stdlib — pipeline
and test code both import this, so no pytest/Databricks imports belong here.
"""
from __future__ import annotations

import functools
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
TABLES_CONFIG_DIR = REPO_ROOT / "config" / "tables"
GOLD_MAPPINGS_FILE = REPO_ROOT / "config" / "gold_mappings.yaml"

VALID_SCD_TYPES = {"SCD1", "SCD2", "none"}


class ConfigError(Exception):
    """Raised when a config file is missing/invalid, naming the file and field."""


@dataclass
class TableConfig:
    table_name: str
    sequence: int
    source_path: str
    bronze_target: str
    silver_target: str
    gold_target: Optional[str]
    primary_key: str
    scd_type: str
    tracked_columns: List[str] = field(default_factory=list)
    mandatory_columns: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)
    business_rules: Dict[str, Any] = field(default_factory=dict)
    date_columns: List[str] = field(default_factory=list)
    expected_file_format: Dict[str, Any] = field(default_factory=dict)
    # SCD2-only — absent from SCD1/none table configs.
    effective_start_col: Optional[str] = None
    effective_end_col: Optional[str] = None
    is_current_col: Optional[str] = None
    volume_folder: Optional[str] = None

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "TableConfig":
        known_fields = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known_fields})


@dataclass
class GoldMapping:
    gold_table: str
    source_tables: List[str] = field(default_factory=list)
    aggregation_type: str = ""
    group_by_columns: List[str] = field(default_factory=list)
    aggregate_columns: List[Dict[str, Any]] = field(default_factory=list)
    reconciliation_rule: str = ""

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "GoldMapping":
        known_fields = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known_fields})


def _validate_required_fields(raw: Dict[str, Any], file_path: Path) -> None:
    for required_field in ("table_name", "sequence"):
        if raw.get(required_field) in (None, ""):
            raise ConfigError(
                f"Invalid config in {file_path.name}: missing required field '{required_field}'"
            )

    scd_type = raw.get("scd_type")
    if scd_type not in VALID_SCD_TYPES:
        raise ConfigError(
            f"Invalid config in {file_path.name}: field 'scd_type' must be one of "
            f"{sorted(VALID_SCD_TYPES)}, got {scd_type!r}"
        )


def _validate_foreign_keys(configs: List[TableConfig]) -> None:
    known_table_names = {c.table_name for c in configs}
    for config in configs:
        for fk_column, fk_reference in (config.foreign_keys or {}).items():
            referenced_table = str(fk_reference).split(".")[0]
            if referenced_table not in known_table_names:
                raise ConfigError(
                    f"Invalid config for table '{config.table_name}': "
                    f"foreign_keys.{fk_column} references unknown table "
                    f"'{referenced_table}' (from '{fk_reference}')"
                )


@functools.lru_cache(maxsize=None)
def load_all_table_configs() -> List[TableConfig]:
    """Scan config/tables/*.yaml, validate, and return configs sorted by sequence."""
    if not TABLES_CONFIG_DIR.is_dir():
        raise ConfigError(f"Table config directory not found: {TABLES_CONFIG_DIR}")

    yaml_files = sorted(TABLES_CONFIG_DIR.glob("*.yaml")) + sorted(TABLES_CONFIG_DIR.glob("*.yml"))
    if not yaml_files:
        raise ConfigError(f"No table config YAML files found in {TABLES_CONFIG_DIR}")

    configs: List[TableConfig] = []
    for file_path in yaml_files:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        _validate_required_fields(raw, file_path)

        try:
            configs.append(TableConfig.from_dict(raw))
        except TypeError as exc:
            raise ConfigError(f"Invalid config in {file_path.name}: {exc}") from exc

    _validate_foreign_keys(configs)

    return sorted(configs, key=lambda c: c.sequence)


@functools.lru_cache(maxsize=None)
def get_table_config(table_name: str) -> TableConfig:
    """Return the TableConfig for table_name, or raise ConfigError if not found."""
    for config in load_all_table_configs():
        if config.table_name == table_name:
            return config

    available = [c.table_name for c in load_all_table_configs()]
    raise ConfigError(f"No table config found for table_name={table_name!r}. Available tables: {available}")


@functools.lru_cache(maxsize=None)
def load_gold_mappings() -> List[GoldMapping]:
    """Read config/gold_mappings.yaml and return its entries as GoldMapping objects."""
    if not GOLD_MAPPINGS_FILE.is_file():
        raise ConfigError(f"Gold mappings file not found: {GOLD_MAPPINGS_FILE}")

    with open(GOLD_MAPPINGS_FILE, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    return [GoldMapping.from_dict(entry) for entry in raw.get("gold_tables", [])]


if __name__ == "__main__":
    print(f"Loading table configs from: {TABLES_CONFIG_DIR}\n")
    for table_config in load_all_table_configs():
        print(f"--- {table_config.table_name} (sequence={table_config.sequence}) ---")
        for f_ in fields(TableConfig):
            print(f"  {f_.name}: {getattr(table_config, f_.name)}")
        print()

    print(f"Loading gold mappings from: {GOLD_MAPPINGS_FILE}\n")
    for mapping in load_gold_mappings():
        print(f"--- {mapping.gold_table} ---")
        for f_ in fields(GoldMapping):
            print(f"  {f_.name}: {getattr(mapping, f_.name)}")
        print()
