"""
Utils package for Domo to Snowflake migration tools.

Exports are loaded lazily so subpackages like translation_difficulty can be used
without importing optional dependencies (e.g. datacompy) required by other utils.
"""

from __future__ import annotations

import importlib
from typing import Any, List

__version__ = "1.0.0"

__all__ = [
    "DomoHandler",
    "SnowflakeHandler",
    "DatasetComparator",
    "GoogleSheets",
    "transform_column_name",
    "check_env_vars",
    "combine_schemas",
    "get_snowflake_table_full_name",
    "mask_sensitive_value",
    "get_env_config",
    "setup_dual_connections",
    "show_mfa_debug_info",
    "reload_environment",
    "ColumnMetadata",
    "Columns",
    "Schema",
    "QueryResult",
]


def __getattr__(name: str) -> Any:
    if name == "DomoHandler":
        from .domo import DomoHandler

        return DomoHandler
    if name == "SnowflakeHandler":
        from .snowflake import SnowflakeHandler

        return SnowflakeHandler
    if name == "DatasetComparator":
        from tools.dataset_comparator import DatasetComparator

        return DatasetComparator
    if name == "GoogleSheets":
        from .gsheets import GoogleSheets

        return GoogleSheets
    if name in (
        "transform_column_name",
        "check_env_vars",
        "combine_schemas",
        "get_snowflake_table_full_name",
        "mask_sensitive_value",
        "get_env_config",
        "setup_dual_connections",
        "show_mfa_debug_info",
        "reload_environment",
        "ColumnMetadata",
        "Columns",
        "Schema",
        "QueryResult",
    ):
        return getattr(importlib.import_module(f"{__name__}.common"), name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    return sorted(set(__all__) | set(globals().keys()))
