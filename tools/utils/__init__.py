"""
Utils package for Domo to Snowflake migration tools.

This package provides utilities for data migration, comparison, and processing
between Domo and Snowflake platforms.
"""

# Main handler classes
from .domo import DomoHandler
from .snowflake import SnowflakeHandler
from tools.dataset_comparator import DatasetComparator
from .gsheets import GoogleSheets

# Legacy compatibility


# Common utilities
from .common import (
    transform_column_name,
    check_env_vars,
    combine_schemas,
    get_snowflake_table_full_name,
    mask_sensitive_value,
    get_env_config,
    setup_dual_connections,
    show_mfa_debug_info,
    reload_environment,
    # Type definitions
    ColumnMetadata,
    Columns,
    Schema,
    QueryResult
)

# Version info
__version__ = "1.0.0"

# Main exports for easy importing
__all__ = [
    # Main classes
    "DomoHandler",
    "SnowflakeHandler", 
    "DatasetComparator",
    "GoogleSheets",
    # Utilities
    "transform_column_name",
    "check_env_vars",
    "combine_schemas",
    "get_snowflake_table_full_name",
    "mask_sensitive_value",
    "get_env_config",
    "setup_dual_connections",
    "show_mfa_debug_info",
    "reload_environment",
    # Types
    "ColumnMetadata",
    "Columns", 
    "Schema",
    "QueryResult"
] 