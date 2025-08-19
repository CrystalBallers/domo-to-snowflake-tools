"""
Pytest configuration and shared fixtures for the test suite.

This module provides common fixtures and test utilities used across
all test modules.
"""

import os
import sys
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    return {
        'GOOGLE_SHEETS_CREDENTIALS_FILE': '/fake/path/credentials.json',
        'MIGRATION_SPREADSHEET_ID': '1234567890abcdef',
        'DOMO_DEVELOPER_TOKEN': 'fake_domo_token',
        'DOMO_INSTANCE': 'test_instance',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA': 'test_schema',
        'EXPORT_DIR': 'test_results/sql/translated'
    }

@pytest.fixture
def mock_google_sheets_data():
    """Mock data from Google Sheets."""
    return [
        ['Dataset ID', 'Name', 'Status', 'Dataflow ID'],
        ['dataset_001', 'sales_data', 'Pending', 'df_001'],
        ['dataset_002', 'customer_data', 'Migrated', 'df_002'],
        ['dataset_003', 'inventory_data', 'Failed', 'df_003'],
        ['dataset_004', 'financial_data', 'Pending', 'df_004']
    ]

@pytest.fixture
def mock_domo_dataset():
    """Mock Domo dataset data."""
    return {
        'id': 'test_dataset_123',
        'name': 'Test Dataset',
        'rows': 1000,
        'columns': [
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'order_date', 'type': 'DATETIME'},
            {'name': 'amount', 'type': 'DOUBLE'}
        ],
        'schema': {
            'columns': [
                {'name': 'customer_id', 'type': 'STRING'},
                {'name': 'order_date', 'type': 'DATETIME'},
                {'name': 'amount', 'type': 'DOUBLE'}
            ]
        }
    }

@pytest.fixture
def mock_snowflake_table():
    """Mock Snowflake table data."""
    return {
        'name': 'TEST_TABLE',
        'rows': 1000,
        'columns': [
            {'COLUMN_NAME': 'CUSTOMER_ID', 'DATA_TYPE': 'VARCHAR'},
            {'COLUMN_NAME': 'ORDER_DATE', 'DATA_TYPE': 'TIMESTAMP'},
            {'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'FLOAT'}
        ]
    }

@pytest.fixture
def mock_polars_dataframe():
    """Mock Polars DataFrame."""
    try:
        import polars as pl
        return pd.DataFrame({
            'customer_id': ['C001', 'C002', 'C003'],
            'order_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'amount': [100.50, 200.75, 150.25]
        })
    except ImportError:
        # If polars not available, return None
        return None

@pytest.fixture
def mock_pandas_dataframe():
    """Mock Pandas DataFrame."""
    import pandas as pd
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003'],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'amount': [100.50, 200.75, 150.25]
    })

@pytest.fixture
def mock_domo_handler():
    """Mock DomoHandler for testing."""
    mock_handler = Mock()
    mock_handler.setup_connection.return_value = True
    mock_handler.test_connection.return_value = True
    mock_handler.get_dataset_schema.return_value = {
        'columns': [
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'order_date', 'type': 'DATETIME'},
            {'name': 'amount', 'type': 'DOUBLE'}
        ]
    }
    mock_handler.extract_data.return_value = None  # Will be set by specific tests
    mock_handler.query_dataset.return_value = {
        'rows': [[1000]],
        'columns': ['count']
    }
    return mock_handler

@pytest.fixture
def mock_snowflake_handler():
    """Mock SnowflakeHandler for testing."""
    mock_handler = Mock()
    mock_handler.setup_connection.return_value = True
    mock_handler.test_connection.return_value = True
    mock_handler.execute_query.return_value = None  # Will be set by specific tests
    mock_handler.cleanup.return_value = None
    return mock_handler

@pytest.fixture
def mock_google_sheets_handler():
    """Mock GoogleSheets handler for testing."""
    mock_handler = Mock()
    mock_handler.read_range.return_value = [
        ['Dataset ID', 'Name', 'Status'],
        ['dataset_001', 'sales_data', 'Pending'],
        ['dataset_002', 'customer_data', 'Migrated']
    ]
    mock_handler.write_range.return_value = True
    return mock_handler

@pytest.fixture
def mock_dataset_comparator():
    """Mock DatasetComparator for testing."""
    mock_comparator = Mock()
    mock_comparator.setup_connections.return_value = True
    mock_comparator.generate_report.return_value = {
        'domo_dataset_id': 'test_123',
        'snowflake_table': 'test_table',
        'overall_match': True,
        'errors': []
    }
    return mock_comparator

@pytest.fixture
def sample_json_mapping():
    """Sample JSON mapping for batch migrations."""
    return {
        'dataset_001': 'sales_table',
        'dataset_002': 'customer_table',
        'dataset_003': 'inventory_table'
    }

@pytest.fixture
def sample_sql_content():
    """Sample SQL content for testing."""
    return """
-- Generated SQL for test dataflow
SELECT 
    customer_id,
    order_date,
    amount
FROM source_table
WHERE order_date >= '2024-01-01'
"""

# Mock classes for testing
class MockArgparseNamespace:
    """Mock argparse Namespace for CLI testing."""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

@pytest.fixture
def mock_args():
    """Factory for creating mock CLI arguments."""
    def _create_args(**kwargs):
        return MockArgparseNamespace(**kwargs)
    return _create_args

# Test data constants
TEST_DATASET_ID = "test_dataset_12345"
TEST_TABLE_NAME = "test_table"
TEST_KEY_COLUMNS = ["customer_id", "order_date"]
TEST_SPREADSHEET_ID = "1234567890abcdef"
TEST_SHEET_NAME = "Test_Sheet"

# Helper functions for tests
def create_test_file(temp_dir: str, filename: str, content: str) -> str:
    """Create a test file with specified content."""
    filepath = os.path.join(temp_dir, filename)
    with open(filepath, 'w') as f:
        f.write(content)
    return filepath

def mock_env_setup(monkeypatch, env_vars: Dict[str, str]):
    """Helper to setup environment variables for testing."""
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)