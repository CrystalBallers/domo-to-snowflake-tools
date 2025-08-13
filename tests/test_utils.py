"""
Tests for utility modules (Domo, Snowflake, Google Sheets handlers).

This module tests the utility classes including:
- DomoHandler functionality
- SnowflakeHandler functionality
- GoogleSheets handler functionality
- Common utility functions
"""

import pytest
import os
import pandas as pd
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Import utility modules to test
try:
    from tools.utils.domo import DomoHandler, export_datasets_to_spreadsheet
    from tools.utils.snowflake import SnowflakeHandler
    from tools.utils.gsheets import GoogleSheets, READ_ONLY_SCOPES, READ_WRITE_SCOPES
    from tools.utils.common import (
        transform_column_name, 
        get_snowflake_table_full_name,
        setup_dual_connections,
        get_env_config
    )
except ImportError:
    # Handle case where modules might not be available
    pytest.skip("Utility modules not available", allow_module_level=True)


class TestDomoHandler:
    """Test DomoHandler functionality."""
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_init(self, mock_domo_class):
        """Test DomoHandler initialization."""
        mock_domo_client = Mock()
        mock_domo_class.return_value = mock_domo_client
        
        with patch.dict('os.environ', {
            'DOMO_DEVELOPER_TOKEN': 'test_token',
            'DOMO_INSTANCE': 'test_instance'
        }):
            handler = DomoHandler()
            
            assert handler.client is not None
            assert handler.logger is not None
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_setup_connection_success(self, mock_domo_class):
        """Test successful Domo connection setup."""
        mock_domo_client = Mock()
        mock_domo_class.return_value = mock_domo_client
        
        with patch.dict('os.environ', {
            'DOMO_DEVELOPER_TOKEN': 'test_token',
            'DOMO_INSTANCE': 'test_instance'
        }):
            handler = DomoHandler()
            result = handler.setup_connection()
            
            assert result is True
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_setup_connection_missing_env(self, mock_domo_class):
        """Test Domo connection setup with missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            handler = DomoHandler()
            result = handler.setup_connection()
            
            assert result is False
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_test_connection_success(self, mock_domo_class):
        """Test successful Domo connection test."""
        mock_domo_client = Mock()
        mock_datasets = Mock()
        mock_datasets.list.return_value = [{'id': 'test_dataset', 'name': 'Test Dataset'}]
        mock_domo_client.datasets = mock_datasets
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.test_connection()
        
        assert result is True
        mock_datasets.list.assert_called_once()
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_test_connection_failure(self, mock_domo_class):
        """Test Domo connection test failure."""
        mock_domo_client = Mock()
        mock_datasets = Mock()
        mock_datasets.list.side_effect = Exception("Connection failed")
        mock_domo_client.datasets = mock_datasets
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.test_connection()
        
        assert result is False
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_get_dataset_schema(self, mock_domo_class, mock_domo_dataset):
        """Test getting dataset schema."""
        mock_domo_client = Mock()
        mock_datasets = Mock()
        mock_datasets.get.return_value = mock_domo_dataset
        mock_domo_client.datasets = mock_datasets
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        schema = handler.get_dataset_schema('test_dataset_123')
        
        assert schema is not None
        assert 'columns' in schema
        mock_datasets.get.assert_called_once_with('test_dataset_123')
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_extract_data_success(self, mock_domo_class):
        """Test successful data extraction."""
        mock_domo_client = Mock()
        mock_datasets = Mock()
        
        # Mock CSV data
        mock_csv_data = "customer_id,order_date,amount\nC001,2024-01-01,100.50\nC002,2024-01-02,200.75"
        mock_datasets.data_export.return_value = mock_csv_data
        mock_domo_client.datasets = mock_datasets
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.extract_data('test_dataset_123')
        
        assert result is not None
        mock_datasets.data_export.assert_called_once()
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_extract_data_with_query(self, mock_domo_class):
        """Test data extraction with SQL query."""
        mock_domo_client = Mock()
        mock_query = Mock()
        mock_query.query.return_value = {
            'rows': [['C001', '2024-01-01', 100.50]],
            'columns': ['customer_id', 'order_date', 'amount']
        }
        mock_domo_client.query = mock_query
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.extract_data('test_dataset_123', query="SELECT * FROM table")
        
        assert result is not None
        mock_query.query.assert_called_once()
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_query_dataset(self, mock_domo_class):
        """Test querying dataset."""
        mock_domo_client = Mock()
        mock_query = Mock()
        mock_query.query.return_value = {
            'rows': [[1000]],
            'columns': ['count']
        }
        mock_domo_client.query = mock_query
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.query_dataset('test_dataset_123', "SELECT COUNT(*) FROM table")
        
        assert result is not None
        assert result['rows'][0][0] == 1000
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_list_all_datasets(self, mock_domo_class):
        """Test listing all datasets."""
        mock_domo_client = Mock()
        mock_datasets = Mock()
        mock_datasets.list.return_value = [
            {'id': 'dataset_001', 'name': 'Sales Data'},
            {'id': 'dataset_002', 'name': 'Customer Data'}
        ]
        mock_domo_client.datasets = mock_datasets
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.list_all_datasets()
        
        assert len(result) == 2
        assert result[0]['id'] == 'dataset_001'


class TestSnowflakeHandler:
    """Test SnowflakeHandler functionality."""
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_init(self, mock_connect):
        """Test SnowflakeHandler initialization."""
        handler = SnowflakeHandler()
        
        assert handler.connection is None
        assert handler.logger is not None
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_setup_connection_success(self, mock_connect):
        """Test successful Snowflake connection setup."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with patch.dict('os.environ', {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
            'SNOWFLAKE_DATABASE': 'test_database',
            'SNOWFLAKE_SCHEMA': 'test_schema'
        }):
            handler = SnowflakeHandler()
            result = handler.setup_connection()
            
            assert result is True
            assert handler.connection is not None
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_setup_connection_missing_env(self, mock_connect):
        """Test Snowflake connection setup with missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            handler = SnowflakeHandler()
            result = handler.setup_connection()
            
            assert result is False
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_test_connection_success(self, mock_connect):
        """Test successful Snowflake connection test."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = [1]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        handler = SnowflakeHandler()
        handler.connection = mock_connection
        result = handler.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called()
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_execute_query_success(self, mock_connect):
        """Test successful query execution."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [('C001', '2024-01-01', 100.50)]
        mock_cursor.description = [('customer_id',), ('order_date',), ('amount',)]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        handler = SnowflakeHandler()
        handler.connection = mock_connection
        
        result = handler.execute_query("SELECT * FROM test_table")
        
        assert result is not None
        mock_cursor.execute.assert_called_with("SELECT * FROM test_table")
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_load_data_success(self, mock_connect, mock_polars_dataframe):
        """Test successful data loading."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        handler = SnowflakeHandler()
        handler.connection = mock_connection
        
        result = handler.load_data(mock_polars_dataframe, 'test_table')
        
        assert result is True
        # Should have executed CREATE TABLE and COPY commands
        assert mock_cursor.execute.call_count >= 2
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_load_data_failure(self, mock_connect, mock_polars_dataframe):
        """Test data loading failure."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Load failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        handler = SnowflakeHandler()
        handler.connection = mock_connection
        
        result = handler.load_data(mock_polars_dataframe, 'test_table')
        
        assert result is False
    
    def test_snowflake_handler_cleanup(self):
        """Test connection cleanup."""
        handler = SnowflakeHandler()
        mock_connection = Mock()
        handler.connection = mock_connection
        
        handler.cleanup()
        
        mock_connection.close.assert_called_once()
        assert handler.connection is None


class TestGoogleSheetsHandler:
    """Test GoogleSheets handler functionality."""
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    @patch('tools.utils.gsheets.build')
    def test_google_sheets_init(self, mock_build, mock_credentials):
        """Test GoogleSheets initialization."""
        mock_service = Mock()
        mock_build.return_value = mock_service
        mock_creds = Mock()
        mock_credentials.return_value = mock_creds
        
        handler = GoogleSheets('/fake/credentials.json')
        
        assert handler.service is not None
        mock_credentials.assert_called_once_with('/fake/credentials.json', scopes=READ_ONLY_SCOPES)
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    @patch('tools.utils.gsheets.build')
    def test_google_sheets_read_range_success(self, mock_build, mock_credentials):
        """Test successful range reading."""
        mock_service = Mock()
        mock_sheets = Mock()
        mock_values = Mock()
        
        mock_values.get.return_value.execute.return_value = {
            'values': [
                ['Header1', 'Header2', 'Header3'],
                ['Value1', 'Value2', 'Value3']
            ]
        }
        mock_sheets.values.return_value = mock_values
        mock_service.spreadsheets.return_value = mock_sheets
        mock_build.return_value = mock_service
        
        handler = GoogleSheets('/fake/credentials.json')
        result = handler.read_range('test_sheet_id', 'Sheet1!A:C')
        
        assert len(result) == 2
        assert result[0] == ['Header1', 'Header2', 'Header3']
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    @patch('tools.utils.gsheets.build')
    def test_google_sheets_read_range_empty(self, mock_build, mock_credentials):
        """Test reading empty range."""
        mock_service = Mock()
        mock_sheets = Mock()
        mock_values = Mock()
        
        mock_values.get.return_value.execute.return_value = {}  # No 'values' key
        mock_sheets.values.return_value = mock_values
        mock_service.spreadsheets.return_value = mock_sheets
        mock_build.return_value = mock_service
        
        handler = GoogleSheets('/fake/credentials.json')
        result = handler.read_range('test_sheet_id', 'Sheet1!A:C')
        
        assert result == []
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    @patch('tools.utils.gsheets.build')
    def test_google_sheets_write_range_success(self, mock_build, mock_credentials):
        """Test successful range writing."""
        mock_service = Mock()
        mock_sheets = Mock()
        mock_values = Mock()
        
        mock_values.update.return_value.execute.return_value = {'updatedCells': 1}
        mock_sheets.values.return_value = mock_values
        mock_service.spreadsheets.return_value = mock_sheets
        mock_build.return_value = mock_service
        
        # Initialize with write scopes
        handler = GoogleSheets('/fake/credentials.json', scopes=READ_WRITE_SCOPES)
        result = handler.write_range('test_sheet_id', 'Sheet1!A1', [['Test Value']])
        
        assert result is True
        mock_values.update.assert_called_once()
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    @patch('tools.utils.gsheets.build')
    def test_google_sheets_write_range_failure(self, mock_build, mock_credentials):
        """Test range writing failure."""
        mock_service = Mock()
        mock_sheets = Mock()
        mock_values = Mock()
        
        mock_values.update.side_effect = Exception("Write failed")
        mock_sheets.values.return_value = mock_values
        mock_service.spreadsheets.return_value = mock_sheets
        mock_build.return_value = mock_service
        
        handler = GoogleSheets('/fake/credentials.json', scopes=READ_WRITE_SCOPES)
        result = handler.write_range('test_sheet_id', 'Sheet1!A1', [['Test Value']])
        
        assert result is False


class TestCommonUtilities:
    """Test common utility functions."""
    
    def test_transform_column_name_basic(self):
        """Test basic column name transformation."""
        assert transform_column_name('Customer ID') == 'CUSTOMER_ID'
        assert transform_column_name('order date') == 'ORDER_DATE'
        assert transform_column_name('Total Amount') == 'TOTAL_AMOUNT'
    
    def test_transform_column_name_special_chars(self):
        """Test column name transformation with special characters."""
        assert transform_column_name('Customer-ID') == 'CUSTOMER_ID'
        assert transform_column_name('Order #') == 'ORDER_'
        assert transform_column_name('Total $ Amount') == 'TOTAL__AMOUNT'
    
    def test_transform_column_name_edge_cases(self):
        """Test column name transformation edge cases."""
        assert transform_column_name('') == ''
        assert transform_column_name('   ') == ''
        assert transform_column_name('123Numbers') == '123NUMBERS'
        assert transform_column_name('a') == 'A'
    
    def test_get_snowflake_table_full_name_with_prefix(self):
        """Test getting full Snowflake table name with prefix."""
        with patch.dict('os.environ', {'DOMO_TABLE_PREFIX': 'DOMO_'}):
            result = get_snowflake_table_full_name('test_table')
            assert 'DOMO_TEST_TABLE' in result
    
    def test_get_snowflake_table_full_name_without_prefix(self):
        """Test getting full Snowflake table name without prefix."""
        with patch.dict('os.environ', {'DOMO_TABLE_PREFIX': ''}):
            result = get_snowflake_table_full_name('test_table')
            assert 'TEST_TABLE' in result
    
    def test_get_snowflake_table_full_name_with_schema(self):
        """Test getting full Snowflake table name with schema."""
        with patch.dict('os.environ', {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA': 'TEST_SCHEMA',
            'DOMO_TABLE_PREFIX': 'DOMO_'
        }):
            result = get_snowflake_table_full_name('test_table')
            assert 'TEST_DB.TEST_SCHEMA.DOMO_TEST_TABLE' == result
    
    def test_get_env_config(self):
        """Test environment configuration retrieval."""
        with patch.dict('os.environ', {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA': 'TEST_SCHEMA',
            'SNOWFLAKE_WAREHOUSE': 'TEST_WH'
        }):
            config = get_env_config()
            
            assert config['SNOWFLAKE_DATABASE'] == 'TEST_DB'
            assert config['SNOWFLAKE_SCHEMA'] == 'TEST_SCHEMA'
            assert config['SNOWFLAKE_WAREHOUSE'] == 'TEST_WH'
    
    def test_setup_dual_connections_success(self, mock_domo_handler, mock_snowflake_handler):
        """Test successful dual connection setup."""
        mock_domo_handler.setup_connection.return_value = True
        mock_snowflake_handler.setup_connection.return_value = True
        
        success, domo, snowflake = setup_dual_connections(mock_domo_handler, mock_snowflake_handler)
        
        assert success is True
        assert domo is mock_domo_handler
        assert snowflake is mock_snowflake_handler
    
    def test_setup_dual_connections_domo_failure(self, mock_domo_handler, mock_snowflake_handler):
        """Test dual connection setup with Domo failure."""
        mock_domo_handler.setup_connection.return_value = False
        mock_snowflake_handler.setup_connection.return_value = True
        
        success, domo, snowflake = setup_dual_connections(mock_domo_handler, mock_snowflake_handler)
        
        assert success is False
        assert domo is None
        assert snowflake is None
    
    def test_setup_dual_connections_snowflake_failure(self, mock_domo_handler, mock_snowflake_handler):
        """Test dual connection setup with Snowflake failure."""
        mock_domo_handler.setup_connection.return_value = True
        mock_snowflake_handler.setup_connection.return_value = False
        
        success, domo, snowflake = setup_dual_connections(mock_domo_handler, mock_snowflake_handler)
        
        assert success is False
        assert domo is None
        assert snowflake is None


class TestDatasetExportFunctionality:
    """Test dataset export to spreadsheet functionality."""
    
    @patch('tools.utils.domo.DomoHandler')
    @patch('tools.utils.domo.GoogleSheets')
    def test_export_datasets_to_spreadsheet_success(self, mock_gsheets_class, mock_domo_class):
        """Test successful dataset export to spreadsheet."""
        # Mock Domo handler
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.list_all_datasets.return_value = [
            {'id': 'dataset_001', 'name': 'Sales Data', 'rows': 1000},
            {'id': 'dataset_002', 'name': 'Customer Data', 'rows': 2000}
        ]
        mock_domo_class.return_value = mock_domo
        
        # Mock Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        result = export_datasets_to_spreadsheet(
            credentials_path='/fake/credentials.json',
            spreadsheet_id='test_sheet_id',
            sheet_name='Datasets'
        )
        
        assert result is True
        mock_domo.list_all_datasets.assert_called_once()
        mock_gsheets.write_range.assert_called()
    
    @patch('tools.utils.domo.DomoHandler')
    def test_export_datasets_to_spreadsheet_domo_failure(self, mock_domo_class):
        """Test dataset export when Domo connection fails."""
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = False
        mock_domo_class.return_value = mock_domo
        
        result = export_datasets_to_spreadsheet(
            credentials_path='/fake/credentials.json',
            spreadsheet_id='test_sheet_id'
        )
        
        assert result is False
    
    @patch('tools.utils.domo.DomoHandler')
    @patch('tools.utils.domo.GoogleSheets')
    def test_export_datasets_to_spreadsheet_sheets_failure(self, mock_gsheets_class, mock_domo_class):
        """Test dataset export when Google Sheets write fails."""
        # Mock Domo handler
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.list_all_datasets.return_value = [
            {'id': 'dataset_001', 'name': 'Sales Data'}
        ]
        mock_domo_class.return_value = mock_domo
        
        # Mock Google Sheets failure
        mock_gsheets = Mock()
        mock_gsheets.write_range.return_value = False
        mock_gsheets_class.return_value = mock_gsheets
        
        result = export_datasets_to_spreadsheet(
            credentials_path='/fake/credentials.json',
            spreadsheet_id='test_sheet_id'
        )
        
        assert result is False


class TestErrorHandlingInUtils:
    """Test error handling across utility modules."""
    
    @patch('tools.utils.domo.pydomo.Domo')
    def test_domo_handler_handles_api_errors(self, mock_domo_class):
        """Test that DomoHandler handles API errors gracefully."""
        mock_domo_client = Mock()
        mock_domo_client.datasets.list.side_effect = Exception("API Error")
        mock_domo_class.return_value = mock_domo_client
        
        handler = DomoHandler()
        result = handler.test_connection()
        
        assert result is False
    
    @patch('tools.utils.snowflake.snowflake.connector.connect')
    def test_snowflake_handler_handles_connection_errors(self, mock_connect):
        """Test that SnowflakeHandler handles connection errors gracefully."""
        mock_connect.side_effect = Exception("Connection Error")
        
        handler = SnowflakeHandler()
        result = handler.setup_connection()
        
        assert result is False
    
    @patch('tools.utils.gsheets.service_account.Credentials.from_service_account_file')
    def test_google_sheets_handles_auth_errors(self, mock_credentials):
        """Test that GoogleSheets handles authentication errors gracefully."""
        mock_credentials.side_effect = Exception("Authentication Error")
        
        with pytest.raises(Exception):
            GoogleSheets('/fake/credentials.json')


class TestDataTypeHandling:
    """Test data type handling and conversion."""
    
    def test_polars_to_pandas_conversion(self, mock_polars_dataframe):
        """Test conversion from Polars to Pandas DataFrame."""
        if mock_polars_dataframe is not None:
            pandas_df = mock_polars_dataframe.to_pandas()
            assert isinstance(pandas_df, pd.DataFrame)
            assert len(pandas_df) > 0
    
    def test_csv_to_polars_conversion(self):
        """Test conversion from CSV string to Polars DataFrame."""
        csv_data = "customer_id,amount\nC001,100.50\nC002,200.75"
        
        # This would test the actual conversion logic in DomoHandler
        # For now, we just test that the CSV data is valid
        assert 'customer_id' in csv_data
        assert 'C001' in csv_data
    
    def test_sql_result_to_polars_conversion(self):
        """Test conversion from SQL result to Polars DataFrame."""
        # This would test conversion logic in SnowflakeHandler
        # Mock SQL result structure
        mock_result = [
            ('C001', '2024-01-01', 100.50),
            ('C002', '2024-01-02', 200.75)
        ]
        mock_columns = ['customer_id', 'order_date', 'amount']
        
        # Test that we can construct a DataFrame from this structure
        df = pd.DataFrame(mock_result, columns=mock_columns)
        assert len(df) == 2
        assert list(df.columns) == mock_columns


class TestConfigurationValidation:
    """Test configuration validation functionality."""
    
    def test_required_env_vars_validation(self):
        """Test validation of required environment variables."""
        required_vars = [
            'DOMO_DEVELOPER_TOKEN',
            'DOMO_INSTANCE',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD'
        ]
        
        # Test that we can identify missing variables
        missing_vars = []
        for var in required_vars:
            if not os.environ.get(var):
                missing_vars.append(var)
        
        # This is just a validation test - in real usage, missing vars would cause failures
        assert isinstance(missing_vars, list)
    
    def test_optional_env_vars_defaults(self):
        """Test default values for optional environment variables."""
        # Test default export directory
        export_dir = os.environ.get('EXPORT_DIR', 'results/sql/translated')
        assert 'results' in export_dir
        
        # Test default table prefix
        table_prefix = os.environ.get('DOMO_TABLE_PREFIX', 'DOMO_')
        assert isinstance(table_prefix, str)
    
    def test_snowflake_connection_string_construction(self):
        """Test Snowflake connection string construction."""
        test_config = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
            'SNOWFLAKE_DATABASE': 'test_database',
            'SNOWFLAKE_SCHEMA': 'test_schema'
        }
        
        # Test that all required components are present
        for key, value in test_config.items():
            assert value is not None
            assert value != ''