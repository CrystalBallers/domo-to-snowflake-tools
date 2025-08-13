"""
Tests for the DatasetComparator class.

This module tests data comparison functionality between Domo and Snowflake including:
- Schema comparison
- Row count comparison  
- Data sample comparison
- Report generation
- Error handling
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import math

from tools.dataset_comparator import DatasetComparator


class TestDatasetComparatorInit:
    """Test DatasetComparator initialization."""
    
    @patch('tools.dataset_comparator.DomoHandler')
    @patch('tools.dataset_comparator.SnowflakeHandler')
    def test_init_creates_handlers(self, mock_sf_class, mock_domo_class):
        """Test that initialization creates Domo and Snowflake handlers."""
        comparator = DatasetComparator()
        
        assert comparator.domo_handler is not None
        assert comparator.snowflake_handler is not None
        assert comparator.errors == []
        assert comparator._domo_connected is False
        assert comparator._snowflake_connected is False


class TestConnectionSetup:
    """Test connection setup functionality."""
    
    @patch('tools.dataset_comparator.setup_dual_connections')
    def test_setup_connections_success(self, mock_setup, mock_domo_handler, mock_snowflake_handler):
        """Test successful connection setup."""
        mock_setup.return_value = (True, mock_domo_handler, mock_snowflake_handler)
        
        comparator = DatasetComparator()
        result = comparator.setup_connections()
        
        assert result is True
        assert comparator._domo_connected is True
        assert comparator._snowflake_connected is True
    
    @patch('tools.dataset_comparator.setup_dual_connections')
    def test_setup_connections_failure(self, mock_setup):
        """Test failed connection setup."""
        mock_setup.return_value = (False, None, None)
        
        comparator = DatasetComparator()
        result = comparator.setup_connections()
        
        assert result is False
        assert comparator._domo_connected is False
        assert comparator._snowflake_connected is False


class TestErrorHandling:
    """Test error handling functionality."""
    
    def test_add_error(self):
        """Test adding errors to the error list."""
        comparator = DatasetComparator()
        
        comparator.add_error("Test Section", "Test Error", "Test Details")
        
        assert len(comparator.errors) == 1
        assert comparator.errors[0]['section'] == "Test Section"
        assert comparator.errors[0]['error'] == "Test Error"
        assert comparator.errors[0]['details'] == "Test Details"


class TestSampleSizeCalculation:
    """Test statistical sample size calculation."""
    
    def test_calculate_sample_size_small_dataset(self):
        """Test sample size calculation for small datasets."""
        comparator = DatasetComparator()
        
        sample_size = comparator.calculate_sample_size(500)
        assert sample_size == 500
    
    def test_calculate_sample_size_large_dataset(self):
        """Test sample size calculation for large datasets."""
        comparator = DatasetComparator()
        
        sample_size = comparator.calculate_sample_size(100000)
        assert sample_size > 0
        assert sample_size < 100000
        assert sample_size > 300  # Should be statistically significant
    
    def test_calculate_sample_size_with_custom_params(self):
        """Test sample size calculation with custom confidence level and margin of error."""
        comparator = DatasetComparator()
        
        sample_size = comparator.calculate_sample_size(
            total_rows=10000,
            confidence_level=0.99,
            margin_of_error=0.01
        )
        assert sample_size > 0
        assert sample_size <= 10000


class TestSchemaComparison:
    """Test schema comparison functionality."""
    
    @patch('tools.dataset_comparator.get_env_config')
    def test_compare_schemas_success(self, mock_env_config, mock_domo_dataset, mock_snowflake_table):
        """Test successful schema comparison."""
        mock_env_config.return_value = {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA': 'TEST_SCHEMA'
        }
        
        comparator = DatasetComparator()
        
        # Mock Domo handler
        comparator.domo_handler.get_dataset_schema.return_value = mock_domo_dataset['schema']
        
        # Mock Snowflake handler
        sf_result = Mock()
        sf_result.to_pandas.return_value = pd.DataFrame(mock_snowflake_table['columns'])
        comparator.snowflake_handler.execute_query.return_value = sf_result
        
        result = comparator.compare_schemas('test_dataset_123', 'TEST_TABLE')
        
        assert 'domo_columns' in result
        assert 'snowflake_columns' in result
        assert 'missing_in_snowflake' in result
        assert 'extra_in_snowflake' in result
        assert 'schema_match' in result
    
    def test_compare_schemas_with_transformation(self, mock_domo_dataset):
        """Test schema comparison with column name transformation."""
        comparator = DatasetComparator()
        
        # Mock successful operations but focus on transformation
        comparator.domo_handler.get_dataset_schema.return_value = mock_domo_dataset['schema']
        
        with patch('tools.dataset_comparator.transform_column_name') as mock_transform:
            mock_transform.side_effect = lambda x: x.upper().replace(' ', '_')
            
            # We'll mock the rest to focus on transformation logic
            with patch.object(comparator.snowflake_handler, 'execute_query') as mock_query:
                sf_result = Mock()
                sf_result.to_pandas.return_value = pd.DataFrame([
                    {'COLUMN_NAME': 'CUSTOMER_ID', 'DATA_TYPE': 'VARCHAR'},
                    {'COLUMN_NAME': 'ORDER_DATE', 'DATA_TYPE': 'TIMESTAMP'},
                    {'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'FLOAT'}
                ])
                mock_query.return_value = sf_result
                
                with patch('tools.dataset_comparator.get_env_config') as mock_env:
                    mock_env.return_value = {'SNOWFLAKE_DATABASE': 'TEST', 'SNOWFLAKE_SCHEMA': 'TEST'}
                    
                    result = comparator.compare_schemas('test_dataset_123', 'TEST_TABLE', transform_names=True)
                    
                    assert result['transform_applied'] is True
    
    def test_compare_schemas_domo_error(self):
        """Test schema comparison when Domo schema retrieval fails."""
        comparator = DatasetComparator()
        
        # Make Domo handler raise an exception
        comparator.domo_handler.get_dataset_schema.side_effect = Exception("Domo connection failed")
        
        result = comparator.compare_schemas('test_dataset_123', 'TEST_TABLE')
        
        assert result['error'] is True
        assert len(comparator.errors) > 0
    
    def test_types_compatible(self):
        """Test data type compatibility checking."""
        comparator = DatasetComparator()
        
        # Test compatible types
        assert comparator._types_compatible('STRING', 'VARCHAR') is True
        assert comparator._types_compatible('LONG', 'INTEGER') is True
        assert comparator._types_compatible('DOUBLE', 'FLOAT') is True
        assert comparator._types_compatible('DATETIME', 'TIMESTAMP') is True
        
        # Test incompatible types (should return True as default)
        assert comparator._types_compatible('UNKNOWN_TYPE', 'SOME_TYPE') is True


class TestRowCountComparison:
    """Test row count comparison functionality."""
    
    def test_compare_row_counts_match(self):
        """Test row count comparison when counts match."""
        comparator = DatasetComparator()
        
        # Mock Domo query
        comparator.domo_handler.query_dataset.return_value = {
            'rows': [[1000]]
        }
        
        # Mock Snowflake query
        sf_result = Mock()
        sf_result.to_pandas.return_value = pd.DataFrame([{'ROW_COUNT': 1000}])
        comparator.snowflake_handler.execute_query.return_value = sf_result
        
        result = comparator.compare_row_counts('test_dataset_123', 'test_table')
        
        assert result['domo_rows'] == 1000
        assert result['snowflake_rows'] == 1000
        assert result['difference'] == 0
        assert result['match'] is True
    
    def test_compare_row_counts_small_difference(self):
        """Test row count comparison with small negligible difference."""
        comparator = DatasetComparator()
        
        # Mock Domo query
        comparator.domo_handler.query_dataset.return_value = {
            'rows': [[10000]]
        }
        
        # Mock Snowflake query (small difference)
        sf_result = Mock()
        sf_result.to_pandas.return_value = pd.DataFrame([{'ROW_COUNT': 10005}])
        comparator.snowflake_handler.execute_query.return_value = sf_result
        
        result = comparator.compare_row_counts('test_dataset_123', 'test_table')
        
        assert result['difference'] == 5
        assert result['match'] is False
        assert result['negligible_analysis']['is_negligible'] is True
    
    def test_analyze_row_count_difference_empty_datasets(self):
        """Test analysis of row count difference for empty datasets."""
        comparator = DatasetComparator()
        
        analysis = comparator._analyze_row_count_difference(0, 0)
        
        assert analysis['is_negligible'] is True
        assert analysis['reason'] == 'Both datasets are empty'
        assert analysis['percentage'] == 0.0
    
    def test_analyze_row_count_difference_one_empty(self):
        """Test analysis when one dataset is empty."""
        comparator = DatasetComparator()
        
        analysis = comparator._analyze_row_count_difference(0, 1000)
        
        assert analysis['is_negligible'] is False
        assert analysis['reason'] == 'One dataset is empty'
        assert analysis['percentage'] == 100.0
    
    def test_analyze_row_count_difference_significant(self):
        """Test analysis of significant row count difference."""
        comparator = DatasetComparator()
        
        analysis = comparator._analyze_row_count_difference(1000, 1500)
        
        assert analysis['is_negligible'] is False
        assert 'Significant difference' in analysis['reason']


class TestDataComparison:
    """Test data sample comparison functionality."""
    
    @patch('tools.dataset_comparator.datacompy.Compare')
    def test_compare_data_samples_success(self, mock_datacompy, mock_polars_dataframe, mock_pandas_dataframe):
        """Test successful data sample comparison."""
        comparator = DatasetComparator()
        
        # Mock Domo data extraction
        comparator.domo_handler.query_dataset.return_value = {'rows': [[1000]]}
        comparator.domo_handler.extract_data.return_value = mock_polars_dataframe
        
        # Mock Snowflake data
        sf_result = Mock()
        sf_result.to_pandas.return_value = mock_pandas_dataframe
        comparator.snowflake_handler.execute_query.return_value = sf_result
        
        # Mock datacompy comparison
        mock_comparison = Mock()
        mock_comparison.matches.return_value = True
        mock_comparison.df1_unq_rows = []
        mock_comparison.df2_unq_rows = []
        mock_comparison.column_stats = None
        mock_comparison.report.return_value = "Comparison report"
        mock_datacompy.return_value = mock_comparison
        
        result = comparator.compare_data_samples(
            'test_dataset_123', 
            'test_table', 
            ['customer_id', 'order_date'],
            sample_size=1000
        )
        
        assert result['sample_size'] == 1000
        assert result['data_match'] is True
        assert 'report_file' in result
    
    def test_compare_data_samples_domo_failure(self):
        """Test data comparison when Domo data extraction fails."""
        comparator = DatasetComparator()
        
        # Make Domo extraction fail
        comparator.domo_handler.query_dataset.side_effect = Exception("Domo query failed")
        
        result = comparator.compare_data_samples(
            'test_dataset_123',
            'test_table',
            ['customer_id']
        )
        
        assert result['error'] is True
        assert len(comparator.errors) > 0
    
    def test_compare_data_samples_with_transformation(self, mock_polars_dataframe, mock_pandas_dataframe):
        """Test data comparison with column name transformation."""
        comparator = DatasetComparator()
        
        # Mock successful data extraction
        comparator.domo_handler.query_dataset.return_value = {'rows': [[1000]]}
        comparator.domo_handler.extract_data.return_value = mock_polars_dataframe
        
        sf_result = Mock()
        sf_result.to_pandas.return_value = mock_pandas_dataframe
        comparator.snowflake_handler.execute_query.return_value = sf_result
        
        with patch('tools.dataset_comparator.transform_column_name') as mock_transform:
            mock_transform.side_effect = lambda x: x.upper()
            
            with patch('tools.dataset_comparator.datacompy.Compare') as mock_datacompy:
                mock_comparison = Mock()
                mock_comparison.matches.return_value = True
                mock_comparison.df1_unq_rows = []
                mock_comparison.df2_unq_rows = []
                mock_comparison.report.return_value = "Report"
                mock_datacompy.return_value = mock_comparison
                
                result = comparator.compare_data_samples(
                    'test_dataset_123',
                    'test_table',
                    ['customer_id'],
                    transform_names=True
                )
                
                assert result['transform_applied'] is True


class TestReportGeneration:
    """Test report generation functionality."""
    
    @patch.object(DatasetComparator, 'setup_connections')
    @patch.object(DatasetComparator, 'compare_schemas')
    @patch.object(DatasetComparator, 'compare_row_counts')
    @patch.object(DatasetComparator, 'compare_data_samples')
    def test_generate_report_success(self, mock_data_comp, mock_row_comp, mock_schema_comp, mock_setup):
        """Test successful report generation."""
        mock_setup.return_value = True
        mock_schema_comp.return_value = {'schema_match': True, 'error': False}
        mock_row_comp.return_value = {'match': True, 'negligible_analysis': {'is_negligible': True}}
        mock_data_comp.return_value = {'data_match': True, 'error': False}
        
        comparator = DatasetComparator()
        comparator._domo_connected = True
        comparator._snowflake_connected = True
        
        result = comparator.generate_report(
            'test_dataset_123',
            'test_table',
            ['customer_id']
        )
        
        assert result['domo_dataset_id'] == 'test_dataset_123'
        assert result['snowflake_table'] == 'test_table'
        assert result['overall_match'] is True
        assert 'timestamp' in result
    
    def test_generate_report_connection_failure(self):
        """Test report generation when connections fail."""
        comparator = DatasetComparator()
        
        with patch.object(comparator, 'setup_connections', return_value=False):
            result = comparator.generate_report(
                'test_dataset_123',
                'test_table',
                ['customer_id']
            )
            
            assert result['overall_match'] is False
            assert len(result['errors']) > 0
    
    def test_generate_report_with_errors(self):
        """Test report generation when individual comparisons have errors."""
        comparator = DatasetComparator()
        
        with patch.object(comparator, 'setup_connections', return_value=True):
            with patch.object(comparator, 'compare_schemas', return_value={'error': True}):
                with patch.object(comparator, 'compare_row_counts', return_value={'match': False}):
                    with patch.object(comparator, 'compare_data_samples', return_value={'error': True}):
                        
                        result = comparator.generate_report(
                            'test_dataset_123',
                            'test_table',
                            ['customer_id']
                        )
                        
                        assert result['overall_match'] is False


class TestSpreadsheetComparison:
    """Test spreadsheet-based comparison functionality."""
    
    @patch('tools.dataset_comparator.GoogleSheets')
    def test_compare_from_spreadsheet_success(self, mock_gsheets_class, mock_google_sheets_data):
        """Test successful comparison from spreadsheet."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        comparator = DatasetComparator()
        
        with patch.object(comparator, 'setup_connections', return_value=True):
            with patch.object(comparator, 'generate_report') as mock_generate:
                mock_generate.return_value = {
                    'overall_match': True,
                    'errors': []
                }
                
                result = comparator.compare_from_spreadsheet(
                    'test_spreadsheet_id',
                    'Test_Sheet',
                    '/fake/credentials.json'
                )
                
                assert result['success'] > 0
                assert result['total'] > 0
    
    @patch('tools.dataset_comparator.GoogleSheets')
    def test_compare_from_spreadsheet_no_data(self, mock_gsheets_class):
        """Test comparison from spreadsheet with no data."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = []
        mock_gsheets_class.return_value = mock_gsheets
        
        comparator = DatasetComparator()
        
        result = comparator.compare_from_spreadsheet(
            'test_spreadsheet_id',
            'Test_Sheet',
            '/fake/credentials.json'
        )
        
        assert result['success'] == 0
        assert result['failed'] == 0
        assert result['total'] == 0
        assert len(result['errors']) > 0


class TestInventoryComparison:
    """Test inventory-based comparison functionality."""
    
    @patch('tools.dataset_comparator.GoogleSheets')
    @patch.dict('os.environ', {
        'MIGRATION_SPREADSHEET_ID': 'test_id',
                    'INTERMEDIATE_MODELS_SHEET_NAME': 'Inventory'
    })
    def test_compare_from_inventory_success(self, mock_gsheets_class):
        """Test successful comparison from inventory."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = [
            ['Output ID', 'Model Name', 'Key Columns'],
            ['dataset_001', 'sales_table', 'id,date'],
            ['dataset_002', 'customer_table', 'customer_id']
        ]
        mock_gsheets_class.return_value = mock_gsheets
        
        comparator = DatasetComparator()
        
        with patch.object(comparator, 'setup_connections', return_value=True):
            with patch.object(comparator, 'generate_report') as mock_generate:
                mock_generate.return_value = {
                    'overall_match': True,
                    'errors': []
                }
                
                result = comparator.compare_from_inventory('/fake/credentials.json')
                
                assert result['success'] >= 0
                assert result['total'] >= 0


class TestPrintReport:
    """Test report printing functionality."""
    
    def test_print_report(self, capsys):
        """Test report printing output."""
        comparator = DatasetComparator()
        
        sample_report = {
            'domo_dataset_id': 'test_dataset_123',
            'snowflake_table': 'test_table',
            'key_columns': ['customer_id'],
            'overall_match': True,
            'timestamp': '2024-01-01T00:00:00',
            'transform_applied': False,
            'errors': [],
            'schema_comparison': {
                'domo_columns': 3,
                'snowflake_columns': 3,
                'schema_match': True,
                'missing_in_snowflake': [],
                'extra_in_snowflake': [],
                'type_mismatches': []
            },
            'row_count_comparison': {
                'domo_rows': 1000,
                'snowflake_rows': 1000,
                'difference': 0,
                'negligible_analysis': {'is_negligible': True, 'reason': 'Perfect match'}
            },
            'data_comparison': {
                'sample_size': 1000,
                'domo_sample_rows': 1000,
                'snowflake_sample_rows': 1000,
                'data_match': True,
                'missing_in_snowflake': 0,
                'extra_in_snowflake': 0,
                'rows_with_differences': 0,
                'report_file': 'test_report.txt'
            }
        }
        
        comparator.print_report(sample_report)
        
        captured = capsys.readouterr()
        assert "DOMO vs SNOWFLAKE COMPARISON REPORT" in captured.out
        assert "✅ PERFECT MATCH" in captured.out
        assert "test_dataset_123" in captured.out
        assert "test_table" in captured.out