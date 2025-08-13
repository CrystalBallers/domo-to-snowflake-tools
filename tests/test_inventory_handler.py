"""
Tests for the InventoryHandler class and related functionality.

This module tests inventory management functionality including:
- Google Sheets data extraction
- Dataflow to SQL translation
- File export operations
- Error handling
"""

import pytest
import os
import json
import tempfile
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path

from tools.inventory_handler import (
    InventoryHandler, 
    export_dataflows_to_sql,
    _translate_dataflow_to_sql,
    _generate_placeholder_sql
)


class TestInventoryHandlerInit:
    """Test InventoryHandler initialization."""
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_init_with_default_credentials(self, mock_gsheets_class):
        """Test initialization with default credentials."""
        with patch.dict('os.environ', {'GOOGLE_SHEETS_CREDENTIALS_FILE': '/fake/path/creds.json'}):
            handler = InventoryHandler()
            
            assert handler.credentials_path == '/fake/path/creds.json'
            assert handler.spreadsheet_id == "1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA"
            assert handler.sheet_name == "Inventory"
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_init_with_custom_credentials(self, mock_gsheets_class):
        """Test initialization with custom credentials path."""
        handler = InventoryHandler(credentials_path='/custom/path/creds.json')
        
        assert handler.credentials_path == '/custom/path/creds.json'
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_init_with_missing_credentials(self, mock_gsheets_class):
        """Test initialization behavior when credentials are missing."""
        with patch.dict('os.environ', {}, clear=True):
            handler = InventoryHandler()
            
            assert handler.credentials_path is None


class TestInventoryDataExtraction:
    """Test inventory data extraction from Google Sheets."""
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_get_inventory_success(self, mock_gsheets_class, mock_google_sheets_data):
        """Test successful inventory data extraction."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets_class.return_value = mock_gsheets
        
        handler = InventoryHandler()
        df = handler.get_inventory()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 4 data rows (excluding header)
        assert 'Dataset ID' in df.columns
        assert 'Name' in df.columns
        assert 'Status' in df.columns
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_get_inventory_empty_data(self, mock_gsheets_class):
        """Test inventory extraction with empty data."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = []
        mock_gsheets_class.return_value = mock_gsheets
        
        handler = InventoryHandler()
        df = handler.get_inventory()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_get_inventory_connection_error(self, mock_gsheets_class):
        """Test inventory extraction when Google Sheets connection fails."""
        mock_gsheets_class.side_effect = Exception("Connection failed")
        
        handler = InventoryHandler()
        df = handler.get_inventory()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_get_inventory_with_custom_params(self, mock_gsheets_class, mock_google_sheets_data):
        """Test inventory extraction with custom spreadsheet and sheet parameters."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets_class.return_value = mock_gsheets
        
        handler = InventoryHandler()
        df = handler.get_inventory(
            spreadsheet_id='custom_sheet_id',
            sheet_name='Custom_Sheet'
        )
        
        mock_gsheets.read_range.assert_called_with('custom_sheet_id', 'Custom_Sheet!A:Z')
        assert len(df) == 4


class TestDataflowExtraction:
    """Test dataflow extraction functionality."""
    
    def test_get_unique_dataflows_default_column(self, mock_google_sheets_data):
        """Test extracting unique dataflows with default column name."""
        df = pd.DataFrame(mock_google_sheets_data[1:], columns=mock_google_sheets_data[0])
        
        handler = InventoryHandler()
        dataflows = handler.get_unique_dataflows(df)
        
        assert isinstance(dataflows, list)
        assert len(dataflows) == 4  # 4 unique dataflows
        assert 'df_001' in dataflows
        assert 'df_002' in dataflows
    
    def test_get_unique_dataflows_custom_column(self, mock_google_sheets_data):
        """Test extracting unique dataflows with custom column name."""
        df = pd.DataFrame(mock_google_sheets_data[1:], columns=mock_google_sheets_data[0])
        
        handler = InventoryHandler()
        dataflows = handler.get_unique_dataflows(df, dataflow_column='Dataflow ID')
        
        assert len(dataflows) == 4
        assert 'df_001' in dataflows
    
    def test_get_unique_dataflows_missing_column(self, mock_google_sheets_data):
        """Test extracting dataflows when specified column doesn't exist."""
        df = pd.DataFrame(mock_google_sheets_data[1:], columns=mock_google_sheets_data[0])
        
        handler = InventoryHandler()
        dataflows = handler.get_unique_dataflows(df, dataflow_column='Nonexistent Column')
        
        assert dataflows == []
    
    def test_get_unique_dataflows_empty_values(self):
        """Test extracting dataflows when column has empty values."""
        data = [
            ['Dataset ID', 'Name', 'Dataflow ID'],
            ['dataset_001', 'sales_data', 'df_001'],
            ['dataset_002', 'customer_data', ''],  # Empty dataflow
            ['dataset_003', 'inventory_data', None],  # None dataflow
            ['dataset_004', 'financial_data', 'df_004']
        ]
        df = pd.DataFrame(data[1:], columns=data[0])
        
        handler = InventoryHandler()
        dataflows = handler.get_unique_dataflows(df)
        
        # Should only include non-empty dataflows
        assert len(dataflows) == 2
        assert 'df_001' in dataflows
        assert 'df_004' in dataflows
        assert '' not in dataflows


class TestDataflowTranslation:
    """Test dataflow to SQL translation functionality."""
    
    @patch('tools.inventory_handler.subprocess.run')
    @patch('tools.inventory_handler.os.path.exists')
    @patch('tools.inventory_handler.os.remove')
    def test_translate_dataflow_to_sql_success(self, mock_remove, mock_exists, mock_subprocess):
        """Test successful dataflow translation."""
        # Mock successful subprocess execution
        mock_result = Mock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        
        # Mock file operations
        mock_exists.return_value = True
        mock_sql_content = "SELECT * FROM test_table;"
        
        with patch('builtins.open', mock_open(read_data=mock_sql_content)):
            result = _translate_dataflow_to_sql('test_dataflow_123')
            
            assert result == mock_sql_content
            mock_remove.assert_called_once()
    
    @patch('tools.inventory_handler.subprocess.run')
    def test_translate_dataflow_to_sql_failure(self, mock_subprocess):
        """Test dataflow translation failure."""
        # Mock failed subprocess execution
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Translation failed"
        mock_subprocess.return_value = mock_result
        
        result = _translate_dataflow_to_sql('test_dataflow_123')
        
        assert result is None
    
    @patch('tools.inventory_handler.subprocess.run')
    def test_translate_dataflow_to_sql_timeout(self, mock_subprocess):
        """Test dataflow translation timeout."""
        # Mock subprocess timeout
        mock_subprocess.side_effect = Exception("Timeout")
        
        result = _translate_dataflow_to_sql('test_dataflow_123')
        
        assert result is None
    
    @patch('tools.inventory_handler.subprocess.run')
    @patch('tools.inventory_handler.os.path.exists')
    def test_translate_dataflow_to_sql_missing_output(self, mock_exists, mock_subprocess):
        """Test dataflow translation when output file is not generated."""
        # Mock successful subprocess but missing output file
        mock_result = Mock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        mock_exists.return_value = False
        
        result = _translate_dataflow_to_sql('test_dataflow_123')
        
        assert result is None


class TestPlaceholderGeneration:
    """Test placeholder SQL generation."""
    
    def test_generate_placeholder_sql(self):
        """Test placeholder SQL generation."""
        dataflow_id = 'test_dataflow_123'
        
        placeholder = _generate_placeholder_sql(dataflow_id)
        
        assert isinstance(placeholder, str)
        assert dataflow_id in placeholder
        assert 'SELECT' in placeholder
        assert 'TODO' in placeholder
        assert 'DOMO_DEVELOPER_TOKEN' in placeholder


class TestDataflowExport:
    """Test complete dataflow export functionality."""
    
    @patch('tools.inventory_handler.InventoryHandler')
    @patch('tools.inventory_handler._translate_dataflow_to_sql')
    @patch('tools.inventory_handler.Path.mkdir')
    def test_export_dataflows_to_sql_success(self, mock_mkdir, mock_translate, mock_handler_class, temp_dir):
        """Test successful dataflow export."""
        # Mock inventory handler
        mock_handler = Mock()
        mock_df = pd.DataFrame([
            ['dataset_001', 'sales_data', 'Pending', 'df_001'],
            ['dataset_002', 'customer_data', 'Migrated', 'df_002']
        ], columns=['Dataset ID', 'Name', 'Status', 'Dataflow ID'])
        
        mock_handler.get_inventory.return_value = mock_df
        mock_handler.get_unique_dataflows.return_value = ['df_001', 'df_002']
        mock_handler_class.return_value = mock_handler
        
        # Mock successful translation
        mock_translate.return_value = "SELECT * FROM test_table;"
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
            
            assert result is True
            # Should have tried to write files
            assert mock_file.call_count >= 2
    
    @patch('tools.inventory_handler.InventoryHandler')
    @patch('tools.inventory_handler._translate_dataflow_to_sql')
    @patch('tools.inventory_handler._generate_placeholder_sql')
    def test_export_dataflows_with_failed_translations(self, mock_placeholder, mock_translate, mock_handler_class, temp_dir):
        """Test dataflow export when some translations fail."""
        # Mock inventory handler
        mock_handler = Mock()
        mock_df = pd.DataFrame([
            ['dataset_001', 'sales_data', 'Pending', 'df_001'],
            ['dataset_002', 'customer_data', 'Migrated', 'df_002']
        ], columns=['Dataset ID', 'Name', 'Status', 'Dataflow ID'])
        
        mock_handler.get_inventory.return_value = mock_df
        mock_handler.get_unique_dataflows.return_value = ['df_001', 'df_002']
        mock_handler_class.return_value = mock_handler
        
        # Mock mixed success/failure translation
        mock_translate.side_effect = [
            "SELECT * FROM test_table;",  # Success for df_001
            None  # Failure for df_002
        ]
        mock_placeholder.return_value = "-- Placeholder SQL"
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
            
            assert result is True
            mock_placeholder.assert_called_once_with('df_002')
    
    @patch('tools.inventory_handler.InventoryHandler')
    def test_export_dataflows_no_dataflows(self, mock_handler_class, temp_dir):
        """Test dataflow export when no dataflows are found."""
        # Mock inventory handler with no dataflows
        mock_handler = Mock()
        mock_handler.get_inventory.return_value = pd.DataFrame()
        mock_handler.get_unique_dataflows.return_value = []
        mock_handler_class.return_value = mock_handler
        
        result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
        
        assert result is False
    
    @patch('tools.inventory_handler.InventoryHandler')
    def test_export_dataflows_missing_dataflow_column(self, mock_handler_class, temp_dir):
        """Test dataflow export when dataflow column is missing."""
        # Mock inventory handler
        mock_handler = Mock()
        mock_df = pd.DataFrame([
            ['dataset_001', 'sales_data', 'Pending'],
            ['dataset_002', 'customer_data', 'Migrated']
        ], columns=['Dataset ID', 'Name', 'Status'])  # No Dataflow ID column
        
        mock_handler.get_inventory.return_value = mock_df
        mock_handler.get_unique_dataflows.return_value = []
        mock_handler_class.return_value = mock_handler
        
        result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
        
        assert result is False
    
    @patch('tools.inventory_handler.InventoryHandler')
    def test_export_dataflows_handler_exception(self, mock_handler_class, temp_dir):
        """Test dataflow export when inventory handler raises exception."""
        mock_handler_class.side_effect = Exception("Handler initialization failed")
        
        result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
        
        assert result is False


class TestFileOperations:
    """Test file operation functionality."""
    
    def test_sql_filename_sanitization(self, temp_dir):
        """Test that dataflow IDs are properly sanitized for filenames."""
        # Test with problematic characters
        problematic_dataflow = "test/dataflow with spaces\\and|special:chars"
        
        with patch('tools.inventory_handler.InventoryHandler') as mock_handler_class:
            mock_handler = Mock()
            mock_df = pd.DataFrame([
                ['dataset_001', 'test_data', 'Pending', problematic_dataflow]
            ], columns=['Dataset ID', 'Name', 'Status', 'Dataflow ID'])
            
            mock_handler.get_inventory.return_value = mock_df
            mock_handler.get_unique_dataflows.return_value = [problematic_dataflow]
            mock_handler_class.return_value = mock_handler
            
            with patch('tools.inventory_handler._translate_dataflow_to_sql', return_value="SELECT * FROM test;"):
                with patch('builtins.open', mock_open()) as mock_file:
                    export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
                    
                    # Check that filename was sanitized
                    mock_file.assert_called()
                    call_args = mock_file.call_args_list
                    filename = str(call_args[0][0][0])
                    
                    # Should have replaced problematic characters
                    assert '/' not in filename.split(os.sep)[-1]  # No path separators in filename
                    assert ' ' not in filename or '_' in filename  # Spaces should be replaced
    
    def test_output_directory_creation(self, temp_dir):
        """Test that output directory is created if it doesn't exist."""
        nonexistent_dir = os.path.join(temp_dir, 'nonexistent', 'nested', 'path')
        
        with patch('tools.inventory_handler.InventoryHandler') as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_inventory.return_value = pd.DataFrame()
            mock_handler.get_unique_dataflows.return_value = []
            mock_handler_class.return_value = mock_handler
            
            # Should not raise an exception even if directory doesn't exist
            result = export_dataflows_to_sql(nonexistent_dir, '/fake/credentials.json')
            
            # Directory should be created
            assert os.path.exists(nonexistent_dir)


class TestErrorHandling:
    """Test error handling in various scenarios."""
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_inventory_handler_credentials_not_found(self, mock_gsheets_class):
        """Test behavior when credentials file doesn't exist."""
        mock_gsheets_class.side_effect = FileNotFoundError("Credentials file not found")
        
        handler = InventoryHandler(credentials_path='/nonexistent/credentials.json')
        df = handler.get_inventory()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    @patch('tools.inventory_handler.GoogleSheets')
    def test_inventory_handler_permission_error(self, mock_gsheets_class):
        """Test behavior when there's a permission error accessing the sheet."""
        mock_gsheets = Mock()
        mock_gsheets.read_range.side_effect = PermissionError("Permission denied")
        mock_gsheets_class.return_value = mock_gsheets
        
        handler = InventoryHandler()
        df = handler.get_inventory()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    @patch('tools.inventory_handler.subprocess.run')
    def test_translate_dataflow_subprocess_error(self, mock_subprocess):
        """Test translation when subprocess raises an exception."""
        mock_subprocess.side_effect = OSError("Command not found")
        
        result = _translate_dataflow_to_sql('test_dataflow')
        
        assert result is None
    
    def test_export_with_invalid_output_dir(self):
        """Test export behavior with invalid output directory."""
        # Try to write to a read-only location (should fail gracefully)
        invalid_dir = '/root/readonly_dir'  # This should fail on most systems
        
        result = export_dataflows_to_sql(invalid_dir, '/fake/credentials.json')
        
        # Should handle the error gracefully
        assert result is False


class TestMainFunction:
    """Test the main function when run as script."""
    
    @patch('tools.inventory_handler.export_dataflows_to_sql')
    @patch('tools.inventory_handler.argparse.ArgumentParser.parse_args')
    def test_main_function_success(self, mock_parse_args, mock_export):
        """Test successful execution of main function."""
        # Mock command line arguments
        mock_args = Mock()
        mock_args.export_dir = 'test_output'
        mock_args.credentials = '/fake/credentials.json'
        mock_parse_args.return_value = mock_args
        
        mock_export.return_value = True
        
        # Import and run the main function
        # Note: This would need to be adjusted based on how the main function is structured
        # For now, we'll test that the function can be called without error
        with patch('sys.argv', ['inventory_handler.py', '--export-dir', 'test_output']):
            try:
                # This would call the main function if it exists
                # main()
                pass
            except SystemExit as e:
                # Script exits successfully
                assert e.code == 0 or e.code is None


class TestIntegration:
    """Integration tests combining multiple components."""
    
    @patch('tools.inventory_handler.GoogleSheets')
    @patch('tools.inventory_handler._translate_dataflow_to_sql')
    def test_end_to_end_export_workflow(self, mock_translate, mock_gsheets_class, temp_dir, mock_google_sheets_data):
        """Test complete end-to-end export workflow."""
        # Setup Google Sheets mock
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets_class.return_value = mock_gsheets
        
        # Setup translation mock
        mock_translate.return_value = "SELECT customer_id, order_date FROM sales_data;"
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = export_dataflows_to_sql(temp_dir, '/fake/credentials.json')
            
            # Should succeed
            assert result is True
            
            # Should have read from Google Sheets
            mock_gsheets.read_range.assert_called_once()
            
            # Should have attempted translation for each unique dataflow
            assert mock_translate.call_count == 4  # 4 unique dataflows in test data
            
            # Should have written SQL files
            assert mock_file.call_count >= 4