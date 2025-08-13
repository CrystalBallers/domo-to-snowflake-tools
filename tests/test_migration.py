"""
Tests for data migration functionality.

This module tests data migration from Domo to Snowflake including:
- Single dataset migration
- Batch migration operations  
- Spreadsheet-based migration
- Connection management
- Error handling and recovery
"""

import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock, mock_open
import pandas as pd

from tools.domo_to_snowflake import (
    migrate_dataset,
    batch_migrate_datasets, 
    migrate_from_spreadsheet,
    MigrationManager
)


class TestSingleDatasetMigration:
    """Test single dataset migration functionality."""
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_success(self, mock_sf_class, mock_domo_class):
        """Test successful single dataset migration."""
        # Mock Domo handler
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.extract_data.return_value = Mock()  # Mock Polars DataFrame
        mock_domo_class.return_value = mock_domo
        
        # Mock Snowflake handler
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf.load_data.return_value = True
        mock_sf_class.return_value = mock_sf
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is True
        mock_domo.setup_connection.assert_called_once()
        mock_sf.setup_connection.assert_called_once()
        mock_domo.extract_data.assert_called_once_with('test_dataset_123')
        mock_sf.load_data.assert_called_once()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_domo_connection_failure(self, mock_sf_class, mock_domo_class):
        """Test migration failure when Domo connection fails."""
        # Mock Domo connection failure
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = False
        mock_domo_class.return_value = mock_domo
        
        # Mock Snowflake handler (won't be reached)
        mock_sf = Mock()
        mock_sf_class.return_value = mock_sf
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is False
        mock_domo.setup_connection.assert_called_once()
        mock_sf.setup_connection.assert_not_called()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_snowflake_connection_failure(self, mock_sf_class, mock_domo_class):
        """Test migration failure when Snowflake connection fails."""
        # Mock Domo handler success
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo_class.return_value = mock_domo
        
        # Mock Snowflake connection failure
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = False
        mock_sf_class.return_value = mock_sf
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is False
        mock_domo.setup_connection.assert_called_once()
        mock_sf.setup_connection.assert_called_once()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_data_extraction_failure(self, mock_sf_class, mock_domo_class):
        """Test migration failure when data extraction fails."""
        # Mock Domo handler with extraction failure
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.extract_data.return_value = None  # Extraction failure
        mock_domo_class.return_value = mock_domo
        
        # Mock Snowflake handler
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf_class.return_value = mock_sf
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is False
        mock_domo.extract_data.assert_called_once()
        mock_sf.load_data.assert_not_called()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_data_loading_failure(self, mock_sf_class, mock_domo_class):
        """Test migration failure when data loading fails."""
        # Mock Domo handler success
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.extract_data.return_value = Mock()  # Mock data
        mock_domo_class.return_value = mock_domo
        
        # Mock Snowflake handler with loading failure
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf.load_data.return_value = False  # Loading failure
        mock_sf_class.return_value = mock_sf
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is False
        mock_sf.load_data.assert_called_once()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migrate_dataset_exception_handling(self, mock_sf_class, mock_domo_class):
        """Test migration handles exceptions gracefully."""
        # Mock Domo handler that raises exception
        mock_domo = Mock()
        mock_domo.setup_connection.side_effect = Exception("Unexpected error")
        mock_domo_class.return_value = mock_domo
        
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        assert result is False


class TestBatchMigration:
    """Test batch migration functionality."""
    
    def test_batch_migrate_datasets_success(self, temp_dir, sample_json_mapping):
        """Test successful batch migration."""
        # Create test JSON file
        json_file = os.path.join(temp_dir, 'test_mapping.json')
        with open(json_file, 'w') as f:
            json.dump(sample_json_mapping, f)
        
        with patch('tools.domo_to_snowflake.migrate_dataset') as mock_migrate:
            mock_migrate.return_value = True
            
            result = batch_migrate_datasets(json_file)
            
            assert result is True
            # Should have called migrate_dataset for each mapping
            assert mock_migrate.call_count == len(sample_json_mapping)
    
    def test_batch_migrate_datasets_partial_failure(self, temp_dir, sample_json_mapping):
        """Test batch migration with some failures."""
        # Create test JSON file
        json_file = os.path.join(temp_dir, 'test_mapping.json')
        with open(json_file, 'w') as f:
            json.dump(sample_json_mapping, f)
        
        with patch('tools.domo_to_snowflake.migrate_dataset') as mock_migrate:
            # Mock mixed success/failure
            mock_migrate.side_effect = [True, False, True]  # 2 success, 1 failure
            
            result = batch_migrate_datasets(json_file)
            
            # Should return False if any migration fails
            assert result is False
            assert mock_migrate.call_count == 3
    
    def test_batch_migrate_datasets_file_not_found(self):
        """Test batch migration when JSON file doesn't exist."""
        result = batch_migrate_datasets('/nonexistent/file.json')
        
        assert result is False
    
    def test_batch_migrate_datasets_invalid_json(self, temp_dir):
        """Test batch migration with invalid JSON file."""
        # Create invalid JSON file
        json_file = os.path.join(temp_dir, 'invalid.json')
        with open(json_file, 'w') as f:
            f.write('{"invalid": json content}')
        
        result = batch_migrate_datasets(json_file)
        
        assert result is False
    
    def test_batch_migrate_datasets_empty_mapping(self, temp_dir):
        """Test batch migration with empty mapping."""
        # Create empty JSON file
        json_file = os.path.join(temp_dir, 'empty.json')
        with open(json_file, 'w') as f:
            json.dump({}, f)
        
        result = batch_migrate_datasets(json_file)
        
        # Empty mapping should still be considered successful
        assert result is True


class TestSpreadsheetMigration:
    """Test spreadsheet-based migration functionality."""
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_migrate_from_spreadsheet_success(self, mock_migrate, mock_gsheets_class, mock_google_sheets_data):
        """Test successful migration from spreadsheet."""
        # Mock Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        # Mock successful migrations
        mock_migrate.return_value = True
        
        result = migrate_from_spreadsheet()
        
        assert result is True
        # Should migrate pending datasets (2 pending in test data)
        pending_count = sum(1 for row in mock_google_sheets_data[1:] if row[2] == 'Pending')
        assert mock_migrate.call_count == pending_count
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_migrate_from_spreadsheet_with_status_updates(self, mock_migrate, mock_gsheets_class, mock_google_sheets_data):
        """Test migration with status updates in spreadsheet."""
        # Mock Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        # Mock successful migrations
        mock_migrate.return_value = True
        
        result = migrate_from_spreadsheet()
        
        assert result is True
        # Should have updated status for successful migrations
        assert mock_gsheets.write_range.call_count > 0
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    def test_migrate_from_spreadsheet_no_data(self, mock_gsheets_class):
        """Test migration when spreadsheet has no data."""
        # Mock empty Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = []
        mock_gsheets_class.return_value = mock_gsheets
        
        result = migrate_from_spreadsheet()
        
        assert result is False
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    def test_migrate_from_spreadsheet_missing_columns(self, mock_gsheets_class):
        """Test migration when required columns are missing."""
        # Mock Google Sheets with missing columns
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = [
            ['Some Column', 'Another Column'],  # Missing required columns
            ['value1', 'value2']
        ]
        mock_gsheets_class.return_value = mock_gsheets
        
        result = migrate_from_spreadsheet()
        
        assert result is False
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_migrate_from_spreadsheet_partial_failures(self, mock_migrate, mock_gsheets_class, mock_google_sheets_data):
        """Test migration with some dataset failures."""
        # Mock Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        # Mock mixed migration results
        mock_migrate.side_effect = [True, False]  # First succeeds, second fails
        
        result = migrate_from_spreadsheet()
        
        # Should still complete even with some failures
        assert result is True  # Or False, depending on implementation
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    def test_migrate_from_spreadsheet_connection_error(self, mock_gsheets_class):
        """Test migration when Google Sheets connection fails."""
        mock_gsheets_class.side_effect = Exception("Connection failed")
        
        result = migrate_from_spreadsheet()
        
        assert result is False
    
    @patch('tools.domo_to_snowflake.GoogleSheets')
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_migrate_from_spreadsheet_custom_params(self, mock_migrate, mock_gsheets_class, mock_google_sheets_data):
        """Test migration with custom spreadsheet parameters."""
        # Mock Google Sheets
        mock_gsheets = Mock()
        mock_gsheets.read_range.return_value = mock_google_sheets_data
        mock_gsheets.write_range.return_value = True
        mock_gsheets_class.return_value = mock_gsheets
        
        mock_migrate.return_value = True
        
        result = migrate_from_spreadsheet(
            credentials_path='/custom/path/creds.json',
            spreadsheet_id='custom_sheet_id',
            sheet_name='Custom_Sheet'
        )
        
        assert result is True
        # Verify custom parameters were used
        mock_gsheets.read_range.assert_called_with('custom_sheet_id', 'Custom_Sheet!A:Z')


class TestMigrationManager:
    """Test MigrationManager context manager functionality."""
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migration_manager_context(self, mock_sf_class, mock_domo_class):
        """Test MigrationManager as context manager."""
        # Mock handlers
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo_class.return_value = mock_domo
        
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf_class.return_value = mock_sf
        
        with MigrationManager() as manager:
            assert manager is not None
            assert hasattr(manager, 'domo_handler')
            assert hasattr(manager, 'snowflake_handler')
        
        # Cleanup should be called
        mock_domo.cleanup.assert_called_once()
        mock_sf.cleanup.assert_called_once()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migration_manager_migrate_dataset(self, mock_sf_class, mock_domo_class):
        """Test dataset migration through MigrationManager."""
        # Mock handlers
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.extract_data.return_value = Mock()
        mock_domo_class.return_value = mock_domo
        
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf.load_data.return_value = True
        mock_sf_class.return_value = mock_sf
        
        with MigrationManager() as manager:
            result = manager.migrate_dataset('test_dataset_123', 'test_table')
            
            assert result is True
            mock_domo.extract_data.assert_called_once_with('test_dataset_123')
            mock_sf.load_data.assert_called_once()
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migration_manager_connection_failure(self, mock_sf_class, mock_domo_class):
        """Test MigrationManager with connection failures."""
        # Mock connection failures
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = False
        mock_domo_class.return_value = mock_domo
        
        mock_sf = Mock()
        mock_sf_class.return_value = mock_sf
        
        with pytest.raises(Exception):  # Should raise exception on connection failure
            with MigrationManager() as manager:
                pass
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler')
    def test_migration_manager_exception_in_context(self, mock_sf_class, mock_domo_class):
        """Test MigrationManager cleanup when exception occurs in context."""
        # Mock handlers
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo_class.return_value = mock_domo
        
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf_class.return_value = mock_sf
        
        with pytest.raises(ValueError):
            with MigrationManager() as manager:
                raise ValueError("Test exception")
        
        # Cleanup should still be called
        mock_domo.cleanup.assert_called_once()
        mock_sf.cleanup.assert_called_once()


class TestTableNameProcessing:
    """Test table name processing and sanitization."""
    
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_table_name_sanitization(self, mock_migrate):
        """Test that table names are properly sanitized."""
        mock_migrate.return_value = True
        
        with patch('tools.domo_to_snowflake.GoogleSheets') as mock_gsheets_class:
            mock_gsheets = Mock()
            mock_gsheets.read_range.return_value = [
                ['Dataset ID', 'Name', 'Status'],
                ['dataset_001', 'Sales Data with Spaces', 'Pending'],
                ['dataset_002', 'Customer-Data-With-Dashes', 'Pending']
            ]
            mock_gsheets.write_range.return_value = True
            mock_gsheets_class.return_value = mock_gsheets
            
            migrate_from_spreadsheet()
            
            # Check that migrate_dataset was called with sanitized table names
            calls = mock_migrate.call_args_list
            assert len(calls) == 2
            
            # First call should have spaces converted to underscores and lowercase
            first_call_table = calls[0][0][1]  # Second argument is table name
            assert ' ' not in first_call_table
            assert first_call_table.islower()
            
            # Second call should handle dashes appropriately
            second_call_table = calls[1][0][1]
            assert second_call_table.islower()
    
    def test_table_prefix_handling(self):
        """Test table name prefix handling."""
        with patch.dict('os.environ', {'DOMO_TABLE_PREFIX': 'MIGRATED_'}):
            with patch('tools.domo_to_snowflake.migrate_dataset') as mock_migrate:
                mock_migrate.return_value = True
                
                with patch('tools.domo_to_snowflake.GoogleSheets') as mock_gsheets_class:
                    mock_gsheets = Mock()
                    mock_gsheets.read_range.return_value = [
                        ['Dataset ID', 'Name', 'Status'],
                        ['dataset_001', 'test_table', 'Pending']
                    ]
                    mock_gsheets.write_range.return_value = True
                    mock_gsheets_class.return_value = mock_gsheets
                    
                    migrate_from_spreadsheet()
                    
                    # Check that prefix was applied
                    call_args = mock_migrate.call_args_list[0]
                    table_name = call_args[0][1]
                    assert table_name.startswith('MIGRATED_')


class TestErrorRecovery:
    """Test error recovery and resilience features."""
    
    @patch('tools.domo_to_snowflake.DomoHandler')
    @patch('tools.domo_to_snowflake.SnowflakeHandler') 
    def test_connection_retry_on_failure(self, mock_sf_class, mock_domo_class):
        """Test connection retry logic on temporary failures."""
        # Mock intermittent connection failure
        mock_domo = Mock()
        mock_domo.setup_connection.side_effect = [False, True]  # Fail first, succeed second
        mock_domo_class.return_value = mock_domo
        
        mock_sf = Mock()
        mock_sf.setup_connection.return_value = True
        mock_sf_class.return_value = mock_sf
        
        # This test would need retry logic to be implemented in the actual migration code
        # For now, we just test the current behavior
        result = migrate_dataset('test_dataset_123', 'test_table')
        
        # With current implementation, should fail on first connection attempt
        assert result is False
    
    @patch('tools.domo_to_snowflake.migrate_dataset')
    def test_batch_migration_continues_after_failure(self, mock_migrate, temp_dir):
        """Test that batch migration continues after individual failures."""
        # Create test JSON file
        mapping = {
            'dataset_001': 'table_001',
            'dataset_002': 'table_002', 
            'dataset_003': 'table_003'
        }
        json_file = os.path.join(temp_dir, 'test_mapping.json')
        with open(json_file, 'w') as f:
            json.dump(mapping, f)
        
        # Mock mixed results - second migration fails
        mock_migrate.side_effect = [True, False, True]
        
        result = batch_migrate_datasets(json_file)
        
        # Should attempt all migrations despite middle failure
        assert mock_migrate.call_count == 3
        assert result is False  # Overall result is failure due to one failed migration
    
    def test_data_validation_before_migration(self):
        """Test data validation before attempting migration."""
        # This would test validation logic if implemented
        # For example, checking if dataset exists, has data, etc.
        pass
    
    def test_rollback_on_migration_failure(self):
        """Test rollback functionality when migration fails partway."""
        # This would test rollback logic if implemented
        # For example, cleaning up partially loaded data
        pass


class TestPerformanceOptimizations:
    """Test performance-related functionality."""
    
    @patch('tools.domo_to_snowflake.MigrationManager')
    def test_connection_reuse_in_batch_operations(self, mock_manager_class):
        """Test that connections are reused in batch operations."""
        mock_manager = Mock()
        mock_manager.migrate_dataset.return_value = True
        mock_manager_class.return_value.__enter__.return_value = mock_manager
        
        # This test would verify that batch operations use MigrationManager
        # to reuse connections instead of creating new ones for each dataset
        pass
    
    def test_parallel_migration_capability(self):
        """Test parallel migration capability."""
        # This would test parallel migration if implemented
        pass
    
    def test_large_dataset_handling(self):
        """Test handling of large datasets."""
        # This would test chunking or streaming for large datasets
        pass


class TestLoggingAndMonitoring:
    """Test logging and monitoring functionality."""
    
    @patch('tools.domo_to_snowflake.logging.getLogger')
    def test_migration_logging(self, mock_logger):
        """Test that migration operations are properly logged."""
        mock_log = Mock()
        mock_logger.return_value = mock_log
        
        with patch('tools.domo_to_snowflake.DomoHandler') as mock_domo_class:
            with patch('tools.domo_to_snowflake.SnowflakeHandler') as mock_sf_class:
                # Mock successful migration
                mock_domo = Mock()
                mock_domo.setup_connection.return_value = True
                mock_domo.extract_data.return_value = Mock()
                mock_domo_class.return_value = mock_domo
                
                mock_sf = Mock()
                mock_sf.setup_connection.return_value = True
                mock_sf.load_data.return_value = True
                mock_sf_class.return_value = mock_sf
                
                migrate_dataset('test_dataset_123', 'test_table')
                
                # Verify logging calls were made
                assert mock_log.info.call_count > 0 or mock_log.debug.call_count > 0
    
    def test_progress_reporting(self):
        """Test progress reporting for long-running operations."""
        # This would test progress reporting if implemented
        pass
    
    def test_migration_metrics_collection(self):
        """Test collection of migration metrics."""
        # This would test metrics collection if implemented
        # Examples: rows migrated, time taken, success rate, etc.
        pass