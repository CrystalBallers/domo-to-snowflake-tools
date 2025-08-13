"""
Tests for the main CLI interface (main.py).

This module tests the command-line interface functionality including:
- Argument parsing
- Command execution
- Error handling
- Integration between different commands
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Import the main module
import main


class TestCLIArgumentParsing:
    """Test argument parsing functionality."""
    
    def test_create_parser_basic(self):
        """Test that parser is created with basic structure."""
        parser = main.create_parser()
        assert parser is not None
        assert parser.description == "Domo to Snowflake Migration Tools"
    
    def test_inventory_subcommand_exists(self):
        """Test that inventory subcommand is available."""
        parser = main.create_parser()
        args = parser.parse_args(['inventory', '--test-connection'])
        assert hasattr(args, 'export_dir')
        assert hasattr(args, 'test_connection')
        assert args.test_connection is True
    
    def test_migrate_subcommand_exists(self):
        """Test that migrate subcommand is available."""
        parser = main.create_parser()
        args = parser.parse_args(['migrate', '--test-connection'])
        assert hasattr(args, 'test_connection')
        assert args.test_connection is True
    
    def test_compare_subcommand_exists(self):
        """Test that compare subcommand is available."""
        parser = main.create_parser()
        args = parser.parse_args(['compare', '--test-connection'])
        assert hasattr(args, 'test_connection')
        assert args.test_connection is True
    
    def test_datasets_subcommand_exists(self):
        """Test that datasets subcommand is available."""
        parser = main.create_parser()
        args = parser.parse_args(['datasets', '--test-connection'])
        assert hasattr(args, 'test_connection')
        assert args.test_connection is True


class TestInventoryCommands:
    """Test inventory command functionality."""
    
    @patch('main.test_inventory_connection')
    def test_inventory_test_connection(self, mock_test_conn, mock_args):
        """Test inventory test connection command."""
        mock_test_conn.return_value = True
        args = mock_args(
            command='inventory',
            test_connection=True,
            export_dir='test_dir',
            credentials=None
        )
        
        result = main.handle_inventory_command(args)
        assert result == 0
        mock_test_conn.assert_called_once()
    
    @patch('main.export_dataflows_to_sql')
    @patch('main.test_inventory_connection')
    def test_inventory_export_success(self, mock_test_conn, mock_export, mock_args):
        """Test successful inventory export."""
        mock_test_conn.return_value = True
        mock_export.return_value = True
        
        args = mock_args(
            command='inventory',
            test_connection=False,
            export_dir='test_results/sql/translated',
            credentials=None
        )
        
        result = main.handle_inventory_command(args)
        assert result == 0
        mock_export.assert_called_once_with('test_results/sql/translated', None)
    
    @patch('main.export_dataflows_to_sql')
    @patch('main.test_inventory_connection')
    def test_inventory_export_failure(self, mock_test_conn, mock_export, mock_args):
        """Test inventory export failure."""
        mock_test_conn.return_value = True
        mock_export.return_value = False
        
        args = mock_args(
            command='inventory',
            test_connection=False,
            export_dir='test_results/sql/translated',
            credentials=None
        )
        
        result = main.handle_inventory_command(args)
        assert result == 1
    
    @patch('main.test_inventory_connection')
    def test_inventory_connection_failure(self, mock_test_conn, mock_args):
        """Test inventory connection failure."""
        mock_test_conn.return_value = False
        
        args = mock_args(
            command='inventory',
            test_connection=True,
            export_dir='test_dir',
            credentials=None
        )
        
        result = main.handle_inventory_command(args)
        assert result == 1


class TestMigrationCommands:
    """Test migration command functionality."""
    
    @patch('main.test_migration_connections')
    def test_migration_test_connection(self, mock_test_conn, mock_args):
        """Test migration test connection command."""
        mock_test_conn.return_value = True
        
        args = mock_args(
            command='migrate',
            test_connection=True,
            dataset_id=None,
            target_table=None,
            batch_file=None,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 0
        mock_test_conn.assert_called_once()
    
    @patch('main.migrate_dataset')
    @patch('main.test_migration_connections')
    def test_single_dataset_migration_success(self, mock_test_conn, mock_migrate, mock_args):
        """Test successful single dataset migration."""
        mock_test_conn.return_value = True
        mock_migrate.return_value = True
        
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id='test_dataset_123',
            target_table='test_table',
            batch_file=None,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 0
        mock_migrate.assert_called_once_with('test_dataset_123', 'test_table')
    
    @patch('main.migrate_dataset')
    @patch('main.test_migration_connections')
    def test_single_dataset_migration_failure(self, mock_test_conn, mock_migrate, mock_args):
        """Test failed single dataset migration."""
        mock_test_conn.return_value = True
        mock_migrate.return_value = False
        
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id='test_dataset_123',
            target_table='test_table',
            batch_file=None,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 1
    
    @patch('main.batch_migrate_datasets')
    @patch('main.test_migration_connections')
    def test_batch_migration_success(self, mock_test_conn, mock_batch_migrate, mock_args, temp_dir, sample_json_mapping):
        """Test successful batch migration."""
        mock_test_conn.return_value = True
        mock_batch_migrate.return_value = True
        
        # Create test JSON file
        import json
        json_file = os.path.join(temp_dir, 'test_mapping.json')
        with open(json_file, 'w') as f:
            json.dump(sample_json_mapping, f)
        
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id=None,
            target_table=None,
            batch_file=json_file,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 0
        mock_batch_migrate.assert_called_once_with(json_file)
    
    @patch('main.migrate_from_spreadsheet')
    @patch('main.test_migration_connections')
    def test_spreadsheet_migration_success(self, mock_test_conn, mock_spreadsheet_migrate, mock_args):
        """Test successful spreadsheet migration."""
        mock_test_conn.return_value = True
        mock_spreadsheet_migrate.return_value = True
        
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id=None,
            target_table=None,
            batch_file=None,
            from_spreadsheet=True,
            credentials=None,
            spreadsheet_id=None,
            sheet_name=None
        )
        
        result = main.handle_migrate_command(args)
        assert result == 0


class TestComparisonCommands:
    """Test comparison command functionality."""
    
    @patch('main.DatasetComparator')
    def test_comparison_test_connection(self, mock_comparator_class, mock_args):
        """Test comparison test connection command."""
        mock_comparator = Mock()
        mock_comparator.setup_connections.return_value = True
        mock_comparator_class.return_value = mock_comparator
        
        args = mock_args(
            command='compare',
            test_connection=True,
            domo_dataset_id=None,
            snowflake_table=None,
            key_columns=None
        )
        
        result = main.handle_compare_command(args)
        assert result == 0
        mock_comparator.setup_connections.assert_called_once()
    
    @patch('main.DatasetComparator')
    def test_single_comparison_success(self, mock_comparator_class, mock_args):
        """Test successful single dataset comparison."""
        mock_comparator = Mock()
        mock_comparator.setup_connections.return_value = True
        mock_comparator.generate_report.return_value = {
            'overall_match': True,
            'errors': []
        }
        mock_comparator_class.return_value = mock_comparator
        
        args = mock_args(
            command='compare',
            test_connection=False,
            domo_dataset_id='test_dataset_123',
            snowflake_table='test_table',
            key_columns=['id', 'date'],
            sample_size=1000,
            transform_columns=False,
            from_spreadsheet=False,
            from_inventory=False
        )
        
        result = main.handle_compare_command(args)
        assert result == 0
        mock_comparator.generate_report.assert_called_once()


class TestDatasetCommands:
    """Test dataset command functionality."""
    
    @patch('main.DomoHandler')
    def test_dataset_test_connection(self, mock_domo_class, mock_args):
        """Test dataset test connection command."""
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.test_connection.return_value = True
        mock_domo_class.return_value = mock_domo
        
        args = mock_args(
            command='datasets',
            test_connection=True,
            export_to_spreadsheet=False,
            list_local=False
        )
        
        result = main.handle_datasets_command(args)
        assert result == 0
        mock_domo.test_connection.assert_called_once()
    
    @patch('main.DomoHandler')
    def test_list_local_datasets(self, mock_domo_class, mock_args):
        """Test listing local datasets."""
        mock_domo = Mock()
        mock_domo.setup_connection.return_value = True
        mock_domo.list_all_datasets.return_value = [
            {'id': 'dataset_001', 'name': 'Test Dataset 1'},
            {'id': 'dataset_002', 'name': 'Test Dataset 2'}
        ]
        mock_domo_class.return_value = mock_domo
        
        args = mock_args(
            command='datasets',
            test_connection=False,
            export_to_spreadsheet=False,
            list_local=True
        )
        
        result = main.handle_datasets_command(args)
        assert result == 0
        mock_domo.list_all_datasets.assert_called_once()


class TestMainFunction:
    """Test main function and overall CLI behavior."""
    
    def test_main_with_no_args_shows_help(self, capsys):
        """Test that main with no args shows help."""
        # This would normally call sys.exit, so we need to catch it
        with pytest.raises(SystemExit) as exc_info:
            with patch('sys.argv', ['main.py']):
                main.main()
        
        assert exc_info.value.code == 1
    
    @patch('main.handle_inventory_command')
    def test_main_calls_inventory_handler(self, mock_handler):
        """Test that main calls the correct handler for inventory command."""
        mock_handler.return_value = 0
        
        with patch('sys.argv', ['main.py', 'inventory', '--test-connection']):
            result = main.main()
        
        assert result == 0
        mock_handler.assert_called_once()
    
    @patch('main.handle_migrate_command')
    def test_main_calls_migrate_handler(self, mock_handler):
        """Test that main calls the correct handler for migrate command."""
        mock_handler.return_value = 0
        
        with patch('sys.argv', ['main.py', 'migrate', '--test-connection']):
            result = main.main()
        
        assert result == 0
        mock_handler.assert_called_once()


class TestErrorHandling:
    """Test error handling in CLI commands."""
    
    @patch('main.export_dataflows_to_sql')
    def test_inventory_handles_exceptions(self, mock_export, mock_args):
        """Test that inventory command handles exceptions gracefully."""
        mock_export.side_effect = Exception("Test exception")
        
        args = mock_args(
            command='inventory',
            test_connection=False,
            export_dir='test_dir',
            credentials=None
        )
        
        result = main.handle_inventory_command(args)
        assert result == 1
    
    @patch('main.migrate_dataset')
    def test_migration_handles_exceptions(self, mock_migrate, mock_args):
        """Test that migration command handles exceptions gracefully."""
        mock_migrate.side_effect = Exception("Test exception")
        
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id='test_123',
            target_table='test_table',
            batch_file=None,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 1
    
    def test_missing_required_args(self, mock_args):
        """Test handling of missing required arguments."""
        args = mock_args(
            command='migrate',
            test_connection=False,
            dataset_id=None,  # Missing required dataset_id
            target_table=None,  # Missing required target_table
            batch_file=None,
            from_spreadsheet=False
        )
        
        result = main.handle_migrate_command(args)
        assert result == 1


class TestEnvironmentVariables:
    """Test environment variable handling."""
    
    def test_default_export_dir(self, monkeypatch):
        """Test default export directory from environment."""
        monkeypatch.setenv('EXPORT_DIR', 'custom/export/path')
        
        parser = main.create_parser()
        args = parser.parse_args(['inventory'])
        
        assert args.export_dir == 'custom/export/path'
    
    def test_fallback_export_dir(self, monkeypatch):
        """Test fallback export directory when env var not set."""
        # Make sure EXPORT_DIR is not set
        monkeypatch.delenv('EXPORT_DIR', raising=False)
        
        parser = main.create_parser()
        args = parser.parse_args(['inventory'])
        
        assert args.export_dir == 'results/sql/translated'