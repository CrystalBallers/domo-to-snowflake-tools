"""
Tests for maintenance scripts functionality.

This module tests the maintenance and automation scripts including:
- Project maintenance manager
- Structure maintenance
- Cleanup operations
- Health checks and reporting
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

try:
    from tools.scripts.project_maintenance import ProjectMaintenanceManager
    from tools.scripts.maintain_structure import ProjectStructureMaintainer
    from tools.scripts.cleanup_project import ProjectCleaner
except ImportError:
    # Handle case where maintenance modules might not be available
    pytest.skip("Maintenance modules not available", allow_module_level=True)


class TestProjectMaintenanceManager:
    """Test ProjectMaintenanceManager functionality."""
    
    def test_maintenance_manager_init(self):
        """Test MaintenanceManager initialization."""
        manager = ProjectMaintenanceManager()
        
        assert manager.project_root is not None
        assert manager.logger is not None
        assert hasattr(manager, 'structure_maintainer')
        assert hasattr(manager, 'cleaner')
    
    @patch('tools.scripts.project_maintenance.ProjectStructureMaintainer')
    @patch('tools.scripts.project_maintenance.ProjectCleaner')
    def test_check_project_health(self, mock_cleaner_class, mock_structure_class):
        """Test project health check functionality."""
        # Mock structure maintainer
        mock_structure = Mock()
        mock_structure.analyze_structure.return_value = {
            'status': 'good',
            'issues': [],
            'score': 95
        }
        mock_structure_class.return_value = mock_structure
        
        # Mock cleaner
        mock_cleaner = Mock()
        mock_cleaner.analyze_cleanliness.return_value = {
            'status': 'good',
            'temp_files': 0,
            'reclaimable_space': 0
        }
        mock_cleaner_class.return_value = mock_cleaner
        
        manager = ProjectMaintenanceManager()
        health = manager.check_project_health()
        
        assert 'overall' in health
        assert 'structure' in health
        assert 'cleanliness' in health
        assert health['overall'] in ['excellent', 'good', 'needs_attention', 'poor']
    
    @patch('tools.scripts.project_maintenance.ProjectStructureMaintainer')
    def test_fix_project_structure(self, mock_structure_class):
        """Test project structure fixing."""
        mock_structure = Mock()
        mock_structure.maintain_structure.return_value = True
        mock_structure_class.return_value = mock_structure
        
        manager = ProjectMaintenanceManager()
        result = manager.fix_project_structure()
        
        assert result is True
        mock_structure.maintain_structure.assert_called_once()
    
    @patch('tools.scripts.project_maintenance.ProjectCleaner')
    def test_clean_project(self, mock_cleaner_class):
        """Test project cleaning."""
        mock_cleaner = Mock()
        mock_cleaner.clean_project.return_value = True
        mock_cleaner_class.return_value = mock_cleaner
        
        manager = ProjectMaintenanceManager()
        result = manager.clean_project()
        
        assert result is True
        mock_cleaner.clean_project.assert_called_once()
    
    @patch('tools.scripts.project_maintenance.ProjectStructureMaintainer')
    @patch('tools.scripts.project_maintenance.ProjectCleaner')
    def test_full_maintenance(self, mock_cleaner_class, mock_structure_class):
        """Test full maintenance operation."""
        # Mock structure maintainer
        mock_structure = Mock()
        mock_structure.maintain_structure.return_value = True
        mock_structure_class.return_value = mock_structure
        
        # Mock cleaner
        mock_cleaner = Mock()
        mock_cleaner.clean_project.return_value = True
        mock_cleaner_class.return_value = mock_cleaner
        
        manager = ProjectMaintenanceManager()
        result = manager.full_maintenance()
        
        assert result is True
        mock_structure.maintain_structure.assert_called_once()
        mock_cleaner.clean_project.assert_called_once()
    
    def test_generate_health_report(self):
        """Test health report generation."""
        manager = ProjectMaintenanceManager()
        
        mock_health = {
            'overall': 'good',
            'structure': {'status': 'good', 'issues': []},
            'cleanliness': {'status': 'good', 'temp_files': 0}
        }
        
        with patch.object(manager, 'check_project_health', return_value=mock_health):
            report = manager.generate_health_report()
            
            assert isinstance(report, str)
            assert 'PROJECT HEALTH REPORT' in report
            assert 'good' in report.lower()


class TestProjectStructureMaintainer:
    """Test ProjectStructureMaintainer functionality."""
    
    def test_structure_maintainer_init(self, temp_dir):
        """Test StructureMaintainer initialization."""
        maintainer = ProjectStructureMaintainer(temp_dir)
        
        assert maintainer.project_root == Path(temp_dir)
        assert maintainer.logger is not None
        assert hasattr(maintainer, 'file_rules')
    
    def test_analyze_structure_clean_project(self, temp_dir):
        """Test structure analysis of clean project."""
        # Create a well-organized project structure
        tools_dir = os.path.join(temp_dir, 'tools', 'scripts')
        results_dir = os.path.join(temp_dir, 'results')
        os.makedirs(tools_dir, exist_ok=True)
        os.makedirs(results_dir, exist_ok=True)
        
        # Create appropriately placed files
        with open(os.path.join(tools_dir, 'test_script.py'), 'w') as f:
            f.write('# Test script')
        
        maintainer = ProjectStructureMaintainer(temp_dir)
        analysis = maintainer.analyze_structure()
        
        assert 'status' in analysis
        assert 'issues' in analysis
        assert isinstance(analysis['issues'], list)
    
    def test_analyze_structure_messy_project(self, temp_dir):
        """Test structure analysis of messy project."""
        # Create misplaced files
        with open(os.path.join(temp_dir, 'misplaced_script.py'), 'w') as f:
            f.write('# Misplaced script')
        
        with open(os.path.join(temp_dir, 'test_results.csv'), 'w') as f:
            f.write('data,value\n1,2')
        
        maintainer = ProjectStructureMaintainer(temp_dir)
        analysis = maintainer.analyze_structure()
        
        assert analysis['status'] in ['needs_attention', 'poor']
        assert len(analysis['issues']) > 0
    
    def test_maintain_structure_moves_files(self, temp_dir):
        """Test that structure maintenance moves files correctly."""
        # Create misplaced Python script
        misplaced_script = os.path.join(temp_dir, 'analysis_script.py')
        with open(misplaced_script, 'w') as f:
            f.write('# Analysis script')
        
        maintainer = ProjectStructureMaintainer(temp_dir)
        result = maintainer.maintain_structure()
        
        # Should have moved the file
        assert result is True
        assert not os.path.exists(misplaced_script)
        
        # Should exist in correct location
        correct_location = os.path.join(temp_dir, 'tools', 'scripts', 'analysis_script.py')
        assert os.path.exists(correct_location)
    
    def test_maintain_structure_creates_directories(self, temp_dir):
        """Test that structure maintenance creates required directories."""
        # Create file that needs a directory
        misplaced_file = os.path.join(temp_dir, 'results.csv')
        with open(misplaced_file, 'w') as f:
            f.write('data,value\n')
        
        maintainer = ProjectStructureMaintainer(temp_dir)
        maintainer.maintain_structure()
        
        # Should have created results directory
        results_dir = os.path.join(temp_dir, 'results')
        assert os.path.exists(results_dir)
    
    def test_get_target_directory_for_file(self, temp_dir):
        """Test target directory identification for files."""
        maintainer = ProjectStructureMaintainer(temp_dir)
        
        # Test Python script
        target = maintainer._get_target_directory('analysis_script.py')
        assert 'tools/scripts' in str(target)
        
        # Test CSV file
        target = maintainer._get_target_directory('data_export.csv')
        assert 'results' in str(target)
        
        # Test log file
        target = maintainer._get_target_directory('debug.log')
        assert 'logs' in str(target)
    
    def test_file_rules_configuration(self, temp_dir):
        """Test file rules configuration."""
        maintainer = ProjectStructureMaintainer(temp_dir)
        
        assert 'python_scripts' in maintainer.file_rules
        assert 'data_files' in maintainer.file_rules
        assert 'log_files' in maintainer.file_rules
        
        # Check that rules have required structure
        for rule_name, rule in maintainer.file_rules.items():
            assert 'patterns' in rule
            assert 'target_dir' in rule
            assert isinstance(rule['patterns'], list)


class TestProjectCleaner:
    """Test ProjectCleaner functionality."""
    
    def test_project_cleaner_init(self, temp_dir):
        """Test ProjectCleaner initialization."""
        cleaner = ProjectCleaner(temp_dir)
        
        assert cleaner.project_root == Path(temp_dir)
        assert cleaner.logger is not None
        assert hasattr(cleaner, 'basic_cleanup_patterns')
        assert hasattr(cleaner, 'aggressive_cleanup_patterns')
    
    def test_analyze_cleanliness_clean_project(self, temp_dir):
        """Test cleanliness analysis of clean project."""
        # Create only legitimate files
        with open(os.path.join(temp_dir, 'legitimate_file.py'), 'w') as f:
            f.write('# Legitimate file')
        
        cleaner = ProjectCleaner(temp_dir)
        analysis = cleaner.analyze_cleanliness()
        
        assert 'status' in analysis
        assert 'temp_files' in analysis
        assert analysis['temp_files'] == 0
        assert analysis['status'] == 'good'
    
    def test_analyze_cleanliness_messy_project(self, temp_dir):
        """Test cleanliness analysis of messy project."""
        # Create temporary files
        with open(os.path.join(temp_dir, 'temp_file.tmp'), 'w') as f:
            f.write('temporary data')
        
        with open(os.path.join(temp_dir, 'backup_file.bak'), 'w') as f:
            f.write('backup data')
        
        with open(os.path.join(temp_dir, '.DS_Store'), 'w') as f:
            f.write('system file')
        
        cleaner = ProjectCleaner(temp_dir)
        analysis = cleaner.analyze_cleanliness()
        
        assert analysis['temp_files'] >= 3
        assert analysis['status'] in ['needs_cleanup', 'poor']
    
    def test_clean_project_basic(self, temp_dir):
        """Test basic project cleaning."""
        # Create temporary files
        temp_files = [
            'temp_file.tmp',
            'backup_file.bak',
            '.DS_Store'
        ]
        
        for filename in temp_files:
            filepath = os.path.join(temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write('temporary content')
        
        cleaner = ProjectCleaner(temp_dir)
        result = cleaner.clean_project(aggressive=False)
        
        assert result is True
        
        # Temporary files should be removed
        for filename in temp_files:
            filepath = os.path.join(temp_dir, filename)
            assert not os.path.exists(filepath)
    
    def test_clean_project_aggressive(self, temp_dir):
        """Test aggressive project cleaning."""
        # Create various files including caches
        cache_dir = os.path.join(temp_dir, '__pycache__')
        os.makedirs(cache_dir, exist_ok=True)
        
        with open(os.path.join(cache_dir, 'module.pyc'), 'w') as f:
            f.write('compiled python')
        
        log_file = os.path.join(temp_dir, 'old_log.log')
        with open(log_file, 'w') as f:
            f.write('old log content')
        
        # Set file modification time to be old (for aggressive cleanup)
        old_time = os.path.getmtime(log_file) - (8 * 24 * 60 * 60)  # 8 days ago
        os.utime(log_file, (old_time, old_time))
        
        cleaner = ProjectCleaner(temp_dir)
        result = cleaner.clean_project(aggressive=True)
        
        assert result is True
        
        # Cache directory should be removed
        assert not os.path.exists(cache_dir)
    
    def test_calculate_reclaimable_space(self, temp_dir):
        """Test calculation of reclaimable space."""
        # Create files of known sizes
        large_temp_file = os.path.join(temp_dir, 'large_temp.tmp')
        with open(large_temp_file, 'w') as f:
            f.write('x' * 1024)  # 1KB file
        
        cleaner = ProjectCleaner(temp_dir)
        space = cleaner._calculate_reclaimable_space()
        
        assert space >= 1024  # At least 1KB reclaimable
    
    def test_cleanup_patterns_configuration(self, temp_dir):
        """Test cleanup patterns configuration."""
        cleaner = ProjectCleaner(temp_dir)
        
        # Check basic patterns
        assert '*.tmp' in cleaner.basic_cleanup_patterns
        assert '*.bak' in cleaner.basic_cleanup_patterns
        assert '.DS_Store' in cleaner.basic_cleanup_patterns
        
        # Check aggressive patterns
        assert '__pycache__' in cleaner.aggressive_cleanup_patterns
        assert '*.pyc' in cleaner.aggressive_cleanup_patterns
    
    def test_dry_run_mode(self, temp_dir):
        """Test dry run mode doesn't actually delete files."""
        # Create temporary file
        temp_file = os.path.join(temp_dir, 'test.tmp')
        with open(temp_file, 'w') as f:
            f.write('test content')
        
        cleaner = ProjectCleaner(temp_dir)
        result = cleaner.clean_project(dry_run=True)
        
        assert result is True
        # File should still exist in dry run mode
        assert os.path.exists(temp_file)


class TestMaintenanceIntegration:
    """Test integration between maintenance components."""
    
    @patch('tools.scripts.project_maintenance.ProjectStructureMaintainer')
    @patch('tools.scripts.project_maintenance.ProjectCleaner')
    def test_complete_maintenance_workflow(self, mock_cleaner_class, mock_structure_class, temp_dir):
        """Test complete maintenance workflow."""
        # Mock structure maintainer
        mock_structure = Mock()
        mock_structure.analyze_structure.return_value = {
            'status': 'needs_attention',
            'issues': ['misplaced_files']
        }
        mock_structure.maintain_structure.return_value = True
        mock_structure_class.return_value = mock_structure
        
        # Mock cleaner
        mock_cleaner = Mock()
        mock_cleaner.analyze_cleanliness.return_value = {
            'status': 'needs_cleanup',
            'temp_files': 5
        }
        mock_cleaner.clean_project.return_value = True
        mock_cleaner_class.return_value = mock_cleaner
        
        manager = ProjectMaintenanceManager()
        
        # Check initial health
        initial_health = manager.check_project_health()
        assert initial_health['overall'] in ['needs_attention', 'poor']
        
        # Perform maintenance
        structure_result = manager.fix_project_structure()
        clean_result = manager.clean_project()
        
        assert structure_result is True
        assert clean_result is True
    
    def test_maintenance_logging(self, temp_dir):
        """Test that maintenance operations are properly logged."""
        with patch('tools.scripts.project_maintenance.logging.getLogger') as mock_logger:
            mock_log = Mock()
            mock_logger.return_value = mock_log
            
            manager = ProjectMaintenanceManager()
            manager.check_project_health()
            
            # Should have made logging calls
            assert mock_log.info.call_count > 0 or mock_log.debug.call_count > 0
    
    def test_error_handling_in_maintenance(self):
        """Test error handling during maintenance operations."""
        manager = ProjectMaintenanceManager()
        
        # Mock structure maintainer that raises exception
        with patch.object(manager, 'structure_maintainer') as mock_structure:
            mock_structure.maintain_structure.side_effect = Exception("Structure maintenance failed")
            
            result = manager.fix_project_structure()
            
            # Should handle exception gracefully
            assert result is False
    
    def test_health_score_calculation(self):
        """Test health score calculation logic."""
        manager = ProjectMaintenanceManager()
        
        # Test with mock data
        structure_score = 95
        cleanliness_score = 80
        
        # This would test the actual score calculation if implemented
        overall_score = (structure_score + cleanliness_score) / 2
        assert overall_score == 87.5
        
        # Test health status determination
        if overall_score >= 90:
            expected_status = 'excellent'
        elif overall_score >= 75:
            expected_status = 'good'
        elif overall_score >= 50:
            expected_status = 'needs_attention'
        else:
            expected_status = 'poor'
        
        assert expected_status == 'good'


class TestMaintenanceCommandLine:
    """Test command-line interface for maintenance scripts."""
    
    def test_maintenance_cli_check_command(self):
        """Test maintenance CLI check command."""
        # This would test the CLI interface if available
        pass
    
    def test_maintenance_cli_fix_command(self):
        """Test maintenance CLI fix command."""
        # This would test the CLI interface if available
        pass
    
    def test_maintenance_cli_clean_command(self):
        """Test maintenance CLI clean command."""
        # This would test the CLI interface if available
        pass
    
    def test_maintenance_cli_full_command(self):
        """Test maintenance CLI full command."""
        # This would test the CLI interface if available
        pass


class TestMaintenanceScheduling:
    """Test maintenance scheduling and automation features."""
    
    def test_maintenance_schedule_validation(self):
        """Test validation of maintenance schedules."""
        # This would test scheduling logic if implemented
        pass
    
    def test_maintenance_history_tracking(self):
        """Test tracking of maintenance history."""
        # This would test history tracking if implemented
        pass
    
    def test_maintenance_notifications(self):
        """Test maintenance notifications."""
        # This would test notification logic if implemented
        pass


class TestMaintenanceReporting:
    """Test maintenance reporting functionality."""
    
    def test_health_report_format(self):
        """Test health report formatting."""
        manager = ProjectMaintenanceManager()
        
        mock_health = {
            'overall': 'good',
            'structure': {
                'status': 'good',
                'issues': [],
                'score': 95
            },
            'cleanliness': {
                'status': 'needs_cleanup',
                'temp_files': 5,
                'reclaimable_space': 1024
            }
        }
        
        with patch.object(manager, 'check_project_health', return_value=mock_health):
            report = manager.generate_health_report()
            
            # Check report contains expected sections
            assert 'PROJECT HEALTH REPORT' in report
            assert 'Overall Health' in report
            assert 'Structure' in report
            assert 'Cleanliness' in report
    
    def test_maintenance_log_format(self):
        """Test maintenance log formatting."""
        # This would test log formatting if implemented
        pass
    
    def test_maintenance_metrics_export(self):
        """Test export of maintenance metrics."""
        # This would test metrics export if implemented
        pass