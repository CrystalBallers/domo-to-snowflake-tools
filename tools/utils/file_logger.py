"""
File logging utilities for Domo-to-Snowflake migration.
Provides structured logging to files with different levels and purposes.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class FileLogger:
    """Enhanced file logging for migration operations."""
    
    def __init__(self, base_dir: str = "results/logs"):
        """
        Initialize file logger.
        
        Args:
            base_dir: Base directory for log files (default: results/logs)
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for this session
        self.session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize loggers
        self.general_logger = None
        self.error_logger = None
        
    def setup_general_logger(self, name: str = "migration") -> logging.Logger:
        """
        Setup general execution logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
        """
        if self.general_logger:
            return self.general_logger
            
        # Create general log file
        log_file = self.base_dir / f"{name}_{self.session_timestamp}.log"
        
        # Configure logger
        logger = logging.getLogger(f"file_{name}")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # Don't propagate to root logger
        
        self.general_logger = logger
        
        # Log session start
        logger.info("="*80)
        logger.info(f"🚀 Migration session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Log file: {log_file}")
        logger.info("="*80)
        
        return logger
    
    def setup_error_logger(self, name: str = "errors") -> logging.Logger:
        """
        Setup error-specific logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured error logger instance
        """
        if self.error_logger:
            return self.error_logger
            
        # Create error log file
        log_file = self.base_dir / f"{name}_{self.session_timestamp}.log"
        
        # Configure logger
        logger = logging.getLogger(f"file_{name}")
        logger.setLevel(logging.WARNING)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.WARNING)
        
        # Formatter with more detail for errors
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # Don't propagate to root logger
        
        self.error_logger = logger
        
        # Log session start
        logger.warning("="*80)
        logger.warning(f"❌ Error tracking started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.warning(f"📁 Error log file: {log_file}")
        logger.warning("="*80)
        
        return logger
    
    def log_comparison_start(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: list, transform_columns: bool = False):
        """Log comparison operation start."""
        if self.general_logger:
            self.general_logger.info("-"*60)
            self.general_logger.info("📊 COMPARISON STARTED")
            self.general_logger.info(f"   Domo Dataset ID: {domo_dataset_id}")
            self.general_logger.info(f"   Snowflake Table: {snowflake_table}")
            self.general_logger.info(f"   Key Columns: {', '.join(key_columns)}")
            self.general_logger.info(f"   Transform Columns: {transform_columns}")
            self.general_logger.info("-"*60)
    
    def log_comparison_result(self, result: dict):
        """Log comparison operation result."""
        if self.general_logger:
            self.general_logger.info("-"*60)
            self.general_logger.info("📈 COMPARISON COMPLETED")
            self.general_logger.info(f"   Overall Match: {result.get('overall_match', 'Unknown')}")
            self.general_logger.info(f"   Schema Match: {result.get('schema_match', 'Unknown')}")
            self.general_logger.info(f"   Data Match: {result.get('data_match', 'Unknown')}")
            if result.get('errors'):
                self.general_logger.warning(f"   Errors Found: {len(result['errors'])}")
            self.general_logger.info("-"*60)
    
    def log_error(self, error_type: str, context: str, error_message: str):
        """Log error to both general and error logs."""
        error_msg = f"[{error_type}] {context}: {error_message}"
        
        if self.general_logger:
            self.general_logger.error(error_msg)
        
        if self.error_logger:
            self.error_logger.error(error_msg)
    
    def log_batch_failure(self, batch_num: int, domo_rows: int, snowflake_rows: int, 
                         error: str):
        """Log batch processing failure."""
        if self.error_logger:
            self.error_logger.error(f"BATCH_FAILURE - Batch {batch_num}: "
                                  f"Domo={domo_rows} rows, Snowflake={snowflake_rows} rows, "
                                  f"Error: {error}")
    
    def close_loggers(self):
        """Close all file handlers."""
        if self.general_logger:
            self.general_logger.info("="*80)
            self.general_logger.info(f"📝 Migration session ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.general_logger.info("="*80)
            
            for handler in self.general_logger.handlers:
                handler.close()
        
        if self.error_logger:
            self.error_logger.warning("="*80)
            self.error_logger.warning(f"❌ Error tracking ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.error_logger.warning("="*80)
            
            for handler in self.error_logger.handlers:
                handler.close()


# Global file logger instance
_file_logger_instance: Optional[FileLogger] = None


def get_file_logger() -> FileLogger:
    """Get or create global file logger instance."""
    global _file_logger_instance
    if _file_logger_instance is None:
        _file_logger_instance = FileLogger()
    return _file_logger_instance


def setup_file_logging() -> tuple[logging.Logger, logging.Logger]:
    """
    Setup file logging for the application.
    
    Returns:
        Tuple of (general_logger, error_logger)
    """
    file_logger = get_file_logger()
    general_logger = file_logger.setup_general_logger()
    error_logger = file_logger.setup_error_logger()
    
    return general_logger, error_logger


def close_file_logging():
    """Close all file logging handlers."""
    global _file_logger_instance
    if _file_logger_instance:
        _file_logger_instance.close_loggers()
        _file_logger_instance = None
