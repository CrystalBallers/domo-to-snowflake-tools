#!/usr/bin/env python3
"""
Domo to Snowflake Migration Tool

This script handles the core functionality for migrating data from Domo to Snowflake.
It provides utilities for data extraction, transformation, and loading operations.

Usage:
    python domo_to_snowflake.py [options]

Author: Migration Team
"""

import os
import sys
import argparse
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Optional

# Add the parent directory to the path to import from tools
sys.path.insert(0, str(Path(__file__).parent))

from utils.snowflake import SnowflakeHandler
from utils.stage_handler import StageHandler
from utils.common import setup_dual_connections
from utils.domo import DomoHandler
from utils.gsheets import GoogleSheets, READ_WRITE_SCOPES

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Google Sheets configuration
SPREADSHEET_ID = os.getenv("MIGRATION_SPREADSHEET_ID", "1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA")
MIGRATION_SHEET_NAME = os.getenv("MIGRATION_SHEET_NAME", "Migration")


def sanitize_table_name(dataset_id: str, dataset_name: str = None, use_prefix: bool = None) -> str:
    """
    Create a Snowflake-compatible table name from dataset ID and name.
    
    Args:
        dataset_id (str): Domo dataset ID
        dataset_name (str): Optional dataset name for better naming
        use_prefix (bool): Whether to add DOMO_ prefix (default: from env var DOMO_TABLE_PREFIX)
        
    Returns:
        str: Sanitized table name safe for Snowflake
    """
    # Prefix is disabled globally as per user request.
    prefix = ""

    # Start with prefix
    table_name = prefix
    
    # If we have a dataset name, use it as base
    if dataset_name and dataset_name.strip() and dataset_name.lower() != 'unknown':
        # Clean the name: lowercase, replace spaces/special chars with underscores
        clean_name = dataset_name.lower().strip()
        clean_name = clean_name.replace(' ', '_').replace('-', '_').replace('.', '_')
        # Remove any non-alphanumeric characters except underscores
        import re
        clean_name = re.sub(r'[^a-z0-9_]', '', clean_name)
        # Ensure it starts with a letter
        if clean_name and clean_name[0].isdigit():
            clean_name = 'tbl_' + clean_name
        table_name += clean_name
    else:
        # Use dataset ID but sanitize it
        # Remove hyphens and other special characters
        clean_id = dataset_id.replace('-', '_').replace('.', '_')
        # Remove any non-alphanumeric characters except underscores
        import re
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '', clean_id)
        # Ensure it starts with a letter
        if clean_id and clean_id[0].isdigit():
            clean_id = 'id_' + clean_id
        table_name += clean_id
    
    # Ensure the name is not too long (Snowflake limit is 255 characters)
    if len(table_name) > 240:  # Leave some room for safety
        table_name = table_name[:240]
    
    # Ensure it ends with alphanumeric
    table_name = table_name.rstrip('_')
    
    logger.debug(f"📝 Generated table name: {dataset_id} -> {table_name}")
    return table_name


class MigrationManager:
    """
    Manages Domo to Snowflake migrations with efficient connection handling.
    
    This class maintains connections to Domo and Snowflake to avoid
    re-authentication on each dataset migration.
    """
    
    def __init__(self):
        """Initialize the MigrationManager."""
        self.domo_handler = None
        self.snowflake_handler = None
        self.stage_handler = None
        self._connections_established = False
    
    def setup_connections(self) -> bool:
        """
        Setup connections to Domo and Snowflake.
        
        Returns:
            bool: True if both connections successful, False otherwise
        """
        success, domo_handler, snowflake_handler = setup_dual_connections()
        
        if success:
            self.domo_handler = domo_handler
            self.snowflake_handler = snowflake_handler
            self.stage_handler = StageHandler(snowflake_handler)
            self._connections_established = True
        
        return success
    
    def migrate_dataset(self, dataset_id: str, target_table: str, chunk_size: int = None) -> bool:
        """
        Migrate a single dataset from Domo to Snowflake using existing connections.
        
        Args:
            dataset_id (str): Domo dataset ID
            target_table (str): Target Snowflake table name
            chunk_size (int): Number of rows to extract (None for all rows, default: None)
            
        Returns:
            bool: True if migration successful, False otherwise
        """
        if not self._connections_established:
            logger.error("❌ Connections not established. Call setup_connections() first.")
            return False
        
        logger.info(f"🚀 Starting migration of dataset {dataset_id} to {target_table}")
        
        try:
            # Extract data from Domo
            logger.info("📥 Extracting data from Domo...")
            
            # Handle auto chunk size
            if chunk_size == "auto":
                logger.info("🔄 X-Small optimized auto-chunk mode: Will determine optimal chunk size based on dataset size for X-Small warehouse")
                # For auto mode, we'll extract all data and let the upload handle X-Small optimized chunking
                df = self.domo_handler.extract_data(dataset_id, chunk_size=None)
            else:
                df = self.domo_handler.extract_data(dataset_id, chunk_size=chunk_size)
            
            if df is None or len(df) == 0:
                logger.warning(f"⚠️  No data found for dataset {dataset_id}")
                return False
            
            logger.info(f"✅ Extracted {len(df)} rows from Domo")
            
            # Load data to Snowflake
            logger.info("📤 Loading data to Snowflake...")
            
            # Pass chunk_size for auto mode
            if chunk_size == "auto":
                success = self.snowflake_handler.upload_data(df, target_table, chunk_size=None)
            else:
                success = self.snowflake_handler.upload_data(df, target_table)
            
            if success:
                # Verify upload
                if self.snowflake_handler.verify_upload(target_table, len(df)):
                    logger.info(f"✅ Successfully migrated dataset {dataset_id} to {target_table}")
                    return True
                else:
                    logger.error(f"❌ Upload verification failed for {target_table}")
                    return False
            else:
                logger.error(f"❌ Failed to load data to Snowflake table {target_table}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration failed for dataset {dataset_id}: {e}")
            return False
    
    def migrate_dataset_to_stage(self, dataset_id: str, stage_name: str, chunk_size: int = None) -> bool:
        """
        Migrate a single dataset from Domo to Snowflake stage.
        
        Args:
            dataset_id (str): Domo dataset ID
            stage_name (str): Target Snowflake stage name
            chunk_size (int): Number of rows to extract (None for all rows, default: None)
            
        Returns:
            bool: True if migration to stage successful, False otherwise
        """
        if not self._connections_established:
            logger.error("❌ Connections not established. Call setup_connections() first.")
            return False
        
        logger.info(f"🚀 Starting migration of dataset {dataset_id} to stage {stage_name}")
        
        try:
            # Extract data from Domo
            logger.info("📥 Extracting data from Domo...")
            
            # Handle auto chunk size
            if chunk_size == "auto":
                logger.info("🔄 X-Small optimized auto-chunk mode: Will determine optimal chunk size based on dataset size for X-Small warehouse")
                df = self.domo_handler.extract_data(dataset_id, chunk_size=None)
            else:
                df = self.domo_handler.extract_data(dataset_id, chunk_size=chunk_size)
            
            if df is None or len(df) == 0:
                logger.warning(f"⚠️  No data found for dataset {dataset_id}")
                return False
            
            logger.info(f"✅ Extracted {len(df)} rows from Domo")
            
            # Create stage if it doesn't exist
            logger.info("🏗️  Creating/verifying stage...")
            if not self.stage_handler.create_stage(stage_name):
                logger.error(f"❌ Failed to create stage {stage_name}")
                return False
            
            # Upload data to stage
            logger.info("📤 Uploading data to stage...")
            if not self.stage_handler.upload_data_to_stage(df, stage_name):
                logger.error(f"❌ Failed to upload data to stage {stage_name}")
                return False
            
            # List stage files for verification
            files = self.stage_handler.list_stage_files(stage_name)
            if files:
                logger.info(f"✅ Successfully migrated dataset {dataset_id} to stage {stage_name}")
                logger.info(f"📁 Stage contains {len(files)} file(s)")
                for file_info in files:
                    logger.info(f"   - {file_info['name']} ({file_info['size']} bytes)")
                return True
            else:
                logger.error(f"❌ No files found in stage {stage_name} after upload")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration to stage failed for dataset {dataset_id}: {e}")
            return False
    
    def migrate_dataset_to_stage_with_filename(self, dataset_id: str, stage_name: str, filename: str, chunk_size: int = None) -> bool:
        """
        Migrate a single dataset from Domo to Snowflake stage with specific filename.
        
        Args:
            dataset_id (str): Domo dataset ID
            stage_name (str): Target Snowflake stage name
            filename (str): Specific filename to use in the stage
            chunk_size (int): Number of rows to extract (None for all rows, default: None)
            
        Returns:
            bool: True if migration to stage successful, False otherwise
        """
        if not self._connections_established:
            logger.error("❌ Connections not established. Call setup_connections() first.")
            return False
        
        logger.info(f"🚀 Starting migration of dataset {dataset_id} to stage {stage_name} as {filename}")
        
        try:
            # Extract data from Domo
            logger.info("📥 Extracting data from Domo...")
            
            # Handle auto chunk size
            if chunk_size == "auto":
                logger.info("🔄 X-Small optimized auto-chunk mode: Will determine optimal chunk size based on dataset size for X-Small warehouse")
                df = self.domo_handler.extract_data(dataset_id, chunk_size=None)
            else:
                df = self.domo_handler.extract_data(dataset_id, chunk_size=chunk_size)
            
            if df is None or len(df) == 0:
                logger.warning(f"⚠️  No data found for dataset {dataset_id}")
                return False
            
            logger.info(f"✅ Extracted {len(df)} rows from Domo")
            
            # Create stage if it doesn't exist
            logger.info("🏗️  Creating/verifying stage...")
            if not self.stage_handler.create_stage(stage_name):
                logger.error(f"❌ Failed to create stage {stage_name}")
                return False
            
            # Upload data to stage with specific filename
            logger.info(f"📤 Uploading data to stage as {filename}...")
            if not self.stage_handler.upload_data_to_stage(df, stage_name, filename):
                logger.error(f"❌ Failed to upload data to stage {stage_name}")
                return False
            
            # List stage files for verification
            files = self.stage_handler.list_stage_files(stage_name)
            if files:
                logger.info(f"✅ Successfully migrated dataset {dataset_id} to stage {stage_name}")
                logger.info(f"📁 Stage contains {len(files)} file(s)")
                for file_info in files:
                    logger.info(f"   - {file_info['name']} ({file_info['size']} bytes)")
                return True
            else:
                logger.error(f"❌ No files found in stage {stage_name} after upload")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration to stage failed for dataset {dataset_id}: {e}")
            return False
    
    def load_from_stage_to_table(self, stage_name: str, target_table: str, file_pattern: str = "*.csv", if_exists: str = 'replace') -> bool:
        """
        Load data from stage to table.
        
        Args:
            stage_name (str): Source Snowflake stage name
            target_table (str): Target Snowflake table name
            file_pattern (str): File pattern to match in stage (default: "*.csv")
            if_exists (str): What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            bool: True if load successful, False otherwise
        """
        if not self._connections_established:
            logger.error("❌ Connections not established. Call setup_connections() first.")
            return False
        
        logger.info(f"🚀 Loading data from stage {stage_name} to table {target_table}")
        
        try:
            # Load data from stage to table
            success = self.stage_handler.load_from_stage_to_table(
                stage_name=stage_name,
                table_name=target_table,
                file_pattern=file_pattern,
                if_exists=if_exists
            )
            
            if success:
                logger.info(f"✅ Successfully loaded data from stage {stage_name} to table {target_table}")
                return True
            else:
                logger.error(f"❌ Failed to load data from stage {stage_name} to table {target_table}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Load from stage failed: {e}")
            return False
    
    def migrate_dataset_via_stage(self, dataset_id: str, stage_name: str, target_table: str, chunk_size: int = None, if_exists: str = 'replace') -> bool:
        """
        Complete migration: Domo → Stage → Table.
        
        Args:
            dataset_id (str): Domo dataset ID
            stage_name (str): Intermediate Snowflake stage name
            target_table (str): Target Snowflake table name
            chunk_size (int): Number of rows to extract (None for all rows, default: None)
            if_exists (str): What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            bool: True if complete migration successful, False otherwise
        """
        logger.info(f"🚀 Starting complete migration: Domo → Stage → Table")
        logger.info(f"   Dataset: {dataset_id}")
        logger.info(f"   Stage: {stage_name}")
        logger.info(f"   Table: {target_table}")
        
        # Step 1: Migrate to stage
        if not self.migrate_dataset_to_stage(dataset_id, stage_name, chunk_size):
            logger.error("❌ Failed to migrate to stage")
            return False
        
        # Step 2: Load from stage to table
        if not self.load_from_stage_to_table(stage_name, target_table, if_exists=if_exists):
            logger.error("❌ Failed to load from stage to table")
            return False
        
        logger.info("✅ Complete migration successful: Domo → Stage → Table")
        return True
    
    def _sanitize_name_for_snowflake(self, name: str) -> str:
        """
        Sanitize a name for Snowflake compatibility.
        
        Args:
            name: Name to sanitize
            
        Returns:
            str: Sanitized name safe for Snowflake identifiers
        """
        import re
        
        # Convert to uppercase
        sanitized = str(name).upper()
        
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^A-Z0-9_]', '_', sanitized)
        
        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading and trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = 'UNNAMED'
        
        # Limit length to 255 characters (Snowflake identifier limit)
        if len(sanitized) > 255:
            sanitized = sanitized[:255]
        
        return sanitized
    
    def cleanup(self):
        """Cleanup connections."""
        try:
            if self.snowflake_handler:
                self.snowflake_handler.cleanup()
                logger.info("✅ Snowflake connection cleaned up")
        except Exception as e:
            logger.warning(f"⚠️  Error during cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        if not self.setup_connections():
            raise RuntimeError("Failed to setup connections")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
    
    def update_spreadsheet_status(self, spreadsheet_id: str, sheet_name: str, 
                                successful_migrations: list, credentials_path: str = None) -> bool:
        """
        Update the status in Google Sheets for successfully migrated datasets.
        
        Args:
            spreadsheet_id (str): Google Sheets spreadsheet ID
            sheet_name (str): Name of the migration sheet tab
            successful_migrations (list): List of successfully migrated dataset IDs
            credentials_path (str): Path to Google Sheets credentials file
            
        Returns:
            bool: True if update successful, False otherwise
        """
        if not successful_migrations:
            logger.info("📝 No successful migrations to update in spreadsheet")
            return True
        
        try:
            # Initialize Google Sheets client if not already done
            if not credentials_path:
                credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
            
            if not credentials_path:
                logger.warning("⚠️  No Google Sheets credentials provided for status update")
                return False
            
            if not os.path.exists(credentials_path):
                logger.warning(f"⚠️  Google Sheets credentials file not found: {credentials_path}")
                return False
            
            gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
            logger.info(f"📝 Updating status for {len(successful_migrations)} successful migrations in spreadsheet")
            
            # Read current data to find row numbers
            df = gsheets_client.read_to_dataframe(
                spreadsheet_id=spreadsheet_id,
                range_name=f"{sheet_name}!A:Z",
                header=True
            )
            
            if df is None or len(df) == 0:
                logger.warning("⚠️  No data found in spreadsheet for status update")
                return False
            
            # Find the Status column
            status_column = None
            possible_status_columns = ['Status', 'status', 'migration_status', 'Migration Status', 'state']
            
            for col in possible_status_columns:
                if col in df.columns:
                    status_column = col
                    break
            
            if not status_column:
                logger.warning("⚠️  Status column not found in spreadsheet")
                return False
            
            # Find the Dataset ID column
            dataset_id_column = None
            possible_id_columns = ['Dataset ID', 'dataset_id', 'DatasetID', 'dataset', 'Dataset', 'ID']
            
            for col in possible_id_columns:
                if col in df.columns:
                    dataset_id_column = col
                    break
            
            if not dataset_id_column:
                logger.warning("⚠️  Dataset ID column not found in spreadsheet")
                return False
            
            # Update status for successful migrations
            updated_count = 0
            for dataset_id in successful_migrations:
                # Find the row with this dataset ID
                mask = df[dataset_id_column].astype(str) == str(dataset_id)
                if mask.any():
                    row_index = mask.idxmax()
                    # Convert to 1-based row number for Google Sheets
                    row_number = row_index + 2  # +2 because Google Sheets is 1-based and we have header
                    
                    # Find the column letter for Status
                    status_col_letter = None
                    for i, col in enumerate(df.columns):
                        if col == status_column:
                            status_col_letter = chr(65 + i)  # A, B, C, etc.
                            break
                    
                    if status_col_letter:
                        # Update the cell
                        range_name = f"{sheet_name}!{status_col_letter}{row_number}"
                        gsheets_client.update_cell(
                            spreadsheet_id=spreadsheet_id,
                            range_name=range_name,
                            value="Migrated"
                        )
                        updated_count += 1
                        logger.info(f"✅ Updated status for dataset {dataset_id} to 'Migrated'")
                    else:
                        logger.warning(f"⚠️  Could not find Status column letter for dataset {dataset_id}")
                else:
                    logger.warning(f"⚠️  Dataset ID {dataset_id} not found in spreadsheet")
            
            logger.info(f"📝 Successfully updated {updated_count} out of {len(successful_migrations)} migrations in spreadsheet")
            return updated_count > 0
            
        except Exception as e:
            logger.error(f"❌ Failed to update spreadsheet status: {e}")
            return False


def migrate_dataset(dataset_id: str, target_table: str) -> bool:
    """
    Migrate a single dataset from Domo to Snowflake.
    
    This is a convenience function that creates a MigrationManager
    for a single dataset migration.
    
    Args:
        dataset_id (str): Domo dataset ID
        target_table (str): Target Snowflake table name
        
    Returns:
        bool: True if migration successful, False otherwise
    """
    with MigrationManager() as manager:
        return manager.migrate_dataset(dataset_id, target_table)


def migrate_dataset_to_stage(dataset_id: str, stage_name: str, chunk_size: int = None) -> bool:
    """
    Migrate a single dataset from Domo to Snowflake stage.
    
    This is a convenience function that creates a MigrationManager
    for a single dataset migration to stage.
    
    Args:
        dataset_id (str): Domo dataset ID
        stage_name (str): Target Snowflake stage name
        chunk_size (int): Number of rows to extract (None for all rows, default: None)
        
    Returns:
        bool: True if migration to stage successful, False otherwise
    """
    with MigrationManager() as manager:
        return manager.migrate_dataset_to_stage(dataset_id, stage_name, chunk_size)


def load_from_stage_to_table(stage_name: str, target_table: str, file_pattern: str = "*.csv", if_exists: str = 'replace') -> bool:
    """
    Load data from stage to table.
    
    This is a convenience function that creates a MigrationManager
    for loading data from stage to table.
    
    Args:
        stage_name (str): Source Snowflake stage name
        target_table (str): Target Snowflake table name
        file_pattern (str): File pattern to match in stage (default: "*.csv")
        if_exists (str): What to do if table exists ('replace', 'append', 'fail')
        
    Returns:
        bool: True if load successful, False otherwise
    """
    with MigrationManager() as manager:
        return manager.load_from_stage_to_table(stage_name, target_table, file_pattern, if_exists)


def migrate_dataset_via_stage(dataset_id: str, stage_name: str, target_table: str, chunk_size: int = None, if_exists: str = 'replace') -> bool:
    """
    Complete migration: Domo → Stage → Table.
    
    This is a convenience function that creates a MigrationManager
    for complete migration via stage.
    
    Args:
        dataset_id (str): Domo dataset ID
        stage_name (str): Intermediate Snowflake stage name
        target_table (str): Target Snowflake table name
        chunk_size (int): Number of rows to extract (None for all rows, default: None)
        if_exists (str): What to do if table exists ('replace', 'append', 'fail')
        
    Returns:
        bool: True if complete migration successful, False otherwise
    """
    with MigrationManager() as manager:
        return manager.migrate_dataset_via_stage(dataset_id, stage_name, target_table, chunk_size, if_exists)


def batch_migrate_datasets(dataset_mapping: dict) -> dict:
    """
    Migrate multiple datasets from Domo to Snowflake using efficient connection management.
    
    Args:
        dataset_mapping (dict): Dictionary mapping dataset_id -> target_table
        
    Returns:
        dict: Results summary with success/failure counts
    """
    logger.info(f"🚀 Starting batch migration of {len(dataset_mapping)} datasets")
    
    results = {
        'total': len(dataset_mapping),
        'successful': 0,
        'failed': 0,
        'details': []
    }
    
    # Use MigrationManager for efficient connection handling
    with MigrationManager() as manager:
        for dataset_id, target_table in dataset_mapping.items():
            try:
                success = manager.migrate_dataset(dataset_id, target_table)
                
                if success:
                    results['successful'] += 1
                    results['details'].append({
                        'dataset_id': dataset_id,
                        'target_table': target_table,
                        'status': 'success'
                    })
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'dataset_id': dataset_id,
                        'target_table': target_table,
                        'status': 'failed'
                    })
                    
            except Exception as e:
                logger.error(f"❌ Error processing dataset {dataset_id}: {e}")
                results['failed'] += 1
                results['details'].append({
                    'dataset_id': dataset_id,
                    'target_table': target_table,
                    'status': 'error',
                    'error': str(e)
                })
    
    # Summary
    logger.info("=" * 80)
    logger.info(f"📊 Batch Migration Summary:")
    logger.info(f"   Total datasets: {results['total']}")
    logger.info(f"   ✅ Successful: {results['successful']}")
    logger.info(f"   ❌ Failed: {results['failed']}")
    logger.info("=" * 80)
    
    return results


def migrate_from_spreadsheet(spreadsheet_id: str, sheet_name: str = "Migration", 
                           credentials_path: str = None, full_table: bool = False, 
                           auto_chunk_size: bool = False) -> dict:
    """
    Migrate datasets from a Google Sheets spreadsheet.
    
    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Name of the sheet tab containing migration data
        credentials_path (str): Path to Google Sheets credentials file
        full_table (bool): If True, upload entire table; if False, limit to first 1000 rows
        auto_chunk_size (bool): If True, automatically determine optimal chunk size for large datasets
        
    Returns:
        dict: Results summary with success/failure counts
    """
    if not credentials_path:
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    
    if not credentials_path:
        logger.error("❌ No Google Sheets credentials provided")
        return {"success": 0, "failed": 0, "total": 0, "errors": ["No credentials provided"]}
    
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Google Sheets credentials file not found: {credentials_path}")
        return {"success": 0, "failed": 0, "total": 0, "errors": [f"Credentials file not found: {credentials_path}"]}
    
    try:
        # Initialize Google Sheets client
        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        logger.info(f"📊 Reading migration data from spreadsheet: {spreadsheet_id}")
        
        # Read the migration sheet
        df = gsheets_client.read_to_dataframe(
            spreadsheet_id=spreadsheet_id,
            range_name=f"{sheet_name}!A:Z",
            header=True
        )
        
        if df is None or len(df) == 0:
            logger.warning("⚠️  No data found in spreadsheet")
            return {"success": 0, "failed": 0, "total": 0, "errors": ["No data found in spreadsheet"]}
        logger.info(f"📋 Found {len(df)} rows in spreadsheet")
        
        # Log first few rows for debugging
        logger.debug(f"📝 First 3 rows: {df.head(3).to_dict('records')}")
        logger.debug(f"📝 Columns found: {list(df.columns)}")
        
        # Find required columns with flexible naming
        dataset_id_column = None
        name_column = None
        status_column = None
        
        # Look for Dataset ID column
        possible_id_columns = ['Dataset ID', 'dataset_id', 'DatasetID', 'dataset', 'Dataset', 'ID', 'Dataset Id']
        for col in possible_id_columns:
            if col in df.columns:
                dataset_id_column = col
                break
        
        # Look for Name column (prefer 'Model Name' if available to use as Snowflake table base)
        possible_name_columns = ['Model Name', 'model_name']
        for col in possible_name_columns:
            if col in df.columns:
                name_column = col
                break
        
        # Look for Status column
        possible_status_columns = ['Status', 'status', 'migration_status', 'Migration Status', 'state']
        for col in possible_status_columns:
            if col in df.columns:
                status_column = col
                break
        
        # Add default columns if missing
        if dataset_id_column is None:
            logger.warning("⚠️  Dataset ID column not found, adding default")
            df['Dataset ID'] = None
            dataset_id_column = 'Dataset ID'
        
        if name_column is None:
            logger.warning("⚠️  Name column not found, adding default")
            df['Name'] = 'Unknown'
            name_column = 'Name'
        
        if status_column is None:
            logger.warning("⚠️  Status column not found, adding default")
            df['Status'] = 'Pending'
            status_column = 'Status'
        
        # Filter rows where Status is not "Migrated"
        if status_column in df.columns:
            # Handle different status values
            df[status_column] = df[status_column].fillna('Pending')
            df[status_column] = df[status_column].astype(str)
            
            # Filter out already migrated datasets
            pending_df = df[~df[status_column].str.contains('Migrated', case=False, na=False)]
            logger.info(f"📋 Found {len(pending_df)} datasets pending migration (excluding already migrated)")
        else:
            pending_df = df
            logger.info(f"📋 No status column found, processing all {len(pending_df)} datasets")
        
        if len(pending_df) == 0:
            logger.info("✅ No datasets pending migration")
            return {"success": 0, "failed": 0, "total": 0, "errors": []}
        
        # Initialize MigrationManager for efficient connection reuse
        with MigrationManager() as migration_manager:
            successful_migrations = []
            failed_migrations = []
            errors = []
            
            for index, row in pending_df.iterrows():
                dataset_id = row[dataset_id_column]
                dataset_name = row[name_column] if name_column else f"Dataset {dataset_id}"
                
                if pd.isna(dataset_id) or dataset_id is None or str(dataset_id).strip() == '':
                    error_msg = f"Row {index + 2}: Empty or invalid Dataset ID"
                    logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_migrations.append(dataset_id)
                    continue
                
                logger.info(f"🔄 Migrating dataset {dataset_id} ({dataset_name})")
                
                # Log chunk size information
                if full_table:
                    logger.info(f"   📊 Full table mode: Will extract all rows")
                else:
                    logger.info(f"   📊 Limited mode: Will extract first 1000 rows")
                
                try:
                    # Generate target table name using the dataset_name resolved above.
                    # With the change above, if the sheet contains 'Model Name', it will be used here.
                    target_table = sanitize_table_name(dataset_id, dataset_name)
                    
                    # Set chunk size based on flags
                    if full_table:
                        chunk_size = None  # No limit, upload entire table
                    elif auto_chunk_size:
                        # Auto-determine chunk size based on dataset size
                        chunk_size = "auto"
                    else:
                        chunk_size = 1000  # Default fixed chunk size
                    
                    # Migrate the dataset
                    success = migration_manager.migrate_dataset(dataset_id, target_table, chunk_size=chunk_size)
                    
                    if success:
                        logger.info(f"✅ Successfully migrated dataset {dataset_id}")
                        successful_migrations.append(dataset_id)
                    else:
                        logger.error(f"❌ Failed to migrate dataset {dataset_id}")
                        failed_migrations.append(dataset_id)
                        errors.append(f"Dataset {dataset_id}: Migration failed")
                
                except Exception as e:
                    error_msg = f"Dataset {dataset_id}: {str(e)}"
                    logger.error(f"❌ Error migrating dataset {dataset_id}: {e}")
                    failed_migrations.append(dataset_id)
                    errors.append(error_msg)
            
            # Update spreadsheet status for successful migrations
            if successful_migrations:
                logger.info(f"📝 Updating spreadsheet status for {len(successful_migrations)} successful migrations")
                update_success = migration_manager.update_spreadsheet_status(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                    successful_migrations=successful_migrations,
                    credentials_path=credentials_path
                )
                
                if update_success:
                    logger.info("✅ Successfully updated spreadsheet status")
                else:
                    logger.warning("⚠️  Failed to update spreadsheet status")
                    errors.append("Failed to update spreadsheet status")
            
            # Return results
            total = len(successful_migrations) + len(failed_migrations)
            return {
                "success": len(successful_migrations),
                "failed": len(failed_migrations),
                "total": total,
                "errors": errors
            }
    
    except Exception as e:
        error_msg = f"Failed to read from spreadsheet: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": 0, "failed": 0, "total": 0, "errors": [error_msg]}


def migrate_from_spreadsheet_to_stage(spreadsheet_id: str, sheet_name: str = "Migration", 
                                    stage_name: str = "migration", credentials_path: str = None, 
                                    full_table: bool = False, auto_chunk_size: bool = False) -> dict:
    """
    Migrate datasets from a Google Sheets spreadsheet to Snowflake stages.
    
    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Name of the sheet tab containing migration data
        stage_name (str): Base name for Snowflake stages (will be prefixed)
        credentials_path (str): Path to Google Sheets credentials file
        full_table (bool): If True, upload entire table; if False, limit to first 1000 rows
        auto_chunk_size (bool): If True, automatically determine optimal chunk size for large datasets
        
    Returns:
        dict: Results summary with success/failure counts
    """
    if not credentials_path:
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    
    if not credentials_path:
        logger.error("❌ No Google Sheets credentials provided")
        return {"success": 0, "failed": 0, "total": 0, "errors": ["No credentials provided"]}
    
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Google Sheets credentials file not found: {credentials_path}")
        return {"success": 0, "failed": 0, "total": 0, "errors": [f"Credentials file not found: {credentials_path}"]}
    
    try:
        # Initialize Google Sheets client
        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        logger.info(f"📊 Reading migration data from spreadsheet: {spreadsheet_id}")
        
        # Read the migration sheet
        df = gsheets_client.read_to_dataframe(
            spreadsheet_id=spreadsheet_id,
            range_name=f"{sheet_name}!A:Z",
            header=True
        )
        
        if df is None or len(df) == 0:
            logger.warning("⚠️  No data found in spreadsheet")
            return {"success": 0, "failed": 0, "total": 0, "errors": ["No data found in spreadsheet"]}
        logger.info(f"📋 Found {len(df)} rows in spreadsheet")
        
        # Log first few rows for debugging
        logger.debug(f"📝 First 3 rows: {df.head(3).to_dict('records')}")
        logger.debug(f"📝 Columns found: {list(df.columns)}")
        
        # Find required columns with flexible naming
        dataset_id_column = None
        name_column = None
        status_column = None
        
        # Look for Dataset ID column
        possible_id_columns = ['Dataset ID', 'dataset_id', 'DatasetID', 'dataset', 'Dataset', 'ID', 'Dataset Id']
        for col in possible_id_columns:
            if col in df.columns:
                dataset_id_column = col
                break
        
        # Look for Name column (prefer 'Model Name' if available to use as Snowflake table base)
        possible_name_columns = ['Model Name', 'model_name']
        for col in possible_name_columns:
            if col in df.columns:
                name_column = col
                break
        
        # Look for Status column
        possible_status_columns = ['Status', 'status', 'migration_status', 'Migration Status', 'state']
        for col in possible_status_columns:
            if col in df.columns:
                status_column = col
                break
        
        # Add default columns if missing
        if dataset_id_column is None:
            logger.warning("⚠️  Dataset ID column not found, adding default")
            df['Dataset ID'] = None
            dataset_id_column = 'Dataset ID'
        
        if name_column is None:
            logger.warning("⚠️  Name column not found, adding default")
            df['Name'] = 'Unknown'
            name_column = 'Name'
        
        if status_column is None:
            logger.warning("⚠️  Status column not found, adding default")
            df['Status'] = 'Pending'
            status_column = 'Status'
        
        # Filter rows where Status is not "Migrated"
        if status_column in df.columns:
            # Handle different status values
            df[status_column] = df[status_column].fillna('Pending')
            df[status_column] = df[status_column].astype(str)
            
            # Filter out already migrated datasets
            pending_df = df[~df[status_column].str.contains('Migrated', case=False, na=False)]
            logger.info(f"📋 Found {len(pending_df)} datasets pending migration (excluding already migrated)")
        else:
            pending_df = df
            logger.info(f"📋 No status column found, processing all {len(pending_df)} datasets")
        
        if len(pending_df) == 0:
            logger.info("✅ No datasets pending migration")
            return {"success": 0, "failed": 0, "total": 0, "errors": []}
        
        # Initialize MigrationManager for efficient connection reuse
        with MigrationManager() as migration_manager:
            successful_migrations = []
            failed_migrations = []
            errors = []
            
            for index, row in pending_df.iterrows():
                dataset_id = row[dataset_id_column]
                dataset_name = row[name_column] if name_column else f"Dataset {dataset_id}"
                
                if pd.isna(dataset_id) or dataset_id is None or str(dataset_id).strip() == '':
                    error_msg = f"Row {index + 2}: Empty or invalid Dataset ID"
                    logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_migrations.append(dataset_id)
                    continue
                
                logger.info(f"🔄 Migrating dataset {dataset_id} ({dataset_name}) to stage")
                
                # Log chunk size information
                if full_table:
                    logger.info(f"   📊 Full table mode: Will extract all rows")
                else:
                    logger.info(f"   📊 Limited mode: Will extract first 1000 rows")
                
                try:
                    # Use the same stage name for all datasets
                    # This creates a single internal stage where all files will be uploaded
                    dataset_stage_name = stage_name
                    
                    # Set chunk size based on flags
                    if full_table:
                        chunk_size = None  # No limit, upload entire table
                    elif auto_chunk_size:
                        # Auto-determine chunk size based on dataset size
                        chunk_size = "auto"
                    else:
                        chunk_size = 1000  # Default fixed chunk size
                    
                    # Generate unique filename for this dataset in the stage
                    # Use Model Name (dataset_name) instead of dataset_id for better readability
                    import time
                    import re
                    timestamp = int(time.time())
                    # Sanitize the model name for filename
                    sanitized_model_name = str(dataset_name).upper()
                    sanitized_model_name = re.sub(r'[^A-Z0-9_]', '_', sanitized_model_name)
                    sanitized_model_name = re.sub(r'_+', '_', sanitized_model_name).strip('_')
                    if not sanitized_model_name:
                        sanitized_model_name = 'UNNAMED'
                    filename = f"{sanitized_model_name}_{timestamp}.csv"
                    
                    # Migrate the dataset to stage with specific filename
                    success = migration_manager.migrate_dataset_to_stage_with_filename(
                        dataset_id=dataset_id, 
                        stage_name=dataset_stage_name, 
                        filename=filename,
                        chunk_size=chunk_size
                    )
                    
                    if success:
                        logger.info(f"✅ Successfully migrated dataset {dataset_id} to stage {dataset_stage_name}")
                        successful_migrations.append({
                            'dataset_id': dataset_id,
                            'stage_name': dataset_stage_name,
                            'dataset_name': dataset_name,
                            'filename': filename
                        })
                    else:
                        logger.error(f"❌ Failed to migrate dataset {dataset_id} to stage")
                        failed_migrations.append(dataset_id)
                        errors.append(f"Dataset {dataset_id}: Migration to stage failed")
                
                except Exception as e:
                    error_msg = f"Dataset {dataset_id}: {str(e)}"
                    logger.error(f"❌ Error migrating dataset {dataset_id} to stage: {e}")
                    failed_migrations.append(dataset_id)
                    errors.append(error_msg)
            
            # Update spreadsheet status for successful migrations
            if successful_migrations:
                logger.info(f"📝 Updating spreadsheet status for {len(successful_migrations)} successful migrations")
                # Extract just the dataset IDs for status update
                dataset_ids = [m['dataset_id'] for m in successful_migrations]
                update_success = migration_manager.update_spreadsheet_status(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                    successful_migrations=dataset_ids,
                    credentials_path=credentials_path
                )
                
                if update_success:
                    logger.info("✅ Successfully updated spreadsheet status")
                else:
                    logger.warning("⚠️  Failed to update spreadsheet status")
                    errors.append("Failed to update spreadsheet status")
            
            # Log stage information
            logger.info("=" * 80)
            logger.info("📊 Stage Migration Summary:")
            logger.info(f"   Total datasets: {len(successful_migrations) + len(failed_migrations)}")
            logger.info(f"   ✅ Successful: {len(successful_migrations)}")
            logger.info(f"   ❌ Failed: {len(failed_migrations)}")
            logger.info("")
            logger.info(f"🏗️  Stage: {stage_name}")
            logger.info("📁 Files uploaded:")
            for migration in successful_migrations:
                logger.info(f"   - {migration['filename']} (Dataset: {migration['dataset_id']} - {migration['dataset_name']})")
            logger.info("")
            logger.info("💡 Next steps:")
            logger.info(f"   1. Review data in stage using: python main.py stage list --stage-name {stage_name}")
            logger.info(f"   2. Load to tables when ready using: python main.py migrate --from-stage --stage-name {stage_name} --target-table <table_name>")
            logger.info("=" * 80)
            
            # Return results
            total = len(successful_migrations) + len(failed_migrations)
            return {
                "success": len(successful_migrations),
                "failed": len(failed_migrations),
                "total": total,
                "errors": errors,
                "stages": successful_migrations
            }
    
    except Exception as e:
        error_msg = f"Failed to read from spreadsheet: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": 0, "failed": 0, "total": 0, "errors": [error_msg]}


def main():
    """
    Main function to handle command line arguments and execute migration.
    """
    parser = argparse.ArgumentParser(
        description="Migrate data from Domo to Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python domo_to_snowflake.py --dataset-id 12345 --target-table sales_data
    python domo_to_snowflake.py --batch-file dataset_mapping.json
    python domo_to_snowflake.py --from-spreadsheet
    python domo_to_snowflake.py --from-spreadsheet --credentials /path/to/creds.json
    python domo_to_snowflake.py --test-connection
        """
    )
    
    parser.add_argument(
        "--dataset-id",
        help="Domo dataset ID to migrate"
    )
    
    parser.add_argument(
        "--target-table",
        help="Target Snowflake table name"
    )
    
    parser.add_argument(
        "--batch-file",
        help="JSON file with dataset ID to table name mappings"
    )
    
    parser.add_argument(
        "--from-spreadsheet",
        action="store_true",
        help="Migrate datasets from Google Sheets Migration tab"
    )
    
    parser.add_argument(
        "--credentials",
        help="Path to Google Sheets credentials JSON file"
    )
    
    parser.add_argument(
        "--spreadsheet-id",
        default=SPREADSHEET_ID,
        help=f"Google Sheets spreadsheet ID (default: {SPREADSHEET_ID})"
    )
    
    parser.add_argument(
        "--sheet-name",
        default=MIGRATION_SHEET_NAME,
        help=f"Migration sheet tab name (default: {MIGRATION_SHEET_NAME})"
    )
    
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test connections to Domo and Snowflake"
    )
    
    args = parser.parse_args()
    
    # Test connection mode
    if args.test_connection:
        logger.info("🧪 Testing connections...")
        
        try:
            # Test Domo connection
            domo_connector = DomoHandler()
            logger.info("✅ Domo connection successful")
            
            # Test Snowflake connection
            snowflake_connector = SnowflakeHandler()
            logger.info("✅ Snowflake connection successful")
            
            logger.info("🎉 All connections tested successfully!")
            return 0
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return 1
    
    # Spreadsheet migration
    if args.from_spreadsheet:
        logger.info("🚀 Starting spreadsheet-based migration...")
        
        results = migrate_from_spreadsheet(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            credentials_path=args.credentials
        )
        
        if 'errors' in results and results['errors']:
            logger.error("❌ Spreadsheet migration failed due to errors:")
            for error in results['errors']:
                logger.error(f"   - {error}")
            return 1
        elif results['failed'] == 0:
            logger.info("🎉 Spreadsheet migration completed successfully!")
            return 0
        else:
            logger.error(f"❌ Spreadsheet migration completed with {results['failed']} failures!")
            return 1
    
    # Single dataset migration
    if args.dataset_id and args.target_table:
        logger.info("🚀 Starting single dataset migration...")
        
        success = migrate_dataset(args.dataset_id, args.target_table)
        
        if success:
            logger.info("🎉 Migration completed successfully!")
            return 0
        else:
            logger.error("❌ Migration failed!")
            return 1
    
    # Batch migration
    if args.batch_file:
        logger.info("🚀 Starting batch migration...")
        
        try:
            import json
            with open(args.batch_file, 'r') as f:
                dataset_mapping = json.load(f)
            
            results = batch_migrate_datasets(dataset_mapping)
            
            if results['failed'] == 0:
                logger.info("🎉 Batch migration completed successfully!")
                return 0
            else:
                logger.error(f"❌ Batch migration completed with {results['failed']} failures!")
                return 1
                
        except Exception as e:
            logger.error(f"❌ Batch migration failed: {e}")
            return 1
    
    # No valid arguments provided
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main()) 