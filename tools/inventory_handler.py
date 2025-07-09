#!/usr/bin/env python3
"""
Inventory Exporter - Google Sheets to SQL Converter

This script extracts inventory data from a Google Sheets document and converts
dataflows to Snowflake SQL using argo-utils-cli.

Dependencies:
    pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 pandas

Usage:
    python inventory_exporter.py --export-dir exported_sql
    
    Or set environment variable:
    export EXPORT_DIR=exported_sql
    python inventory_exporter.py
"""

import os
import sys
import argparse
import logging
import subprocess
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dotenv import load_dotenv

# Import utility modules
from .utils.gsheets import GoogleSheets, READ_WRITE_SCOPES

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
INVENTORY_SHEET_NAME = "Inventory"
DATAFLOW_COLUMN_NAME = "Dataflow ID"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


class InventoryHandler:
    """
    Handler class for Google Sheets inventory data.
    
    This class handles authentication with Google Sheets API and provides
    methods to extract inventory data from a specific spreadsheet.
    """
    
    def __init__(self, credentials_path: str = None, spreadsheet_id: str = SPREADSHEET_ID):
        """
        Initialize the InventoryHandler.
        
        Args:
            credentials_path (str): Path to the service account JSON file
            spreadsheet_id (str): Google Sheets spreadsheet ID
        """
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        self.gsheets_client = None
        
        self._authenticate()
    
    def _authenticate(self) -> None:
        """
        Authenticate with Google Sheets API using service account credentials.
        
        Raises:
            FileNotFoundError: If credentials file is not found
            Exception: If authentication fails
        """
        if not self.credentials_path:
            raise ValueError("Credentials path not provided. Set GOOGLE_SHEETS_CREDENTIALS_FILE or pass credentials_path")
        
        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"Credentials file not found: {self.credentials_path}")
        
        try:
            self.gsheets_client = GoogleSheets(credentials_path=self.credentials_path)
            logger.info("✅ Successfully authenticated with Google Sheets API")
        except Exception as e:
            logger.error(f"❌ Failed to authenticate with Google Sheets API: {e}")
            raise
    
    def get_inventory(self, sheet_name: str = INVENTORY_SHEET_NAME) -> pd.DataFrame:
        """
        Extract inventory data from the specified Google Sheets tab.
        
        Args:
            sheet_name (str): Name of the sheet tab to extract from
            
        Returns:
            pd.DataFrame: Inventory data as a pandas DataFrame
            
        Raises:
            Exception: If API request fails
            ValueError: If no data is found
        """
        logger.info(f"📖 Extracting inventory data from sheet: {sheet_name}")
        
        try:
            # Use the gsheets module to read data
            polars_df = self.gsheets_client.read_to_dataframe(
                spreadsheet_id=self.spreadsheet_id,
                range_name=f"{sheet_name}!A:Z",
                header=True
            )
            
            if polars_df is None or len(polars_df) == 0:
                raise ValueError(f"No data found in sheet: {sheet_name}")
            
            # Convert polars DataFrame to pandas DataFrame
            df = polars_df.to_pandas()
            
            logger.info(f"✅ Successfully extracted {len(df)} rows from {sheet_name}")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error extracting inventory data: {e}")
            raise
    
    def get_unique_dataflows(self, df: pd.DataFrame, dataflow_column: str = DATAFLOW_COLUMN_NAME) -> List[str]:
        """
        Extract unique dataflow values from the inventory DataFrame.
        
        Args:
            df (pd.DataFrame): Inventory DataFrame
            dataflow_column (str): Name of the dataflow column
            
        Returns:
            List[str]: List of unique dataflow values
            
        Raises:
            KeyError: If dataflow column is not found
        """
        if dataflow_column not in df.columns:
            available_columns = list(df.columns)
            logger.error(f"❌ Column '{dataflow_column}' not found. Available columns: {available_columns}")
            raise KeyError(f"Column '{dataflow_column}' not found in DataFrame")
        
        # Get unique values, excluding empty strings
        unique_dataflows = df[dataflow_column].dropna().astype(str).str.strip()
        unique_dataflows = unique_dataflows[unique_dataflows != ''].unique().tolist()
        
        logger.info(f"📊 Found {len(unique_dataflows)} unique dataflows")
        return unique_dataflows


def _translate_dataflow_to_sql(dataflow: str, argo_utils_path: str = None) -> Optional[str]:
    """
    Translate a dataflow to Snowflake SQL using argo-utils-cli.
    
    Args:
        dataflow (str): Dataflow identifier to translate
        argo_utils_path (str): Path to argo-utils-cli directory
        
    Returns:
        Optional[str]: Generated SQL code or None if translation fails
    """
    logger.info(f"🔄 Translating dataflow: {dataflow}")
    
    try:
        # Determine argo-utils-cli path
        if not argo_utils_path:
            argo_utils_path = os.path.join(os.getcwd(), "argo-utils-cli")
        
        if not os.path.exists(argo_utils_path):
            logger.warning(f"⚠️  argo-utils-cli not found at {argo_utils_path}")
            logger.info("📝 Will generate placeholder SQL (counted as failure)")
            return None
        
        # Check if we have the required environment variables for Domo API
        token = os.getenv("DOMO_DEVELOPER_TOKEN")
        instance = os.getenv("DOMO_INSTANCE")
        
        if not token or not instance:
            logger.warning("⚠️  DOMO_DEVELOPER_TOKEN or DOMO_INSTANCE not set")
            logger.info("📝 Will generate placeholder SQL (counted as failure)")
            return None
        
        # Try to use argo-utils-cli service etl-translator command
        cmd = [
            "python", "-m", "domo_utils.cli.main",
            "service", "etl-translator",
            dataflow,
            "--dialect", "snowflake",
            "--output-dir", "/tmp",
            "--output-name", f"temp_dataflow_{dataflow}.sql"
        ]
        
        # Execute the command
        result = subprocess.run(
            cmd,
            cwd=argo_utils_path,
            capture_output=True,
            text=True,
            timeout=60  # Increased timeout for API calls
        )
        
        if result.returncode == 0:
            # Read the generated SQL file
            temp_file = f"/tmp/temp_dataflow_{dataflow}.sql"
            if os.path.exists(temp_file):
                with open(temp_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # Clean up temporary file
                os.remove(temp_file)
                
                logger.info(f"✅ Successfully translated dataflow: {dataflow}")
                return sql_content
            else:
                logger.warning(f"⚠️  SQL file not generated for dataflow: {dataflow}")
                return None
        else:
            logger.warning(f"⚠️  Translation failed for {dataflow}: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Translation timeout for dataflow: {dataflow}")
        return None
    except Exception as e:
        logger.error(f"❌ Error translating dataflow {dataflow}: {e}")
        return None


def _generate_placeholder_sql(dataflow: str) -> str:
    """
    Generate a placeholder SQL file when translation fails.
    
    Args:
        dataflow (str): Dataflow identifier
        
    Returns:
        str: Placeholder SQL content
    """
    return f"""-- Placeholder SQL for dataflow: {dataflow}
-- Generated by inventory_exporter.py
-- TODO: Implement actual SQL logic for this dataflow

-- SETUP REQUIRED FOR REAL TRANSLATION:
-- 1. Set environment variables:
--    export DOMO_DEVELOPER_TOKEN="your_developer_token"
--    export DOMO_INSTANCE="your_instance_name"
-- 2. Ensure argo-utils-cli is properly installed and configured
-- 3. Re-run the export to get actual SQL translation

-- Example structure:
SELECT 
    -- Add your columns here
    *
FROM 
    -- Add your source tables here
    source_table
WHERE 
    -- Add your conditions here
    1=1;

-- Notes:
-- - This is a placeholder generated because real translation was not available
-- - For dataflow ID: {dataflow}
-- - To get actual SQL, configure Domo API credentials and re-run
-- - The real translator converts Domo Magic ETL to Snowflake SQL
-- - Consider the business requirements for this specific dataflow
"""


def export_dataflows_to_sql(output_dir: str, credentials_path: str = None) -> bool:
    """
    Export dataflows from Google Sheets inventory to SQL files.
    
    This function:
    1. Extracts inventory data from Google Sheets
    2. Gets unique dataflow values
    3. Translates each dataflow to SQL using argo-utils-cli
    4. Saves SQL files to the output directory
    
    Args:
        output_dir (str): Directory to save SQL files
        credentials_path (str): Path to Google Sheets credentials file
        
    Returns:
        bool: True if export successful, False otherwise
    """
    logger.info("🚀 Starting dataflow export process...")
    
    try:
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Output directory: {output_path.absolute()}")
        
        # Initialize inventory handler
        extractor = InventoryHandler(credentials_path=credentials_path)
        
        # Extract inventory data
        df = extractor.get_inventory()
        
        # Check if the dataflow column exists and try alternatives
        dataflow_column = None
        possible_column_names = [DATAFLOW_COLUMN_NAME, "dataflow", "Dataflow", "DataFlow", "dataflow_id", "Dataflow_ID"]
        
        for col_name in possible_column_names:
            if col_name in df.columns:
                dataflow_column = col_name
                logger.info(f"✅ Found dataflow column: '{col_name}'")
                break
        
        if not dataflow_column:
            logger.error(f"❌ No dataflow column found. Available columns: {list(df.columns)}")
            logger.error(f"   Searched for: {possible_column_names}")
            return False
        
        # Get unique dataflows using the found column
        unique_dataflows = extractor.get_unique_dataflows(df, dataflow_column=dataflow_column)
        
        if not unique_dataflows:
            logger.warning("⚠️  No dataflows found in inventory")
            return False
        
        # Process each dataflow
        successful_exports = 0
        failed_exports = 0
        
        for dataflow in unique_dataflows:
            try:
                # Translate dataflow to SQL
                sql_content = _translate_dataflow_to_sql(dataflow)
                
                if sql_content:
                    # Real translation succeeded
                    sql_filename = f"{dataflow.replace(' ', '_').replace('/', '_')}.sql"
                    sql_filepath = output_path / sql_filename
                    
                    with open(sql_filepath, 'w', encoding='utf-8') as f:
                        f.write(sql_content)
                    
                    logger.info(f"💾 Saved (SUCCESS): {sql_filepath}")
                    successful_exports += 1
                else:
                    # Translation failed, generate placeholder
                    placeholder_sql = _generate_placeholder_sql(dataflow)
                    sql_filename = f"{dataflow.replace(' ', '_').replace('/', '_')}.sql"
                    sql_filepath = output_path / sql_filename
                    
                    with open(sql_filepath, 'w', encoding='utf-8') as f:
                        f.write(placeholder_sql)
                    
                    logger.warning(f"💾 Saved (PLACEHOLDER): {sql_filepath}")
                    failed_exports += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing dataflow {dataflow}: {e}")
                # Still try to generate placeholder for this dataflow
                try:
                    placeholder_sql = _generate_placeholder_sql(dataflow)
                    sql_filename = f"{dataflow.replace(' ', '_').replace('/', '_')}.sql"
                    sql_filepath = output_path / sql_filename
                    
                    with open(sql_filepath, 'w', encoding='utf-8') as f:
                        f.write(placeholder_sql)
                    
                    logger.warning(f"💾 Saved (ERROR PLACEHOLDER): {sql_filepath}")
                except Exception as placeholder_error:
                    logger.error(f"❌ Failed to create placeholder for {dataflow}: {placeholder_error}")
                
                failed_exports += 1
        
        # Summary
        total_dataflows = len(unique_dataflows)
        logger.info("=" * 80)
        logger.info(f"📊 Export Summary:")
        logger.info(f"   Total dataflows: {total_dataflows}")
        logger.info(f"   ✅ Real translations: {successful_exports}")
        logger.info(f"   ⚠️  Placeholder files: {failed_exports}")
        logger.info(f"   📁 Output directory: {output_path.absolute()}")
        logger.info("=" * 80)
        
        if failed_exports > 0:
            logger.info("💡 To get real SQL translations:")
            logger.info("   1. Set DOMO_DEVELOPER_TOKEN and DOMO_INSTANCE environment variables")
            logger.info("   2. Ensure argo-utils-cli is properly installed (pip install -e .)")
            logger.info("   3. Re-run the export command")
        
        # Return True if we generated any files (even if they're placeholders)
        return total_dataflows > 0
        
    except Exception as e:
        logger.error(f"❌ Export process failed: {e}")
        return False


def main():
    """
    Main function to handle command line arguments and execute export.
    """
    parser = argparse.ArgumentParser(
        description="Export inventory dataflows from Google Sheets to SQL files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python inventory_exporter.py --export-dir exported_sql
    python inventory_exporter.py --export-dir /path/to/output --credentials /path/to/creds.json
    
Environment Variables:
    EXPORT_DIR: Default export directory
    GOOGLE_SHEETS_CREDENTIALS_FILE: Path to Google Sheets credentials file
        """
    )
    
    parser.add_argument(
        "--export-dir",
        default=os.getenv("EXPORT_DIR", "exported_sql"),
        help="Directory to save SQL files (default: exported_sql)"
    )
    
    parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Google Sheets connection and show inventory preview"
    )
    
    args = parser.parse_args()
    
    # Validate credentials
    if not args.credentials:
        logger.error("❌ Credentials file not specified")
        logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
        return 1
    
    if not os.path.exists(args.credentials):
        logger.error(f"❌ Credentials file not found: {args.credentials}")
        return 1
    
    # Test connection mode
    if args.test_connection:
        try:
            logger.info("🧪 Testing Google Sheets connection...")
            extractor = InventoryHandler(credentials_path=args.credentials)
            df = extractor.get_inventory()
            
            logger.info("✅ Connection successful!")
            logger.info(f"📊 Inventory preview (first 5 rows):")
            print(df.head())
            
            # Check for dataflow column
            dataflow_column = None
            possible_column_names = [DATAFLOW_COLUMN_NAME, "dataflow", "Dataflow", "DataFlow", "dataflow_id", "Dataflow_ID"]
            
            for col_name in possible_column_names:
                if col_name in df.columns:
                    dataflow_column = col_name
                    logger.info(f"✅ Found dataflow column: '{col_name}'")
                    break
            
            if dataflow_column:
                unique_dataflows = extractor.get_unique_dataflows(df, dataflow_column=dataflow_column)
                logger.info(f"📋 Found {len(unique_dataflows)} dataflows: {unique_dataflows[:10]}...")  # Show first 10
            else:
                logger.warning(f"⚠️  No dataflow column found. Available columns: {list(df.columns)}")
            
            return 0
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return 1
    
    # Export mode
    logger.info("🚀 Starting inventory export...")
    logger.info(f"📁 Export directory: {args.export_dir}")
    logger.info(f"🔑 Credentials file: {args.credentials}")
    
    success = export_dataflows_to_sql(
        output_dir=args.export_dir,
        credentials_path=args.credentials
    )
    
    if success:
        logger.info("🎉 Export completed successfully!")
        return 0
    else:
        logger.error("❌ Export failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 