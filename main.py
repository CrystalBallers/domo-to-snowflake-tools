#!/usr/bin/env python3
"""
Domo to Snowflake Data Transfer Script

This script extracts data from multiple Domo datasets using SQL queries and uploads them to Snowflake.
It supports both full table extraction and custom SQL queries.

Usage:
    python main.py  # Processes all datasets in LIST_OF_DATASETS
    python main.py --dry-run  # Test extraction without uploading
    python main.py --if-exists append  # Append data instead of replacing

Environment Variables Required:
    # Domo Authentication
    DOMO_DEVELOPER_TOKEN or (DOMO_CLIENT_ID and DOMO_CLIENT_SECRET)
    DOMO_INSTANCE
    
    # Snowflake Connection
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
"""

# LISTA DE DATASETS A PROCESAR
# Formato: [dataset_id, table_name]
LIST_OF_DATASETS = [
    ["a3894480-f0b0-4c70-a59d-83b8589bfddf",	"amazon_order_items"],
]

import os
import sys
import argparse
import logging
import time
from typing import Optional, List, Tuple
from pathlib import Path

from dotenv import load_dotenv

# Import utility modules
from utils.domo import DomoHandler
from utils.snowflake import SnowflakeHandler

# Load environment variables
root_dir = Path(__file__).parent
env_path = root_dir / ".env"
print(f"🔍 Looking for .env file at: {env_path}")
load_dotenv(env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('domo_to_snowflake.log')
    ]
)
logger = logging.getLogger(__name__)


class DomoToSnowflakeTransfer:
    """Handles the transfer of data from Domo to Snowflake."""
    
    def __init__(self):
        """Initialize the transfer handler."""
        self.domo_handler = DomoHandler()
        self.snowflake_handler = SnowflakeHandler()
        
    def transfer_data(self, dataset_id: str, table_name: str, query: Optional[str] = None, 
                     if_exists: str = 'replace') -> bool:
        """
        Complete data transfer from Domo to Snowflake.
        
        Args:
            dataset_id: Domo dataset ID
            table_name: Target Snowflake table name
            query: Optional custom SQL query
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            bool: True if transfer successful, False otherwise
        """
        logger.info("🚀 Starting Domo to Snowflake transfer")
        
        # Setup connections
        if not self.domo_handler.setup_auth():
            return False
            
        # if not self.snowflake_handler.setup_connection():
        #     return False
        
        # Extract data
        df = self.domo_handler.extract_data(dataset_id, query)
        if df is None:
            return False
        
        # Upload to Snowflake
        if not self.snowflake_handler.upload_data(df, table_name, if_exists):
            return False
        
        # Verify upload
        if not self.snowflake_handler.verify_upload(table_name, len(df)):
            return False
        
        logger.info("✅ Transfer completed successfully!")
        return True
    
    def cleanup(self):
        """Cleanup connections."""
        self.snowflake_handler.cleanup()


def main():
    """Main function to handle command line arguments and execute transfer for multiple datasets."""
    parser = argparse.ArgumentParser(
        description="Transfer data from multiple Domo datasets to Snowflake tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--query", 
        help="Custom SQL query to apply to all datasets (default: 'SELECT * FROM table')"
    )
    
    parser.add_argument(
        "--if-exists",
        choices=['replace', 'append', 'fail'],
        default='replace',
        help="What to do if table exists (default: replace)"
    )
    
    parser.add_argument(
        "--dry-run",
        action='store_true',
        help="Extract data but don't upload to Snowflake"
    )
    
    parser.add_argument(
        "--disable-cleaning",
        action='store_true',
        help="Disable automatic data cleaning (not recommended)"
    )
    
    parser.add_argument(
        "--numeric-threshold",
        type=float,
        default=0.5,
        help="Threshold for numeric conversion (0.0-1.0, default: 0.5)"
    )
    
    parser.add_argument(
        "--date-threshold",
        type=float,
        default=0.3,
        help="Threshold for date conversion (0.0-1.0, default: 0.3)"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000000,
        help="Number of rows to extract per chunk (default: 1,000,000)"
    )
    
    args = parser.parse_args()
    
    # Check if we have datasets to process
    if not LIST_OF_DATASETS:
        logger.error("❌ No datasets configured in LIST_OF_DATASETS")
        sys.exit(1)
    
    logger.info(f"🚀 Starting transfer for {len(LIST_OF_DATASETS)} datasets")
    
    # Initialize transfer handler
    transfer = DomoToSnowflakeTransfer()
    
    # Track results
    successful_transfers = 0
    failed_transfers = 0
    
    try:
        # Setup connections once for all datasets
        if not args.dry_run:
            if not transfer.domo_handler.setup_auth():
                logger.error("❌ Failed to authenticate with Domo")
                sys.exit(1)
                
            if not transfer.snowflake_handler.setup_connection():
                logger.error("❌ Failed to connect to Snowflake")
                sys.exit(1)
                time.sleep(60)
        else:
            # For dry run, only setup Domo auth
            if not transfer.domo_handler.setup_auth():
                logger.error("❌ Failed to authenticate with Domo")
                sys.exit(1)
        
        # Process each dataset
        for i, (dataset_id, table_name) in enumerate(LIST_OF_DATASETS, 1):
            logger.info(f"\n📊 Processing dataset {i}/{len(LIST_OF_DATASETS)}")
            logger.info(f"Dataset ID: {dataset_id}")
            logger.info(f"Table Name: {table_name}")
            
            try:
                if args.dry_run:
                    # Dry run - only extract data
                    df = transfer.domo_handler.extract_data(dataset_id, args.query)
                    if df is not None:
                        logger.info(f"✅ Dry run successful for {table_name} - extracted {len(df)} rows")
                        logger.info(f"Data preview:\n{df.head()}")
                        successful_transfers += 1
                    else:
                        logger.error(f"❌ Dry run failed for {table_name}")
                        failed_transfers += 1
                else:
                    # Full transfer
                    success = transfer.transfer_data(
                        dataset_id=dataset_id,
                        table_name=table_name,
                        query=args.query,
                        if_exists=args.if_exists
                    )
                    
                    if success:
                        successful_transfers += 1
                        logger.info(f"✅ Transfer completed for {table_name}")
                    else:
                        failed_transfers += 1
                        logger.error(f"❌ Transfer failed for {table_name}")
                        
            except Exception as e:
                failed_transfers += 1
                logger.error(f"❌ Unexpected error processing {table_name}: {e}")
                continue  # Continue with next dataset
        
        # Summary
        logger.info(f"\n📋 Transfer Summary:")
        logger.info(f"✅ Successful: {successful_transfers}")
        logger.error(f"❌ Failed: {failed_transfers}")
        logger.info(f"📊 Total: {len(LIST_OF_DATASETS)}")
        
        if failed_transfers > 0:
            logger.warning(f"⚠️  {failed_transfers} transfers failed")
            sys.exit(1)
        else:
            logger.info("🎉 All transfers completed successfully!")
                
    except KeyboardInterrupt:
        logger.info("Transfer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        transfer.cleanup()


if __name__ == "__main__":
    main() 