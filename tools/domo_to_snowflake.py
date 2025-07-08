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
from pathlib import Path
from dotenv import load_dotenv

# Add the parent directory to the path to import from tools
sys.path.insert(0, str(Path(__file__).parent))

from utils.snowflake import SnowflakeHandler
from utils.domo import DomoHandler

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_dataset(dataset_id: str, target_table: str) -> bool:
    """
    Migrate a single dataset from Domo to Snowflake.
    
    Args:
        dataset_id (str): Domo dataset ID
        target_table (str): Target Snowflake table name
        
    Returns:
        bool: True if migration successful, False otherwise
    """
    logger.info(f"🚀 Starting migration of dataset {dataset_id} to {target_table}")
    
    try:
        # Initialize handlers
        domo_handler = DomoHandler()
        snowflake_handler = SnowflakeHandler()
        
        # Setup connections
        if not domo_handler.setup_auth():
            logger.error("❌ Failed to authenticate with Domo")
            return False
        
        if not snowflake_handler.setup_connection():
            logger.error("❌ Failed to connect to Snowflake")
            return False
        
        # Extract data from Domo
        logger.info("📥 Extracting data from Domo...")
        df = domo_handler.extract_data(dataset_id)
        
        if df is None or len(df) == 0:
            logger.warning(f"⚠️  No data found for dataset {dataset_id}")
            return False
        
        logger.info(f"✅ Extracted {len(df)} rows from Domo")
        
        # Load data to Snowflake
        logger.info("📤 Loading data to Snowflake...")
        success = snowflake_handler.upload_data(df, target_table)
        
        if success:
            # Verify upload
            if snowflake_handler.verify_upload(target_table, len(df)):
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
    finally:
        # Cleanup connections
        try:
            snowflake_handler.cleanup()
        except:
            pass


def batch_migrate_datasets(dataset_mapping: dict) -> dict:
    """
    Migrate multiple datasets from Domo to Snowflake.
    
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
    
    for dataset_id, target_table in dataset_mapping.items():
        try:
            success = migrate_dataset(dataset_id, target_table)
            
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