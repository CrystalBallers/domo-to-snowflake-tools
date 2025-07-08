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
    # ["bfd072c5-5545-43fc-a9bd-12ea72ec9e5e", "ns_locations"],
    # ["1a0426d1-e1a5-423c-b806-b67ab2f3e7e5", "ns_po_qty_by_sku"],
    # ["86578fd6-ac2b-46f5-b905-3d6965a7faae", "UPCs"],
    # ["209b7484-c2d6-4f9c-8fbf-48bf2312a0d5", "advertising_keywords"],
    # ["4c063636-9fa4-4ee1-8127-9d1bd7be0752", "idq_report"],
    # ["d28d511f-217c-4b95-af63-175e76902176", "View of Products w/ Categories v2"],
    # ["48a7b1ce-8446-4292-a906-cdd0f1f19158", "ware2go_inventory"],
    # ["3066d81f-78cf-4515-b6dd-8e26db1ef53d", "clients"],
    # ["24051eff-a74c-484b-b60f-995b0a7a23b0", "advertising portfolios with brands"],
    # ["3ce76e89-22bd-417a-a8c5-e4c57b4c64c5", "scraped_asins"],
    # ["bd48da9a-123c-4165-883e-5ce6b975106f", "Brands"],
    # ["57ecb8ff-fc13-4f85-9541-2eb523d3d24d", "ns_local_inventory_by_sku"],
    # ["3f9e87d8-d723-47d4-a443-56675fac1a14", "ns_netsuite_items"],
    # ["bf009d14-af05-4a47-bee9-f70236113ed3", "advertising_targets"],

    # ["8e5b68e3-e603-4798-b072-8435e12dd21b", "vw_products (ASIN,UPC,SKU,BUNDLE)"],
    # ["2bc10da6-b8cd-4705-9103-c0a786f2a911", "amazon_skus"],
    # ["84a75c13-6f71-47c9-8739-3aee9de29250", "inventory_vendors"],
    # ["e99813a1-a712-4da2-aeee-4e666f376ad3", "google_ads_campaings_by_day"],
    # ["49f2ce09-cffc-411d-add4-584c70c03e4d", "subscribe_save"],
    # ["c33ab671-e64a-4e31-bf8a-7923e144ee39", "advertising_campaigns"],
    # ["ac7dfacf-0041-4792-83be-753e3d44149d", "advertising_portfolios"],
    # ["a38df5a8-3e20-4f6b-85bc-2317a81a85f7", "2024 BFCM Tracker - All Teams"],
    ["92e03f2c-fbea-4269-820c-e4daf872a7c0", "sales_by_products_archive"],
    ["381807dc-d329-439f-9535-cb9125f03909", "vw_inventory_reordering"],
    ["3144f5f2-9b6b-4484-a2d1-fc727fed442a", "OpenExchangeCurrencyConversion"],
    ["1c8899d8-8f42-4a60-b523-15c0ccc18707", "advertising_campaign_performance"],
    ["5aed56d8-ce97-43d9-ad53-e9a202cf5e9d", "advertising_asin_metrics"],
    ["d7889ba8-a3fb-4f76-b80b-97311d1ffa3b", "product_upcs"],
    ["e7087273-f3c4-4d79-9ed9-62272cd15396", "countries"],
    ["aedbd18d-fc71-44f1-b554-c39a1991112f", "UPCs w/ Categories v2"],
    ["c2bf2dfd-2479-4c16-9cdd-75ab0cd15bd4", "loreal 445 calendar 6.10.21.xlsx"],
    ["c3ce53d9-3872-4dc1-8f21-a77b6e59ac09", "Historical 1P + 2P Sales Data (Pro Forma)"],
    ["aa378e22-c604-4a4d-a92f-6dfd4be8aa95", "amazon_inventory_log"],
    ["3b9132c7-a662-4041-afd7-bd73e3d9eaf1", "ns_local_inventory_mfn_by_sku"],
    ["f0e4d808-e224-42e0-ab11-33a1bc1fbc34", "portfolio_opcs"],
    ["40cd2430-b70f-47cc-af1f-36474f68740a", "amazon_asin_transparency_status_log"],
    ["96775fde-be62-42b2-b109-c342391739fd", "ns_nonsellable_inventory_by_sku"],
    ["d19bd8df-29e2-4628-ac94-659e4a968e70", "advertising_metrics"],
    ["bc155304-3ee8-4445-b7c3-d6edf96c7e4f", "Demand Forecast"],
    ["afeb261b-3270-409d-80d4-475a8c570940", "Meltables 2025"],
    ["86c9c87f-7fb4-4e5b-9d13-1ae13d5799fc", "amazon_asins"],
    ["3e5c6924-8825-440c-8423-e31327d27a05", "Active, Suppressed and Out of Stock BB"],
    ["f78a615a-ac43-43f9-8a3d-068469fb67b5", "ns_purchase_orders"],
    ["b7f6bd0c-1830-4c24-9cf2-53ea86aa9c75", "advertising_walmart_metrics"],
    ["9b6bf6e5-a91b-42d1-9e83-29f05a0df918", "amazon_seller_central_accounts"],
    ["53c6b7ef-5891-4214-b22a-127e12de5241", "UPCs Tag Categories"],
    ["4d8f7ff6-2132-4034-92a6-ac6df4af6d7c", "purchase_orders"],
    ["b0c317a0-850a-4c42-aa94-1a7b88d67462", "Brand DoC & Attributes"],
    ["5d745a5d-2a9a-4bae-bb84-110b770e9da8", "subscribe_save_subscriptions"],

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