#!/usr/bin/env python3
"""
Test Google Sheets Integration

This test script accesses a specific Google Sheets document and reads the Inventory tab.
Spreadsheet URL: https://docs.google.com/spreadsheets/d/1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA/edit
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import polars as pl

# Import utility modules
from utils.gsheets import GoogleSheets, READ_WRITE_SCOPES

# Load environment variables
root_dir = Path(__file__).parent
env_path = root_dir / ".env"
load_dotenv(env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Spreadsheet configuration
SPREADSHEET_ID = "1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA"
INVENTORY_TAB = "Inventory"


def test_google_sheets_connection():
    """Test Google Sheets connection and authentication."""
    logger.info("🧪 Testing Google Sheets connection...")
    
    # Get credentials file path
    credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_file:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS_FILE not set in .env file")
        logger.error("Please set the path to your service account JSON file")
        return False
    
    if not os.path.exists(credentials_file):
        logger.error(f"❌ Credentials file not found: {credentials_file}")
        return False
    
    try:
        # Initialize Google Sheets client
        sheets_client = GoogleSheets(credentials_path=credentials_file)
        logger.info("✅ Google Sheets client initialized successfully")
        return sheets_client
    except Exception as e:
        logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
        return False


def test_read_inventory_tab(sheets_client):
    """Read data from the Inventory tab."""
    logger.info(f"📖 Reading data from '{INVENTORY_TAB}' tab...")
    
    try:
        # Read data from Inventory tab
        df = sheets_client.read_to_dataframe(
            spreadsheet_id=SPREADSHEET_ID,
            range_name=f"{INVENTORY_TAB}!A:Z",  # Read all columns
            header=True
        )
        
        if df is not None and len(df) > 0:
            logger.info(f"✅ Successfully read {len(df)} rows from Inventory tab")
            return df
        else:
            logger.warning("⚠️  No data found in Inventory tab")
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to read Inventory tab: {e}")
        return None


def print_inventory_data(df):
    """Print inventory data to console."""
    if df is None:
        logger.error("❌ No data to print")
        return
    
    logger.info("📊 INVENTORY DATA:")
    logger.info("=" * 80)
    
    # Print basic info
    logger.info(f"📋 Rows: {len(df)}")
    logger.info(f"📋 Columns: {len(df.columns)}")
    logger.info(f"📋 Column names: {list(df.columns)}")
    logger.info("=" * 80)
    
    # Print first few rows
    logger.info("🔍 First 10 rows:")
    print(df.head(10))
    
    logger.info("=" * 80)
    
    # Print data types
    logger.info("📊 Data types:")
    for col, dtype in df.schema.items():
        logger.info(f"  {col}: {dtype}")
    
    logger.info("=" * 80)
    
    # Print summary statistics for numeric columns
    numeric_cols = []
    for col, dtype in df.schema.items():
        dtype_str = str(dtype).lower()
        if any(num_type in dtype_str for num_type in ['int', 'float']):
            numeric_cols.append(col)
    
    if numeric_cols:
        logger.info("📈 Summary for numeric columns:")
        for col in numeric_cols:
            try:
                stats = df.select(col).describe()
                logger.info(f"  {col}: {stats}")
            except Exception as e:
                logger.warning(f"  {col}: Could not calculate stats - {e}")
    else:
        logger.info("📈 No numeric columns found")
    
    logger.info("=" * 80)


def test_spreadsheet_properties(sheets_client):
    """Test getting spreadsheet properties."""
    logger.info("📋 Getting spreadsheet properties...")
    
    try:
        properties = sheets_client.get_sheet_properties(SPREADSHEET_ID)
        
        logger.info(f"📄 Spreadsheet title: {properties.get('properties', {}).get('title', 'Unknown')}")
        
        sheets = properties.get('sheets', [])
        logger.info(f"📑 Available tabs ({len(sheets)}):")
        
        for sheet in sheets:
            sheet_props = sheet.get('properties', {})
            title = sheet_props.get('title', 'Unknown')
            sheet_id = sheet_props.get('sheetId', 'Unknown')
            logger.info(f"  - {title} (ID: {sheet_id})")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to get spreadsheet properties: {e}")
        return False


def main():
    """Main test function."""
    logger.info("🚀 Starting Google Sheets test...")
    logger.info(f"📊 Target Spreadsheet ID: {SPREADSHEET_ID}")
    logger.info(f"📋 Target Tab: {INVENTORY_TAB}")
    logger.info("=" * 80)
    
    # Test 1: Connection
    sheets_client = test_google_sheets_connection()
    if not sheets_client:
        logger.error("❌ Connection test failed. Exiting.")
        return 1
    
    # Test 2: Spreadsheet properties
    logger.info("\n" + "=" * 80)
    test_spreadsheet_properties(sheets_client)
    
    # Test 3: Read Inventory tab
    logger.info("\n" + "=" * 80)
    df = test_read_inventory_tab(sheets_client)
    
    # Test 4: Print data
    if df is not None:
        logger.info("\n" + "=" * 80)
        print_inventory_data(df)
        
        # Save to local file for inspection
        try:
            output_file = "inventory_data.csv"
            df.write_csv(output_file)
            logger.info(f"💾 Data saved to {output_file}")
        except Exception as e:
            logger.warning(f"⚠️  Could not save to CSV: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("🎉 Google Sheets test completed!")
    
    return 0 if df is not None else 1


if __name__ == "__main__":
    sys.exit(main()) 