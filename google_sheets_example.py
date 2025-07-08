#!/usr/bin/env python3
"""
Google Sheets Example Script

This script demonstrates how to use Google Sheets with the Domo-to-Snowflake project.
It can read data from Google Sheets and write data to Google Sheets.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Import utility modules
from utils.gsheets import GoogleSheets
from utils.domo import DomoHandler
from utils.snowflake import SnowflakeHandler

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


def read_from_google_sheets():
    """Example: Read data from Google Sheets."""
    # Get credentials file path
    credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_file:
        logger.error("GOOGLE_SHEETS_CREDENTIALS_FILE not set")
        return None
    
    # Initialize Google Sheets client
    sheets_client = GoogleSheets(credentials_path=credentials_file)
    
    # Get spreadsheet ID from environment
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("GOOGLE_SHEETS_SPREADSHEET_ID not set")
        return None
    
    # Read data from first sheet
    df = sheets_client.read_to_dataframe(spreadsheet_id, "Sheet1!A:Z", header=True)
    if df is not None and len(df) > 0:
        logger.info(f"Read {len(df)} rows from Google Sheets")
        logger.info(f"Columns: {df.columns}")
        return df
    
    return None


def write_to_google_sheets(df, worksheet_name="Data_Export"):
    """Example: Write data to Google Sheets."""
    # Get credentials file path
    credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_file:
        logger.error("GOOGLE_SHEETS_CREDENTIALS_FILE not set")
        return False
    
    # Initialize Google Sheets client with write permissions
    from utils.gsheets import READ_WRITE_SCOPES
    sheets_client = GoogleSheets(credentials_path=credentials_file, scopes=READ_WRITE_SCOPES)
    
    # Get spreadsheet ID from environment
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("GOOGLE_SHEETS_SPREADSHEET_ID not set")
        return False
    
    try:
        # Write DataFrame to worksheet
        result = sheets_client.write_dataframe(df, spreadsheet_id, f"{worksheet_name}!A1", include_header=True)
        logger.info(f"Successfully wrote data to worksheet: {worksheet_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to write data to Google Sheets: {e}")
        return False


def domo_to_google_sheets():
    """Example: Extract data from Domo and write to Google Sheets."""
    domo_handler = DomoHandler()
    
    # Setup Domo authentication
    if not domo_handler.setup_auth():
        logger.error("Failed to authenticate with Domo")
        return False
    
    # Example dataset ID (replace with actual ID)
    dataset_id = "your-dataset-id-here"
    
    # Extract data from Domo
    df = domo_handler.extract_data(dataset_id)
    if df is None:
        logger.error("Failed to extract data from Domo")
        return False
    
    # Write to Google Sheets
    success = write_to_google_sheets(df, "Domo_Data")
    return success


def main():
    """Main function to demonstrate Google Sheets functionality."""
    logger.info("🚀 Starting Google Sheets example...")
    
    # Example 1: Read from Google Sheets
    logger.info("\n--- Reading from Google Sheets ---")
    df = read_from_google_sheets()
    
    if df is not None:
        logger.info(f"Successfully read {len(df)} rows")
        
        # Example 2: Write back to Google Sheets
        logger.info("\n--- Writing to Google Sheets ---")
        success = write_to_google_sheets(df, "Example_Output")
        
        if success:
            logger.info("✅ Google Sheets operations completed successfully!")
        else:
            logger.error("❌ Failed to write to Google Sheets")
    else:
        logger.error("❌ Failed to read from Google Sheets")
    
    # Example 3: Domo to Google Sheets (uncomment to use)
    # logger.info("\n--- Domo to Google Sheets ---")
    # success = domo_to_google_sheets()
    # if success:
    #     logger.info("✅ Domo to Google Sheets completed successfully!")
    # else:
    #     logger.error("❌ Failed Domo to Google Sheets")


if __name__ == "__main__":
    main() 