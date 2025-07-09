#!/usr/bin/env python3
"""
Extract Domo lineage data for datasets marked as Required=True in Google Sheets.

This script:
1. Reads the "Datasets" tab from Google Sheets
2. Filters datasets where "Required" column is True
3. Extracts lineage data for each dataset using argo-domo
4. Saves lineage JSON files in a lineage_data directory
5. Extracts Dataflow IDs from the lineage data
"""

import os
import sys
import json
import subprocess
import logging
import re
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv
import pandas as pd

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from tools.utils.gsheets import GoogleSheets, READ_WRITE_SCOPES

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    """
    Sanitize a dataset name to create a valid filename.
    
    Args:
        name (str): Original dataset name
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace special characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    # Limit length
    if len(sanitized) > 100:
        sanitized = sanitized[:100]
    
    return sanitized


def get_required_datasets_from_spreadsheet(spreadsheet_id: str, credentials_path: str) -> List[Dict]:
    """
    Get datasets marked as Required=True from Google Sheets.
    
    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        credentials_path (str): Path to Google Sheets credentials file
        
    Returns:
        List[Dict]: List of required datasets with id, name, and other data
    """
    try:
        logger.info(f"📊 Reading datasets from spreadsheet: {spreadsheet_id}")
        
        # Initialize Google Sheets client
        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        
        # Read the Datasets tab
        polars_df = gsheets_client.read_to_dataframe(
            spreadsheet_id=spreadsheet_id,
            range_name="Datasets!A:Z",
            header=True
        )
        
        if polars_df is None or len(polars_df) == 0:
            logger.error("❌ No data found in Datasets tab")
            return []
        
        df = polars_df.to_pandas()
        logger.info(f"📋 Found {len(df)} rows in Datasets tab")
        
        # Find the Required column
        required_column = None
        possible_required_columns = ['Required', 'required', 'REQUIRED', 'is_required', 'Is Required']
        
        for col in possible_required_columns:
            if col in df.columns:
                required_column = col
                break
        
        if not required_column:
            logger.error("❌ Required column not found in spreadsheet")
            logger.info(f"📋 Available columns: {list(df.columns)}")
            return []
        
        # Find Dataset ID and Name columns
        dataset_id_column = None
        name_column = None
        
        possible_id_columns = ['Dataset ID', 'dataset_id', 'DatasetID', 'ID', 'id']
        for col in possible_id_columns:
            if col in df.columns:
                dataset_id_column = col
                break
        
        possible_name_columns = ['Name', 'name', 'Dataset Name', 'dataset_name']
        for col in possible_name_columns:
            if col in df.columns:
                name_column = col
                break
        
        if not dataset_id_column or not name_column:
            logger.error("❌ Dataset ID or Name column not found")
            return []
        
        # Filter datasets where Required is True
        required_datasets = []
        for index, row in df.iterrows():
            required_value = row[required_column]
            
            # Check if Required is True (case insensitive)
            if str(required_value).lower() in ['true', '1', 'yes', 'y']:
                dataset_info = {
                    'id': str(row[dataset_id_column]),
                    'name': str(row[name_column]),
                    'row_index': index + 2  # +2 for 1-based indexing and header
                }
                required_datasets.append(dataset_info)
        
        logger.info(f"✅ Found {len(required_datasets)} datasets marked as Required=True")
        return required_datasets
        
    except Exception as e:
        logger.error(f"❌ Failed to read datasets from spreadsheet: {e}")
        return []


def extract_lineage_for_dataset(dataset_id: str, dataset_name: str, output_dir: str) -> Optional[str]:
    """
    Extract lineage data for a specific dataset using argo-domo.
    
    Args:
        dataset_id (str): Domo dataset ID
        dataset_name (str): Dataset name for filename
        output_dir (str): Directory to save the lineage JSON file
        
    Returns:
        Optional[str]: Path to the saved JSON file or None if failed
    """
    try:
        # Sanitize filename
        sanitized_name = sanitize_filename(dataset_name)
        output_file = os.path.join(output_dir, f"{sanitized_name}.json")
        
        logger.info(f"🔄 Extracting lineage for dataset: {dataset_id} ({dataset_name})")
        
        # Build the argo-domo command
        cmd = [
            "argo-domo", "lineage", "export", "DATA_SOURCE",
            dataset_id,
            "--format", "json"
        ]
        
        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60  # 60 second timeout
        )
        
        if result.returncode == 0:
            # Save the output to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(result.stdout)
            
            logger.info(f"✅ Lineage extracted and saved to: {output_file}")
            return output_file
        else:
            logger.error(f"❌ Failed to extract lineage for {dataset_id}: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Timeout extracting lineage for dataset {dataset_id}")
        return None
    except Exception as e:
        logger.error(f"❌ Error extracting lineage for dataset {dataset_id}: {e}")
        return None


def extract_dataflow_ids_from_lineage(lineage_file: str, dataset_id: str) -> List[str]:
    """
    Extracts downstream Dataflow IDs that consume a dataset (i.e., are its children).
    
    Args:
        lineage_file (str): Path to the lineage JSON file
        dataset_id (str): Original dataset ID for matching
        
    Returns:
        List[str]: List of unique child Dataflow IDs found
    """
    try:
        with open(lineage_file, 'r', encoding='utf-8') as f:
            lineage_data = json.load(f)
        
        dataflow_ids = set()
        
        entities = lineage_data.get('entities')
        if not entities:
            logger.warning(f"⚠️ No 'entities' found in lineage file: {lineage_file}")
            return []

        # --- Method 1: Direct lookup of the dataset entity to find its children ---
        dataset_entity_key = f"DATA_SOURCE{dataset_id}"
        dataset_entity = entities.get(dataset_entity_key)

        if dataset_entity:
            # Get child dataflows (downstream)
            if 'children' in dataset_entity:
                for child in dataset_entity.get('children', []):
                    if child.get('type') == 'DATAFLOW' and child.get('id'):
                        dataflow_ids.add(str(child.get('id')))
        else:
            logger.warning(f"⚠️ Could not find entity '{dataset_entity_key}' directly. Falling back to full scan.")
            # --- Method 2: Full scan (fallback) ---
            # Find DATAFLOW entities that list the target dataset as a parent.
            for entity in entities.values():
                if entity.get('type') == 'DATAFLOW' and entity.get('id'):
                    # Check if the dataflow consumes our dataset (i.e., dataset is a parent)
                    if 'parents' in entity:
                        for parent in entity.get('parents', []):
                            if parent.get('type') == 'DATA_SOURCE' and parent.get('id') == dataset_id:
                                dataflow_ids.add(str(entity.get('id')))

        unique_dataflow_ids = sorted(list(dataflow_ids))
        logger.info(f"📊 Found {len(unique_dataflow_ids)} child Dataflow IDs for dataset {dataset_id}: {unique_dataflow_ids}")
        return unique_dataflow_ids
        
    except Exception as e:
        logger.error(f"❌ Error extracting Dataflow IDs from {lineage_file}: {e}")
        return []


def main():
    """
    Main function to extract lineage data for required datasets.
    """
    # Configuration
    spreadsheet_id = os.getenv("MIGRATION_SPREADSHEET_ID")
    credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    output_dir = "lineage_data"
    
    if not spreadsheet_id:
        logger.error("❌ MIGRATION_SPREADSHEET_ID not set")
        return 1
    
    if not credentials_path:
        logger.error("❌ GOOGLE_SHEETS_CREDENTIALS_FILE not set")
        return 1
    
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Credentials file not found: {credentials_path}")
        return 1
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"📁 Output directory: {output_dir}")
    
    # Get required datasets from spreadsheet
    required_datasets = get_required_datasets_from_spreadsheet(spreadsheet_id, credentials_path)
    
    if not required_datasets:
        logger.error("❌ No required datasets found")
        return 1
    
    # Process each required dataset
    results = []
    
    for dataset in required_datasets:
        dataset_id = dataset['id']
        dataset_name = dataset['name']
        
        logger.info(f"🔄 Processing dataset: {dataset_name} ({dataset_id})")
        
        # Extract lineage
        lineage_file = extract_lineage_for_dataset(dataset_id, dataset_name, output_dir)
        
        if lineage_file:
            # Extract Dataflow IDs
            dataflow_ids = extract_dataflow_ids_from_lineage(lineage_file, dataset_id)
            
            result = {
                'dataset_id': dataset_id,
                'dataset_name': dataset_name,
                'lineage_file': lineage_file,
                'dataflow_ids': dataflow_ids,
                'row_index': dataset['row_index']
            }
            results.append(result)
        else:
            logger.warning(f"⚠️  Skipping dataset {dataset_id} due to lineage extraction failure")
    
    # Save summary results
    summary_file = os.path.join(output_dir, "lineage_summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ Lineage extraction completed for {len(results)} datasets")
    logger.info(f"📊 Summary saved to: {summary_file}")
    
    # Create and display DataFrame
    if results:
        # Prepare data for DataFrame
        df_data = []
        for result in results:
            df_data.append({
                'Dataset ID': result['dataset_id'],
                'Dataset Name': result['dataset_name'],
                'Dataflow IDs': ', '.join(result['dataflow_ids']) if result['dataflow_ids'] else 'None',
                'Dataflow Count': len(result['dataflow_ids']),
                'Lineage File': os.path.basename(result['lineage_file']),
                'Row Index': result['row_index']
            })
        
        df = pd.DataFrame(df_data)
        
        # Print results as DataFrame
        print("\n" + "="*100)
        print("📊 LINEAGE EXTRACTION RESULTS")
        print("="*100)
        print(df.to_string(index=False))
        print("\n" + "="*100)
        
        # Print summary statistics
        print(f"📈 SUMMARY STATISTICS:")
        print(f"   • Total datasets processed: {len(results)}")
        print(f"   • Datasets with Dataflows: {len([r for r in results if r['dataflow_ids']])}")
        print(f"   • Total Dataflow IDs found: {sum(len(r['dataflow_ids']) for r in results)}")
        print(f"   • Average Dataflows per dataset: {sum(len(r['dataflow_ids']) for r in results) / len(results):.2f}")
        print("="*100)
        
        # Show datasets without Dataflows (if any)
        datasets_without_dataflows = [r for r in results if not r['dataflow_ids']]
        if datasets_without_dataflows:
            print(f"\n⚠️  DATASETS WITHOUT DATAFLOWS ({len(datasets_without_dataflows)}):")
            for dataset in datasets_without_dataflows:
                print(f"   • {dataset['dataset_name']} ({dataset['dataset_id']})")
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1) 