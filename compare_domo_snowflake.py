#!/usr/bin/env python3
"""
Script to compare Domo datasets with Snowflake tables during migration.
Allows validation of schemas, row counts and data using primary keys.
"""

import os
import sys
import logging
import argparse
import math
import re
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import datacompy

# Add tools to path
sys.path.insert(0, str(Path(__file__).parent / "tools"))

# Import utilities from tools
from tools.utils.domo_compare import Domo
from tools.utils.snowflake import SnowflakeHandler
from tools.utils.domo import DomoHandler

# Load environmxent variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# DIRECT CONFIGURATION - EDIT THESE VALUES ACCORDING TO YOUR NEEDS
# =============================================================================

# Default configuration (can be overridden with command line arguments)
DEFAULT_CONFIG = {
    'domo_dataset_id': '383336aa-ba94-4eb8-be9b-bccc94ffff40',  # Domo dataset ID
    'snowflake_table': 'int_view_of_upcs_w_categories_v2',       # Snowflake table name
    'key_columns': ['id', 'loreal_media_categories'],              # Key columns for comparison
    'sample_size': None,                        # None = calculate automatically, or specific number
    'interactive': True,                        # True = interactive mode, False = use default values
    'transform_column_names': True             # True = apply column name transformation, False = use original names
}

# =============================================================================
# END OF CONFIGURATION
# =============================================================================


def transform_column_name(column_name):
    """
    Transform Domo column names to match Snowflake naming conventions:
    1. Replace spaces with underscores
    2. Replace "#" with "number" and "&" with "and"
    3. Remove parentheses
    4. Convert to uppercase

    Example: "cabinet & drawing(completed)" -> "CABINET_AND_DRAWING_COMPLETED"
    """
    # Convert to lowercase for case-insensitive comparison
    name = column_name.lower()

    # Replace special characters with words
    name = re.sub(r"\bno\.\s*", "number_", name)
    name = name.replace("#", "number")
    name = name.replace("&", "and")

    # Remove parentheses
    name = name.replace("(", "_").replace(")", "_")
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = name.replace("/", "_")
    name = name.replace(".", "_")
    name = name.replace("?", "_")

    # Replace spaces with underscores
    name = name.replace(" ", "_")

    # Remove any consecutive underscores that might have been created
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    name = name.strip("_")

    # Convert to uppercase
    name = name.upper()

    return name


def check_env_vars(reload_env: bool = True):
    """Check and display the status of environment variables
    
    Args:
        reload_env: Whether to reload environment variables from .env file
    """
    if reload_env:
        load_dotenv(override=True)
        logger.info("🔄 Environment variables reloaded from .env file")
    
    required_vars = {
        'DOMO_INSTANCE': 'Domo Instance',
        'DOMO_DEVELOPER_TOKEN': 'Domo Developer Token',
        'SNOWFLAKE_USER': 'Snowflake User',
        'SNOWFLAKE_ACCOUNT': 'Snowflake Account',
        'SNOWFLAKE_WAREHOUSE': 'Snowflake Warehouse',
        'SNOWFLAKE_DATABASE': 'Snowflake Database',
        'SNOWFLAKE_SCHEMA': 'Snowflake Schema'
    }
    
    missing_vars = []
    loaded_vars = {}
    
    # Check required variables
    for var, description in required_vars.items():
        value = os.getenv(var)
        if value:
            loaded_vars[var] = value
            # Show only first characters for security
            masked_value = value[:3] + '*' * (len(value) - 6) + value[-3:] if len(value) > 6 else '***'
            logger.info(f"✅ {var}: {masked_value}")
        else:
            missing_vars.append(var)
            logger.warning(f"❌ {var}: NOT FOUND")
    
    if missing_vars:
        logger.error(f"Missing required variables: {', '.join(missing_vars)}")
        logger.error("Please verify that the .env file exists and contains all required variables")
        return False
    
    logger.info("✅ All required environment variables are configured")
    return True


class DatasetComparator:
    """Main class to compare Domo datasets with Snowflake tables using the tools utilities"""
    
    def __init__(self):
        self.domo = Domo()
        self.snowflake_handler = SnowflakeHandler()
        self.errors = []  # List to store errors
        self._snowflake_connected = False
    
    def setup_connections(self) -> bool:
        """Setup connections to both Domo and Snowflake"""
        try:
            logger.info("🔧 Setting up connections...")
            
            # Setup Snowflake connection
            if not self.snowflake_handler.setup_connection():
                logger.error("❌ Failed to connect to Snowflake")
                return False
            
            self._snowflake_connected = True
            logger.info("✅ All connections established successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            return False
    
    def add_error(self, section: str, error: str, details: str = ""):
        """Add error to error list"""
        self.errors.append({
            'section': section,
            'error': error,
            'details': details
        })
        logger.error(f"Error in {section}: {error}")
        if details:
            logger.error(f"Details: {details}")
    
    def calculate_sample_size(self, total_rows: int, confidence_level: float = 0.95, margin_of_error: float = 0.05) -> int:
        """
        Calculate statistically significant sample size
        
        Args:
            total_rows: Total number of rows
            confidence_level: Confidence level (default 95%)
            margin_of_error: Margin of error (default 5%)
        
        Returns:
            Recommended sample size
        """
        if total_rows <= 1000:
            return min(total_rows, 1000)
        
        # Formula for finite population
        z_score = 1.96  # For 95% confidence
        p = 0.5  # Most conservative proportion
        
        numerator = (z_score ** 2) * p * (1 - p)
        denominator = (margin_of_error ** 2)
        
        sample_size = numerator / denominator
        sample_size = sample_size / (1 + (sample_size - 1) / total_rows)
        
        return min(int(math.ceil(sample_size)), total_rows)
    
    def compare_schemas(self, domo_dataset_id: str, snowflake_table: str, transform_names: bool = False) -> Dict[str, Any]:
        """Compare schemas between Domo and Snowflake"""
        logger.info("Comparing schemas...")
        
        try:
            # Get Domo schema using the Domo utility
            domo_schema = self.domo.get_dataset_schema(domo_dataset_id)
            domo_columns = {col['name']: col['type'] for col in domo_schema['columns']}
            
            # Apply column name transformation if enabled
            if transform_names:
                logger.info("Applying column name transformation to Domo schema...")
                transformed_domo_columns = {}
                for original_name, col_type in domo_columns.items():
                    transformed_name = transform_column_name(original_name)
                    transformed_domo_columns[transformed_name] = col_type
                    logger.info(f"   '{original_name}' -> '{transformed_name}'")
                domo_columns = transformed_domo_columns
        except Exception as e:
            self.add_error("Domo Schema", f"Could not get Domo schema", str(e))
            return {
                'domo_columns': 0,
                'snowflake_columns': 0,
                'missing_in_snowflake': [],
                'extra_in_snowflake': [],
                'common_columns': 0,
                'type_mismatches': [],
                'schema_match': False,
                'error': True
            }
        
        try:
            # Get Snowflake schema using the Snowflake utility
            query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM {os.getenv('SNOWFLAKE_DATABASE')}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{snowflake_table.upper()}'
            AND TABLE_SCHEMA = '{os.getenv('SNOWFLAKE_SCHEMA').upper()}'
            AND TABLE_CATALOG = '{os.getenv('SNOWFLAKE_DATABASE').upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            sf_result = self.snowflake_handler.execute_query(query)
            if sf_result is not None:
                sf_schema_df = sf_result.to_pandas()
                sf_columns = {col['COLUMN_NAME']: col['DATA_TYPE'] for _, col in sf_schema_df.iterrows()}
            else:
                raise Exception("Failed to get Snowflake schema")
                
            logger.info(f"Snowflake columns: {list(sf_columns.keys())}")
        except Exception as e:
            self.add_error("Snowflake Schema", f"Could not get Snowflake schema", str(e))
            return {
                'domo_columns': len(domo_columns),
                'snowflake_columns': 0,
                'missing_in_snowflake': [],
                'extra_in_snowflake': [],
                'common_columns': 0,
                'type_mismatches': [],
                'schema_match': False,
                'error': True
            }
        
        # Compare columns
        domo_cols_set = set(domo_columns.keys())
        sf_cols_set = set(sf_columns.keys())
        
        missing_in_sf = domo_cols_set - sf_cols_set
        extra_in_sf = sf_cols_set - domo_cols_set
        common_cols = domo_cols_set & sf_cols_set
        
        # Compare data types for common columns
        type_mismatches = []
        for col in common_cols:
            domo_type = domo_columns[col]
            sf_type = sf_columns[col]
            if not self._types_compatible(domo_type, sf_type):
                type_mismatches.append({
                    'column': col,
                    'domo_type': domo_type,
                    'snowflake_type': sf_type
                })
        
        return {
            'domo_columns': len(domo_columns),
            'snowflake_columns': len(sf_columns),
            'missing_in_snowflake': list(missing_in_sf),
            'extra_in_snowflake': list(extra_in_sf),
            'common_columns': len(common_cols),
            'type_mismatches': type_mismatches,
            'schema_match': len(missing_in_sf) == 0 and len(extra_in_sf) == 0 and len(type_mismatches) == 0,
            'transform_applied': transform_names
        }
    
    def _types_compatible(self, domo_type: str, sf_type: str) -> bool:
        """Check if data types are compatible"""
        type_mapping = {
            'STRING': ['VARCHAR', 'STRING', 'TEXT', 'CHAR'],
            'LONG': ['NUMBER', 'INTEGER', 'BIGINT', 'INT'],
            'DOUBLE': ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'],
            'DATETIME': ['TIMESTAMP', 'DATETIME', 'DATE']
        }
        
        for domo_key, sf_types in type_mapping.items():
            if domo_type == domo_key:
                return any(sf_type.upper().startswith(sf) for sf in sf_types)
        
        return True  # Default assume compatibility
    
    def compare_row_counts(self, domo_dataset_id: str, snowflake_table: str) -> Dict[str, Any]:
        """Compare row counts between Domo and Snowflake"""
        logger.info("Comparing row counts...")
        
        # Get Domo count using the Domo utility
        try:
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo.query_dataset(domo_dataset_id, domo_count_query)
            domo_count = domo_result['rows'][0][0] if domo_result['rows'] else 0
        except Exception as e:
            self.add_error("Domo Count", f"Could not get row count from Domo", str(e))
            domo_count = 0
        
        # Get Snowflake count using the Snowflake utility
        try:
            sf_count_query = f"SELECT COUNT(*) as row_count FROM {snowflake_table}"
            sf_result = self.snowflake_handler.execute_query(sf_count_query)
            if sf_result is not None:
                sf_count = int(sf_result.to_pandas().iloc[0]['ROW_COUNT'])
            else:
                raise Exception("Failed to get Snowflake count")
        except Exception as e:
            self.add_error("Snowflake Count", f"Could not get row count from Snowflake", str(e))
            sf_count = 0
        
        # Analyze if difference is negligible
        negligible_analysis = self._is_row_count_difference_negligible(domo_count, sf_count)
        
        return {
            'domo_rows': domo_count,
            'snowflake_rows': sf_count,
            'difference': sf_count - domo_count,
            'match': domo_count == sf_count,
            'negligible_analysis': negligible_analysis
        }
    
    def _is_row_count_difference_negligible(self, domo_count: int, snowflake_count: int) -> Dict[str, Any]:
        """Determine if the difference in row counts is statistically negligible"""
        if domo_count == 0 and snowflake_count == 0:
            return {
                'is_negligible': True,
                'reason': 'Both datasets are empty',
                'difference': 0,
                'percentage': 0.0
            }
        
        if domo_count == 0:
            return {
                'is_negligible': False,
                'reason': 'Domo dataset is empty but Snowflake has data',
                'difference': snowflake_count,
                'percentage': 100.0
            }
        
        if snowflake_count == 0:
            return {
                'is_negligible': False,
                'reason': 'Snowflake table is empty but Domo has data',
                'difference': domo_count,
                'percentage': 100.0
            }
        
        # Calculate difference and percentage
        difference = abs(snowflake_count - domo_count)
        percentage = (difference / max(domo_count, snowflake_count)) * 100
        
        # Determine if negligible based on multiple criteria
        is_negligible = False
        reason = ""
        
        # Criterion 1: Very small absolute difference (≤ 10 rows)
        if difference <= 10:
            is_negligible = True
            reason = f"Absolute difference is very small ({difference} rows)"
        
        # Criterion 2: Very small percentage difference (≤ 0.1%)
        elif percentage <= 0.1:
            is_negligible = True
            reason = f"Percentage difference is very small ({percentage:.3f}%)"
        
        # Criterion 3: Small percentage difference (≤ 1%) for large datasets (≥ 10,000 rows)
        elif percentage <= 1.0 and max(domo_count, snowflake_count) >= 10000:
            is_negligible = True
            reason = f"Small percentage difference ({percentage:.3f}%) for large dataset"
        
        # Criterion 4: Medium percentage difference (≤ 5%) for very large datasets (≥ 100,000 rows)
        elif percentage <= 5.0 and max(domo_count, snowflake_count) >= 100000:
            is_negligible = True
            reason = f"Medium percentage difference ({percentage:.3f}%) for very large dataset"
        
        else:
            is_negligible = False
            reason = f"Significant difference: {difference} rows ({percentage:.3f}%)"
        
        return {
            'is_negligible': is_negligible,
            'reason': reason,
            'difference': difference,
            'percentage': percentage
        }
    
    def compare_data_samples(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: List[str], sample_size: Optional[int] = None, 
                           transform_names: bool = False) -> Dict[str, Any]:
        """Compare data samples using key columns"""
        logger.info("Comparing data samples...")
        
        # Get total Domo count for calculating sample
        try:
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo.query_dataset(domo_dataset_id, domo_count_query)
            total_domo_rows = domo_result['rows'][0][0] if domo_result['rows'] else 0
            logger.info(f"Total Domo rows: {total_domo_rows}")
        except Exception as e:
            self.add_error("Total Domo Count", f"Could not get total Domo row count", str(e))
            total_domo_rows = 0
        
        # Calculate sample size if not specified
        if sample_size is None:
            sample_size = self.calculate_sample_size(total_domo_rows)
        
        logger.info(f"Total rows in Domo: {total_domo_rows}, Sample to compare: {sample_size}")
        
        # Get Domo sample using the Domo utility
        try:
            key_cols_str = ', '.join(key_columns)
            domo_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
            logger.info(f"Domo Query: {domo_query}")
            domo_result = self.domo.query_dataset(domo_dataset_id, domo_query)
            
            if not domo_result['rows']:
                self.add_error("Domo Sample", "Could not get data from Domo")
                return self._get_error_data_comparison_result(sample_size)
            
            domo_df = pd.DataFrame(domo_result['rows'], columns=domo_result['columns'])
            
            # Apply column name transformation if enabled
            if transform_names:
                logger.info("Applying column name transformation to Domo DataFrame...")
                original_columns = domo_df.columns.tolist()
                transformed_columns = [transform_column_name(col) for col in original_columns]
                domo_df.columns = transformed_columns
                
                # Transform key columns as well
                transformed_key_columns = [transform_column_name(col) for col in key_columns]
                logger.info(f"Transformed key columns: {key_columns} -> {transformed_key_columns}")
                key_columns = transformed_key_columns
            
            logger.info(f"Domo DataFrame: {len(domo_df)} rows, {len(domo_df.columns)} columns")
        except Exception as e:
            self.add_error("Domo Sample", f"Could not get Domo sample", str(e))
            return self._get_error_data_comparison_result(sample_size)
        
        # Get Snowflake sample using the Snowflake utility
        try:
            key_cols_str = ', '.join([f'"{col}"' for col in key_columns])
            sf_query = f"""
            SELECT * FROM {snowflake_table}
            ORDER BY {key_cols_str}
            LIMIT {sample_size}
            """
            sf_result = self.snowflake_handler.execute_query(sf_query)
            if sf_result is not None:
                sf_df = sf_result.to_pandas()
            else:
                raise Exception("Failed to get Snowflake sample")
                
            logger.info(f"Snowflake DataFrame: {len(sf_df)} rows, {len(sf_df.columns)} columns")
        except Exception as e:
            self.add_error("Snowflake Sample", f"Could not get Snowflake sample", str(e))
            return {
                'sample_size': sample_size,
                'domo_sample_rows': len(domo_df),
                'snowflake_sample_rows': 0,
                'data_match': False,
                'mismatched_rows': [],
                'missing_in_snowflake': len(domo_df),
                'extra_in_snowflake': 0,
                'rows_with_differences': 0,
                'error': True
            }
        
        # Compare data
        try:
            comparison_result = self._compare_dataframes(domo_df, sf_df, key_columns)
            
            # Generate detailed report
            detailed_report = self._generate_detailed_row_comparison(domo_df, sf_df, key_columns)
            
            # Save detailed report to file
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"comparison_detailed_report_{timestamp}.txt"
            
            with open(log_filename, 'w', encoding='utf-8') as f:
                f.write(f"DETAILED COMPARISON REPORT\n")
                f.write(f"Domo Dataset: {domo_dataset_id}\n")
                f.write(f"Snowflake Table: {snowflake_table}\n")
                f.write(f"Key Columns: {', '.join(key_columns)}\n")
                f.write(f"Column Transformation: {'Applied' if transform_names else 'Not Applied'}\n")
                f.write(f"Timestamp: {pd.Timestamp.now().isoformat()}\n")
                f.write(detailed_report)
            
            print(f"\n📄 Detailed report saved to: {log_filename}")
            
        except Exception as e:
            self.add_error("Data Comparison", f"Error comparing DataFrames", str(e))
            return self._get_error_data_comparison_result(sample_size, len(domo_df), len(sf_df))
        
        return {
            'sample_size': sample_size,
            'domo_sample_rows': len(domo_df),
            'snowflake_sample_rows': len(sf_df),
            'data_match': comparison_result['match'],
            'mismatched_rows': comparison_result['mismatched_rows'],
            'missing_in_snowflake': comparison_result['missing_in_snowflake'],
            'extra_in_snowflake': comparison_result['extra_in_snowflake'],
            'rows_with_differences': comparison_result['rows_with_differences'],
            'transform_applied': transform_names
        }
    
    def _get_error_data_comparison_result(self, sample_size: int, domo_rows: int = 0, sf_rows: int = 0) -> Dict[str, Any]:
        """Get error result for data comparison"""
        return {
            'sample_size': sample_size,
            'domo_sample_rows': domo_rows,
            'snowflake_sample_rows': sf_rows,
            'data_match': False,
            'mismatched_rows': [],
            'missing_in_snowflake': 0,
            'extra_in_snowflake': 0,
            'rows_with_differences': 0,
            'error': True
        }
    
    def _compare_dataframes(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                          key_columns: List[str]) -> Dict[str, Any]:
        """Compare two DataFrames using key columns"""
        # Verify that key columns exist
        missing_key_cols_domo = [col for col in key_columns if col not in domo_df.columns]
        missing_key_cols_sf = [col for col in key_columns if col not in sf_df.columns]
        
        if missing_key_cols_domo:
            self.add_error("Key Columns", f"Missing key columns in Domo: {missing_key_cols_domo}")
        if missing_key_cols_sf:
            self.add_error("Key Columns", f"Missing key columns in Snowflake: {missing_key_cols_sf}")
        
        # Create composite keys
        def clean_key_value(val):
            if pd.isna(val) or str(val).strip() == '' or str(val).strip().lower() == 'null':
                return 'NULL'
            return str(val).strip()
        
        domo_df['_key'] = domo_df[key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        sf_df['_key'] = sf_df[key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        
        # Find differences
        domo_keys = set(domo_df['_key'])
        sf_keys = set(sf_df['_key'])
        
        missing_in_sf = domo_keys - sf_keys
        extra_in_sf = sf_keys - domo_keys
        common_keys = domo_keys & sf_keys
        
        # Compare common rows and count unique rows with differences
        mismatched_rows = []
        total_rows_with_differences = 0
        
        for key in list(common_keys)[:10]:  # Limit to 10 examples for report
            domo_row = domo_df[domo_df['_key'] == key].iloc[0]
            sf_row = sf_df[sf_df['_key'] == key].iloc[0]
            
            has_differences = False
            for col in domo_df.columns:
                if col != '_key' and col in sf_df.columns:
                    if str(domo_row[col]) != str(sf_row[col]):
                        has_differences = True
                        mismatched_rows.append({
                            'key': key,
                            'column': col,
                            'domo_value': domo_row[col],
                            'snowflake_value': sf_row[col]
                        })
        
        # Count all rows with differences
        for key in common_keys:
            domo_row = domo_df[domo_df['_key'] == key].iloc[0]
            sf_row = sf_df[sf_df['_key'] == key].iloc[0]
            
            for col in domo_df.columns:
                if col != '_key' and col in sf_df.columns:
                    if str(domo_row[col]) != str(sf_row[col]):
                        total_rows_with_differences += 1
                        break  # Count row only once
        
        return {
            'match': len(missing_in_sf) == 0 and len(extra_in_sf) == 0 and total_rows_with_differences == 0,
            'missing_in_snowflake': len(missing_in_sf),
            'extra_in_snowflake': len(extra_in_sf),
            'mismatched_rows': mismatched_rows,
            'rows_with_differences': total_rows_with_differences
        }
    
    def _generate_detailed_row_comparison(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                                       key_columns: List[str]) -> str:
        """Generate detailed row comparison report"""
        report_lines = []
        report_lines.append("\n" + "="*80)
        report_lines.append("DETAILED ROW COMPARISON REPORT")
        report_lines.append("="*80)
        
        # Create composite keys
        def clean_key_value(val):
            if pd.isna(val) or str(val).strip() == '' or str(val).strip().lower() == 'null':
                return 'NULL'
            return str(val).strip()
        
        domo_df['_key'] = domo_df[key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        sf_df['_key'] = sf_df[key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        
        # Find common rows
        domo_keys = set(domo_df['_key'])
        sf_keys = set(sf_df['_key'])
        common_keys = domo_keys & sf_keys
        
        report_lines.append(f"\n📊 STATISTICS:")
        report_lines.append(f"   Total rows in Domo: {len(domo_df)}")
        report_lines.append(f"   Total rows in Snowflake: {len(sf_df)}")
        report_lines.append(f"   Common rows: {len(common_keys)}")
        report_lines.append(f"   Rows only in Domo: {len(domo_keys - sf_keys)}")
        report_lines.append(f"   Rows only in Snowflake: {len(sf_keys - domo_keys)}")
        
        # Show examples of rows with differences
        if common_keys:
            report_lines.append(f"\n🔍 DETAILED COMPARISON OF FIRST 5 ROWS:")
            
            for i, key in enumerate(list(common_keys)[:5], 1):
                domo_row = domo_df[domo_df['_key'] == key].iloc[0]
                sf_row = sf_df[sf_df['_key'] == key].iloc[0]
                
                report_lines.append(f"\n   📋 ROW {i}:")
                report_lines.append(f"      Key: {key}")
                
                # Show differences in columns
                differences = []
                for col in domo_df.columns:
                    if col != '_key' and col in sf_df.columns:
                        domo_val = domo_row[col]
                        sf_val = sf_row[col]
                        if str(domo_val) != str(sf_val):
                            differences.append({
                                'column': col,
                                'domo_value': domo_val,
                                'snowflake_value': sf_val
                            })
                
                if differences:
                    report_lines.append(f"      🔄 DIFFERENCES FOUND ({len(differences)} columns):")
                    for diff in differences:
                        report_lines.append(f"         {diff['column']}:")
                        report_lines.append(f"            Domo:      '{diff['domo_value']}'")
                        report_lines.append(f"            Snowflake: '{diff['snowflake_value']}'")
                else:
                    report_lines.append(f"      ✅ No differences found in this row")
        
        report_lines.append("\n" + "="*80)
        return "\n".join(report_lines)
    
    def generate_report(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sample_size: Optional[int] = None,
                       transform_names: bool = False) -> Dict[str, Any]:
        """Generate complete comparison report"""
        logger.info(f"Starting comparison: Domo dataset {domo_dataset_id} vs Snowflake table {snowflake_table}")
        
        # Clear previous errors
        self.errors = []
        
        # Setup connections if not already done
        if not self._snowflake_connected:
            if not self.setup_connections():
                return {
                    'domo_dataset_id': domo_dataset_id,
                    'snowflake_table': snowflake_table,
                    'key_columns': key_columns,
                    'overall_match': False,
                    'errors': [{'section': 'Connection', 'error': 'Failed to setup connections', 'details': ''}],
                    'timestamp': pd.Timestamp.now().isoformat(),
                    'transform_applied': transform_names
                }
        
        # Compare schemas
        schema_comparison = self.compare_schemas(domo_dataset_id, snowflake_table, transform_names)
        
        # Compare row counts
        row_count_comparison = self.compare_row_counts(domo_dataset_id, snowflake_table)
        
        # Compare data samples
        data_comparison = self.compare_data_samples(domo_dataset_id, snowflake_table, key_columns, sample_size, transform_names)
        
        # Generate summary (only if no critical errors)
        overall_match = False
        if not schema_comparison.get('error') and not data_comparison.get('error'):
            # Check if row count difference is negligible
            row_count_acceptable = (
                row_count_comparison['match'] or 
                row_count_comparison.get('negligible_analysis', {}).get('is_negligible', False)
            )
            
            overall_match = (
                schema_comparison['schema_match'] and 
                row_count_acceptable and 
                data_comparison.get('data_match', False)
            )
        
        report = {
            'domo_dataset_id': domo_dataset_id,
            'snowflake_table': snowflake_table,
            'key_columns': key_columns,
            'overall_match': overall_match,
            'schema_comparison': schema_comparison,
            'row_count_comparison': row_count_comparison,
            'data_comparison': data_comparison,
            'errors': self.errors,
            'timestamp': pd.Timestamp.now().isoformat(),
            'transform_applied': transform_names
        }
        
        return report
    
    def print_report(self, report: Dict[str, Any]):
        """Print report in readable format"""
        print("\n" + "="*80)
        print("DOMO vs SNOWFLAKE COMPARISON REPORT")
        print("="*80)
        
        print(f"📊 Domo Dataset: {report['domo_dataset_id']}")
        print(f"❄️  Snowflake Table: {report['snowflake_table']}")
        print(f"🔑 Key Columns: {', '.join(report['key_columns'])}")
        print(f"⏰ Timestamp: {report['timestamp']}")
        
        # Show transformation status
        if report.get('transform_applied', False):
            print(f"🔄 Column Name Transformation: APPLIED")
        else:
            print(f"🔄 Column Name Transformation: NOT APPLIED")
        
        # Show errors if any
        if report.get('errors'):
            print(f"\n⚠️  ERRORS FOUND ({len(report['errors'])}):")
            for i, error in enumerate(report['errors'], 1):
                print(f"   {i}. {error['section']}: {error['error']}")
                if error['details']:
                    print(f"      Details: {error['details']}")
        
        # Overall summary
        if report.get('errors'):
            overall_status = "❌ COMPARISON INCOMPLETE - ERRORS FOUND"
        elif report['overall_match']:
            overall_status = "✅ PERFECT MATCH"
        else:
            overall_status = "❌ DISCREPANCIES FOUND"
        
        print(f"\n🎯 OVERALL STATUS: {overall_status}")
        
        # Schema
        print("\n📋 COMPARISON OF SCHEMAS:")
        schema = report['schema_comparison']
        if schema.get('error'):
            print("   ❌ Error getting schemas")
        else:
            print(f"   Columns in Domo: {schema['domo_columns']}")
            print(f"   Columns in Snowflake: {schema['snowflake_columns']}")
            print(f"   Common columns: {schema['common_columns']}")
            
            if schema.get('transform_applied', False):
                print(f"   🔄 Column names were transformed for comparison")
            
            if schema['missing_in_snowflake']:
                print(f"   ❌ Missing in Snowflake: {schema['missing_in_snowflake']}")
            if schema['extra_in_snowflake']:
                print(f"   ⚠️  Extra in Snowflake: {schema['extra_in_snowflake']}")
            if schema['type_mismatches']:
                print(f"   🔄 Type Incompatibilities: {len(schema['type_mismatches'])}")
        
        # Row count
        print("\n📊 COMPARISON OF ROWS:")
        rows = report['row_count_comparison']
        print(f"   Rows in Domo: {rows['domo_rows']:,}")
        print(f"   Rows in Snowflake: {rows['snowflake_rows']:,}")
        print(f"   Difference: {rows['difference']:,}")
        
        # Show negligible analysis
        negligible = rows.get('negligible_analysis', {})
        if negligible:
            if negligible.get('is_negligible', False):
                print(f"   ✅ Difference is NEGLIGIBLE: {negligible.get('reason', '')}")
            else:
                print(f"   ⚠️  Difference is SIGNIFICANT: {negligible.get('reason', '')}")
        
        if rows['difference'] != 0:
            if negligible.get('is_negligible', False):
                print(f"   🔄 Row count difference (but negligible)")
            else:
                print(f"   ❌ Row count difference")
        else:
            print(f"   ✅ Row count matches")
        
        # Data
        print("\n🔍 COMPARISON OF DATA:")
        data = report['data_comparison']
        if data.get('error'):
            print("   ❌ Error comparing data")
        else:
            print(f"   Sample size: {data['sample_size']:,}")
            print(f"   Rows in Domo sample: {data['domo_sample_rows']:,}")
            print(f"   Rows in Snowflake sample: {data['snowflake_sample_rows']:,}")
            
            if data.get('transform_applied', False):
                print(f"   🔄 Column names were transformed for data comparison")
            
            if data.get('missing_in_snowflake', 0) > 0:
                print(f"   ❌ Missing {data['missing_in_snowflake']} rows in Snowflake")
            if data.get('extra_in_snowflake', 0) > 0:
                print(f"   ⚠️  {data['extra_in_snowflake']} extra rows in Snowflake")
            if data.get('rows_with_differences', 0) > 0:
                print(f"   🔄 {data['rows_with_differences']} rows with different data")
            
            if data.get('data_match', False):
                print(f"   ✅ Data samples match")
            else:
                print(f"   ❌ Data samples do not match")
        
        print("\n" + "="*80)
    
    def cleanup(self):
        """Clean up resources"""
        if self.snowflake_handler:
            self.snowflake_handler.cleanup()


def get_config_from_args():
    """Get configuration from command line arguments"""
    parser = argparse.ArgumentParser(description='Compare Domo datasets with Snowflake tables')
    parser.add_argument('--domo-dataset-id', help='ID of Domo dataset')
    parser.add_argument('--snowflake-table', help='Name of Snowflake table')
    parser.add_argument('--key-columns', nargs='+', help='Key columns for comparison')
    parser.add_argument('--sample-size', type=int, help='Custom sample size (optional)')
    parser.add_argument('--interactive', action='store_true', help='Interactive mode for confirming sample size')
    parser.add_argument('--no-interactive', action='store_true', help='Disable interactive mode')
    parser.add_argument('--transform-columns', action='store_true', help='Apply column name transformation')
    parser.add_argument('--no-transform-columns', action='store_true', help='Do not apply column name transformation')
    parser.add_argument('--reload-env', action='store_true', help='Force reload environment variables from .env file')
    parser.add_argument('--test-datacompy', action='store_true', help='Run datacompy test function')
    
    args = parser.parse_args()
    
    config = DEFAULT_CONFIG.copy()
    
    # Override with arguments if provided
    if args.domo_dataset_id:
        config['domo_dataset_id'] = args.domo_dataset_id
    if args.snowflake_table:
        config['snowflake_table'] = args.snowflake_table
    if args.key_columns:
        config['key_columns'] = args.key_columns
    if args.sample_size is not None:
        config['sample_size'] = args.sample_size
    if args.interactive:
        config['interactive'] = True
    if args.no_interactive:
        config['interactive'] = False
    if args.transform_columns:
        config['transform_column_names'] = True
    if args.no_transform_columns:
        config['transform_column_names'] = False
    
    return config


def main():
    # Check if we should run the datacompy test
    import sys
    if '--test-datacompy' in sys.argv:
        test_datacompy()
        return
    
    # Get configuration (from code or arguments)
    config = get_config_from_args()
    
    # Verify that configuration is complete
    if config['domo_dataset_id'] == 'tu-dataset-id-here' or config['snowflake_table'] == 'TU_TABLE_HERE':
        print("⚠️  WARNING: You have not configured the default values in the script.")
        print("Please edit the DEFAULT_CONFIG section at the top of the file.")
        print("Or use command line arguments:")
        print("python compare_domo_snowflake.py --domo-dataset-id 'tu-id' --snowflake-table 'TU_TABLE' --key-columns 'id' 'date'")
        return
    
    # Verify environment variables
    print("🔍 Verifying environment variables...")
    env_check = check_env_vars()
    if not env_check:
        print("\n⚠️  WARNING: Missing environment variables")
        print("The script will continue but may fail in some operations.")
        print("Please check the .env file and make sure all required variables are set.")
        
        # Ask if to continue
        response = input("\nDo you want to continue anyway? (y/n): ").lower()
        if response != 'y':
            print("Operation cancelled.")
            return
    
    print("\n🚀 Starting comparison...")
    
    try:
        comparator = DatasetComparator()
    except Exception as e:
        print(f"❌ Error initializing comparator: {e}")
        print("Verify connection credentials.")
        return
    
    try:
        # Show configuration that will be used
        print("🔧 CONFIGURATION:")
        print(f"   Domo Dataset: {config['domo_dataset_id']}")
        print(f"   Snowflake Table: {config['snowflake_table']}")
        print(f"   Key Columns: {', '.join(config['key_columns'])}")
        print(f"   Interactive mode: {'Yes' if config['interactive'] else 'No'}")
        print(f"   Column Transformation: {'Yes' if config['transform_column_names'] else 'No'}")
        if config['sample_size']:
            print(f"   Sample size: {config['sample_size']:,}")
        else:
            print(f"   Sample size: Automatic")
        
        # If interactive, get dataset information first
        if config['interactive'] and config['sample_size'] is None:
            print("\n🔍 Getting information from Domo dataset...")
            try:
                domo_count_query = "SELECT COUNT(*) as row_count FROM table"
                domo_result = comparator.domo.query_dataset(config['domo_dataset_id'], domo_count_query)
                total_rows = domo_result['rows'][0][0] if domo_result['rows'] else 0
                
                suggested_sample = comparator.calculate_sample_size(total_rows)
                print(f"📊 Total rows in Domo: {total_rows:,}")
                print(f"📈 Suggested sample size: {suggested_sample:,}")
                
                response = input(f"Do you want to use sample of {suggested_sample:,} rows? (y/n/custom): ").lower()
                
                if response == 'y':
                    config['sample_size'] = suggested_sample
                elif response == 'custom':
                    custom_size = input("Enter desired sample size: ")
                    try:
                        config['sample_size'] = int(custom_size)
                    except ValueError:
                        print("❌ Invalid size, using suggested")
                        config['sample_size'] = suggested_sample
                else:
                    config['sample_size'] = 1000  # Default
            except Exception as e:
                print(f"⚠️  Could not get dataset information: {e}")
                print("Using default sample size: 1000")
                config['sample_size'] = 1000
        
        # Generate and show report
        report = comparator.generate_report(
            config['domo_dataset_id'],
            config['snowflake_table'],
            config['key_columns'],
            config['sample_size'],
            config['transform_column_names']
        )
        
        comparator.print_report(report)
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        print(f"❌ Critical error: {e}")
        print("\n💡 Suggestions for solution:")
        print("1. Verify that the Domo dataset ID is correct")
        print("2. Verify that the Snowflake table exists and is accessible")
        print("3. Verify that the key columns exist in both sources")
        print("4. Verify that the credentials have sufficient permissions")
    finally:
        try:
            comparator.cleanup()
        except:
            pass  # Ignore cleanup errors


def test_datacompy():
    """
    Función de prueba para el paquete datacompy.
    Esta función prueba las capacidades de datacompy para comparar DataFrames.
    """
    print("\n" + "="*80)
    print("PRUEBA DEL PAQUETE DATACOMPY")
    print("="*80)
    
    # Crear DataFrames de ejemplo para la prueba
    print("\n📊 Creando DataFrames de ejemplo...")
    
    # DataFrame base (simulando datos de Domo)
    df_domo = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'nombre': ['Producto A', 'Producto B', 'Producto C', 'Producto D', 'Producto E'],
        'precio': [10.50, 25.30, 15.75, 30.00, 12.25],
        'categoria': ['Electrónicos', 'Ropa', 'Hogar', 'Electrónicos', 'Hogar'],
        'stock': [100, 50, 75, 200, 30]
    })
    
    # DataFrame modificado (simulando datos de Snowflake con algunas diferencias)
    df_snowflake = pd.DataFrame({
        'id': [1, 2, 3, 4, 6],  # ID 5 eliminado, ID 6 agregado
        'nombre': ['Producto A', 'Producto B Modificado', 'Producto C', 'Producto D', 'Producto F'],  # Cambios en nombres
        'precio': [10.50, 25.30, 16.00, 30.00, 18.99],  # Precio cambiado para id=3, nuevo precio para id=6
        'categoria': ['Electrónicos', 'Ropa', 'Hogar', 'Electrónicos', 'Deportes'],  # Nueva categoría
        'stock': [100, 50, 75, 200, 45],  # Stock para nuevo producto
        'descuento': [0.1, 0.15, 0.05, 0.2, 0.1]  # Nueva columna
    })
    
    print(f"✅ DataFrame Domo creado: {df_domo.shape}")
    print(f"✅ DataFrame Snowflake creado: {df_snowflake.shape}")
    
    # Mostrar los DataFrames
    print("\n📋 DataFrame Domo (simulado):")
    print(df_domo.to_string(index=False))
    
    print("\n📋 DataFrame Snowflake (simulado):")
    print(df_snowflake.to_string(index=False))
    
    # Realizar comparación con datacompy
    print("\n🔍 Iniciando comparación con datacompy...")
    
    try:
        # Crear el objeto Compare
        compare = datacompy.Compare(
            df_domo,
            df_snowflake,
            join_columns='id',  # Columna clave para la comparación
            abs_tol=0.01,  # Tolerancia absoluta para valores numéricos
            rel_tol=0.01,  # Tolerancia relativa para valores numéricos
            df1_name='Domo',
            df2_name='Snowflake'
        )
        
        # Mostrar resumen de la comparación
        print("\n📊 RESUMEN DE LA COMPARACIÓN:")
        print(f"   DataFrames coinciden: {'✅ SÍ' if compare.matches() else '❌ NO'}")
        print(f"   Filas en Domo: {len(df_domo)}")
        print(f"   Filas en Snowflake: {len(df_snowflake)}")
        print(f"   Filas en común: {len(compare.intersect_rows)}")
        print(f"   Filas solo en Domo: {len(compare.df1_unq_rows)}")
        print(f"   Filas solo en Snowflake: {len(compare.df2_unq_rows)}")
        
        # Mostrar diferencias en columnas
        if compare.column_stats is not None and len(compare.column_stats) > 0:
            print(f"\n📋 DIFERENCIAS POR COLUMNA:")
            col_diff = compare.column_stats[compare.column_stats['matches'] == False]
            if len(col_diff) > 0:
                for _, row in col_diff.iterrows():
                    print(f"   🔄 {row['column']}: {row['all_match']} ({row['null_diff']} nulls diff)")
            else:
                print("   ✅ Todas las columnas comunes coinciden")
        
        # Mostrar columnas que existen solo en un DataFrame
        if hasattr(compare, 'df1_unq_columns') and compare.df1_unq_columns:
            print(f"\n📝 Columnas solo en Domo: {', '.join(compare.df1_unq_columns)}")
        
        if hasattr(compare, 'df2_unq_columns') and compare.df2_unq_columns:
            print(f"📝 Columnas solo en Snowflake: {', '.join(compare.df2_unq_columns)}")
        
        # Mostrar reporte completo
        print("\n📄 REPORTE DETALLADO:")
        print("-" * 60)
        print(compare.report())
        
        # Casos específicos de prueba
        print("\n🧪 CASOS DE PRUEBA ESPECÍFICOS:")
        
        # Prueba 1: DataFrames idénticos
        print("\n   🧪 Prueba 1: DataFrames idénticos")
        df_identical = df_domo.copy()
        compare_identical = datacompy.Compare(df_domo, df_identical, join_columns='id')
        print(f"      Resultado: {'✅ CORRECTO' if compare_identical.matches() else '❌ ERROR'}")
        
        # Prueba 2: Solo diferencias en una columna
        print("\n   🧪 Prueba 2: Solo diferencias en precios")
        df_price_diff = df_domo.copy()
        df_price_diff.loc[0, 'precio'] = 999.99  # Cambiar un precio
        compare_price = datacompy.Compare(df_domo, df_price_diff, join_columns='id')
        print(f"      Coinciden: {'✅ SÍ' if compare_price.matches() else '❌ NO (esperado)'}")
        
        # Prueba 3: Tolerancia numérica
        print("\n   🧪 Prueba 3: Tolerancia numérica")
        df_tolerance = df_domo.copy()
        df_tolerance.loc[0, 'precio'] = 10.51  # Diferencia pequeña
        compare_tolerance = datacompy.Compare(
            df_domo, df_tolerance, 
            join_columns='id',
            abs_tol=0.1  # Tolerancia de 0.1
        )
        print(f"      Con tolerancia 0.1: {'✅ COINCIDEN' if compare_tolerance.matches() else '❌ NO COINCIDEN'}")
        
        print("\n✅ Prueba de datacompy completada exitosamente!")
        
    except Exception as e:
        print(f"\n❌ Error durante la prueba de datacompy: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
    
    print("\n" + "="*80)
    print("FIN DE LA PRUEBA DE DATACOMPY")
    print("="*80)


if __name__ == "__main__":
    main() 