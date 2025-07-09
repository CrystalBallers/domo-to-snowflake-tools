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
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from tools.utils.domo_compare import Domo

def reload_env_vars():
    """Reload environment variables from .env file"""
    load_dotenv(override=True)  # override=True forces reload of existing variables
    logger.info("🔄 Environment variables reloaded from .env file")

def show_current_totp_debug():
    """Show current TOTP passcode for debugging purposes"""
    passcode = os.getenv('SNOWFLAKE_PASSCODE')
    if passcode:
        masked_passcode = passcode[:2] + '*' * (len(passcode) - 2) if len(passcode) > 2 else '***'
        print(f"📱 Current TOTP passcode: {masked_passcode}")
        print(f"⏰ Timestamp: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💡 Remember: TOTP codes expire every 30 seconds")
    else:
        print("📱 No TOTP passcode found in environment variables")

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _determine_auth_method() -> str:
    """
    Determine the authentication method based on environment variables.
    
    Returns:
        str: Authentication method ('key_pair', 'mfa', 'sso', 'password')
    """
    # Check for key pair authentication
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
        return "key_pair"
    
    # Check for MFA
    if os.getenv("SNOWFLAKE_PASSCODE"):
        return "mfa"
    
    # Check for SSO
    if os.getenv("SNOWFLAKE_AUTHENTICATOR"):
        return "sso"
    
    # Default to password authentication
    return "password"


# Check if environment variables were loaded correctly
def check_env_vars(reload_env: bool = True):
    """Check and display the status of environment variables
    
    Args:
        reload_env: Whether to reload environment variables from .env file
    """
    if reload_env:
        reload_env_vars()
    required_vars = {
        'DOMO_INSTANCE': 'Domo Instance',
        'DOMO_DEVELOPER_TOKEN': 'Domo Developer Token',
        'SNOWFLAKE_USER': 'Snowflake User',
        'SNOWFLAKE_ACCOUNT': 'Snowflake Account',
        'SNOWFLAKE_WAREHOUSE': 'Snowflake Warehouse',
        'SNOWFLAKE_DATABASE': 'Snowflake Database',
        'SNOWFLAKE_SCHEMA': 'Snowflake Schema'
    }
    
    # Optional variables that depend on authentication method
    optional_vars = {
        'SNOWFLAKE_PASSWORD': 'Snowflake Password',
        'SNOWFLAKE_PASSCODE': 'Snowflake TOTP Passcode',
        'SNOWFLAKE_PRIVATE_KEY_PATH': 'Snowflake Private Key Path',
        'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': 'Snowflake Private Key Passphrase',
        'SNOWFLAKE_AUTHENTICATOR': 'Snowflake Authenticator'
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
    
    # Check optional variables and determine authentication method
    auth_method = _determine_auth_method()
    logger.info(f"🔐 Authentication method: {auth_method}")
    
    # Show optional variables that are configured
    for var, description in optional_vars.items():
        value = os.getenv(var)
        if value:
            # Show masked value for security
            masked_value = value[:3] + '*' * (len(value) - 6) + value[-3:] if len(value) > 6 else '***'
            logger.info(f"✅ {var}: {masked_value}")
    
    # Validate authentication method requirements
    if auth_method == "mfa":
        if not os.getenv("SNOWFLAKE_PASSWORD"):
            logger.error("❌ SNOWFLAKE_PASSWORD is required for MFA authentication")
            return False
        if not os.getenv("SNOWFLAKE_PASSCODE"):
            logger.error("❌ SNOWFLAKE_PASSCODE is required for MFA authentication")
            logger.error("💡 Set SNOWFLAKE_PASSCODE to your current TOTP code (6 digits)")
            return False
    elif auth_method == "key_pair":
        if not os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            logger.error("❌ SNOWFLAKE_PRIVATE_KEY_PATH is required for key pair authentication")
            return False
    elif auth_method == "password":
        if not os.getenv("SNOWFLAKE_PASSWORD"):
            logger.error("❌ SNOWFLAKE_PASSWORD is required for password authentication")
            return False
    
    logger.info("✅ All required environment variables are configured")
    return True

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


class SnowflakeConnector:
    """Class to handle connections and queries to Snowflake"""
    
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish connection with Snowflake using multiple authentication methods"""
        try:
            # Reload environment variables to get fresh TOTP codes
            reload_env_vars()
            
            # Get connection parameters
            snowflake_config = {
                'user': os.getenv('SNOWFLAKE_USER'),
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA')
            }
            
            # Check required parameters
            required_params = ['user', 'account']
            missing_params = [k for k in required_params if not snowflake_config[k]]
            
            if missing_params:
                logger.error(f"Missing required Snowflake parameters: {missing_params}")
                logger.error("Please set the following environment variables:")
                for param in missing_params:
                    logger.error(f"  - SNOWFLAKE_{param.upper()}")
                raise ValueError(f"Missing required parameters: {missing_params}")
            
            # Determine authentication method
            auth_method = _determine_auth_method()
            logger.info(f"Using Snowflake authentication method: {auth_method}")
            
            if auth_method == "key_pair":
                # RSA Key Pair Authentication (recommended for automated scripts)
                private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
                private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
                
                if not private_key_path:
                    logger.error("SNOWFLAKE_PRIVATE_KEY_PATH is required for key pair authentication")
                    raise ValueError("Private key path is required for key pair authentication")
                
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                
                # Load private key
                with open(private_key_path, 'rb') as key_file:
                    private_key = load_pem_private_key(
                        key_file.read(),
                        password=private_key_passphrase.encode() if private_key_passphrase else None,
                    )
                
                # Convert to DER format for Snowflake
                private_key_der = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                snowflake_config['private_key'] = private_key_der
                
            elif auth_method == "mfa":
                # MFA with TOTP
                password = os.getenv("SNOWFLAKE_PASSWORD")
                passcode = os.getenv("SNOWFLAKE_PASSCODE")
                
                if not password:
                    logger.error("SNOWFLAKE_PASSWORD is required for MFA authentication")
                    raise ValueError("Password is required for MFA authentication")
                
                if not passcode:
                    logger.error("SNOWFLAKE_PASSCODE is required for MFA authentication")
                    logger.error("Set SNOWFLAKE_PASSCODE to your current TOTP code")
                    logger.error("💡 TOTP codes expire every 30 seconds - make sure to use a fresh code")
                    raise ValueError("TOTP passcode is required for MFA authentication")
                
                # Validate passcode format (should be 6 digits)
                if not passcode.isdigit() or len(passcode) != 6:
                    logger.error(f"Invalid TOTP passcode format: {passcode}")
                    logger.error("TOTP passcode should be 6 digits (e.g., 123456)")
                    raise ValueError(f"Invalid TOTP passcode format")
                
                logger.info(f"Using MFA authentication with passcode: {passcode[:2]}****")
                logger.info(f"📱 TOTP passcode loaded at: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
                snowflake_config['password'] = password
                snowflake_config['passcode'] = passcode
                
            elif auth_method == "sso":
                # SSO Authentication
                authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
                snowflake_config['authenticator'] = authenticator
                
            else:
                # Standard password authentication
                password = os.getenv("SNOWFLAKE_PASSWORD")
                if not password:
                    logger.error("SNOWFLAKE_PASSWORD is required for password authentication")
                    raise ValueError("Password is required for password authentication")
                snowflake_config['password'] = password
            
            # Remove None values
            snowflake_config = {k: v for k, v in snowflake_config.items() if v is not None}
            
            # Create connection
            logger.info("Connecting to Snowflake...")
            self.conn = snowflake.connector.connect(**snowflake_config)
            
            # Test connection
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"✅ Connected to Snowflake version: {version}")
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            
            # Provide helpful error messages
            error_str = str(e)
            if "MFA with TOTP is required" in error_str:
                logger.error("💡 MFA (Multi-Factor Authentication) is required.")
                logger.error("   Set SNOWFLAKE_PASSCODE to your current TOTP code, or")
                logger.error("   Use key pair authentication (see README for setup)")
            elif "TOTP Invalid" in error_str:
                logger.error("💡 TOTP code is invalid or has expired.")
                logger.error("   TOTP codes expire every 30 seconds.")
                logger.error("   Please generate a fresh code and update SNOWFLAKE_PASSCODE")
            elif "Failed to authenticate" in error_str:
                logger.error("💡 Authentication failed. Check your credentials.")
            elif "cryptography" in error_str:
                logger.error("💡 Install cryptography: pip install cryptography")
            elif "private key" in error_str.lower():
                logger.error("💡 Check your private key path and passphrase")
            
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            return pd.DataFrame(results, columns=columns)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def process_column_name(self, column_name: str, for_snowflake_query: bool = False) -> str:
        """
        Process column name to maintain case-sensitivity
        
        Args:
            column_name: Column name
            for_snowflake_query: True if for Snowflake query, False for comparison
        
        Returns:
            Processed column name
        """
        if for_snowflake_query:
            # For Snowflake queries, add quotes if they don't have
            if column_name.startswith('"') and column_name.endswith('"'):
                return column_name  # Already has quotes
            else:
                return f'"{column_name}"'  # Add quotes
        else:
            # For comparison, remove quotes if they have
            return column_name.strip('"')
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema in Snowflake"""
        # Use current database and schema from environment variables
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name.upper()}'
        AND TABLE_SCHEMA = '{schema.upper()}'
        AND TABLE_CATALOG = '{database.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        
        logger.info(f"Executing schema query: {query}")
        result = self.execute_query(query)
        logger.info(f"Schema query returned {len(result)} columns")
        logger.info(f"Column names found: {result['COLUMN_NAME'].tolist()}")
        
        return result.to_dict('records')
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count of table"""
        query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        logger.info(f"Executing count query: {query}")
        result = self.execute_query(query)
        logger.info(f"Count result: {result}")
        logger.info(f"Columns of result: {result.columns.tolist()}")
        logger.info(f"First row: {result.iloc[0].to_dict()}")
        # Use ROW_COUNT in uppercase as Snowflake returns
        count = int(result.iloc[0]['ROW_COUNT'])
        logger.info(f"Final count: {count}")
        return count
    
    def get_sample_data(self, table_name: str, key_columns: List[str], limit: int = 1000) -> pd.DataFrame:
        """Get sample data using key columns for sorting"""
        # Process column names for Snowflake query
        processed_key_columns = [self.process_column_name(col, for_snowflake_query=True) for col in key_columns]
        
        key_cols_str = ', '.join(processed_key_columns)
        query = f"""
        SELECT * FROM {table_name}
        ORDER BY {key_cols_str}
        LIMIT {limit}
        """
        return self.execute_query(query)
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()


class DatasetComparator:
    """Main class to compare Domo datasets with Snowflake tables"""
    
    def __init__(self):
        self.domo = Domo()
        self.snowflake = SnowflakeConnector()
        self.errors = []  # List to store errors
    
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
            # Get Domo schema
            domo_schema = self.domo.get_dataset_schema(domo_dataset_id)
            domo_columns = {col['name']: col['type'] for col in domo_schema['columns']}  # Maintain original case
            
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
            # Get Snowflake schema
            sf_schema = self.snowflake.get_table_schema(snowflake_table)
            sf_columns = {col['COLUMN_NAME']: col['DATA_TYPE'] for col in sf_schema}  # Maintain original case
            logger.info(f"Snowflake columns after processing: {list(sf_columns.keys())}")
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
        
        # Compare columns (maintain original case)
        domo_cols_set = set(domo_columns.keys())
        sf_cols_set = set(sf_columns.keys())
        
        logger.info(f"Domo columns set: {domo_cols_set}")
        logger.info(f"Snowflake columns set: {sf_cols_set}")
        
        missing_in_sf = domo_cols_set - sf_cols_set
        extra_in_sf = sf_cols_set - domo_cols_set
        common_cols = domo_cols_set & sf_cols_set
        
        logger.info(f"Missing in Snowflake: {missing_in_sf}")
        logger.info(f"Extra in Snowflake: {extra_in_sf}")
        logger.info(f"Common columns: {common_cols}")
        
        # Compare data types for common columns
        type_mismatches = []
        for col in common_cols:
            domo_type = domo_columns[col]
            sf_type = sf_columns[col]
            # Basic type mapping (may need adjustments)
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
    
    def is_row_count_difference_negligible(self, domo_count: int, snowflake_count: int) -> Dict[str, Any]:
        """
        Determine if the difference in row counts is statistically negligible
        
        Args:
            domo_count: Number of rows in Domo
            snowflake_count: Number of rows in Snowflake
        
        Returns:
            Dictionary with analysis results
        """
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
    
    def compare_row_counts(self, domo_dataset_id: str, snowflake_table: str) -> Dict[str, Any]:
        """Compare row counts"""
        logger.info("Comparing row counts...")
        
        # Get Domo count
        try:
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo.query_dataset(domo_dataset_id, domo_count_query)
            domo_count = domo_result['rows'][0][0] if domo_result['rows'] else 0
        except Exception as e:
            self.add_error("Domo Count", f"Could not get row count from Domo", str(e))
            domo_count = 0
        
        # Get Snowflake count
        try:
            sf_count = self.snowflake.get_row_count(snowflake_table)
        except Exception as e:
            self.add_error("Snowflake Count", f"Could not get row count from Snowflake", str(e))
            sf_count = 0
        
        # Analyze if difference is negligible
        negligible_analysis = self.is_row_count_difference_negligible(domo_count, sf_count)
        
        return {
            'domo_rows': domo_count,
            'snowflake_rows': sf_count,
            'difference': sf_count - domo_count,
            'match': domo_count == sf_count,
            'negligible_analysis': negligible_analysis
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
        
        # Get Domo sample
        try:
            key_cols_str = ', '.join(key_columns)
            domo_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
            logger.info(f"Domo Query: {domo_query}")
            domo_result = self.domo.query_dataset(domo_dataset_id, domo_query)
            
            if not domo_result['rows']:
                self.add_error("Domo Sample", "Could not get data from Domo")
                return {
                    'sample_size': sample_size,
                    'domo_sample_rows': 0,
                    'snowflake_sample_rows': 0,
                    'data_match': False,
                    'mismatched_rows': [],
                    'missing_in_snowflake': 0,
                    'extra_in_snowflake': 0,
                    'rows_with_differences': 0,
                    'error': True
                }
            
            domo_df = pd.DataFrame(domo_result['rows'], columns=domo_result['columns'])
            
            # Apply column name transformation if enabled
            if transform_names:
                logger.info("Applying column name transformation to Domo DataFrame...")
                original_columns = domo_df.columns.tolist()
                transformed_columns = [transform_column_name(col) for col in original_columns]
                domo_df.columns = transformed_columns
                
                # Log the transformations
                for orig, trans in zip(original_columns, transformed_columns):
                    if orig != trans:
                        logger.info(f"   '{orig}' -> '{trans}'")
                
                # Transform key columns as well
                transformed_key_columns = [transform_column_name(col) for col in key_columns]
                logger.info(f"Transformed key columns: {key_columns} -> {transformed_key_columns}")
                key_columns = transformed_key_columns
            
            logger.info(f"Domo DataFrame: {len(domo_df)} rows, {len(domo_df.columns)} columns")
            logger.info(f"Domo Columns: {domo_df.columns.tolist()}")
        except Exception as e:
            self.add_error("Domo Sample", f"Could not get Domo sample", str(e))
            return {
                'sample_size': sample_size,
                'domo_sample_rows': 0,
                'snowflake_sample_rows': 0,
                'data_match': False,
                'mismatched_rows': [],
                'missing_in_snowflake': 0,
                'extra_in_snowflake': 0,
                'rows_with_differences': 0,
                'error': True
            }
        
        # Get Snowflake sample
        try:
            sf_df = self.snowflake.get_sample_data(snowflake_table, key_columns, sample_size)
            logger.info(f"Snowflake DataFrame: {len(sf_df)} rows, {len(sf_df.columns)} columns")
            logger.info(f"Snowflake Columns: {sf_df.columns.tolist()}")
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
            detailed_report = self.generate_detailed_row_comparison(domo_df, sf_df, key_columns)
            logger.info("Detailed report generated")
            
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
            return {
                'sample_size': sample_size,
                'domo_sample_rows': len(domo_df),
                'snowflake_sample_rows': len(sf_df),
                'data_match': False,
                'mismatched_rows': [],
                'missing_in_snowflake': 0,
                'extra_in_snowflake': 0,
                'rows_with_differences': 0,
                'error': True
            }
        
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
    
    def _compare_dataframes(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                          key_columns: List[str]) -> Dict[str, Any]:
        """Compare two DataFrames using key columns"""
        # DO NOT normalize column names - maintain original case
        # domo_df.columns = domo_df.columns.str.upper()
        # sf_df.columns = sf_df.columns.str.upper()
        
        # Process key column names for comparison
        processed_key_columns = [self.snowflake.process_column_name(col, for_snowflake_query=False) for col in key_columns]
        
        # Verify that key columns exist (case-sensitive comparison)
        missing_key_cols_domo = [col for col in processed_key_columns if col not in domo_df.columns]
        missing_key_cols_sf = [col for col in processed_key_columns if col not in sf_df.columns]
        
        if missing_key_cols_domo:
            self.add_error("Key Columns", f"Missing key columns in Domo: {missing_key_cols_domo}")
        if missing_key_cols_sf:
            self.add_error("Key Columns", f"Missing key columns in Snowflake: {missing_key_cols_sf}")
        
        # Create composite keys
        # Handle null/empty values consistently
        def clean_key_value(val):
            if pd.isna(val) or str(val).strip() == '' or str(val).strip().lower() == 'null':
                return 'NULL'
            return str(val).strip()
        
        domo_df['_key'] = domo_df[processed_key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        sf_df['_key'] = sf_df[processed_key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        
        # Find differences
        domo_keys = set(domo_df['_key'])
        sf_keys = set(sf_df['_key'])
        
        missing_in_sf = domo_keys - sf_keys
        extra_in_sf = sf_keys - domo_keys
        common_keys = domo_keys & sf_keys
        
        # Compare common rows and count unique rows with differences
        rows_with_differences = set()  # Use set to count unique rows
        mismatched_rows = []
        
        for key in list(common_keys)[:10]:  # Limit to 10 examples for report
            domo_row = domo_df[domo_df['_key'] == key].iloc[0]
            sf_row = sf_df[sf_df['_key'] == key].iloc[0]
            
            has_differences = False
            # Compare values (excluding the key)
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
            
            if has_differences:
                rows_with_differences.add(key)
        
        # Count all rows with differences (not just the first 10)
        total_rows_with_differences = 0
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
    
    def generate_report(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sample_size: Optional[int] = None,
                       transform_names: bool = False) -> Dict[str, Any]:
        """Generate complete comparison report"""
        logger.info(f"Starting comparison: Domo dataset {domo_dataset_id} vs Snowflake table {snowflake_table}")
        
        # Clear previous errors
        self.errors = []
        
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
                print(f"   ✅ Domo sample data matches")
            else:
                print(f"   ❌ Domo sample data does not match")
        
        print("\n" + "="*80)
    
    def cleanup(self):
        """Clean up resources"""
        self.snowflake.close()

    def generate_detailed_row_comparison(self, domo_df: pd.DataFrame, sf_df: pd.DataFrame, 
                                       key_columns: List[str]) -> str:
        """
        Generate detailed row comparison report
        
        Args:
            domo_df: Domo DataFrame
            sf_df: Snowflake DataFrame
            key_columns: Key columns
        
        Returns:
            String with detailed report
        """
        report_lines = []
        report_lines.append("\n" + "="*80)
        report_lines.append("REPORTE DETALLADO DE COMPARACIÓN DE FILAS")
        report_lines.append("="*80)
        
        # Process key column names
        processed_key_columns = [self.snowflake.process_column_name(col, for_snowflake_query=False) for col in key_columns]
        
        # Create composite keys
        # Handle null/empty values consistently
        def clean_key_value(val):
            if pd.isna(val) or str(val).strip() == '' or str(val).strip().lower() == 'null':
                return 'NULL'
            return str(val).strip()
        
        domo_df['_key'] = domo_df[processed_key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        sf_df['_key'] = sf_df[processed_key_columns].applymap(clean_key_value).agg('|'.join, axis=1)
        
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
            report_lines.append(f"\n🔍 COMPARISON DETAILED OF 5 ROWS (of {len(common_keys)} total):")
            
            for i, key in enumerate(list(common_keys)[:5], 1):
                domo_row = domo_df[domo_df['_key'] == key].iloc[0]
                sf_row = sf_df[sf_df['_key'] == key].iloc[0]
                
                report_lines.append(f"\n   📋 ROW {i}:")
                report_lines.append(f"      Key: {key}")
                
                # Show values of key columns
                report_lines.append(f"      🔑 Key Columns:")
                for col in processed_key_columns:
                    domo_val = domo_row[col]
                    sf_val = sf_row[col]
                    status = "✅" if str(domo_val) == str(sf_val) else "❌"
                    report_lines.append(f"         {col}: {status}")
                    if str(domo_val) != str(sf_val):
                        report_lines.append(f"            Domo:     '{domo_val}'")
                        report_lines.append(f"            Snowflake: '{sf_val}'")
                
                # Show differences in other columns
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
                        report_lines.append(f"            Domo:     '{diff['domo_value']}'")
                        report_lines.append(f"            Snowflake: '{diff['snowflake_value']}'")
                else:
                    report_lines.append(f"      ✅ No differences found in this row")
            
        
        # Show rows missing in Snowflake
        missing_in_sf = domo_keys - sf_keys
        if missing_in_sf:
            report_lines.append(f"\n❌ ROWS MISSING IN SNOWFLAKE ({len(missing_in_sf)}):")
            for i, key in enumerate(list(missing_in_sf), 1):
                domo_row = domo_df[domo_df['_key'] == key].iloc[0]
                report_lines.append(f"   {i}. Key: {key}")
                for col in processed_key_columns:
                    report_lines.append(f"      {col}: '{domo_row[col]}'")
        
        # Show extra rows in Snowflake
        extra_in_sf = sf_keys - domo_keys
        if extra_in_sf:
            report_lines.append(f"\n⚠️  EXTRA ROWS IN SNOWFLAKE ({len(extra_in_sf)}):")
            for i, key in enumerate(list(extra_in_sf), 1):
                sf_row = sf_df[sf_df['_key'] == key].iloc[0]
                report_lines.append(f"   {i}. Key: {key}")
                for col in processed_key_columns:
                    report_lines.append(f"      {col}: '{sf_row[col]}'")
        
        report_lines.append("\n" + "="*80)
        return "\n".join(report_lines)


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
    
    # Handle reload-env argument
    if args.reload_env:
        print("🔄 Reloading environment variables...")
        reload_env_vars()
        
        # Show current TOTP passcode for debugging
        passcode = os.getenv('SNOWFLAKE_PASSCODE')
        if passcode:
            masked_passcode = passcode[:2] + '*' * (len(passcode) - 2) if len(passcode) > 2 else '***'
            print(f"📱 Current TOTP passcode: {masked_passcode}")
        else:
            print("📱 No TOTP passcode found")
    
    return config


def main():
    # Get configuration (from code or arguments)
    config = get_config_from_args()
    
    # Verify that configuration is complete
    if config['domo_dataset_id'] == 'tu-dataset-id-here' or config['snowflake_table'] == 'TU_TABLE_HERE':
        print("⚠️  WARNING: You have not configured the default values in the script.")
        print("Please edit the DEFAULT_CONFIG section at the top of the file.")
        print("Or use command line arguments:")
        print("python compare_domo_snowflake.py --domo-dataset-id 'tu-id' --snowflake-table 'TU_TABLE' --key-columns 'id' 'date'")
        return
    
    # Verify environment variables with detailed information
    print("🔍 Verifying environment variables...")
    env_check = check_env_vars()
    if not env_check:
        print("\n⚠️  WARNING: Missing environment variables")
        print("The script will continue but may fail in some operations.")
        print("Please:")
        print("1. Copy the file 'env_template.txt' to '.env'")
        print("2. Complete the values in the '.env' file")
        print("3. Ensure that the '.env' file is in the same folder as the script")
        
        # Ask if to continue
        response = input("\nDo you want to continue anyway? (y/n): ").lower()
        if response != 'y':
            print("Operation cancelled.")
            return
    
    print("\n🚀 Starting comparison...")
    
    # Show TOTP debug info if using MFA
    auth_method = _determine_auth_method()
    if auth_method == "mfa":
        print("\n🔐 Using MFA authentication - TOTP Debug Info:")
        show_current_totp_debug()
    
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


if __name__ == "__main__":
    main() 