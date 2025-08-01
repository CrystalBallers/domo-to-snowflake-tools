"""
Snowflake utilities for data transfer operations.

This module handles all Snowflake-related operations including:
- Connection setup with multiple authentication methods
- Data upload to Snowflake tables
- Table creation and verification
"""

import os
import logging
import time
from typing import Optional
import polars as pl
from dotenv import load_dotenv

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None  # type: ignore

logger = logging.getLogger(__name__)

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
        print(f"⏰ Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💡 Remember: TOTP codes expire every 30 seconds")
    else:
        print("📱 No TOTP passcode found in environment variables")


class SnowflakeHandler:
    """Handles all Snowflake operations including connection and data upload."""
    
    def __init__(self):
        """Initialize the Snowflake handler."""
        self.conn = None
        
    def setup_connection(self) -> bool:
        """
        Setup Snowflake connection using environment variables.
        Supports multiple authentication methods including MFA.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not SNOWFLAKE_AVAILABLE:
            logger.error("Snowflake connector not available")
            return False
            
        try:
            # Reload environment variables to get fresh TOTP codes
            reload_env_vars()
            
            # Get connection parameters
            snowflake_config = {
                'user': os.getenv("SNOWFLAKE_USER"),
                'account': os.getenv("SNOWFLAKE_ACCOUNT"),
                'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
                'database': os.getenv("SNOWFLAKE_DATABASE"),
                'schema': os.getenv("SNOWFLAKE_SCHEMA"),
                'role': os.getenv("SNOWFLAKE_ROLE")  # Add role support
            }
            
            # Check required parameters
            required_params = ['user', 'account']
            missing_params = [k for k in required_params if not snowflake_config[k]]
            
            if missing_params:
                logger.error(f"Missing required Snowflake parameters: {missing_params}")
                logger.error("Please set the following environment variables:")
                for param in missing_params:
                    logger.error(f"  - SNOWFLAKE_{param.upper()}")
                return False
            
            # Determine authentication method
            auth_method = self._determine_auth_method()
            logger.info(f"Using Snowflake authentication method: {auth_method}")
            
            if auth_method == "key_pair":
                # RSA Key Pair Authentication (recommended for automated scripts)
                private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
                private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
                
                if not private_key_path:
                    logger.error("SNOWFLAKE_PRIVATE_KEY_PATH is required for key pair authentication")
                    return False
                
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
                    return False
                
                if not passcode:
                    logger.error("SNOWFLAKE_PASSCODE is required for MFA authentication")
                    logger.error("Set SNOWFLAKE_PASSCODE to your current TOTP code")
                    logger.error("💡 TOTP codes expire every 30 seconds - make sure to use a fresh code")
                    return False
                
                # Validate passcode format (should be 6 digits)
                if not passcode.isdigit() or len(passcode) != 6:
                    logger.error(f"Invalid TOTP passcode format: {passcode}")
                    logger.error("TOTP passcode should be 6 digits (e.g., 123456)")
                    return False
                
                logger.info(f"Using MFA authentication with passcode: {passcode[:2]}****")
                logger.info(f"📱 TOTP passcode loaded at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
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
                    return False
                snowflake_config['password'] = password
            
            # Remove None values
            snowflake_config = {k: v for k, v in snowflake_config.items() if v is not None}

            # Log connection info (excluding sensitive data)  
            connection_info = []
            connection_info.append(f"User: {snowflake_config.get('user', 'Not set')}")
            connection_info.append(f"Account: {snowflake_config.get('account', 'Not set')}")
            connection_info.append(f"Warehouse: {snowflake_config.get('warehouse', 'Not set')}")
            connection_info.append(f"Database: {snowflake_config.get('database', 'Not set')}")
            connection_info.append(f"Schema: {snowflake_config.get('schema', 'Not set')}")
            if snowflake_config.get('role'):
                connection_info.append(f"Role: {snowflake_config.get('role')}")
            else:
                connection_info.append("Role: Using default role")

            logger.info("Connecting to Snowflake with:")
            for info in connection_info:
                logger.info(f"  {info}")

            # Create connection
            if snowflake is None:
                logger.error("Snowflake connector not available")
                return False

            self.conn = snowflake.connector.connect(**snowflake_config)
            
            # Test connection
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"✅ Connected to Snowflake version: {version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            
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
            
            return False
    
    def _determine_auth_method(self) -> str:
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
    
    def upload_data(self, df: pl.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
        """
        Upload DataFrame to Snowflake table using cursor method.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        if self.conn is None:
            logger.error("Snowflake connection not established")
            return False
        
        try:
            logger.info(f"Uploading {len(df)} rows to Snowflake table: {table_name}")
            return self._upload_via_cursor(df, table_name, if_exists)
            
        except Exception as e:
            logger.error(f"Failed to upload data to Snowflake: {e}")
            return False 
    
    def _upload_via_cursor(self, df: pl.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
        """
        Upload DataFrame using cursor method with proper SQL execution.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            
            # Escape table name for SQL operations
            escaped_table_name = f'"{table_name.upper()}"'
            
            # Handle table existence
            if if_exists == 'replace':
                cursor.execute(f"DROP TABLE IF EXISTS {escaped_table_name}")
                logger.info(f"Dropped existing table: {table_name}")
            
            # Create table if it doesn't exist
            if if_exists in ['replace', 'fail']:
                create_sql = self._generate_create_table_sql(df, table_name)
                cursor.execute(create_sql)
                logger.info(f"Created table: {table_name}")
            
            # Prepare data for insertion
            df_clean = df.fill_null('NULL')
            columns = list(df_clean.columns)
            # Escape column names for INSERT statement
            escaped_columns = [f'"{col}"' for col in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Insert data in batches
            batch_size = 1000  # Smaller batch size for better performance
            total_rows = len(df_clean)
            
            for i in range(0, total_rows, batch_size):
                batch = df_clean.slice(i, batch_size)
                values = [tuple(row) for row in batch.iter_rows()]
                
                insert_sql = f"INSERT INTO {escaped_table_name} ({', '.join(escaped_columns)}) VALUES ({placeholders})"
                cursor.executemany(insert_sql, values)
                
                logger.info(f"Inserted batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size}")
            
            cursor.close()
            logger.info(f"✅ Uploaded {total_rows} rows via cursor method")
            return True
            
        except Exception as e:
            logger.error(f"Cursor upload failed: {e}")
            return False
    
    def _generate_create_table_sql(self, df: pl.DataFrame, table_name: str) -> str:
        """
        Generate CREATE TABLE SQL based on DataFrame schema.
        
        Args:
            df: DataFrame to analyze
            table_name: Table name
            
        Returns:
            str: CREATE TABLE SQL statement
        """
        columns = []
        
        for col_name, dtype in df.schema.items():
            # Map polars dtypes to Snowflake types
            if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                sf_type = "INTEGER"
            elif dtype in [pl.Float32, pl.Float64]:
                sf_type = "FLOAT"
            elif dtype == pl.Boolean:
                sf_type = "BOOLEAN"
            elif dtype in [pl.Datetime, pl.Datetime('ms'), pl.Datetime('us'), pl.Datetime('ns')]:
                sf_type = "TIMESTAMP"
            else:
                sf_type = "VARCHAR(16777216)"  # Snowflake max varchar size
            
            # Escape column names to handle reserved keywords and special characters
            escaped_col_name = f'"{col_name}"'
            columns.append(f"{escaped_col_name} {sf_type}")
        
        columns_sql = ', '.join(columns)
        return f"CREATE TABLE {table_name.upper()} ({columns_sql})"
    
    def verify_upload(self, table_name: str, expected_rows: int) -> bool:
        """
        Verify the upload by checking row count in Snowflake.
        
        Args:
            table_name: Table name to verify
            expected_rows: Expected number of rows
            
        Returns:
            bool: True if verification successful, False otherwise
        """
        if self.conn is None:
            logger.error("Snowflake connection not established")
            return False
        
        try:
            cursor = self.conn.cursor()
            # Escape table name to handle special characters
            escaped_table_name = f'"{table_name.upper()}"'
            cursor.execute(f"SELECT COUNT(*) FROM {escaped_table_name}")
            actual_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Verification: Expected {expected_rows} rows, found {actual_rows} rows")
            
            if actual_rows == expected_rows:
                logger.info("✅ Upload verification successful")
                return True
            else:
                logger.warning(f"⚠️  Row count mismatch: expected {expected_rows}, got {actual_rows}")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying upload: {e}")
            return False
    
    def execute_query(self, query: str) -> Optional[pl.DataFrame]:
        """
        Executes a SQL query and returns the result as a Polars DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Optional[pl.DataFrame]: A Polars DataFrame with the query results,
                                    or None if the query fails.
        """
        if not self.conn:
            logger.error("❌ No active Snowflake connection.")
            return None
        
        try:
            logger.info(f"Executing query: {query[:100]}...")
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Fetch results into a pandas DataFrame first
            pandas_df = cursor.fetch_pandas_all()

            if pandas_df is not None and not pandas_df.empty:
                # Convert to Polars DataFrame
                polars_df = pl.from_pandas(pandas_df)
                logger.info(f"✅ Query returned {len(polars_df)} rows.")
                return polars_df
            else:
                logger.info("ℹ️ Query executed successfully, but returned no rows.")
                return pl.DataFrame() # Return empty DataFrame for consistency

        except Exception as e:
            logger.error(f"❌ Failed to execute query: {e}")
            return None
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def get_table_columns(self, database: str, schema: str, table_name: str, role: str = "DBT_ROLE", warehouse: str = None) -> list[str]:
        """
        Get all column names from a specific table in Snowflake.
        
        Args:
            database: Database name
            schema: Schema name  
            table_name: Table name
            role: Snowflake role to use (default: "DBT_ROLE")
            warehouse: Warehouse to use (if None, uses environment variable)
            
        Returns:
            list[str]: List of column names, empty list if error or table not found
        """
        if not self.conn:
            logger.error("❌ No active Snowflake connection.")
            return []
        
        try:
            cursor = self.conn.cursor()
            
            # Set the role first
            cursor.execute(f"USE ROLE {role}")
            logger.debug(f"Set role to: {role}")
            
            # Ensure warehouse is active before running queries
            warehouse_to_use = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
            if warehouse_to_use:
                cursor.execute(f"USE WAREHOUSE {warehouse_to_use}")
                logger.debug(f"Activated warehouse: {warehouse_to_use}")
            
            # Query to get column information from INFORMATION_SCHEMA
            query = f"""
            SELECT COLUMN_NAME 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema.upper()}' 
            AND TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            logger.info(f"Getting columns for table: {database}.{schema}.{table_name} using role: {role}")
            cursor.execute(query)
            
            # Fetch all column names
            results = cursor.fetchall()
            columns = [row[0] for row in results]
            
            cursor.close()
            
            if columns:
                logger.info(f"✅ Found {len(columns)} columns in {table_name}")
                logger.debug(f"Columns: {columns}")
            else:
                logger.warning(f"⚠️  No columns found for table {database}.{schema}.{table_name}")
                logger.warning("   Table may not exist or you may not have permissions")
            
            return columns
            
        except Exception as e:
            logger.error(f"❌ Failed to get columns for table {table_name}: {e}")
            return []

    def cleanup(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed") 