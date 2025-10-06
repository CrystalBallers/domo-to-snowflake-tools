"""
Snowflake Stage Handler for data migration operations.

This module handles all Snowflake stage operations including:
- Stage creation and management
- Data upload to stages
- Data loading from stages to tables
- Stage cleanup and maintenance
"""

import os
import logging
import tempfile
import pandas as pd
from typing import Optional, Dict, List
from pathlib import Path

logger = logging.getLogger(__name__)


class StageHandler:
    """Handles all Snowflake stage operations for data migration."""
    
    def __init__(self, snowflake_handler):
        """
        Initialize the Stage handler.
        
        Args:
            snowflake_handler: SnowflakeHandler instance for database operations
        """
        self.sf = snowflake_handler
        self.stage_prefix = "DOMO_MIGRATION_STAGE"
        
    def create_stage(self, stage_name: str, database: str = None, schema: str = None) -> bool:
        """
        Create a Snowflake stage for data migration.
        
        Args:
            stage_name: Name of the stage to create
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            bool: True if stage created successfully, False otherwise
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return False
        
        try:
            # Get database and schema from environment if not provided
            if not database:
                database = os.getenv("SNOWFLAKE_DATABASE")
            if not schema:
                schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be provided or set in environment variables")
                return False
            
            cursor = self.sf.conn.cursor()
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # Create stage SQL (only if it doesn't exist)
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {database}.{schema}.{full_stage_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' 
                          SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                          ESCAPE_UNENCLOSED_FIELD = 'NONE' ESCAPE = 'NONE'
                          COMPRESSION = 'GZIP')
            """
            
            logger.info(f"Creating stage: {database}.{schema}.{full_stage_name}")
            cursor.execute(create_stage_sql)
            cursor.close()
            
            logger.info(f"✅ Stage created successfully: {full_stage_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create stage: {e}")
            return False
    
    def upload_data_to_stage(self, df: pd.DataFrame, stage_name: str, file_name: str = None) -> bool:
        """
        Upload DataFrame to Snowflake stage as CSV file.
        
        Args:
            df: DataFrame to upload
            stage_name: Name of the stage
            file_name: Name of the file in the stage (auto-generated if None)
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return False
        
        try:
            # Get database and schema from environment
            database = os.getenv("SNOWFLAKE_DATABASE")
            schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be set in environment variables")
                return False
            
            # Generate file name if not provided
            if not file_name:
                import time
                timestamp = int(time.time())
                file_name = f"data_{timestamp}.csv"
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # Normalize column names for Snowflake compatibility
            logger.info("🔧 Normalizing columns for stage upload...")
            df_normalized = self._normalize_column_names(df)
            
            # Clean empty strings and whitespace-only strings - replace with NaN so they become NULL in CSV
            logger.info("🧹 Cleaning empty strings and whitespace...")
            # Replace empty strings and whitespace-only strings with NaN
            df_normalized = df_normalized.replace(r'^\s*$', pd.NA, regex=True)
            # Also replace actual empty strings
            df_normalized = df_normalized.replace('', pd.NA)
            
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                # Write DataFrame to CSV with quoted column names and proper NULL handling
                df_normalized.to_csv(temp_file.name, index=False, na_rep='', quoting=0)  # quoting=0 adds quotes only when necessary
                temp_file_path = temp_file.name
            
            try:
                cursor = self.sf.conn.cursor()
                
                # Upload file to stage
                put_sql = f"PUT file://{temp_file_path} @{database}.{schema}.{full_stage_name}/{file_name}"
                logger.info(f"Uploading file to stage: {put_sql}")
                cursor.execute(put_sql)
                cursor.close()
                
                logger.info(f"✅ Data uploaded to stage: {full_stage_name}/{file_name}")
                logger.info(f"📊 Uploaded {len(df)} rows")
                return True
                
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
                
        except Exception as e:
            logger.error(f"❌ Failed to upload data to stage: {e}")
            return False
    
    def load_from_stage_to_table(self, stage_name: str, table_name: str, file_pattern: str = "*.csv", 
                                if_exists: str = 'replace', database: str = None, schema: str = None) -> bool:
        """
        Load data from stage to table using COPY INTO command.
        
        Args:
            stage_name: Name of the stage
            table_name: Target table name
            file_pattern: File pattern to match in stage (default: "*.csv")
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            bool: True if load successful, False otherwise
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return False
        
        try:
            # Get database and schema from environment if not provided
            if not database:
                database = os.getenv("SNOWFLAKE_DATABASE")
            if not schema:
                schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be provided or set in environment variables")
                return False
            
            cursor = self.sf.conn.cursor()
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # Handle table existence
            if if_exists == 'replace':
                cursor.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table_name.upper()}")
                logger.info(f"Dropped existing table: {table_name}")
            
            # Create table if it doesn't exist (using stage file structure)
            if if_exists in ['replace', 'fail']:
                create_sql = self._generate_create_table_from_stage(full_stage_name, table_name, database, schema)
                if create_sql:
                    cursor.execute(create_sql)
                    logger.info(f"Created table: {table_name}")
            
            # Load data from stage to table
            copy_sql = f"""
            COPY INTO {database}.{schema}.{table_name.upper()}
            FROM @{database}.{schema}.{full_stage_name}
            FILES = ('{file_pattern}')
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' 
                          SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                          ESCAPE_UNENCLOSED_FIELD = 'NONE' ESCAPE = 'NONE'
                          COMPRESSION = 'GZIP')
            ON_ERROR = 'CONTINUE'
            """
            
            logger.info(f"Loading data from stage to table: {copy_sql}")
            cursor.execute(copy_sql)
            
            # Get load results
            result = cursor.fetchone()
            if result:
                rows_loaded = result[1] if len(result) > 1 else 0
                logger.info(f"✅ Loaded {rows_loaded} rows from stage to table")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load data from stage to table: {e}")
            return False
    
    def list_stage_files(self, stage_name: str, database: str = None, schema: str = None) -> List[Dict]:
        """
        List files in a Snowflake stage.
        
        Args:
            stage_name: Name of the stage
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            List[Dict]: List of file information dictionaries
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return []
        
        try:
            # Get database and schema from environment if not provided
            if not database:
                database = os.getenv("SNOWFLAKE_DATABASE")
            if not schema:
                schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be provided or set in environment variables")
                return []
            
            cursor = self.sf.conn.cursor()
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # List files in stage
            list_sql = f"LIST @{database}.{schema}.{full_stage_name}"
            cursor.execute(list_sql)
            
            files = []
            for row in cursor.fetchall():
                files.append({
                    'name': row[0],
                    'size': row[1],
                    'last_modified': row[2],
                    'status': row[3] if len(row) > 3 else 'UNKNOWN'
                })
            
            cursor.close()
            logger.info(f"📁 Found {len(files)} files in stage {full_stage_name}")
            return files
            
        except Exception as e:
            logger.error(f"❌ Failed to list stage files: {e}")
            return []
    
    def remove_stage_files(self, stage_name: str, file_pattern: str = "*", database: str = None, schema: str = None) -> bool:
        """
        Remove files from a Snowflake stage.
        
        Args:
            stage_name: Name of the stage
            file_pattern: File pattern to match for removal (default: "*" for all files)
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            bool: True if removal successful, False otherwise
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return False
        
        try:
            # Get database and schema from environment if not provided
            if not database:
                database = os.getenv("SNOWFLAKE_DATABASE")
            if not schema:
                schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be provided or set in environment variables")
                return False
            
            cursor = self.sf.conn.cursor()
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # Remove files from stage
            remove_sql = f"REMOVE @{database}.{schema}.{full_stage_name}/{file_pattern}"
            logger.info(f"Removing files from stage: {remove_sql}")
            cursor.execute(remove_sql)
            
            cursor.close()
            logger.info(f"✅ Files removed from stage: {full_stage_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to remove stage files: {e}")
            return False
    
    def drop_stage(self, stage_name: str, database: str = None, schema: str = None) -> bool:
        """
        Drop a Snowflake stage.
        
        Args:
            stage_name: Name of the stage to drop
            database: Database name (uses environment variable if None)
            schema: Schema name (uses environment variable if None)
            
        Returns:
            bool: True if stage dropped successfully, False otherwise
        """
        if not self.sf.conn:
            logger.error("❌ No active Snowflake connection.")
            return False
        
        try:
            # Get database and schema from environment if not provided
            if not database:
                database = os.getenv("SNOWFLAKE_DATABASE")
            if not schema:
                schema = os.getenv("SNOWFLAKE_SCHEMA")
            
            if not database or not schema:
                logger.error("❌ Database and schema must be provided or set in environment variables")
                return False
            
            cursor = self.sf.conn.cursor()
            
            # Use the stage name as provided (no prefix for user-specified stages)
            full_stage_name = stage_name.upper()
            
            # Drop stage
            drop_sql = f"DROP STAGE IF EXISTS {database}.{schema}.{full_stage_name}"
            logger.info(f"Dropping stage: {drop_sql}")
            cursor.execute(drop_sql)
            
            cursor.close()
            logger.info(f"✅ Stage dropped: {full_stage_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to drop stage: {e}")
            return False
    
    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names for Snowflake compatibility.
        (Reusing the same logic from SnowflakeHandler)
        """
        import re
        
        # Snowflake reserved words that should be uppercase
        reserved_words = {
            'date', 'dates', 'time', 'timestamp', 'year', 'month', 'day',
            'hour', 'minute', 'second', 'order', 'group', 'select', 'from',
            'where', 'and', 'or', 'not', 'null', 'true', 'false', 'case',
            'when', 'then', 'else', 'end', 'as', 'in', 'like', 'between',
            'is', 'exists', 'all', 'any', 'some', 'distinct', 'count',
            'sum', 'avg', 'min', 'max', 'first', 'last', 'limit', 'offset'
        }
        
        # Create a mapping of old names to new names
        column_mapping = {}
        new_columns = []
        
        for col in df.columns:
            original_name = col
            
            # Apply normalization rules
            normalized = col
            
            # Replace spaces with underscores
            normalized = normalized.replace(' ', '_')
            
            # Replace # with 'number'
            normalized = normalized.replace('#', 'number')
            
            # Replace special characters with underscores (except alphanumeric and underscore)
            normalized = re.sub(r'[^a-zA-Z0-9_]', '_', normalized)
            
            # Convert to lowercase
            normalized = normalized.lower()
            
            # Remove multiple consecutive underscores
            normalized = re.sub(r'_+', '_', normalized)
            
            # Remove leading and trailing underscores
            normalized = normalized.strip('_')
            
            # Ensure column name is not empty
            if not normalized:
                normalized = 'unnamed_column'
            
            # Convert all columns to UPPERCASE for consistency
            normalized = normalized.upper()
            
            # Handle reserved words - store with quotes
            if normalized in reserved_words:
                # Store the name with quotes for Snowflake compatibility
                final_name = f'"{normalized}"'
                logger.info(f"🔄 Reserved word detected: '{original_name}' -> '{final_name}' (quoted UPPERCASE)")
            else:
                # Ensure uniqueness (add number suffix if duplicate)
                counter = 1
                final_name = normalized
                while final_name in new_columns:
                    final_name = f"{normalized}_{counter}"
                    counter += 1
            
            column_mapping[original_name] = final_name
            new_columns.append(final_name)
            
            # Log the transformation if it changed (but not for reserved words already logged)
            if original_name != final_name and normalized not in reserved_words:
                logger.info(f"🔄 Column normalized: '{original_name}' -> '{final_name}'")
        
        # Rename columns in the DataFrame
        df_normalized = df.rename(column_mapping)
        
        logger.info(f"✅ Columns normalized: {len(column_mapping)}")
        
        return df_normalized
    
    def _generate_create_table_from_stage(self, stage_name: str, table_name: str, database: str, schema: str) -> str:
        """
        Generate CREATE TABLE SQL based on stage file structure.
        This is a simplified version that creates a table with VARCHAR columns.
        """
        try:
            cursor = self.sf.conn.cursor()
            
            # Get file list from stage to determine structure
            list_sql = f"LIST @{database}.{schema}.{stage_name}"
            cursor.execute(list_sql)
            files = cursor.fetchall()
            
            if not files:
                logger.warning("⚠️  No files found in stage to determine table structure")
                return None
            
            # For now, create a generic table structure
            # In a real implementation, you might want to sample the first file
            # to determine actual column types
            create_sql = f"""
            CREATE TABLE {database}.{schema}.{table_name.upper()} (
                -- This is a generic structure. 
                -- You may want to modify this based on your actual data structure.
                -- All columns are VARCHAR for maximum compatibility.
                COLUMN1 VARCHAR(16777216),
                COLUMN2 VARCHAR(16777216),
                COLUMN3 VARCHAR(16777216)
            )
            """
            
            cursor.close()
            return create_sql
            
        except Exception as e:
            logger.error(f"❌ Failed to generate CREATE TABLE SQL: {e}")
            return None
