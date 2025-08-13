"""
Domo utilities for data extraction operations.

This module handles all Domo-related operations including:
- Authentication setup
- Data extraction from datasets
- Data cleaning and preprocessing
"""

import os
import logging
import sys
from typing import Optional
import polars as pl
import pandas as pd  # Still needed for to_dataframe conversion
from pathlib import Path

import subprocess              
import json                   
from collections import deque   # ←  used for the BFS queue

# Add argo-utils-cli/src to Python path for domo_utils module
argo_utils_path = Path(__file__).parent.parent.parent / "argo-utils-cli" / "src"
if argo_utils_path.exists():
    sys.path.insert(0, str(argo_utils_path))
else:
    print(f"⚠️  Warning: argo-utils-cli/src not found at {argo_utils_path}")

from domo_utils.auth import DeveloperTokenAuth, ClientCredentialsAuth
from domo_utils.api import get_dataset_api
from domo_utils.utils.pandas_utils import to_dataframe
from domo_utils.exceptions import DomoUtilsError

logger = logging.getLogger(__name__)


class DomoHandler:
    """Handles all Domo operations including authentication and data extraction."""
    
    def __init__(self):
        """Initialize the Domo handler."""
        self.auth_client = None
        self.dataset_api = None
        
    def setup_auth(self) -> bool:
        """
        Setup Domo authentication using environment variables.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            # Get all environment config at once
            from .common import get_env_config
            env_config = get_env_config()
            
            dev_token = env_config.get("DOMO_DEVELOPER_TOKEN")
            instance_id = env_config.get("DOMO_INSTANCE")
            
            if dev_token and instance_id:
                logger.info("Using Developer Token authentication")
                self.auth_client = DeveloperTokenAuth(
                    token=dev_token,
                    instance_id=instance_id
                )
                self.auth_client.connect()
                self.dataset_api = get_dataset_api(self.auth_client)
                return True
            
            # Try client credentials
            client_id = env_config.get("DOMO_CLIENT_ID")
            client_secret = env_config.get("DOMO_CLIENT_SECRET")
            
            if client_id and client_secret and instance_id:
                logger.info("Using Client Credentials authentication")
                self.auth_client = ClientCredentialsAuth(
                    client_id=client_id,
                    client_secret=client_secret,
                    api_host=f"{instance_id}.domo.com"
                )
                self.auth_client.connect()
                self.dataset_api = get_dataset_api(self.auth_client)
                return True
                
            logger.error("No valid Domo authentication found")
            logger.error("Please set one of:")
            logger.error("  - DOMO_DEVELOPER_TOKEN and DOMO_INSTANCE")
            logger.error("  - DOMO_CLIENT_ID, DOMO_CLIENT_SECRET, and DOMO_INSTANCE")
            return False
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Domo: {e}")
            return False
    
    def extract_data(self, dataset_id: str, query: Optional[str] = None, 
                    chunk_size: int = 1000000, enable_auto_type_conversion: bool = False) -> Optional[pl.DataFrame]:
        """
        Extract data from Domo dataset.
        
        Args:
            dataset_id: Domo dataset ID
            query: Optional custom SQL query
            chunk_size: Number of rows to extract per chunk
            enable_auto_type_conversion: Whether to enable automatic type conversion (default: False)
            
        Returns:
            Optional[pl.DataFrame]: Extracted data or None if failed
        """
        if self.dataset_api is None:
            logger.error("Domo dataset API not initialized")
            return None
        
        try:
            # Build query
            if query:
                base_query = query
                logger.info(f"Using custom query: {query}")
            else:
                base_query = "SELECT * FROM table limit 1000"
                logger.info("Using default query: SELECT * FROM table")
            
            # Get dataset info
            dataset_info = self.dataset_api.get(dataset_id)
            total_rows = dataset_info.row_count or 0
            logger.info(f"Dataset {dataset_id} has {total_rows} rows")
            
            # Extract data
            if total_rows <= chunk_size:
                # Single chunk extraction
                return self._extract_single_chunk(dataset_id, base_query, enable_auto_type_conversion)
            else:
                # Paginated extraction
                return self._extract_with_pagination(dataset_id, base_query, chunk_size, total_rows, enable_auto_type_conversion)
                
        except Exception as e:
            logger.error(f"Failed to extract data from Domo: {e}")
            return None
    
    def _extract_single_chunk(self, dataset_id: str, query: str, enable_auto_type_conversion: bool = False) -> Optional[pl.DataFrame]:
        """
        Extract data in a single chunk.
        
        Args:
            dataset_id: Domo dataset ID
            query: SQL query
            enable_auto_type_conversion: Whether to enable automatic type conversion
            
        Returns:
            Optional[pl.DataFrame]: Extracted data or None if failed
        """
        try:
            logger.info("Extracting data in single chunk...")
            
            # Execute query
            result = self.dataset_api.query(dataset_id, query)
            
            # Convert to DataFrame (pandas first, then to polars)
            pandas_df = to_dataframe(result)
            
            if pandas_df is not None and len(pandas_df) > 0:
                # Convert pandas to polars with safe conversion
                try:
                    df = self._safe_pandas_to_polars(pandas_df)
                    if df is not None:
                        logger.info(f"✅ Extracted {len(df)} rows")
                        # Use the provided auto-type conversion setting
                        return self._clean_dataframe(df, enable_auto_type_conversion=enable_auto_type_conversion)
                    else:
                        logger.error("Failed to convert single chunk")
                        return None
                except Exception as conversion_error:
                    logger.error(f"Failed to convert pandas to polars: {conversion_error}")
                    # Try fallback method
                    df = self._fallback_pandas_to_polars(pandas_df)
                    if df is not None:
                        logger.info(f"✅ Extracted {len(df)} rows (fallback method)")
                        # Use the provided auto-type conversion setting
                        return self._clean_dataframe(df, enable_auto_type_conversion=enable_auto_type_conversion)
                    else:
                        logger.error("Fallback conversion also failed")
                        return None
            else:
                logger.warning("No data returned from query")
                return pl.DataFrame()
                
        except Exception as e:
            logger.error(f"Single chunk extraction failed: {e}")
            return None
    
    def _extract_with_pagination(self, dataset_id: str, base_query: str, chunk_size: int, 
                                total_rows: int, enable_auto_type_conversion: bool = False) -> Optional[pl.DataFrame]:
        """
        Extract data using pagination for large datasets.
        
        Args:
            dataset_id: Domo dataset ID
            base_query: Base SQL query
            chunk_size: Number of rows per chunk
            total_rows: Total number of rows in dataset
            enable_auto_type_conversion: Whether to enable automatic type conversion
            
        Returns:
            Optional[pl.DataFrame]: Extracted data or None if failed
        """
        try:
            logger.info(f"Extracting data in chunks of {chunk_size} rows...")
            
            all_data = []
            offset = 0
            
            while offset < total_rows:
                # Build paginated query
                limit = min(chunk_size, total_rows - offset)
                paginated_query = f"{base_query} LIMIT {limit} OFFSET {offset}"
                
                logger.info(f"Extracting chunk: offset={offset}, limit={limit}")
                
                # Execute query
                result = self.dataset_api.query(dataset_id, paginated_query)
                
                # Convert to DataFrame (pandas first, then to polars)
                pandas_chunk_df = to_dataframe(result)
                
                if pandas_chunk_df is not None and len(pandas_chunk_df) > 0:
                    # Convert pandas to polars with proper error handling
                    try:
                        chunk_df = self._safe_pandas_to_polars(pandas_chunk_df)
                        if chunk_df is not None:
                            all_data.append(chunk_df)
                            logger.info(f"Extracted chunk: {len(chunk_df)} rows")
                        else:
                            logger.error(f"Failed to convert chunk at offset {offset}")
                            break
                    except Exception as conversion_error:
                        logger.error(f"Failed to convert pandas to polars at offset {offset}: {conversion_error}")
                        # Try a fallback approach
                        chunk_df = self._fallback_pandas_to_polars(pandas_chunk_df)
                        if chunk_df is not None:
                            all_data.append(chunk_df)
                            logger.info(f"Extracted chunk (fallback method): {len(chunk_df)} rows")
                        else:
                            logger.error(f"Fallback conversion also failed at offset {offset}")
                            break
                else:
                    logger.warning(f"No data in chunk at offset {offset}")
                    break
                
                offset += limit
                
                # Safety check
                if len(all_data) > 100:  # Prevent infinite loops
                    logger.error("Too many chunks extracted, stopping")
                    break
            
            if all_data:
                # Combine all chunks
                combined_df = pl.concat(all_data, how="vertical")
                logger.info(f"✅ Extracted {len(combined_df)} rows in {len(all_data)} chunks")
                # Use the provided auto-type conversion setting
                return self._clean_dataframe(combined_df, enable_auto_type_conversion=enable_auto_type_conversion)
            else:
                logger.warning("No data extracted from any chunk")
                return pl.DataFrame()
                
        except Exception as e:
            logger.error(f"Pagination extraction failed: {e}")
            return None
    
    def _clean_dataframe(self, df: pl.DataFrame, enable_auto_type_conversion: bool = False) -> pl.DataFrame:
        """
        Clean and preprocess DataFrame.
        
        Args:
            df: Raw DataFrame
            enable_auto_type_conversion: Whether to enable automatic type conversion (default: False)
            
        Returns:
            pl.DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Cleaning DataFrame...")
            
            # Remove completely empty rows (all columns null)
            # In Polars, we need to filter out rows where all values are null
            df = df.filter(~pl.all_horizontal(pl.all().is_null()))
            
            if df.is_empty():
                logger.warning("DataFrame is empty after cleaning")
                return df
            
            # Clean column names
            df = df.rename(lambda col: col.strip())
            
            # Only perform automatic type conversion if explicitly enabled
            if enable_auto_type_conversion:
                logger.info("Automatic type conversion enabled - analyzing column types")
                
                # Handle data types
                for col in df.columns:
                    # Skip if column is already numeric
                    if df.schema[col] in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]:
                        continue
                    
                    # Try to convert to numeric if possible
                    numeric_threshold = 0.8  # Increased threshold to be more conservative
                    non_null_values = df.filter(pl.col(col).is_not_null()).select(pl.col(col))
                    
                    if len(non_null_values) > 0:
                        # Count how many values can be converted to numeric
                        try:
                            numeric_count = non_null_values.select(pl.col(col).cast(pl.Float64).is_not_null()).sum().item()
                            
                            # Convert if threshold is met
                            if numeric_count / len(non_null_values) >= numeric_threshold:
                                try:
                                    df = df.with_columns(pl.col(col).cast(pl.Float64))
                                    logger.info(f"Converted column '{col}' to numeric")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                
                # Handle date columns
                for col in df.columns:
                    if df.schema[col] == pl.Utf8:
                        # Try to convert to datetime
                        date_threshold = 0.8  # Increased threshold to be more conservative
                        non_null_values = df.filter(pl.col(col).is_not_null()).select(pl.col(col))
                        
                        if len(non_null_values) > 0:
                            try:
                                date_count = non_null_values.select(pl.col(col).str.to_datetime().is_not_null()).sum().item()
                                
                                # Convert if threshold is met
                                if date_count / len(non_null_values) >= date_threshold:
                                    try:
                                        df = df.with_columns(pl.col(col).str.to_datetime())
                                        logger.info(f"Converted column '{col}' to datetime")
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                
                # Handle boolean columns
                for col in df.columns:
                    if df.schema[col] == pl.Utf8:
                        # Check if column contains boolean-like values
                        try:
                            unique_values = df.filter(pl.col(col).is_not_null()).select(pl.col(col).unique()).to_series()
                            if len(unique_values) <= 2:
                                bool_like = all(str(val).lower() in ['true', 'false', '1', '0', 'yes', 'no'] 
                                              for val in unique_values)
                                if bool_like:
                                    try:
                                        df = df.with_columns(
                                            pl.when(pl.col(col).str.to_lowercase().is_in(['true', '1', 'yes']))
                                            .then(True)
                                            .otherwise(False)
                                            .cast(pl.Boolean)
                                            .alias(col)
                                        )
                                        logger.info(f"Converted column '{col}' to boolean")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
            else:
                logger.info("Automatic type conversion disabled - preserving original data types")
            
            logger.info(f"✅ DataFrame cleaned: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"DataFrame cleaning failed: {e}")
            return df  # Return original if cleaning fails
    
    def _safe_pandas_to_polars(self, pandas_df: pd.DataFrame) -> Optional[pl.DataFrame]:
        """
        Safely convert pandas DataFrame to polars DataFrame with data type handling.
        
        Args:
            pandas_df: Pandas DataFrame to convert
            
        Returns:
            Optional[pl.DataFrame]: Converted polars DataFrame or None if failed
        """
        try:
            # Log DataFrame info for debugging
            logger.debug(f"Converting pandas DataFrame: {pandas_df.shape}")
            logger.debug(f"Data types: {pandas_df.dtypes.to_dict()}")
            
            # Check for problematic columns and fix them
            pandas_df_cleaned = pandas_df.copy()
            
            for col in pandas_df_cleaned.columns:
                dtype = pandas_df_cleaned[col].dtype
                
                # Handle object columns that might contain mixed types
                if dtype == 'object':
                    # Convert all values to strings to avoid type conflicts
                    pandas_df_cleaned[col] = pandas_df_cleaned[col].astype(str)
                    # Replace 'nan' strings with actual NaN values
                    pandas_df_cleaned[col] = pandas_df_cleaned[col].replace('nan', pd.NA)
                
                # Handle integer columns with NaN values
                elif str(dtype).startswith('int') and pandas_df_cleaned[col].isnull().any():
                    # Convert to nullable integer type
                    pandas_df_cleaned[col] = pandas_df_cleaned[col].astype('Int64')
                
                # Handle float columns with potential infinity values
                elif str(dtype).startswith('float'):
                    # Replace inf/-inf with NaN
                    pandas_df_cleaned[col] = pandas_df_cleaned[col].replace([float('inf'), float('-inf')], pd.NA)
            
            # Convert to polars
            polars_df = pl.from_pandas(pandas_df_cleaned)
            
            logger.debug(f"Successfully converted to polars: {polars_df.shape}")
            return polars_df
            
        except Exception as e:
            logger.error(f"Safe pandas to polars conversion failed: {e}")
            return None
    
    def _fallback_pandas_to_polars(self, pandas_df: pd.DataFrame) -> Optional[pl.DataFrame]:
        """
        Fallback method to convert pandas DataFrame to polars using more aggressive cleaning.
        
        Args:
            pandas_df: Pandas DataFrame to convert
            
        Returns:
            Optional[pl.DataFrame]: Converted polars DataFrame or None if failed
        """
        try:
            logger.info("Using fallback conversion method - converting all columns to strings")
            
            # Convert all columns to strings first
            pandas_df_str = pandas_df.copy()
            for col in pandas_df_str.columns:
                pandas_df_str[col] = pandas_df_str[col].astype(str)
                # Replace 'nan' with actual None
                pandas_df_str[col] = pandas_df_str[col].replace('nan', None)
            
            # Create polars DataFrame manually
            data_dict = {}
            for col in pandas_df_str.columns:
                # Clean column name
                clean_col = str(col).strip()
                data_dict[clean_col] = pandas_df_str[col].tolist()
            
            polars_df = pl.DataFrame(data_dict)
            
            logger.info(f"Fallback conversion successful: {polars_df.shape}")
            return polars_df
            
        except Exception as e:
            logger.error(f"Fallback pandas to polars conversion failed: {e}")
            return None
    
    def get_all_datasets(self, batch_size: int = 500) -> list:
        """
        Get all datasets from Domo using the search API with pagination.
        
        Args:
            batch_size (int): Number of datasets to fetch per batch
            
        Returns:
            list: List of datasets with id, name, and other metadata
        """
        logger.info(f"🔍 Fetching all datasets from Domo (batch size: {batch_size})")
        
        if self.dataset_api is None:
            logger.error("❌ Domo dataset API not initialized")
            return []
        
        try:
            all_datasets = []
            offset = 0
            total_fetched = 0
            
            while True:
                logger.info(f"📥 Fetching batch: offset={offset}, limit={batch_size}")
                
                try:
                    # Use the search API to get datasets
                    search_results = self.dataset_api.search(
                        limit=batch_size, 
                        offset=offset,
                        filters=[],  # No filters to get all datasets
                        sort=None
                    )
                    
                    if not search_results or len(search_results) == 0:
                        logger.info(f"✅ No more datasets found at offset {offset}")
                        break
                    
                    # Extract dataset information
                    batch_datasets = []
                    for dataset in search_results:
                        dataset_info = {
                            'id': dataset.id,
                            'name': dataset.name,
                            'description': getattr(dataset, 'description', ''),
                            'created': getattr(dataset, 'created', ''),
                            'last_updated': getattr(dataset, 'last_updated', ''),
                            'row_count': getattr(dataset, 'row_count', 0),
                            'column_count': getattr(dataset, 'column_count', 0),
                            'owner': getattr(dataset.owner, 'name', '') if hasattr(dataset, 'owner') and dataset.owner else ''
                        }
                        batch_datasets.append(dataset_info)
                    
                    all_datasets.extend(batch_datasets)
                    total_fetched += len(batch_datasets)
                    
                    logger.info(f"✅ Fetched {len(batch_datasets)} datasets (total: {total_fetched})")
                    
                    # If we got fewer results than the batch size, we've reached the end
                    if len(batch_datasets) < batch_size:
                        logger.info("✅ Reached end of datasets")
                        break
                    
                    offset += batch_size
                    
                except Exception as e:
                    logger.error(f"❌ Error fetching batch at offset {offset}: {e}")
                    break
            
            logger.info(f"🎉 Successfully fetched {len(all_datasets)} total datasets")
            return all_datasets
            
        except Exception as e:
            logger.error(f"❌ Failed to get datasets from Domo: {e}")
            return [] 

    def get_all_dataflows(self, dataset_id_list: list[str]) -> pd.DataFrame:
        """
        Lineage crawl starting from the datasets in ``dataset_id_list``.
        Returns a DataFrame with columns:
            • Dataflow ID
            • Source Dataset IDs  (comma + newline-separated)
            • Output Dataset IDs  (comma + newline-separated)
        """

        logger.info("🔍 Fetching all dataflows from Domo")

        visited_datasets: set[str] = set()
        # tmp dict to deduplicate and merge inputs/outputs per dataflow
        dataflows_tmp = {
                "Dataflow ID": [],
                "Source Dataset IDs": [],
                "Output Dataset IDs": []
        }
    

        queue: deque[str] = deque(dataset_id_list)

        while queue:
            dataset_id = queue.popleft()
            if dataset_id in visited_datasets:
                continue
            visited_datasets.add(dataset_id)

            logger.info(f"🔍 Fetching lineage for dataset {dataset_id}")

            cmd = [
                "argo-domo",
                "lineage",
                "export",
                "DATA_SOURCE",
                dataset_id,
                "--format",
                "json",
            ]

            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
                lineage = json.loads(proc.stdout)
            except subprocess.CalledProcessError as exc:
                logger.error(f"❌ argo-domo export failed for {dataset_id}: {exc.stderr}")
                continue
            except json.JSONDecodeError:
                logger.error(f"❌ Unable to parse JSON for {dataset_id}")
                continue

            # entities is a dict, iterate over its values
            entities = lineage.get("entities", {}).values()

            for entity in entities:
                if entity.get("type") != "DATAFLOW":
                    continue

                # collect parent / child dataset IDs
                parent_ids = [
                    p.get("id")
                    for p in entity.get("parents", [])
                    if p.get("type") == "DATA_SOURCE"
                ]
                child_ids = [
                    c.get("id")
                    for c in entity.get("children", [])
                    if c.get("type") == "DATA_SOURCE"
                ]

                # keep only dataflows that actually touch the current dataset
                if dataset_id not in child_ids:
                    continue

                df_id = entity["id"]
                # rec = dataflows_tmp[df_id]

        
                dataflows_tmp["Dataflow ID"].append(entity["id"])
                dataflows_tmp["Source Dataset IDs"].append(f",\n".join(map(str, parent_ids)))
                dataflows_tmp["Output Dataset IDs"].append(f",\n".join(map(str, child_ids)))

        dataflows_df = pd.DataFrame(dataflows_tmp)

        # Merge Source Dataset IDs and Output Dataset IDs into a single when Dataflow ID is the same
        dataflows_df = dataflows_df.groupby("Dataflow ID").agg({
            "Source Dataset IDs": lambda x: x.str.cat(sep=",\n"),
            "Output Dataset IDs": lambda x: x.str.cat(sep=",\n")
        }).reset_index()

        logger.info(f"✅ Collected {len(dataflows_df)} dataflows")
        return dataflows_df

    def get_dataset_schema(self, dataset_id: str) -> dict:
        """
        Get the schema of a dataset.
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            dict: The dataset schema with columns information
        """
        try:
            if not self.dataset_api:
                logger.error("Dataset API not initialized. Call setup_auth() first.")
                return {"columns": []}
            
            # Get dataset info including schema
            dataset_info = self.dataset_api.get_dataset(dataset_id)
            schema = dataset_info.get('schema', {})
            
            logger.info(f"Retrieved schema for dataset {dataset_id}")
            return schema
            
        except Exception as e:
            logger.error(f"Error getting schema for dataset {dataset_id}: {e}")
            return {"columns": []}

    def query_dataset(self, dataset_id: str, query: str) -> dict:
        """
        Execute a simple SQL query on a dataset.
        
        Args:
            dataset_id: The dataset ID
            query: The SQL query to execute
            
        Returns:
            dict: Query result with columns and rows
        """
        try:
            if not self.dataset_api:
                logger.error("Dataset API not initialized. Call setup_auth() first.")
                return {"datasource": "", "columns": [], "rows": []}
            
            # For simple queries (like COUNT), use extract_data with the query
            df = self.extract_data(dataset_id, query, chunk_size=1000)
            
            if df is None:
                return {"datasource": "", "columns": [], "rows": []}
            
            # Convert polars DataFrame to the expected format
            columns = df.columns
            rows = df.rows()
            
            result = {
                "datasource": dataset_id,
                "columns": columns,
                "rows": rows
            }
            
            logger.info(f"Query executed successfully on dataset {dataset_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error querying dataset {dataset_id}: {e}")
            return {"datasource": "", "columns": [], "rows": []}


def export_datasets_to_spreadsheet(spreadsheet_id: str, sheet_name: str = "Datasets", 
                                 credentials_path: str = None) -> bool:
    """
    Export all datasets from Domo to Google Sheets.
    
    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Name of the sheet tab (default: "Datasets")
        credentials_path (str): Path to Google Sheets credentials file
        
    Returns:
        bool: True if export successful, False otherwise
    """
    if not credentials_path:
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    
    if not credentials_path:
        logger.error("❌ No Google Sheets credentials provided")
        return False
    
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Google Sheets credentials file not found: {credentials_path}")
        return False
    
    try:
        logger.info(f"📊 Exporting datasets to spreadsheet: {spreadsheet_id}")
        logger.info(f"📄 Sheet name: {sheet_name}")
        
        # Initialize Domo handler and get datasets
        domo_handler = DomoHandler()
        if not domo_handler.setup_auth():
            logger.error("❌ Failed to authenticate with Domo")
            return False
        
        datasets = domo_handler.get_all_datasets()
        
        if not datasets:
            logger.error("❌ No datasets found to export")
            return False
        
        # Import GoogleSheets here to avoid circular imports
        from .gsheets import GoogleSheets, READ_WRITE_SCOPES
        
        # Initialize Google Sheets client
        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        
        # Prepare data for export
        headers = ['Dataset ID', 'Name', 'Description', 'Created', 'Last Updated', 'Row Count', 'Column Count', 'Owner']
        data_rows = []
        
        for dataset in datasets:
            # Convert datetime objects to strings to avoid JSON serialization issues
            created_date = dataset['created']
            if hasattr(created_date, 'strftime'):
                created_date = created_date.strftime('%Y-%m-%d %H:%M:%S')
            elif created_date is None:
                created_date = ''
            
            last_updated = dataset['last_updated']
            if hasattr(last_updated, 'strftime'):
                last_updated = last_updated.strftime('%Y-%m-%d %H:%M:%S')
            elif last_updated is None:
                last_updated = ''
            
            row = [
                str(dataset['id']),
                str(dataset['name']),
                str(dataset['description']),
                str(created_date),
                str(last_updated),
                int(dataset['row_count']),
                int(dataset['column_count']),
                str(dataset['owner'])
            ]
            data_rows.append(row)
        
        # Combine headers and data
        all_data = [headers] + data_rows
        
        # Write to Google Sheets
        logger.info(f"📝 Writing {len(data_rows)} datasets to spreadsheet...")
        
        # First, try to clear the existing sheet if it exists
        try:
            # Read a small range to check if sheet exists
            existing_data = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A1:A1")
            if existing_data:
                logger.info(f"📄 Sheet '{sheet_name}' exists, clearing content...")
                # Clear the sheet by writing empty data to a large range
                gsheets_client.write_range(spreadsheet_id, f"{sheet_name}!A1:Z10000", [])
        except Exception:
            logger.info(f"📄 Sheet '{sheet_name}' doesn't exist, will be created automatically")
        
        # Write the new data
        gsheets_client.write_range(spreadsheet_id, f"{sheet_name}!A1", all_data)
        
        logger.info(f"✅ Successfully exported {len(data_rows)} datasets to {sheet_name}")
        logger.info(f"📊 Columns: {', '.join(headers)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to export datasets to spreadsheet: {e}")
        return False 