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
import pandas as pd
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
                    chunk_size: int = 1000000, enable_auto_type_conversion: bool = False) -> Optional[pd.DataFrame]:
        """
        Extract data from Domo dataset.
        
        Args:
            dataset_id: Domo dataset ID
            query: Optional custom SQL query
            chunk_size: Number of rows to extract per chunk
            enable_auto_type_conversion: Whether to enable automatic type conversion (default: False)
            
        Returns:
            Optional[pd.DataFrame]: Extracted data or None if failed
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
                # Use chunk_size parameter to determine limit
                if chunk_size is not None:
                    base_query = f"SELECT * FROM table limit {chunk_size}"
                    logger.info(f"Using default query with limit: SELECT * FROM table limit {chunk_size}")
                else:
                    base_query = "SELECT * FROM table"
                    logger.info("Using default query: SELECT * FROM table (no limit)")
            
            # Get dataset info
            dataset_info = self.dataset_api.get(dataset_id)
            total_rows = dataset_info.row_count or 0
            logger.info(f"Dataset {dataset_id} has {total_rows} rows")
            
            # Extract data
            if chunk_size is None or total_rows <= chunk_size:
                # Single chunk extraction (either no limit or dataset fits in chunk)
                return self._extract_single_chunk(dataset_id, base_query, enable_auto_type_conversion)
            else:
                # Paginated extraction
                return self._extract_with_pagination(dataset_id, base_query, chunk_size, total_rows, enable_auto_type_conversion)
                
        except Exception as e:
            logger.error(f"Failed to extract data from Domo: {e}")
            return None
    
    def _extract_single_chunk(self, dataset_id: str, query: str, enable_auto_type_conversion: bool = False) -> Optional[pd.DataFrame]:
        """
        Extract data in a single chunk.
        
        Args:
            dataset_id: Domo dataset ID
            query: SQL query
            enable_auto_type_conversion: Whether to enable automatic type conversion
            
        Returns:
            Optional[pd.DataFrame]: Extracted data or None if failed
        """
        try:
            logger.info("Extracting data in single chunk...")
            
            # Execute query
            result = self.dataset_api.query(dataset_id, query)
            
            # Convert to DataFrame (pandas directly)
            pandas_df = to_dataframe(result)
            
            if pandas_df is not None and len(pandas_df) > 0:
                # Clean DataFrame if needed using pandas operations
                cleaned_df = self._clean_pandas_dataframe(pandas_df, enable_auto_type_conversion)
                logger.info(f"✅ Extracted {len(cleaned_df)} rows")
                return cleaned_df
            else:
                logger.warning("No data returned from query")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Single chunk extraction failed: {e}")
            return None
    
    def _extract_with_pagination(self, dataset_id: str, base_query: str, chunk_size: int, 
                                total_rows: int, enable_auto_type_conversion: bool = False) -> Optional[pd.DataFrame]:
        """
        Extract data using pagination for large datasets.
        
        Args:
            dataset_id: Domo dataset ID
            base_query: Base SQL query
            chunk_size: Number of rows per chunk
            total_rows: Total number of rows in dataset
            enable_auto_type_conversion: Whether to enable automatic type conversion
            
        Returns:
            Optional[pd.DataFrame]: Extracted data or None if failed
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
                
                # Convert to DataFrame (pandas directly)
                pandas_chunk_df = to_dataframe(result)
                
                if pandas_chunk_df is not None and len(pandas_chunk_df) > 0:
                    # Clean chunk DataFrame
                    chunk_df = self._clean_pandas_dataframe(pandas_chunk_df, enable_auto_type_conversion)
                    all_data.append(chunk_df)
                    logger.info(f"Extracted chunk: {len(chunk_df)} rows")
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
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"✅ Extracted {len(combined_df)} rows in {len(all_data)} chunks")
                # Use the provided auto-type conversion setting
                return self._clean_pandas_dataframe(combined_df, enable_auto_type_conversion=enable_auto_type_conversion)
            else:
                logger.warning("No data extracted from any chunk")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Pagination extraction failed: {e}")
            return None
    
    def _clean_pandas_dataframe(self, df: pd.DataFrame, enable_auto_type_conversion: bool = False) -> pd.DataFrame:
        """
        Clean and preprocess pandas DataFrame.
        
        Args:
            df: Raw pandas DataFrame
            enable_auto_type_conversion: Whether to enable automatic type conversion (default: False)
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Cleaning DataFrame...")
            
            # Remove completely empty rows (all columns null)
            df = df.dropna(how='all')
            
            if df.empty:
                logger.warning("DataFrame is empty after cleaning")
                return df
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Only perform automatic type conversion if explicitly enabled
            if enable_auto_type_conversion:
                logger.info("Automatic type conversion enabled - analyzing column types")
                
                # Handle data types
                for col in df.columns:
                    # Skip if column is already numeric
                    if pd.api.types.is_numeric_dtype(df[col]):
                        continue
                    
                    # Try to convert to numeric if possible
                    numeric_threshold = 0.8  # Increased threshold to be more conservative
                    non_null_values = df[col].dropna()
                    
                    if len(non_null_values) > 0:
                        # Count how many values can be converted to numeric
                        try:
                            numeric_series = pd.to_numeric(non_null_values, errors='coerce')
                            numeric_count = numeric_series.notna().sum()
                            
                            # Convert if threshold is met
                            if numeric_count / len(non_null_values) >= numeric_threshold:
                                try:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                                    logger.info(f"Converted column '{col}' to numeric")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                
                # Handle date columns
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Try to convert to datetime
                        date_threshold = 0.8  # Increased threshold to be more conservative
                        non_null_values = df[col].dropna()
                        
                        if len(non_null_values) > 0:
                            try:
                                date_series = pd.to_datetime(non_null_values, errors='coerce')
                                date_count = date_series.notna().sum()
                                
                                # Convert if threshold is met
                                if date_count / len(non_null_values) >= date_threshold:
                                    try:
                                        df[col] = pd.to_datetime(df[col], errors='coerce')
                                        logger.info(f"Converted column '{col}' to datetime")
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                
                # Handle boolean columns
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Check if column contains boolean-like values
                        try:
                            unique_values = df[col].dropna().unique()
                            if len(unique_values) <= 2:
                                bool_like = all(str(val).lower() in ['true', 'false', '1', '0', 'yes', 'no'] 
                                              for val in unique_values)
                                if bool_like:
                                    try:
                                        df[col] = df[col].str.lower().map({
                                            'true': True, '1': True, 'yes': True,
                                            'false': False, '0': False, 'no': False
                                        })
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
            dataset_info = self.dataset_api.get(dataset_id)
            
            # This method is now replaced by the hybrid approach in get_all_stg_files.py
            # Left here for compatibility if called directly
            logger.warning("⚠️  get_dataset_schema() called directly - using legacy fallback")
            logger.warning("📍 Use the hybrid approach in generate_stg_files_from_dataframe() for better results")
            
            try:
                # Simple fallback: extract sample data
                sample_df = self.extract_data(dataset_id, "SELECT * FROM table LIMIT 1", chunk_size=999999999)
                
                if sample_df is not None and not sample_df.empty:
                    columns_list = []
                    for col_name in sample_df.columns:
                        columns_list.append({
                            'name': col_name,
                            'type': 'STRING',  # Default fallback
                            'id': col_name,
                            'visible': True
                        })
                    
                    logger.info(f"Retrieved basic schema for dataset {dataset_id}: {len(columns_list)} columns")
                    return {"columns": columns_list}
                else:
                    logger.warning(f"Could not extract any data from dataset {dataset_id}")
                    return {"columns": []}
            except Exception as e:
                logger.error(f"Fallback schema extraction failed: {e}")
                return {"columns": []}
            
        except Exception as e:
            logger.error(f"Error getting schema for dataset {dataset_id}: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
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
            
            # Convert pandas DataFrame to the expected format
            columns = df.columns.tolist()
            rows = df.values.tolist()
            
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