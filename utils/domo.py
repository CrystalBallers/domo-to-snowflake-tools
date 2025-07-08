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

# Add argo-utils-cli/src to Python path for domo_utils module
argo_utils_path = Path(__file__).parent.parent / "argo-utils-cli" / "src"
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
            # Try developer token first
            dev_token = os.getenv("DOMO_DEVELOPER_TOKEN")
            instance_id = os.getenv("DOMO_INSTANCE")
            
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
            client_id = os.getenv("DOMO_CLIENT_ID")
            client_secret = os.getenv("DOMO_CLIENT_SECRET")
            
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
                    chunk_size: int = 1000000) -> Optional[pd.DataFrame]:
        """
        Extract data from Domo dataset.
        
        Args:
            dataset_id: Domo dataset ID
            query: Optional custom SQL query
            chunk_size: Number of rows to extract per chunk
            
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
                base_query = "SELECT * FROM table"
                logger.info("Using default query: SELECT * FROM table")
            
            # Get dataset info
            dataset_info = self.dataset_api.get(dataset_id)
            total_rows = dataset_info.row_count or 0
            logger.info(f"Dataset {dataset_id} has {total_rows} rows")
            
            # Extract data
            if total_rows <= chunk_size:
                # Single chunk extraction
                return self._extract_single_chunk(dataset_id, base_query)
            else:
                # Paginated extraction
                return self._extract_with_pagination(dataset_id, base_query, chunk_size, total_rows)
                
        except Exception as e:
            logger.error(f"Failed to extract data from Domo: {e}")
            return None
    
    def _extract_single_chunk(self, dataset_id: str, query: str) -> Optional[pd.DataFrame]:
        """
        Extract data in a single chunk.
        
        Args:
            dataset_id: Domo dataset ID
            query: SQL query
            
        Returns:
            Optional[pd.DataFrame]: Extracted data or None if failed
        """
        try:
            logger.info("Extracting data in single chunk...")
            
            # Execute query
            result = self.dataset_api.query(dataset_id, query)
            
            # Convert to DataFrame
            df = to_dataframe(result)
            
            if df is not None and len(df) > 0:
                logger.info(f"✅ Extracted {len(df)} rows")
                return self._clean_dataframe(df)
            else:
                logger.warning("No data returned from query")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Single chunk extraction failed: {e}")
            return None
    
    def _extract_with_pagination(self, dataset_id: str, base_query: str, chunk_size: int, 
                                total_rows: int) -> Optional[pd.DataFrame]:
        """
        Extract data using pagination for large datasets.
        
        Args:
            dataset_id: Domo dataset ID
            base_query: Base SQL query
            chunk_size: Number of rows per chunk
            total_rows: Total number of rows in dataset
            
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
                
                # Convert to DataFrame
                chunk_df = to_dataframe(result)
                
                if chunk_df is not None and len(chunk_df) > 0:
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
                return self._clean_dataframe(combined_df)
            else:
                logger.warning("No data extracted from any chunk")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Pagination extraction failed: {e}")
            return None
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and preprocess DataFrame.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Cleaning DataFrame...")
            
            # Remove completely empty rows and columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            if df.empty:
                logger.warning("DataFrame is empty after cleaning")
                return df
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Handle data types
            for col in df.columns:
                # Skip if column is already numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    continue
                
                # Try to convert to numeric if possible
                numeric_threshold = 0.5  # Configurable
                non_null_values = df[col].dropna()
                
                if len(non_null_values) > 0:
                    # Count how many values can be converted to numeric
                    numeric_count = 0
                    for val in non_null_values:
                        try:
                            float(val)
                            numeric_count += 1
                        except (ValueError, TypeError):
                            pass
                    
                    # Convert if threshold is met
                    if numeric_count / len(non_null_values) >= numeric_threshold:
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            logger.info(f"Converted column '{col}' to numeric")
                        except Exception:
                            pass
            
            # Handle date columns
            for col in df.columns:
                if pd.api.types.is_object_dtype(df[col]):
                    # Try to convert to datetime
                    date_threshold = 0.3  # Configurable
                    non_null_values = df[col].dropna()
                    
                    if len(non_null_values) > 0:
                        date_count = 0
                        for val in non_null_values:
                            try:
                                pd.to_datetime(val)
                                date_count += 1
                            except (ValueError, TypeError):
                                pass
                        
                        # Convert if threshold is met
                        if date_count / len(non_null_values) >= date_threshold:
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                                logger.info(f"Converted column '{col}' to datetime")
                            except Exception:
                                pass
            
            # Handle boolean columns
            for col in df.columns:
                if pd.api.types.is_object_dtype(df[col]):
                    # Check if column contains boolean-like values
                    unique_values = df[col].dropna().unique()
                    if len(unique_values) <= 2:
                        bool_like = all(str(val).lower() in ['true', 'false', '1', '0', 'yes', 'no'] 
                                      for val in unique_values)
                        if bool_like:
                            try:
                                df[col] = df[col].map({
                                    'true': True, 'false': False,
                                    '1': True, '0': False,
                                    'yes': True, 'no': False
                                }).astype('boolean')
                                logger.info(f"Converted column '{col}' to boolean")
                            except Exception:
                                pass
            
            logger.info(f"✅ DataFrame cleaned: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"DataFrame cleaning failed: {e}")
            return df  # Return original if cleaning fails 