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
                    chunk_size: int = None, enable_auto_type_conversion: bool = False) -> Optional[pd.DataFrame]:
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
            if chunk_size is None:
                # No limit specified - use pagination with reasonable chunk size for large datasets
                if total_rows > 1000000:
                    # For large datasets, use pagination with 1M chunks
                    return self._extract_with_pagination(dataset_id, base_query, 1000000, total_rows, enable_auto_type_conversion)
                else:
                    # For smaller datasets, use single chunk
                    return self._extract_single_chunk(dataset_id, base_query, enable_auto_type_conversion)
            elif total_rows <= chunk_size:
                # Single chunk extraction (dataset fits in specified chunk size)
                return self._extract_single_chunk(dataset_id, base_query, enable_auto_type_conversion)
            else:
                # Paginated extraction with specified chunk size
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
                            'type': getattr(dataset, 'data_provider_type', '') or '',
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

    def get_all_dataflows(self, dataset_id_list: list[str] = None) -> pd.DataFrame:
        """
        Fetch dataflows directly from Domo's dataflow API and return a DataFrame
        with columns:
            • Dataflow ID
            • Source Dataset IDs  (comma + newline-separated input datasource IDs)
            • Output Dataset IDs  (comma + newline-separated output datasource IDs)

        Much faster than crawling per-dataset lineage: one paginated ``list``
        call returns every dataflow with its inputs/outputs already attached, so
        the whole instance is covered in a handful of requests instead of one
        per dataset.

        Args:
            dataset_id_list: If provided (non-empty), keep only dataflows that
                output to at least one dataset in this list (preserves the
                previous scope). If None/empty, return every dataflow.
        """
        from domo_utils.api import get_dataflow_api

        logger.info("🔍 Fetching all dataflows from Domo (dataflow API)")
        dataflow_api = get_dataflow_api(self.auth_client)

        # Paginate through every dataflow.
        all_flows = []
        offset, page_size = 0, 50
        while True:
            batch = dataflow_api.list(limit=page_size, offset=offset)
            if not batch:
                break
            all_flows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        logger.info(f"📊 Retrieved {len(all_flows)} dataflows; extracting inputs/outputs...")

        scope = {str(d).strip() for d in (dataset_id_list or []) if str(d).strip()}

        dataflows_tmp = {"Dataflow ID": [], "Source Dataset IDs": [], "Output Dataset IDs": []}
        for flow in all_flows:
            source_ids = [
                inp.datasource_id for inp in (flow.inputs or [])
                if getattr(inp, "datasource_id", None)
            ]
            output_ids = [
                out.datasource_id for out in (flow.outputs or [])
                if getattr(out, "datasource_id", None)
            ]

            # Preserve previous scope: keep a dataflow only if it outputs to one
            # of the requested datasets (when a scope was given).
            if scope and not (set(output_ids) & scope):
                continue

            dataflows_tmp["Dataflow ID"].append(str(flow.id))
            dataflows_tmp["Source Dataset IDs"].append(",\n".join(map(str, source_ids)))
            dataflows_tmp["Output Dataset IDs"].append(",\n".join(map(str, output_ids)))

        dataflows_df = pd.DataFrame(dataflows_tmp)
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


def _condense_sccs(source_map):
    """
    Collapse every cycle in the output→sources graph to a single node (Tarjan's
    strongly-connected-components), so the lineage can be treated as a DAG.

    Domo lets a dataset feed back into its own upstream, so the raw graph can
    contain cycles. Collapsing each strongly connected component to one node
    yields an acyclic "condensation" that depth/ordering algorithms can run on
    without looping forever.

    Returns a dict with:
        nodes        : list of every output id (the keys of source_map)
        scc_id       : {node -> component index}
        scc_members  : {component index -> [member nodes]}
        comp_sources : {component index -> set(predecessor component indices)}  (cross edges only)
        comp_succ    : {component index -> set(successor component indices)}    (cross edges only)
        num_comps    : number of components
    """
    import sys

    nodes = list(source_map)
    # Edges only to nodes that are themselves outputs; datasources are leaves.
    adj = {n: [s for s in source_map[n] if s in source_map] for n in nodes}

    sys.setrecursionlimit(max(10000, len(nodes) * 4 + 1000))

    # --- Tarjan: strongly connected components ---
    index, low, on_stack, stack, scc_id, counter = {}, {}, set(), [], {}, [0]
    # scc_members[i] = list of all nodes in SCC i (component ids assigned in completion order)
    scc_members: dict[int, list] = {}

    def strongconnect(v):
        index[v] = low[v] = counter[0]; counter[0] += 1
        stack.append(v); on_stack.add(v)
        for w in adj[v]:
            if w not in index:
                strongconnect(w); low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            cid = len(scc_members)
            members = []
            while True:
                w = stack.pop(); on_stack.discard(w)
                scc_id[w] = cid
                members.append(w)
                if w == v:
                    break
            scc_members[cid] = members

    for v in nodes:
        if v not in index:
            strongconnect(v)

    num_comps = len(scc_members)

    # --- Cross-component edges only (intra-cycle edges add no layer) ---
    comp_sources = {i: set() for i in range(num_comps)}  # predecessors (upstream)
    comp_succ = {i: set() for i in range(num_comps)}      # successors (dependents)
    for n in nodes:
        ci = scc_id[n]
        for s in adj[n]:
            cs = scc_id[s]
            if cs != ci:
                comp_sources[ci].add(cs)
                comp_succ[cs].add(ci)

    return {
        "nodes": nodes,
        "scc_id": scc_id,
        "scc_members": scc_members,
        "comp_sources": comp_sources,
        "comp_succ": comp_succ,
        "num_comps": num_comps,
    }


def _compute_lineage_depths(source_map):
    """
    Depth = how many dataflow layers feed each dataset, over the output→sources map.

        - a pure datasource (never an output of a dataflow) → 0
        - a dataset whose inputs are all datasources         → 1
        - otherwise → 1 + max(depth of its immediate sources)

    Cycles are collapsed to a single node (see _condense_sccs) and the longest
    path is computed over the acyclic condensation, so every dataset in a cycle
    shares one consistent depth and edges inside the cycle add no layer.

    Returns:
        depths:    {output_dataset_id: depth}
        cycle_map: {output_dataset_id: [other_members]} for nodes in cycles (SCC size > 1).
                   Nodes not in a cycle are absent from this dict.
    """
    scc = _condense_sccs(source_map)
    scc_id, comp_sources, scc_members = scc["scc_id"], scc["comp_sources"], scc["scc_members"]

    # --- Longest path over the acyclic condensation ---
    comp_depth = {}

    def cd(i):
        if i not in comp_depth:
            comp_depth[i] = 1 + max((cd(j) for j in comp_sources[i]), default=0)
        return comp_depth[i]

    depths = {n: cd(scc_id[n]) for n in scc["nodes"]}

    # cycle_map: for each node in a non-trivial SCC, list the *other* members.
    cycle_map: dict[str, list[str]] = {}
    for members in scc_members.values():
        if len(members) > 1:
            for m in members:
                cycle_map[m] = [x for x in members if x != m]

    return depths, cycle_map


def _compute_migration_order(expanded_df, source_map, cost_by_id):
    """
    Assign a 1-based "Migration Order" to every output dataset using
    critical-path-priority list scheduling: migrate the highest-value chain first.

    Priority of each dataset = its *downstream weighted reach* (the classic
    "b-level" in list scheduling): its own Cost plus the costliest chain of
    everything that depends on it. Completing a high-priority dataset unlocks the
    most expensive downstream work, so it is scheduled earliest — which front-loads
    the biggest savings.

    Scheduling rules:
      • Dependencies respected: a dataset is never ordered before any upstream
        dataset it derives from (Kahn topological order).
      • Cycles (SCCs) collapse to one unit and share a single order number.
      • Among datasets *ready* at a step, the one(s) on the costliest downstream
        path go first; cheap dead-end branches are deferred to higher numbers
        even when they could technically run earlier.
      • Datasets ready with the exact same priority share an order number.

    Args:
        expanded_df : DataFrame with at least an "Output Dataset ID" column.
        source_map  : {output_id -> [immediate source ids]} (datasources excluded as leaves).
        cost_by_id  : {output_id -> float cost}; missing/blank treated as 0.

    Returns:
        expanded_df with an int "Migration Order" column added.
    """
    scc = _condense_sccs(source_map)
    scc_id = scc["scc_id"]
    scc_members = scc["scc_members"]
    comp_sources = scc["comp_sources"]   # predecessors (upstream)
    comp_succ = scc["comp_succ"]          # successors (dependents)
    num_comps = scc["num_comps"]

    def _cost(node):
        try:
            return float(str(cost_by_id.get(node, 0) or 0).replace(",", ""))
        except (TypeError, ValueError):
            return 0.0

    # Cost of a component = sum of its members' costs.
    comp_cost = {i: sum(_cost(m) for m in members) for i, members in scc_members.items()}

    # Downstream weighted longest path (b-level) over the acyclic condensation.
    down = {}

    def b_level(i):
        if i not in down:
            down[i] = comp_cost[i] + max((b_level(j) for j in comp_succ[i]), default=0.0)
        return down[i]

    for i in range(num_comps):
        b_level(i)

    # Round so float noise never splits genuine ties (or merges real differences).
    priority = {i: round(down[i], 9) for i in range(num_comps)}

    # Kahn list-scheduling: each step admits the ready component(s) of highest
    # priority (exact ties batched); their dependents unlock for later steps.
    remaining_preds = {i: len(comp_sources[i]) for i in range(num_comps)}
    ready = [i for i in range(num_comps) if remaining_preds[i] == 0]
    order_of_comp = {}
    step = 0

    while ready:
        top = max(priority[i] for i in ready)
        batch = [i for i in ready if priority[i] == top]
        batch_set = set(batch)
        step += 1
        for i in batch:
            order_of_comp[i] = step
        next_ready = [i for i in ready if i not in batch_set]
        for i in batch:
            for j in comp_succ[i]:
                remaining_preds[j] -= 1
                if remaining_preds[j] == 0:
                    next_ready.append(j)
        ready = next_ready

    order_by_id = {n: order_of_comp[scc_id[n]] for n in scc["nodes"]}
    expanded_df["Migration Order"] = (
        expanded_df["Output Dataset ID"].map(order_by_id).fillna(0).astype(int)
    )
    return expanded_df


def _col_a1(idx0: int) -> str:
    """0-based column index → A1 letter (0→A, 25→Z, 26→AA)."""
    s, n = "", idx0
    while True:
        s = chr(ord("A") + n % 26) + s
        n = n // 26 - 1
        if n < 0:
            break
    return s


def _align_columns_to_existing_header(gsheets_client, spreadsheet_id, sheet_name, headers, data_rows):
    """
    Reorder outgoing columns to match the header already present in the sheet,
    so an existing layout is never scrambled by the fixed write order. New
    columns we produce that aren't in the sheet yet are appended at the end
    (schema evolution is allowed).

    Returns (ordered_headers, ordered_rows). If the sheet has no header yet, the
    inputs are returned unchanged. Raises ValueError if the sheet contains a
    column we do NOT produce — clearing+rewriting would drop its data — so the
    caller MUST call this BEFORE clearing to abort without destroying data.
    """
    try:
        existing = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!1:1")
    except Exception:
        existing = []
    existing_header = [str(h).strip() for h in (existing[0] if existing else []) if str(h).strip()]
    if not existing_header:
        return headers, data_rows  # fresh sheet → keep our default order

    new_set, old_set = set(headers), set(existing_header)
    unknown = sorted(old_set - new_set)  # in sheet but not produced → would be lost
    if unknown:
        raise ValueError(
            f"Header mismatch in '{sheet_name}'. The sheet has column(s) this export "
            f"does not produce: {unknown}. Aborting WITHOUT clearing to avoid losing "
            f"their data. Remove those columns or update the export, then retry."
        )

    # Keep the sheet's column order for shared columns, then append any new ones.
    ordered_headers = existing_header + [h for h in headers if h not in old_set]
    index_by_name = {name: i for i, name in enumerate(headers)}
    order = [index_by_name[name] for name in ordered_headers]
    ordered_rows = [[row[i] for i in order] for row in data_rows]
    return ordered_headers, ordered_rows


def _write_owned_columns_by_key(gsheets_client, spreadsheet_id, sheet_name,
                                df, owned, key_col) -> int:
    """
    Write ONLY the ``owned`` columns of ``df`` to ``sheet_name``, leaving every
    other column in the tab untouched (never cleared, never written).

    Existing row order is preserved (keyed by ``key_col``) so unowned columns
    stay aligned with their row; rows whose key is new are appended last. Owned
    columns are written grouped into contiguous A1 runs (one API call per run).

    This is the safe alternative to clearing the whole sheet: a curated column a
    user added (or another tool's column) can never be destroyed by this export.

    Returns the number of data rows written.
    """
    try:
        existing_values = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A1:Z100000")
    except Exception:  # noqa: BLE001
        existing_values = []
    existing_header = [str(h).strip() for h in existing_values[0]] if existing_values else []

    # Preserve the sheet's existing row order (by key); append brand-new keys last.
    existing_order: list[str] = []
    if existing_header and key_col in existing_header:
        k_idx = existing_header.index(key_col)
        for r in existing_values[1:]:
            if len(r) > k_idx and str(r[k_idx]).strip():
                existing_order.append(str(r[k_idx]).strip())

    new_ids = df[key_col].astype(str).str.strip().tolist()
    new_set, existing_set = set(new_ids), set(existing_order)
    ordered_ids = [i for i in existing_order if i in new_set] + [i for i in new_ids if i not in existing_set]
    removed = [i for i in existing_order if i not in new_set]
    added = [i for i in new_ids if i not in existing_set]
    if existing_order and (removed or added):
        preserved_now = [h for h in existing_header if h not in owned]
        logger.warning(
            f"⚠️  Row set changed since last write (+{len(added)} / -{len(removed)}). "
            f"Unowned columns ({preserved_now or '(none)'}) are kept by row position, so rows "
            f"at/after the first change may no longer line up — review them after this run."
        )
    df = (
        df.assign(_key=df[key_col].astype(str).str.strip())
        .set_index("_key").reindex(ordered_ids).reset_index(drop=True)
    )

    # Owned columns keep their current positions; new owned columns append at end.
    final_header = list(existing_header)
    for c in owned:
        if c not in final_header:
            final_header.append(c)
    owned_positions = [i for i, h in enumerate(final_header) if h in owned]

    runs: list[list[int]] = []
    for idx in owned_positions:
        if runs and idx == runs[-1][-1] + 1:
            runs[-1].append(idx)
        else:
            runs.append([idx])

    n_rows = len(df)
    preserved = [h for h in final_header if h not in owned]
    logger.info(f"📝 Writing {n_rows} rows across {len(owned_positions)} owned column(s); "
                f"preserving untouched: {preserved or '(none)'}")

    if not existing_values:
        logger.info(f"📄 Sheet '{sheet_name}' doesn't exist, creating it...")
        try:
            gsheets_client.create_sheet(spreadsheet_id, sheet_name)
        except Exception:  # noqa: BLE001
            pass

    for run in runs:
        start, end = run[0], run[-1]
        cols = [final_header[i] for i in run]
        block = [cols]
        for _, row in df.iterrows():
            block.append(["" if pd.isna(row.get(c)) else str(row.get(c, "")) for c in cols])
        gsheets_client.clear_range(
            spreadsheet_id, f"{sheet_name}!{_col_a1(start)}1:{_col_a1(end)}100000")
        gsheets_client.write_range(
            spreadsheet_id, f"{sheet_name}!{_col_a1(start)}1", block)
    return n_rows


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
        
        # Columns this export OWNS. Anything else in the tab (e.g. '# Cards',
        # QA columns) is left completely untouched — never cleared, never written.
        OWNED = ['Dataset ID', 'Name', 'Type', 'Description', 'Created',
                 'Last Updated', 'Row Count', 'Column Count', 'Owner']
        records = []
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

            records.append({
                'Dataset ID': str(dataset['id']),
                'Name': str(dataset['name']),
                'Type': str(dataset.get('type', '')),
                'Description': str(dataset['description']),
                'Created': str(created_date),
                'Last Updated': str(last_updated),
                'Row Count': int(dataset['row_count']),
                'Column Count': int(dataset['column_count']),
                'Owner': str(dataset['owner']),
            })
        datasets_df = pd.DataFrame(records, columns=OWNED)

        # Write ONLY the owned columns, preserving any other column (e.g. '# Cards')
        # and the existing row order keyed by Dataset ID.
        n = _write_owned_columns_by_key(
            gsheets_client, spreadsheet_id, sheet_name, datasets_df, OWNED, 'Dataset ID')

        logger.info(f"✅ Successfully exported {n} datasets to {sheet_name}")
        logger.info(f"📊 Owned columns: {', '.join(OWNED)}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to export datasets to spreadsheet: {e}")
        return False


def export_dataflows_to_spreadsheet(spreadsheet_id: str, sheet_name: str = None,
                                    credentials_path: str = None,
                                    datasets_sheet_name: str = None) -> bool:
    """
    Build the dataflow lineage table and write it to Google Sheets.

    Reads the dataset IDs from the "All Datasets" tab, crawls Domo lineage for
    each one, then writes one row per Output Dataset ID to the "All Dataflows"
    tab with the columns:
        • Output Dataset ID
        • Dataflow ID
        • Source Dataset IDs       (immediate sources)
        • All Source Dataset IDs   (full recursive lineage)

    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Destination tab (default: ALL_DATAFLOWS_SHEET_NAME env or "All Dataflows")
        credentials_path (str): Path to Google Sheets credentials file
        datasets_sheet_name (str): Source tab with dataset IDs (default: DATASETS_SHEET_NAME env or "All Datasets")

    Returns:
        bool: True if export successful, False otherwise
    """
    if sheet_name is None:
        sheet_name = os.getenv("ALL_DATAFLOWS_SHEET_NAME", "All Dataflows")
    if datasets_sheet_name is None:
        datasets_sheet_name = os.getenv("DATASETS_SHEET_NAME", "All Datasets")

    if not credentials_path:
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_path:
        logger.error("❌ No Google Sheets credentials provided")
        return False
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Google Sheets credentials file not found: {credentials_path}")
        return False

    try:
        # Imported here to avoid circular imports.
        from .gsheets import GoogleSheets, READ_WRITE_SCOPES
        from .lineage import collect_all_sources, parse_sources

        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)

        # 1) Read the seed dataset IDs from the "All Datasets" tab.
        logger.info(f"📖 Reading dataset IDs from '{datasets_sheet_name}' tab...")
        datasets_df = gsheets_client.read_to_dataframe(
            spreadsheet_id=spreadsheet_id,
            range_name=f"{datasets_sheet_name}!A:Z",
            header=True,
        )
        if datasets_df is None or len(datasets_df) == 0:
            logger.error(f"❌ No data found in '{datasets_sheet_name}' tab")
            return False

        id_column = next((c for c in ["Dataset ID", "dataset_id", "DatasetID", "ID", "id"]
                          if c in datasets_df.columns), None)
        if not id_column:
            logger.error(f"❌ 'Dataset ID' column not found in '{datasets_sheet_name}' tab")
            logger.info(f"📋 Available columns: {list(datasets_df.columns)}")
            return False

        dataset_ids = [str(v).strip() for v in datasets_df[id_column].dropna().tolist() if str(v).strip()]
        if not dataset_ids:
            logger.error("❌ No dataset IDs to crawl")
            return False
        logger.info(f"📊 Crawling lineage for {len(dataset_ids)} datasets...")

        # 2) Crawl Domo lineage.
        domo_handler = DomoHandler()
        if not domo_handler.setup_auth():
            logger.error("❌ Failed to authenticate with Domo")
            return False

        dataflows_df = domo_handler.get_all_dataflows(dataset_ids)
        if dataflows_df is None or dataflows_df.empty:
            logger.warning("⚠️  No dataflows found for the given datasets")
            return False

        # 3) Expand to one row per Output Dataset ID (dedup the source IDs).
        expanded_rows = []
        for _, row in dataflows_df.iterrows():
            dataflow_id = row["Dataflow ID"]
            source_ids = sorted(set(parse_sources(row.get("Source Dataset IDs"))))
            source_ids_str = ",\n".join(source_ids)
            for output_id in dict.fromkeys(parse_sources(row.get("Output Dataset IDs"))):
                expanded_rows.append({
                    "Output Dataset ID": output_id,
                    "Dataflow ID": dataflow_id,
                    "Source Dataset IDs": source_ids_str,
                })

        if not expanded_rows:
            logger.warning("⚠️  No output datasets resolved from dataflows")
            return False

        expanded_df = pd.DataFrame(expanded_rows)

        # 4) Compute the full recursive lineage per Output Dataset ID. A dataset
        #    can be produced by more than one dataflow, so union their sources.
        source_map: dict[str, list[str]] = {}
        for _, row in expanded_df.iterrows():
            out_id = row["Output Dataset ID"]
            bucket = source_map.setdefault(out_id, [])
            for src in parse_sources(row["Source Dataset IDs"]):
                if src not in bucket:
                    bucket.append(src)

        expanded_df["All Source Dataset IDs"] = (
            expanded_df["Output Dataset ID"]
            .apply(lambda out: collect_all_sources(out, source_map))
            .apply(lambda lst: ",\n".join(lst))
        )

        # Depth = number of dataflow layers feeding each output dataset.
        depths, cycle_map = _compute_lineage_depths(source_map)
        expanded_df["Depth"] = expanded_df["Output Dataset ID"].map(depths).fillna(0).astype(int)

        # Notes column — surface cycle membership and other useful diagnostics.
        def _build_note(row):
            out_id = row["Output Dataset ID"]
            notes = []

            if out_id in cycle_map:
                partners = cycle_map[out_id]
                partner_list = ", ".join(partners)
                notes.append(
                    f"CYCLE ({len(partners) + 1} nodes): this dataset is part of a "
                    f"circular dependency with {partner_list}. "
                    f"Depth is estimated via SCC condensation (not a strict layer count)."
                )

            depth = int(row["Depth"])
            direct_sources = [s for s in parse_sources(row["Source Dataset IDs"]) if s]
            all_sources = [s for s in parse_sources(row["All Source Dataset IDs"]) if s]
            transitive = [s for s in all_sources if s not in direct_sources]

            if depth >= 5:
                notes.append(
                    f"DEEP CHAIN (depth {depth}): {len(all_sources)} total upstream datasets "
                    f"({len(direct_sources)} direct, {len(transitive)} transitive)."
                )
            elif transitive:
                notes.append(
                    f"{len(all_sources)} total upstream datasets "
                    f"({len(direct_sources)} direct, {len(transitive)} transitive)."
                )

            return " | ".join(notes)

        expanded_df["Notes"] = expanded_df.apply(_build_note, axis=1)

        # Columns this export OWNS. Anything else in the tab (Cost, or any column
        # you add) is left completely untouched — never cleared, never written.
        OWNED = ["Output Dataset ID", "Dataflow ID", "Source Dataset IDs",
                 "All Source Dataset IDs", "Depth", "Notes", "Migration Order"]

        # Read the current sheet once: its header, its row order (to keep
        # untouched columns aligned), and the Cost values (read-only, to compute
        # the migration order — we do NOT write Cost back).
        try:
            existing_values = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A1:Z100000")
        except Exception:
            existing_values = []
        existing_header = [str(h).strip() for h in existing_values[0]] if existing_values else []

        cost_by_id: dict[str, str] = {}
        existing_order: list[str] = []
        if existing_header and "Output Dataset ID" in existing_header:
            oid_idx = existing_header.index("Output Dataset ID")
            cost_idx = existing_header.index("Cost") if "Cost" in existing_header else None
            for r in existing_values[1:]:
                if len(r) <= oid_idx or not str(r[oid_idx]).strip():
                    continue
                oid = str(r[oid_idx]).strip()
                existing_order.append(oid)
                if cost_idx is not None and len(r) > cost_idx:
                    cost_by_id[oid] = str(r[cost_idx]).strip()
        logger.info(f"💲 Read {len(cost_by_id)} Cost value(s) for ordering — the Cost column will NOT be modified")

        # Migration Order = critical-path-priority schedule (highest-cost chain first).
        expanded_df = _compute_migration_order(expanded_df, source_map, cost_by_id)

        # Keep the sheet's existing row order so untouched columns (e.g. Cost)
        # stay aligned with their dataset; brand-new datasets are appended last.
        new_ids = expanded_df["Output Dataset ID"].astype(str).str.strip().tolist()
        new_set, existing_set = set(new_ids), set(existing_order)
        ordered_ids = [i for i in existing_order if i in new_set] + [i for i in new_ids if i not in existing_set]
        removed = [i for i in existing_order if i not in new_set]
        added = [i for i in new_ids if i not in existing_set]
        if existing_order and (removed or added):
            logger.warning(
                f"⚠️  Dataset set changed since last write (+{len(added)} / -{len(removed)}). "
                f"Untouched columns like 'Cost' are kept by row position, so rows at/after the "
                f"first change may no longer line up — review the 'Cost' column after this run."
            )
        expanded_df = (
            expanded_df.assign(_oid=expanded_df["Output Dataset ID"].astype(str).str.strip())
            .set_index("_oid").reindex(ordered_ids).reset_index(drop=True)
        )

        # 5) Write ONLY the owned columns, each into its current position, so the
        #    Cost column (and any other) is preserved byte-for-byte.
        final_header = list(existing_header)
        for c in OWNED:
            if c not in final_header:
                final_header.append(c)
        owned_positions = [i for i, h in enumerate(final_header) if h in OWNED]

        # Group owned column indices into contiguous runs (one write per run).
        runs: list[list[int]] = []
        for idx in owned_positions:
            if runs and idx == runs[-1][-1] + 1:
                runs[-1].append(idx)
            else:
                runs.append([idx])

        n_rows = len(expanded_df)
        preserved = [h for h in final_header if h not in OWNED]
        logger.info(f"📝 Writing {n_rows} rows across {len(owned_positions)} owned column(s); "
                    f"preserving untouched: {preserved or '(none)'}")

        if not existing_values:
            logger.info(f"📄 Sheet '{sheet_name}' doesn't exist, creating it...")
            try:
                gsheets_client.create_sheet(spreadsheet_id, sheet_name)
            except Exception:
                pass

        for run in runs:
            start, end = run[0], run[-1]
            cols = [final_header[i] for i in run]
            block = [cols]
            for _, row in expanded_df.iterrows():
                block.append(["" if pd.isna(row.get(c)) else str(row.get(c, "")) for c in cols])
            gsheets_client.clear_range(
                spreadsheet_id, f"{sheet_name}!{_col_a1(start)}1:{_col_a1(end)}100000")
            gsheets_client.write_range(
                spreadsheet_id, f"{sheet_name}!{_col_a1(start)}1", block)

        logger.info(f"✅ Exported {n_rows} dataflow rows to '{sheet_name}' (Cost column left untouched)")
        logger.info(f"📊 Owned columns: {', '.join(c for c in final_header if c in OWNED)}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to export dataflows to spreadsheet: {e}")
        return False


def _count_cards_per_dataset(auth_client, page_size: int = 2000) -> dict:
    """
    Count how many Domo cards reference each dataset.

    Uses the unified search API, which returns every card with its
    ``dataSourceIds`` attached — so the whole instance is covered in a handful
    of paginated calls instead of one request per dataset.

    Returns:
        dict mapping dataset_id -> card count (datasets with no cards are absent).
    """
    from collections import Counter
    from domo_utils.api import get_search_api
    from domo_utils.models.search import EntityType

    search_api = get_search_api(auth_client)
    counts: "Counter[str]" = Counter()
    offset, total = 0, 0
    while True:
        resp = search_api.search(entities=[EntityType.CARD], limit=page_size, offset=offset)
        objs = resp.model_dump().get("search_objects") or []
        if not objs:
            break
        for o in objs:
            total += 1
            for dsid in (o.get("dataSourceIds") or []):
                if dsid:
                    counts[str(dsid)] += 1
        offset += len(objs)
        if len(objs) < page_size:
            break
    logger.info("🃏 Scanned %s cards across %s dataset(s) with at least one card",
                total, len(counts))
    return dict(counts)


def count_cards_to_spreadsheet(spreadsheet_id: str, sheet_name: str = None,
                               credentials_path: str = None) -> bool:
    """
    Count Domo cards per dataset and write a "# Cards" column to the datasets tab.

    Reads the dataset IDs from the datasets tab, counts cards per dataset via the
    Domo search API, then writes ONLY the "# Cards" column (creating it at the end
    of the header if absent). Every other column is left untouched and row order
    is preserved, so the count stays aligned with each dataset.

    Args:
        spreadsheet_id (str): Google Sheets spreadsheet ID
        sheet_name (str): Datasets tab (default: DATASETS_SHEET_NAME env or "All Datasets")
        credentials_path (str): Path to Google Sheets credentials file

    Returns:
        bool: True if successful, False otherwise
    """
    if sheet_name is None:
        sheet_name = os.getenv("DATASETS_SHEET_NAME", "All Datasets")
    if not credentials_path:
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_path or not os.path.exists(credentials_path):
        logger.error("❌ Google Sheets credentials file not found: %s", credentials_path)
        return False

    try:
        from .gsheets import GoogleSheets, READ_WRITE_SCOPES

        # 1) Authenticate to Domo and count cards per dataset.
        handler = DomoHandler()
        if not handler.setup_auth():
            logger.error("❌ Failed to authenticate with Domo")
            return False
        counts = _count_cards_per_dataset(handler.auth_client)

        # 2) Read the datasets tab and locate the Dataset ID column.
        gsheets_client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        rows = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A1:Z100000")
        if not rows:
            logger.error("❌ No data found in '%s' tab", sheet_name)
            return False
        header = [str(h).strip() for h in rows[0]]
        oid_idx = next((header.index(c) for c in ["Dataset ID", "dataset_id", "DatasetID", "ID", "id"]
                        if c in header), None)
        if oid_idx is None:
            logger.error("❌ 'Dataset ID' column not found in '%s' tab. Columns: %s",
                         sheet_name, header)
            return False

        # 3) Locate (or append) the "# Cards" column.
        if "# Cards" in header:
            cards_idx = header.index("# Cards")
        else:
            cards_idx = len(header)
            logger.info("➕ '# Cards' column not present; creating it at column %s",
                        _col_a1(cards_idx))
        cards_col = _col_a1(cards_idx)

        # 4) Build the single column in existing row order (count, or 0 if none).
        column = [["# Cards"]]
        matched = 0
        for r in rows[1:]:
            oid = str(r[oid_idx]).strip() if len(r) > oid_idx else ""
            if oid:
                column.append([counts.get(oid, 0)])
                matched += 1
            else:
                column.append([""])

        # 5) Write ONLY that column; everything else stays byte-for-byte.
        logger.info("📝 Writing '# Cards' to column %s for %s dataset(s); "
                    "all other columns left untouched", cards_col, matched)
        gsheets_client.clear_range(spreadsheet_id, f"{sheet_name}!{cards_col}1:{cards_col}100000")
        gsheets_client.write_range(spreadsheet_id, f"{sheet_name}!{cards_col}1", column)
        logger.info("✅ Card counts written to '%s' (column %s)", sheet_name, cards_col)
        return True

    except Exception as e:  # noqa: BLE001
        logger.error("❌ Failed to count cards to spreadsheet: %s", e)
        return False