"""
Dataset comparison utilities for validating data migration between Domo and Snowflake.

This module provides the DatasetComparator class which uses datacompy to perform
comprehensive comparisons between Domo datasets and Snowflake tables.
"""

import os
import re
import logging
import math
import random
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import datacompy
from datetime import datetime

from .utils.common import transform_column_name, get_snowflake_table_full_name, setup_dual_connections, get_env_config
from .utils.domo import DomoHandler
from .utils.snowflake import SnowflakeHandler
from .utils.gsheets import GoogleSheets, READ_WRITE_SCOPES


def _escape_domo_column_name(column_name: str) -> str:
    """
    Escapa nombres de columnas para consultas SQL de Domo.
    
    Args:
        column_name: Nombre original de la columna
        
    Returns:
        Nombre de columna escapado para SQL de Domo
        
    Examples:
        >>> _escape_domo_column_name("Site Code")
        '"Site Code"'
        >>> _escape_domo_column_name("site_code")
        'site_code'
        >>> _escape_domo_column_name("Total Revenue")
        '"Total Revenue"'
    """
    # Si el nombre tiene espacios o caracteres especiales, envolver en comillas dobles
    if re.search(r'[^a-zA-Z0-9_]', column_name):
        return f'"{column_name}"'
    return column_name

def _escape_domo_column_list(column_names: List[str]) -> str:
    """
    Escapa una lista de nombres de columnas para consultas SQL de Domo.
    
    Args:
        column_names: Lista de nombres de columnas
        
    Returns:
        String con nombres de columnas escapados, separados por comas
        
    Examples:
        >>> _escape_domo_column_list(["Site Code", "Total Revenue"])
        '"Site Code", "Total Revenue"'
    """
    escaped_columns = [_escape_domo_column_name(col) for col in column_names]
    return ', '.join(escaped_columns)

def _normalize_snowflake_column_list(column_names: List[str]) -> str:
    """
    Normaliza una lista de nombres de columnas para consultas SQL de Snowflake.
    
    Args:
        column_names: Lista de nombres de columnas
        
    Returns:
        String con nombres de columnas normalizados, separados por comas
        
    Examples:
        >>> _normalize_snowflake_column_list(["Site Code", "Total Revenue"])
        'SITE_CODE, TOTAL_REVENUE'
    """
    normalized_columns = [transform_column_name(col) for col in column_names]
    return ', '.join(normalized_columns)


class DatasetComparator:
    """Main class to compare Domo datasets with Snowflake tables using datacompy."""
    
    def __init__(self):
        """Initialize the comparator with handlers for Domo and Snowflake."""
        # Import handlers here to avoid circular imports
        from .utils.domo import DomoHandler
        from .utils.snowflake import SnowflakeHandler
        
        self.domo_handler = DomoHandler()
        self.snowflake_handler = SnowflakeHandler()
        self.errors = []
        self._domo_connected = False
        self._snowflake_connected = False
        self.logger = logging.getLogger("DatasetComparator")
    
    def setup_connections(self) -> bool:
        """Setup connections to both Domo and Snowflake."""
        success, domo_handler, snowflake_handler = setup_dual_connections(
            self.domo_handler, self.snowflake_handler
        )
        
        if success:
            self.domo_handler = domo_handler
            self.snowflake_handler = snowflake_handler
            self._domo_connected = True
            self._snowflake_connected = True
        
        return success
    
    def add_error(self, section: str, error: str, details: str = ""):
        """Add error to the error list."""
        self.errors.append({
            'section': section,
            'error': error,
            'details': details
        })
        self.logger.error(f"Error in {section}: {error}")
        if details:
            self.logger.error(f"Details: {details}")
    
    def calculate_sample_size(self, total_rows: int, confidence_level: float = 0.95, 
                            margin_of_error: float = 0.05) -> int:
        """
        Calculate statistically significant sample size.
        
        Args:
            total_rows: Total number of rows in the dataset
            confidence_level: Statistical confidence level (default 95%)
            margin_of_error: Acceptable margin of error (default 5%)
        
        Returns:
            Recommended sample size
        """
        if total_rows <= 1000:
            return min(total_rows, 1000)
        
        # Statistical formula for finite population
        z_score = 1.96  # For 95% confidence
        p = 0.5  # Most conservative proportion
        
        numerator = (z_score ** 2) * p * (1 - p)
        denominator = (margin_of_error ** 2)
        
        sample_size = numerator / denominator
        sample_size = sample_size / (1 + (sample_size - 1) / total_rows)
        
        return min(int(math.ceil(sample_size)), total_rows)
    
    def compare_schemas(self, domo_dataset_id: str, snowflake_table: str, 
                       transform_names: bool = False) -> Dict[str, Any]:
        """
        Compare schemas between Domo and Snowflake.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            transform_names: Whether to apply column name transformation
            
        Returns:
            Dictionary with schema comparison results
        """
        self.logger.info("📋 Comparing schemas...")
        
        try:
            # Get Domo schema using DomoHandler
            domo_schema = self.domo_handler.get_dataset_schema(domo_dataset_id)
            self.logger.info(f"🔍 Raw Domo schema response: {domo_schema}")
            
            # Check if schema has columns
            if 'columns' not in domo_schema:
                self.logger.warning(f"⚠️ No 'columns' key in Domo schema. Available keys: {list(domo_schema.keys())}")
                # Try alternative approach - extract columns from actual data
                try:
                    sample_df = self.domo_handler.extract_data(
                        dataset_id=domo_dataset_id, 
                        query="SELECT * FROM table LIMIT 1",
                        enable_auto_type_conversion=True
                    )
                    if sample_df is not None and not sample_df.empty:
                        # Create schema from DataFrame columns
                        domo_columns = {col: 'UNKNOWN' for col in sample_df.columns}
                        self.logger.info(f"✅ Extracted schema from data sample: {len(domo_columns)} columns")
                    else:
                        raise Exception("No data available to infer schema")
                except Exception as fallback_error:
                    self.logger.error(f"❌ Fallback schema extraction failed: {fallback_error}")
                    raise Exception(f"Cannot get Domo schema: no 'columns' in response and data extraction failed")
            else:
                domo_columns = {col['name']: col['type'] for col in domo_schema['columns']}
                self.logger.info(f"✅ Extracted {len(domo_columns)} columns from Domo schema")
            
            # Apply column name transformation if enabled
            if transform_names:
                self.logger.info("🔄 Applying column name transformation to Domo columns...")
                self.logger.info(f"🔍 Original Domo columns: {list(domo_columns.keys())}")
                transformed_domo_columns = {}
                # Store the mapping for reverse lookup
                self._domo_original_columns = {}
                for original_name, col_type in domo_columns.items():
                    transformed_name = transform_column_name(original_name)
                    transformed_domo_columns[transformed_name] = col_type
                    # Store mapping: transformed_name -> original_name (for reverse lookup)
                    self._domo_original_columns[transformed_name] = original_name
                    self.logger.debug(f"  '{original_name}' → '{transformed_name}'")
                domo_columns = transformed_domo_columns
                self.logger.info(f"✅ Domo columns transformed: {len(domo_columns)} columns")
                self.logger.info(f"🔍 Transformed Domo columns: {list(domo_columns.keys())}")
                self.logger.info(f"📋 Stored reverse mapping for {len(self._domo_original_columns)} columns")
                self.logger.info(f"🔍 Mapping example: 'SITE_CODE' → '{self._domo_original_columns.get('SITE_CODE', 'NOT_FOUND')}'")
            else:
                self.logger.info("⚠️ Column name transformation DISABLED for Domo")
                # Even without transformation, store the mapping for consistency
                self._domo_original_columns = {col: col for col in domo_columns.keys()}
                
        except Exception as e:
            self.add_error("Domo Schema", "Could not get Domo schema", str(e))
            return self._get_error_schema_result()
        
        try:
            # Get Snowflake schema
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {get_env_config().get('SNOWFLAKE_DATABASE')}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{snowflake_table.upper()}'
            AND TABLE_SCHEMA = '{get_env_config().get('SNOWFLAKE_SCHEMA').upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            sf_result = self.snowflake_handler.execute_query(query)
            if sf_result is not None:
                sf_schema_df = sf_result  # Already pandas DataFrame
                sf_columns = {row['COLUMN_NAME']: row['DATA_TYPE'] for _, row in sf_schema_df.iterrows()}
                
                # Apply column name transformation to Snowflake columns if enabled
                if transform_names:
                    self.logger.info("🔄 Applying column name transformation to Snowflake columns...")
                    self.logger.info(f"🔍 Original Snowflake columns: {list(sf_columns.keys())}")
                    transformed_sf_columns = {}
                    for original_name, col_type in sf_columns.items():
                        transformed_name = transform_column_name(original_name)
                        transformed_sf_columns[transformed_name] = col_type
                        self.logger.debug(f"  '{original_name}' → '{transformed_name}'")
                    sf_columns = transformed_sf_columns
                    self.logger.info(f"✅ Snowflake columns transformed: {len(sf_columns)} columns")
                    self.logger.info(f"🔍 Transformed Snowflake columns: {list(sf_columns.keys())}")
                else:
                    self.logger.info("⚠️ Column name transformation DISABLED for Snowflake")
            else:
                raise Exception("Failed to get Snowflake schema")
                
        except Exception as e:
            self.add_error("Snowflake Schema", "Could not get Snowflake schema", str(e))
            return self._get_error_schema_result(len(domo_columns))
        
        # Compare columns
        domo_cols_set = set(domo_columns.keys())
        sf_cols_set = set(sf_columns.keys())
        
        self.logger.info(f"🔍 Final column sets for comparison:")
        self.logger.info(f"  Domo columns ({len(domo_cols_set)}): {sorted(list(domo_cols_set))}")
        self.logger.info(f"  Snowflake columns ({len(sf_cols_set)}): {sorted(list(sf_cols_set))}")
        
        missing_in_sf = list(domo_cols_set - sf_cols_set)
        extra_in_sf = list(sf_cols_set - domo_cols_set)
        common_cols = domo_cols_set & sf_cols_set
        
        self.logger.info(f"📊 Comparison results:")
        self.logger.info(f"  Common columns: {len(common_cols)}")
        self.logger.info(f"  Missing in Snowflake: {len(missing_in_sf)}")
        self.logger.info(f"  Extra in Snowflake: {len(extra_in_sf)}")
        if missing_in_sf:
            self.logger.info(f"  Missing columns: {missing_in_sf}")
        if extra_in_sf:
            self.logger.info(f"  Extra columns: {extra_in_sf}")
        
        # Check type compatibility for common columns
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
        
        schema_match = (len(missing_in_sf) == 0 and len(extra_in_sf) == 0 and 
                       len(type_mismatches) == 0)
        
        return {
            'domo_columns': len(domo_columns),
            'snowflake_columns': len(sf_columns),
            'missing_in_snowflake': missing_in_sf,
            'extra_in_snowflake': extra_in_sf,
            'common_columns': len(common_cols),
            'type_mismatches': type_mismatches,
            'schema_match': schema_match,
            'transform_applied': transform_names
        }
    
    def _get_error_schema_result(self, domo_columns: int = 0) -> Dict[str, Any]:
        """Get error result for schema comparison."""
        return {
            'domo_columns': domo_columns,
            'snowflake_columns': 0,
            'missing_in_snowflake': [],
            'extra_in_snowflake': [],
            'common_columns': 0,
            'type_mismatches': [],
            'schema_match': False,
            'error': True
        }
    
    def _types_compatible(self, domo_type: str, sf_type: str) -> bool:
        """Check if data types are compatible between Domo and Snowflake."""
        type_mapping = {
            'STRING': ['VARCHAR', 'STRING', 'TEXT', 'CHAR'],
            'LONG': ['NUMBER', 'INTEGER', 'BIGINT', 'INT'],
            'DOUBLE': ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'],
            'DATETIME': ['TIMESTAMP', 'DATETIME', 'DATE']
        }
        
        for domo_key, sf_types in type_mapping.items():
            if domo_type == domo_key:
                return any(sf_type.upper().startswith(sf) for sf in sf_types)
        
        return True  # Assume compatibility by default
    
    def compare_row_counts(self, domo_dataset_id: str, snowflake_table: str) -> Dict[str, Any]:
        """Compare row counts between Domo and Snowflake."""
        self.logger.info("📊 Comparing row counts...")
        
        # Get Domo count using DomoHandler for simple queries
        try:
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo_handler.query_dataset(domo_dataset_id, domo_count_query)
            domo_count = domo_result['rows'][0][0] if domo_result['rows'] else 0
        except Exception as e:
            self.add_error("Domo Count", "Could not get row count from Domo", str(e))
            domo_count = 0
        
        # Get Snowflake count
        try:
            sf_count_query = f"SELECT COUNT(*) as row_count FROM {snowflake_table}"
            sf_result = self.snowflake_handler.execute_query(sf_count_query)
            if sf_result is not None:
                sf_count = int(sf_result.iloc[0]['ROW_COUNT'])  # Already pandas DataFrame
            else:
                raise Exception("Failed to get Snowflake count")
        except Exception as e:
            self.add_error("Snowflake Count", "Could not get row count from Snowflake", str(e))
            sf_count = 0
        
        # Analyze if difference is negligible
        negligible_analysis = self._analyze_row_count_difference(domo_count, sf_count)
        
        return {
            'domo_rows': domo_count,
            'snowflake_rows': sf_count,
            'difference': sf_count - domo_count,
            'match': domo_count == sf_count,
            'negligible_analysis': negligible_analysis
        }
    
    def _analyze_row_count_difference(self, domo_count: int, snowflake_count: int) -> Dict[str, Any]:
        """Determine if the difference in row counts is statistically negligible."""
        if domo_count == 0 and snowflake_count == 0:
            return {'is_negligible': True, 'reason': 'Both datasets are empty', 'percentage': 0.0}
        
        if domo_count == 0 or snowflake_count == 0:
            return {
                'is_negligible': False,
                'reason': 'One dataset is empty',
                'percentage': 100.0
            }
        
        difference = abs(snowflake_count - domo_count)
        percentage = (difference / max(domo_count, snowflake_count)) * 100
        
        # Determine negligible thresholds
        if difference <= 10:
            return {
                'is_negligible': True,
                'reason': f'Very small absolute difference ({difference} rows)',
                'percentage': percentage
            }
        elif percentage <= 0.1:
            return {
                'is_negligible': True,
                'reason': f'Very small percentage difference ({percentage:.3f}%)',
                'percentage': percentage
            }
        elif percentage <= 1.0 and max(domo_count, snowflake_count) >= 10000:
            return {
                'is_negligible': True,
                'reason': f'Small percentage difference for large dataset ({percentage:.3f}%)',
                'percentage': percentage
            }
        else:
            return {
                'is_negligible': False,
                'reason': f'Significant difference: {difference} rows ({percentage:.3f}%)',
                'percentage': percentage
            }
    
    def compare_data_samples(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: List[str], sample_size: Optional[int] = None, 
                           transform_names: bool = False, schema_comparison: Dict[str, Any] = None, 
                           sampling_method: str = "random") -> Dict[str, Any]:
        """
        Compare data samples using datacompy.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample (auto-calculated if None)
            transform_names: Whether to apply column name transformation
            schema_comparison: Schema comparison results to include in report
            sampling_method: Sampling method ('random' for smart random with fallback, 'ordered' for direct ordered)
            
        Returns:
            Dictionary with data comparison results
        """
        self.logger.info("🔍 Comparing data samples...")
        
        # Always normalize key columns for comparison (regardless of transform_names setting)
        # This ensures compatibility between Domo and Snowflake column names
        normalized_key_columns = [transform_column_name(col) for col in key_columns]
        self.logger.info(f"🔄 Key columns normalized for comparison: {key_columns} → {normalized_key_columns}")
        
        # Create reverse mapping from normalized names back to original Domo names
        # This is needed because Domo queries need original column names
        domo_column_mapping = {}
        if hasattr(self, '_domo_original_columns') and self._domo_original_columns:
            # _domo_original_columns already has the correct mapping: {transformed_name: original_name}
            domo_column_mapping = self._domo_original_columns.copy()
            self.logger.info(f"🔍 Created domo_column_mapping with {len(domo_column_mapping)} entries")
            self.logger.info(f"🔍 Example mapping: 'SITE_CODE' → '{domo_column_mapping.get('SITE_CODE', 'NOT_FOUND')}'")
        
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        self.logger.info(f"🔍 Debugging column mapping:")
        self.logger.info(f"  Normalized key columns: {normalized_key_columns}")
        self.logger.info(f"  Domo column mapping available: {hasattr(self, '_domo_original_columns')}")
        if hasattr(self, '_domo_original_columns'):
            self.logger.info(f"  _domo_original_columns keys: {list(self._domo_original_columns.keys())}")
            self.logger.info(f"  _domo_original_columns values: {list(self._domo_original_columns.values())}")
        
        for normalized_key in normalized_key_columns:
            self.logger.info(f"🔍 Looking for '{normalized_key}' in domo_column_mapping...")
            self.logger.info(f"  Available keys: {list(domo_column_mapping.keys())}")
            if normalized_key in domo_column_mapping:
                domo_key_columns.append(domo_column_mapping[normalized_key])
                self.logger.info(f"  ✅ '{normalized_key}' → '{domo_column_mapping[normalized_key]}'")
            else:
                # Fallback: assume the key column name is already in Domo format
                domo_key_columns.append(normalized_key)
                self.logger.info(f"  ⚠️ '{normalized_key}' → '{normalized_key}' (no mapping found)")
        
        self.logger.info(f"🔄 Key columns mapped back to Domo format: {normalized_key_columns} → {domo_key_columns}")
        
        # Get total count and calculate sample size if needed
        try:
            # Use DomoHandler for count query
            domo_count_query = "SELECT COUNT(*) as row_count FROM table"
            domo_result = self.domo_handler.query_dataset(domo_dataset_id, domo_count_query)
            total_domo_rows = domo_result['rows'][0][0] if domo_result['rows'] else 0
            
            if sample_size is None:
                sample_size = self.calculate_sample_size(total_domo_rows)
                
            self.logger.info(f"Total rows: {total_domo_rows:,}, Sample size: {sample_size:,}")
            
        except Exception as e:
            self.add_error("Sample Size Calculation", "Could not get total row count", str(e))
            sample_size = sample_size or 1000
        
        # Get synchronized samples based on chosen sampling method
        if sampling_method == "ordered":
            # Direct ordered sampling - skip random sampling entirely
            self.logger.info("🎯 Using direct ordered sampling (no random attempt)")
            actual_sampling_method = "Ordered Sampling"
            try:
                # Get Domo sample using ordered method
                key_cols_str = _escape_domo_column_list(key_columns)
                sample_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
                
                domo_df = self.domo_handler.extract_data(
                    dataset_id=domo_dataset_id, 
                    query=sample_query,
                    chunk_size=999999999,  # Force single chunk to avoid pagination issues
                    enable_auto_type_conversion=True
                )
                
                if domo_df is None or domo_df.empty:
                    raise Exception("No data returned from Domo")
                
                # Get Snowflake sample using ordered method  
                sf_key_cols_str = _normalize_snowflake_column_list(key_columns)
                sf_query = f"SELECT * FROM {snowflake_table} ORDER BY {sf_key_cols_str} LIMIT {sample_size}"
                sf_df = self.snowflake_handler.execute_query(sf_query)
                
                if sf_df is None or sf_df.empty:
                    raise Exception("No data returned from Snowflake")
                    
            except Exception as ordered_error:
                self.add_error("Data Sampling", "Both random and ordered sampling failed", str(ordered_error))
                return {'error': f"All sampling methods failed: {ordered_error}"}
        else:
            # Random sampling with fallback (default behavior)
            actual_sampling_method = "Random Sampling"
            try:
                # Use the new smart random sampling approach
                domo_df, sf_df = self._get_smart_random_samples(
                    domo_dataset_id, snowflake_table, key_columns, sample_size
                )
                
            except Exception as e:
                self.logger.error(f"❌ Smart random sampling failed: {e}")
                self.logger.info("🔄 Falling back to ordered sampling...")
                actual_sampling_method = "Ordered Sampling"
                
                # Fallback to original deterministic sampling
                try:
                    # Get Domo sample using original method
                    key_cols_str = _escape_domo_column_list(domo_key_columns)
                    sample_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
                    
                    domo_df = self.domo_handler.extract_data(
                        dataset_id=domo_dataset_id, 
                        query=sample_query,
                        chunk_size=999999999,  # Force single chunk to avoid pagination issues
                        enable_auto_type_conversion=True
                    )
                    
                    if domo_df is None or domo_df.empty:
                        raise Exception("No data returned from Domo")
                    
                    # Get Snowflake sample using original method  
                    sf_key_cols_str = _normalize_snowflake_column_list(key_columns)
                    sf_query = f"SELECT * FROM {snowflake_table} ORDER BY {sf_key_cols_str} LIMIT {sample_size}"
                    sf_df = self.snowflake_handler.execute_query(sf_query)
                    
                    if sf_df is None or sf_df.empty:
                        raise Exception("Failed to get Snowflake sample")
                    
                    self.logger.info("✅ Fallback sampling completed")
                    
                except Exception as fallback_error:
                    self.add_error("Sample Extraction", "Both smart and fallback sampling failed", str(fallback_error))
                    return self._get_error_data_result(sample_size)
        
        # Apply column transformation if enabled (applies to both sampling methods)
        if transform_names:
            self.logger.info("🔄 Applying full column name transformation...")
            
            # Transform Domo columns
            original_domo_columns = domo_df.columns.tolist()
            transformed_domo_columns = [transform_column_name(col) for col in original_domo_columns]
            domo_df.columns = transformed_domo_columns
            
            # Transform Snowflake columns (may have different case)
            original_sf_columns = sf_df.columns.tolist()
            transformed_sf_columns = [transform_column_name(col) for col in original_sf_columns]
            sf_df.columns = transformed_sf_columns
            
            # Use normalized key columns (already transformed above)
            key_columns_for_comparison = normalized_key_columns
            
            self.logger.info(f"🔄 Full column transformation applied to both DataFrames")
        else:
            # Even without full transformation, we need to use normalized key columns
            key_columns_for_comparison = normalized_key_columns
            self.logger.info(f"🔄 Using normalized key columns without full column transformation")
        
        # Use datacompy for comparison
        try:
            self.logger.info(f"📊 Domo DataFrame shape: {domo_df.shape}, columns: {list(domo_df.columns)}")
            self.logger.info(f"📊 Snowflake DataFrame shape: {sf_df.shape}, columns: {list(sf_df.columns)}")
            self.logger.info(f"📊 Key columns for comparison: {key_columns_for_comparison}")
            
            # Normalize data types for key columns to ensure compatibility
            for col in key_columns_for_comparison:
                # Find matching columns case-insensitively
                domo_col = None
                sf_col = None
                
                # Find column in Domo DataFrame (case-insensitive)
                for domo_column in domo_df.columns:
                    if domo_column.lower() == col.lower():
                        domo_col = domo_column
                        break
                
                # Find column in Snowflake DataFrame (case-insensitive)
                for sf_column in sf_df.columns:
                    if sf_column.lower() == col.lower():
                        sf_col = sf_column
                        break
                
                if domo_col and sf_col:
                    domo_dtype = str(domo_df[domo_col].dtype)
                    sf_dtype = str(sf_df[sf_col].dtype)
                    
                    self.logger.info(f"Key column '{col}': Domo '{domo_col}'={domo_dtype}, Snowflake '{sf_col}'={sf_dtype}")
                    
                    # If types are different, convert both to string for compatibility
                    if domo_dtype != sf_dtype:
                        self.logger.info(f"Converting column '{col}' to string for compatibility")
                        domo_df[domo_col] = domo_df[domo_col].astype(str)
                        sf_df[sf_col] = sf_df[sf_col].astype(str)
                    
                    # Rename columns to match key_columns for datacompy compatibility
                    if domo_col != col:
                        domo_df = domo_df.rename(columns={domo_col: col})
                        self.logger.info(f"Renamed Domo column '{domo_col}' to '{col}'")
                    if sf_col != col:
                        sf_df = sf_df.rename(columns={sf_col: col})
                        self.logger.info(f"Renamed Snowflake column '{sf_col}' to '{col}'")
                else:
                    # If column not found, try to find it with the original name (before normalization)
                    original_col_name = None
                    for original_key in key_columns:
                        if transform_column_name(original_key) == col:
                            original_col_name = original_key
                            break
                    
                    if original_col_name:
                        self.logger.info(f"Trying to find original column name '{original_col_name}' for normalized key '{col}'")
                        
                        # Try to find the original column name in Domo
                        for domo_column in domo_df.columns:
                            if domo_column.lower() == original_col_name.lower():
                                domo_col = domo_column
                                break
                        
                        # Try to find the normalized column name in Snowflake
                        for sf_column in sf_df.columns:
                            if sf_column.lower() == col.lower():
                                sf_col = sf_column
                                break
                        
                        if domo_col and sf_col:
                            self.logger.info(f"Found columns with original/normalized names: Domo '{domo_col}', Snowflake '{sf_col}'")
                            
                            # Rename both columns to the normalized name for datacompy
                            domo_df = domo_df.rename(columns={domo_col: col})
                            sf_df = sf_df.rename(columns={sf_col: col})
                            
                            self.logger.info(f"Renamed columns for datacompy compatibility: '{domo_col}' → '{col}', '{sf_col}' → '{col}'")
                        else:
                            self.logger.warning(f"Could not find matching columns for key '{col}' (original: '{original_col_name}'). Domo: {domo_col}, Snowflake: {sf_col}")
                    else:
                        self.logger.warning(f"Key column '{col}' not found in both DataFrames. Domo: {domo_col}, Snowflake: {sf_col}")
            
            comparison = datacompy.Compare(
                domo_df,
                sf_df,
                join_columns=key_columns_for_comparison,
                df1_name='Domo',
                df2_name='Snowflake'
            )
            
            # Save detailed report
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            # Use the provided Snowflake table name as base (now expected to be the Model Name)
            safe_base = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(snowflake_table).strip()) or "report"
            
            # Create QA reports directory structure [[memory:5267850]]
            qa_reports_dir = "results/txt/qa"
            os.makedirs(qa_reports_dir, exist_ok=True)
            
            # report_filename = os.path.join(qa_reports_dir, f"{safe_base}_{timestamp}.txt")
            report_filename = f"{safe_base}_{timestamp}.txt"
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(f"COMPARISON REPORT\n")
                f.write(f"Domo Dataset: {domo_dataset_id}\n")
                f.write(f"Snowflake Table: {snowflake_table}\n")
                f.write(f"Key Columns: {', '.join(key_columns_for_comparison)}\n")
                f.write(f"Transform Applied: {transform_names}\n")
                f.write(f"Timestamp: {pd.Timestamp.now().isoformat()}\n")
                f.write("="*80 + "\n")
                
                # Get the datacompy report and modify it to include column names
                datacompy_report = comparison.report()
                
                # Add column names information to the Column Summary section
                if schema_comparison and not schema_comparison.get('error'):
                    # Get the actual column names from the DataFrames being compared
                    domo_cols_set = set(domo_df.columns)
                    sf_cols_set = set(sf_df.columns)
                    
                    # Calculate the actual differences based on the DataFrames being compared
                    missing_in_sf = list(domo_cols_set - sf_cols_set)
                    extra_in_sf = list(sf_cols_set - domo_cols_set)
                    
                    # Debug: print what we have
                    self.logger.info(f"DataFrame comparison debug:")
                    self.logger.info(f"  Domo columns: {list(domo_cols_set)}")
                    self.logger.info(f"  Snowflake columns: {list(sf_cols_set)}")
                    self.logger.info(f"  Missing in Snowflake: {missing_in_sf}")
                    self.logger.info(f"  Extra in Snowflake: {extra_in_sf}")
                    
                    # Find the Column Summary section and add column names
                    lines = datacompy_report.split('\n')
                    modified_lines = []
                    
                    for i, line in enumerate(lines):
                        modified_lines.append(line)
                        
                        # After "Number of columns in Domo but not in Snowflake" line, add the column names
                        if "Number of columns in Domo but not in Snowflake:" in line and missing_in_sf:
                            missing_cols = ', '.join(missing_in_sf)
                            modified_lines.append(f"Missing columns: {missing_cols}")
                        
                        # After "Number of columns in Snowflake but not in Domo" line, add the column names
                        elif "Number of columns in Snowflake but not in Domo:" in line and extra_in_sf:
                            extra_cols = ', '.join(extra_in_sf)
                            modified_lines.append(f"Extra columns: {extra_cols}")
                    
                    # Write the modified report
                    f.write('\n'.join(modified_lines))
                else:
                    # Write the original report if no schema comparison available
                    f.write(datacompy_report)
            
            self.logger.info(f"📄 Detailed report saved to: {report_filename}")
            
            return {
                'sample_size': sample_size,
                'domo_sample_rows': len(domo_df),
                'snowflake_sample_rows': len(sf_df),
                'data_match': comparison.matches(),
                'missing_in_snowflake': len(comparison.df1_unq_rows),
                'extra_in_snowflake': len(comparison.df2_unq_rows),
                'rows_with_differences': self._count_differing_rows(comparison),
                'transform_applied': transform_names,
                'report_file': report_filename,
                'comparison_object': comparison,  # Add comparison object for executive summary
                'sampling_method': actual_sampling_method  # Track which sampling method was actually used
            }
            
        except Exception as e:
            self.add_error("Data Comparison", "Error using datacompy", str(e))
            return self._get_error_data_result(sample_size, len(domo_df), len(sf_df))
    
    def _count_differing_rows(self, comparison: datacompy.Compare) -> int:
        """Count rows with differences from datacompy comparison."""
        try:
            if hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                # Count rows where any column has differences
                differing_columns = comparison.column_stats[comparison.column_stats['matches'] == False]
                return len(differing_columns)
            else:
                return 0
        except:
            return 0
    
    def _get_error_data_result(self, sample_size: int, domo_rows: int = 0, 
                              sf_rows: int = 0) -> Dict[str, Any]:
        """Get error result for data comparison."""
        return {
            'sample_size': sample_size,
            'domo_sample_rows': domo_rows,
            'snowflake_sample_rows': sf_rows,
            'data_match': False,
            'missing_in_snowflake': 0,
            'extra_in_snowflake': 0,
            'rows_with_differences': 0,
            'error': True
        }
    
    def _get_all_unique_keys(self, domo_dataset_id: str, key_columns: List[str]) -> pd.DataFrame:
        """
        Obtiene todas las combinaciones únicas de keys del dataset de Domo.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            
        Returns:
            DataFrame con todas las combinaciones únicas de keys
        """
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        if hasattr(self, '_domo_original_columns') and self._domo_original_columns:
            for normalized_key in key_columns:
                if normalized_key in self._domo_original_columns:
                    domo_key_columns.append(self._domo_original_columns[normalized_key])
                else:
                    # Fallback: assume the key column name is already in Domo format
                    domo_key_columns.append(normalized_key)
        else:
            # No mapping available, use as-is
            domo_key_columns = key_columns
        
        key_cols_str = _escape_domo_column_list(domo_key_columns)
        all_keys_query = f"SELECT DISTINCT {key_cols_str} FROM table"
        
        self.logger.info(f"📋 Getting all unique key combinations for columns: {key_columns} → {domo_key_columns}")
        
        all_keys_df = self.domo_handler.extract_data(
            dataset_id=domo_dataset_id,
            query=all_keys_query,
            enable_auto_type_conversion=False  # Keep original types for exact matching
        )
        
        if all_keys_df is None or all_keys_df.empty:
            raise Exception("Could not retrieve unique keys from Domo dataset")
        
        self.logger.info(f"📊 Found {len(all_keys_df)} unique key combinations")
        return all_keys_df
    
    def _build_efficient_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye una cláusula WHERE eficiente y compatible con MySQL.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            
        Returns:
            String con la cláusula WHERE optimizada y compatible con MySQL
        """
        if sampled_keys_df.empty:
            raise Exception("No sampled keys provided")
        
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        if hasattr(self, '_domo_original_columns') and self._domo_original_columns:
            for normalized_key in key_columns:
                if normalized_key in self._domo_original_columns:
                    domo_key_columns.append(self._domo_original_columns[normalized_key])
                else:
                    # Fallback: assume the key column name is already in Domo format
                    domo_key_columns.append(normalized_key)
        else:
            # No mapping available, use as-is
            domo_key_columns = key_columns
        
        if len(domo_key_columns) == 1:
            # Un solo key column: usar simple IN (muy eficiente)
            col = domo_key_columns[0]
            values = sampled_keys_df[key_columns[0]].dropna().tolist()  # Use normalized name for DataFrame access
            
            # Manejar diferentes tipos de datos
            if all(isinstance(v, str) for v in values):
                # String values: escapar comillas (no podemos usar backslash en f-strings)
                escaped_values = []
                for v in values:
                    escaped_val = str(v).replace("'", "''")  # Escapar comillas simples
                    escaped_values.append(f"'{escaped_val}'")
            else:
                # Numeric or other values
                escaped_values = [str(v) for v in values]
            
            values_str = ', '.join(escaped_values)
            escaped_col = _escape_domo_column_name(col)
            where_clause = f"{escaped_col} IN ({values_str})"
            
        elif len(key_columns) == 2:
            # Dos key columns: usar ORs para máxima compatibilidad con MySQL
            # (IN con tuples puede fallar en algunas versiones de MySQL)
            where_clause = self._build_or_where_clause(sampled_keys_df, key_columns)
                
        else:
            # 3+ columns: usar approach de ORs (pero son pocas filas, así que está bien)
            where_clause = self._build_or_where_clause(sampled_keys_df, key_columns)
        
        self.logger.info(f"🔍 Built efficient WHERE clause for {len(sampled_keys_df)} combinations")
        self.logger.debug(f"WHERE clause (first 200 chars): {where_clause[:200]}...")
        
        return where_clause
    
    def _build_or_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE usando ORs para casos complejos (fallback).
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas  
            key_columns: Lista de columnas que actúan como keys (normalizadas)
            
        Returns:
            String con la cláusula WHERE usando ORs
        """
        # Map normalized key columns back to original Domo names for queries
        domo_key_columns = []
        if hasattr(self, '_domo_original_columns') and self._domo_original_columns:
            for normalized_key in key_columns:
                if normalized_key in self._domo_original_columns:
                    domo_key_columns.append(self._domo_original_columns[normalized_key])
                else:
                    # Fallback: assume the key column name is already in Domo format
                    domo_key_columns.append(normalized_key)
        else:
            # No mapping available, use as-is
            domo_key_columns = key_columns
        
        where_conditions = []
        
        for _, row in sampled_keys_df.iterrows():
            row_conditions = []
            
            for i, col in enumerate(domo_key_columns):
                value = row[key_columns[i]]  # Use normalized name for DataFrame access
                escaped_col = _escape_domo_column_name(col)
                
                # Manejar diferentes tipos de datos
                if pd.isna(value) or value is None:
                    row_conditions.append(f"{escaped_col} IS NULL")
                elif isinstance(value, str):
                    if value.strip() == '':
                        row_conditions.append(f"({escaped_col} = '' OR {escaped_col} IS NULL)")
                    else:
                        escaped_value = str(value).replace("'", "''")
                        row_conditions.append(f"{escaped_col} = '{escaped_value}'")
                else:
                    row_conditions.append(f"{escaped_col} = {value}")
            
            # Unir condiciones de esta fila con AND
            row_condition = "(" + " AND ".join(row_conditions) + ")"
            where_conditions.append(row_condition)
        
        # Unir todas las filas con OR
        return " OR ".join(where_conditions)
    
    def _build_snowflake_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE para Snowflake usando nombres de columnas normalizados.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas
            key_columns: Lista de columnas que actúan como keys
            
        Returns:
            String con la cláusula WHERE optimizada para Snowflake
        """
        if sampled_keys_df.empty:
            raise Exception("No sampled keys provided")
        
        if len(key_columns) == 1:
            # Un solo key column: usar simple IN (muy eficiente)
            col = key_columns[0]
            normalized_col = transform_column_name(col)
            values = sampled_keys_df[col].dropna().tolist()
            
            # Manejar diferentes tipos de datos
            if all(isinstance(v, str) for v in values):
                # String values: escapar comillas
                escaped_values = []
                for v in values:
                    escaped_val = str(v).replace("'", "''")  # Escapar comillas simples
                    escaped_values.append(f"'{escaped_val}'")
            else:
                # Numeric or other values
                escaped_values = [str(v) for v in values]
            
            values_str = ', '.join(escaped_values)
            where_clause = f"{normalized_col} IN ({values_str})"
            
        elif len(key_columns) == 2:
            # Dos key columns: usar ORs para máxima compatibilidad
            where_clause = self._build_snowflake_or_where_clause(sampled_keys_df, key_columns)
                
        else:
            # 3+ columns: usar approach de ORs
            where_clause = self._build_snowflake_or_where_clause(sampled_keys_df, key_columns)
        
        self.logger.info(f"🔍 Built Snowflake WHERE clause for {len(sampled_keys_df)} combinations")
        self.logger.debug(f"Snowflake WHERE clause (first 200 chars): {where_clause[:200]}...")
        
        return where_clause
    
    def _build_snowflake_or_where_clause(self, sampled_keys_df: pd.DataFrame, key_columns: List[str]) -> str:
        """
        Construye cláusula WHERE para Snowflake usando ORs para casos complejos.
        
        Args:
            sampled_keys_df: DataFrame con las combinaciones de keys seleccionadas  
            key_columns: Lista de columnas que actúan como keys
            
        Returns:
            String con la cláusula WHERE usando ORs para Snowflake
        """
        where_conditions = []
        
        for _, row in sampled_keys_df.iterrows():
            row_conditions = []
            
            for col in key_columns:
                value = row[col]
                normalized_col = transform_column_name(col)
                
                # Manejar diferentes tipos de datos
                if pd.isna(value) or value is None:
                    row_conditions.append(f"{normalized_col} IS NULL")
                elif isinstance(value, str):
                    if value.strip() == '':
                        row_conditions.append(f"({normalized_col} = '' OR {normalized_col} IS NULL)")
                    else:
                        escaped_value = str(value).replace("'", "''")
                        row_conditions.append(f"{normalized_col} = '{escaped_value}'")
                else:
                    row_conditions.append(f"{normalized_col} = {value}")
            
            # Unir condiciones de esta fila con AND
            row_condition = "(" + " AND ".join(row_conditions) + ")"
            where_conditions.append(row_condition)
        
        # Unir todas las filas con OR
        return " OR ".join(where_conditions)
    
    def _get_smart_random_samples(self, domo_dataset_id: str, snowflake_table: str, 
                                 key_columns: List[str], sample_size: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Obtiene muestras aleatorias sincronizadas usando random determinístico en Python.
        
        Esta es la "solución ganadora": rápida, garantiza mismas filas, y reproducible.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            snowflake_table: Nombre de la tabla en Snowflake
            key_columns: Lista de columnas que actúan como keys
            sample_size: Número de filas a muestrear
            
        Returns:
            Tuple[domo_df, snowflake_df] con exactamente las mismas filas
        """
        self.logger.info("🎲 Using smart random sampling (Python-controlled)")
        
        # Paso 1: Obtener TODAS las combinaciones únicas (UNA sola query rápida)
        all_keys_df = self._get_all_unique_keys(domo_dataset_id, key_columns)
        
        if len(all_keys_df) <= sample_size:
            # Si hay pocas keys únicas, usar todas
            self.logger.info(f"📊 Only {len(all_keys_df)} unique keys available, using all")
            sampled_keys_df = all_keys_df
        else:
            # Paso 2: Python hace el random sampling (DETERMINÍSTICO)
            self.logger.info(f"🎲 Randomly sampling {sample_size} from {len(all_keys_df)} unique keys...")
            
            # Seed fijo para reproducibilidad (puedes cambiarlo por datetime si quieres variedad)
            random.seed(42)
            
            # Random sampling en Python (muy rápido)
            sampled_indices = random.sample(range(len(all_keys_df)), sample_size)
            sampled_keys_df = all_keys_df.iloc[sampled_indices].reset_index(drop=True)
        
        self.logger.info(f"✅ Selected {len(sampled_keys_df)} key combinations for sampling")
        
        # Paso 3: Usar batching inteligente para evitar queries demasiado largos
        max_batch_size = 10  # Muy pequeño para evitar errores de API de Domo
        domo_dfs = []
        sf_dfs = []
        
        if len(sampled_keys_df) > max_batch_size:
            self.logger.info(f"🔄 Using batching: {len(sampled_keys_df)} keys split into batches of {max_batch_size}")
            
            # Dividir en batches
            for i in range(0, len(sampled_keys_df), max_batch_size):
                batch_end = min(i + max_batch_size, len(sampled_keys_df))
                batch_keys_df = sampled_keys_df.iloc[i:batch_end].reset_index(drop=True)
                
                self.logger.info(f"📦 Processing batch {i//max_batch_size + 1}: {len(batch_keys_df)} keys (rows {i+1}-{batch_end})")
                
                # Procesar este batch
                domo_batch_df, sf_batch_df = self._get_batch_data(
                    domo_dataset_id, snowflake_table, key_columns, batch_keys_df
                )
                
                if domo_batch_df is not None and not domo_batch_df.empty:
                    domo_dfs.append(domo_batch_df)
                if sf_batch_df is not None and not sf_batch_df.empty:
                    sf_dfs.append(sf_batch_df)
            
            # Combinar todos los batches
            if domo_dfs and sf_dfs:
                domo_df = pd.concat(domo_dfs, ignore_index=True)
                sf_df = pd.concat(sf_dfs, ignore_index=True)
                self.logger.info(f"✅ Combined {len(domo_dfs)} batches: Domo {len(domo_df)} rows, Snowflake {len(sf_df)} rows")
            else:
                raise Exception("No data returned from any batch")
                
        else:
            # Sample size pequeño, usar método original
            self.logger.info(f"📦 Single batch processing: {len(sampled_keys_df)} keys")
            domo_df, sf_df = self._get_batch_data(
                domo_dataset_id, snowflake_table, key_columns, sampled_keys_df
            )
        
        # Verificar que ambos DataFrames tienen datos
        if domo_df is None or domo_df.empty:
            raise Exception("No data returned from Domo with sampled keys")
        if sf_df is None or sf_df.empty:
            raise Exception("No data returned from Snowflake with sampled keys")
            
        
        # Validar resultados finales
        self.logger.info(f"✅ Smart random sampling completed:")
        self.logger.info(f"   📊 Domo: {len(domo_df)} rows, {len(domo_df.columns)} columns") 
        self.logger.info(f"   📊 Snowflake: {len(sf_df)} rows, {len(sf_df.columns)} columns")
        
        # Advertir si hay discrepancias significativas en el número de filas
        expected_rows = len(sampled_keys_df)
        if len(domo_df) < expected_rows * 0.8:
            self.logger.warning(f"⚠️ Domo returned fewer rows than expected: {len(domo_df)} vs {expected_rows}")
        if len(sf_df) < expected_rows * 0.8:
            self.logger.warning(f"⚠️ Snowflake returned fewer rows than expected: {len(sf_df)} vs {expected_rows}")
        
        return domo_df, sf_df
    
    def _get_batch_data(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], batch_keys_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Obtiene datos para un batch específico de keys.
        
        Args:
            domo_dataset_id: ID del dataset de Domo
            snowflake_table: Nombre de la tabla en Snowflake
            key_columns: Lista de columnas que actúan como keys
            batch_keys_df: DataFrame con las combinaciones de keys para este batch
            
        Returns:
            Tuple[domo_df, snowflake_df] para este batch
        """
        # Construir WHERE clause para Domo (con escape)
        domo_where_clause = self._build_efficient_where_clause(batch_keys_df, key_columns)
        
        # Construir WHERE clause para Snowflake (con nombres normalizados)
        sf_where_clause = self._build_snowflake_where_clause(batch_keys_df, key_columns)
        
        # Ejecutar query en Domo
        self.logger.info(f"📥 Getting {len(batch_keys_df)} rows from Domo...")
        domo_query = f"SELECT * FROM table WHERE {domo_where_clause}"
        domo_df = self.domo_handler.extract_data(
            dataset_id=domo_dataset_id,
            query=domo_query,
            chunk_size=999999999,  # Force single chunk to avoid pagination issues with WHERE clauses
            enable_auto_type_conversion=True
        )
        
        if domo_df is None or domo_df.empty:
            self.logger.warning(f"⚠️ No data returned from Domo for this batch")
            domo_df = pd.DataFrame()
        
        # Ejecutar query en Snowflake
        self.logger.info(f"📥 Getting {len(batch_keys_df)} rows from Snowflake...")
        sf_query = f"SELECT * FROM {snowflake_table} WHERE {sf_where_clause}"
        sf_df = self.snowflake_handler.execute_query(sf_query)
        
        if sf_df is None or sf_df.empty:
            self.logger.warning(f"⚠️ No data returned from Snowflake for this batch")
            sf_df = pd.DataFrame()
        
        self.logger.info(f"✅ Batch completed: Domo {len(domo_df)} rows, Snowflake {len(sf_df)} rows")
        return domo_df, sf_df
    
    def generate_report(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sample_size: Optional[int] = None,
                       transform_names: bool = False, sampling_method: str = "random") -> Dict[str, Any]:
        """
        Generate complete comparison report.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample
            transform_names: Whether to apply column name transformation
            sampling_method: Sampling method ('random' or 'ordered')
            
        Returns:
            Complete comparison report dictionary
        """
        self.logger.info(f"🚀 Starting comparison: {domo_dataset_id} vs {snowflake_table}")
        
        # Clear previous errors
        self.errors = []
        
        # Setup connections if needed
        if not self._domo_connected or not self._snowflake_connected:
            if not self.setup_connections():
                return self._get_connection_error_report(domo_dataset_id, snowflake_table, 
                                                       key_columns, transform_names)
        
        # Perform comparisons
        schema_comparison = self.compare_schemas(domo_dataset_id, snowflake_table, transform_names)
        row_count_comparison = self.compare_row_counts(domo_dataset_id, snowflake_table)
        data_comparison = self.compare_data_samples(domo_dataset_id, snowflake_table, 
                                                   key_columns, sample_size, transform_names, schema_comparison, sampling_method)
        
        # Determine overall match
        overall_match = False
        if not schema_comparison.get('error') and not data_comparison.get('error'):
            row_count_ok = (row_count_comparison['match'] or 
                          row_count_comparison.get('negligible_analysis', {}).get('is_negligible', False))
            
            overall_match = (schema_comparison['schema_match'] and 
                           row_count_ok and 
                           data_comparison.get('data_match', False))
        
        return {
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
    
    def _get_connection_error_report(self, domo_dataset_id: str, snowflake_table: str, 
                                   key_columns: List[str], transform_names: bool) -> Dict[str, Any]:
        """Get error report for connection failures."""
        return {
            'domo_dataset_id': domo_dataset_id,
            'snowflake_table': snowflake_table,
            'key_columns': key_columns,
            'overall_match': False,
            'errors': [{'section': 'Connection', 'error': 'Failed to setup connections', 'details': ''}],
            'timestamp': pd.Timestamp.now().isoformat(),
            'transform_applied': transform_names
        }
    
    def print_report(self, report: Dict[str, Any]):
        """Print comparison report in a readable format."""
        print("\n" + "="*80)
        print("DOMO vs SNOWFLAKE COMPARISON REPORT")
        print("="*80)
        
        print(f"📊 Domo Dataset: {report['domo_dataset_id']}")
        print(f"❄️  Snowflake Table: {report['snowflake_table']}")
        print(f"🔑 Key Columns: {', '.join(report['key_columns'])}")
        print(f"⏰ Timestamp: {report['timestamp']}")
        print(f"🔄 Column Transformation: {'Applied' if report.get('transform_applied') else 'Not Applied'}")
        
        # Show errors
        if report.get('errors'):
            print(f"\n⚠️  ERRORS ({len(report['errors'])}):")
            for i, error in enumerate(report['errors'], 1):
                print(f"   {i}. {error['section']}: {error['error']}")
                if error.get('details'):
                    print(f"      Details: {error['details']}")
        
        # Overall status
        if report.get('errors'):
            print(f"\n🎯 OVERALL STATUS: ❌ ERRORS FOUND")
        elif report['overall_match']:
            print(f"\n🎯 OVERALL STATUS: ✅ PERFECT MATCH")
        else:
            print(f"\n🎯 OVERALL STATUS: ❌ DISCREPANCIES FOUND")
        
        # Schema comparison
        schema = report['schema_comparison']
        print(f"\n📋 SCHEMA COMPARISON:")
        if schema.get('error'):
            print("   ❌ Error getting schemas")
        else:
            print(f"   Domo columns: {schema['domo_columns']}")
            print(f"   Snowflake columns: {schema['snowflake_columns']}")
            print(f"   Common columns: {schema['common_columns']}")
            
            if schema['missing_in_snowflake']:
                print(f"   ❌ Missing in Snowflake: {schema['missing_in_snowflake']}")
            if schema['extra_in_snowflake']:
                print(f"   ⚠️  Extra in Snowflake: {schema['extra_in_snowflake']}")
            if schema['type_mismatches']:
                print(f"   🔄 Type mismatches: {len(schema['type_mismatches'])}")
            
            if schema['schema_match']:
                print(f"   ✅ Schema matches")
            else:
                print(f"   ❌ Schema differences found")
        
        # Row count comparison
        rows = report['row_count_comparison']
        print(f"\n📊 ROW COUNT COMPARISON:")
        print(f"   Domo rows: {rows['domo_rows']:,}")
        print(f"   Snowflake rows: {rows['snowflake_rows']:,}")
        print(f"   Difference: {rows['difference']:,}")
        
        negligible = rows.get('negligible_analysis', {})
        if negligible:
            if negligible.get('is_negligible'):
                print(f"   ✅ Difference is negligible: {negligible.get('reason')}")
            else:
                print(f"   ❌ Significant difference: {negligible.get('reason')}")
        
        # Data comparison
        data = report['data_comparison']
        print(f"\n🔍 DATA COMPARISON:")
        if data.get('error'):
            print("   ❌ Error comparing data")
        else:
            print(f"   Sample size: {data['sample_size']:,}")
            print(f"   Domo sample rows: {data['domo_sample_rows']:,}")
            print(f"   Snowflake sample rows: {data['snowflake_sample_rows']:,}")
            
            if data.get('missing_in_snowflake', 0) > 0:
                print(f"   ❌ Missing in Snowflake: {data['missing_in_snowflake']}")
            if data.get('extra_in_snowflake', 0) > 0:
                print(f"   ⚠️  Extra in Snowflake: {data['extra_in_snowflake']}")
            if data.get('rows_with_differences', 0) > 0:
                print(f"   🔄 Rows with differences: {data['rows_with_differences']}")
            
            if data.get('data_match'):
                print(f"   ✅ Data samples match")
            else:
                print(f"   ❌ Data differences found")
            
            if data.get('report_file'):
                print(f"   📄 Detailed report: {data['report_file']}")
        
        print("="*80)
    
    def compare_from_spreadsheet(self, spreadsheet_id: str, sheet_name: str = None,
                                credentials_path: str = None, sampling_method: str = "random") -> Dict[str, Any]:
        """
        Compare multiple datasets from Google Sheets configuration.
        
        Expected columns in spreadsheet:
        - Output ID: Domo dataset ID
        - Table Name: Snowflake table name
        - Key Columns: Comma-separated list of key columns
        - Sample Size: (Optional) Number of rows to sample
        - Transform Columns: (Optional) True/False for column transformation
        - Status: (Optional) Track comparison status
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Sheet name containing comparison configurations (uses COMPARISON_SHEET_NAME env var if None)
            credentials_path: Path to Google Sheets credentials file
            sampling_method: Sampling method ('random' or 'ordered')
            
        Returns:
            Dictionary with comparison results summary
        """
        try:
            # Get sheet name from environment if not provided
            if sheet_name is None:
                env_config = get_env_config()
                sheet_name = env_config.get("COMPARISON_SHEET_NAME", "QA - Test")
            
            self.logger.info(f"🚀 Starting spreadsheet-based comparisons...")
            self.logger.info(f"📋 Spreadsheet ID: {spreadsheet_id}")
            self.logger.info(f"📄 Sheet name: {sheet_name}")
            
            # Import GoogleSheets here to avoid circular imports
            from .utils.gsheets import GoogleSheets, READ_WRITE_SCOPES
            
            if not credentials_path:
                credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
            
            if not credentials_path:
                raise Exception("Google Sheets credentials not provided")
            
            if not os.path.exists(credentials_path):
                raise Exception(f"Google Sheets credentials file not found: {credentials_path}")
            
            # Initialize Google Sheets client
            gsheets_client = GoogleSheets(
                credentials_path=credentials_path,
                scopes=READ_WRITE_SCOPES
            )
            
            # Read comparison configurations
            self.logger.info(f"📖 Reading comparison configurations from {sheet_name}...")
            data = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A:Z")
            
            if not data or len(data) < 2:
                raise Exception(f"No data found in sheet '{sheet_name}' or missing headers")
            
            # Convert to DataFrame
            import pandas as pd
            headers = data[0]
            rows = data[1:]
            
            # Normalize row lengths to match header length
            num_cols = len(headers)
            normalized_rows = []
            for row in rows:
                # Ensure each row has the same number of columns as headers
                normalized_row = row + [''] * (num_cols - len(row)) if len(row) < num_cols else row[:num_cols]
                normalized_rows.append(normalized_row)
            
            df = pd.DataFrame(normalized_rows, columns=headers)
            
            # Remove empty rows
            df = df.dropna(how='all')
            
            self.logger.info(f"📊 Found {len(df)} comparison configurations")
            
            # Look for required columns with flexible naming
            dataset_id_column = None
            table_name_column = None
            key_columns_column = None
            sample_size_column = None
            transform_columns_column = None
            status_column = None
            
            # Find Output ID column
            possible_dataset_id_columns = ['Output ID', 'output_id', 'Dataset ID', 'dataset_id', 'Domo Dataset ID', 'domo_dataset_id', 'ID', 'id']
            for col in possible_dataset_id_columns:
                if col in df.columns:
                    dataset_id_column = col
                    break
            
            # Find Table Name column (accept 'Model Name' as preferred)
            possible_table_columns = ['Model Name', 'model_name']
            for col in possible_table_columns:
                if col in df.columns:
                    table_name_column = col
                    break
            
            # Find Key Columns column
            possible_key_columns = ['Key Columns', 'key_columns', 'Keys', 'keys', 'Join Columns', 'join_columns']
            for col in possible_key_columns:
                if col in df.columns:
                    key_columns_column = col
                    break
            
            # Find Sample Size column (optional)
            possible_sample_columns = ['Sample Size', 'sample_size', 'Sample', 'sample']
            for col in possible_sample_columns:
                if col in df.columns:
                    sample_size_column = col
                    break
            
            # Find Transform Columns column (optional)
            possible_transform_columns = ['Transform Columns', 'transform_columns', 'Transform', 'transform']
            for col in possible_transform_columns:
                if col in df.columns:
                    transform_columns_column = col
                    break
            
            # Find Status column (optional)
            possible_status_columns = ['Status', 'status', 'comparison_status', 'Comparison Status', 'state']
            for col in possible_status_columns:
                if col in df.columns:
                    status_column = col
                    break
            
            # Find Notes column (optional)
            notes_column = None
            possible_notes_columns = ['Notes', 'notes', 'Note', 'note', 'Comments', 'comments']
            for col in possible_notes_columns:
                if col in df.columns:
                    notes_column = col
                    break
            
            # Validate required columns
            if dataset_id_column is None:
                raise Exception("Required column 'Output ID' not found in spreadsheet")
            
            if table_name_column is None:
                raise Exception("Required column 'Table Name' not found in spreadsheet")
            
            if key_columns_column is None:
                raise Exception("Required column 'Key Columns' not found in spreadsheet")
            
            # Filter rows where Status is "Testing" (if status column exists)
            if status_column and status_column in df.columns:
                df[status_column] = df[status_column].fillna('Pending')
                df[status_column] = df[status_column].astype(str)
                testing_df = df[df[status_column].str.contains('Testing', case=False, na=False)]
                self.logger.info(f"📋 Found {len(testing_df)} comparisons in 'Testing' status")
            else:
                testing_df = df
                self.logger.info(f"📋 No status column found, processing all {len(testing_df)} comparisons")
            
            if len(testing_df) == 0:
                self.logger.info("✅ No comparisons in 'Testing' status")
                return {"success": 0, "failed": 0, "total": 0, "errors": []}
            
            # Setup connections once
            if not self.setup_connections():
                raise Exception("Failed to setup connections")
            
            # Process comparisons
            successful_comparisons = []
            failed_comparisons = []
            errors = []
            
            for index, row in testing_df.iterrows():
                dataset_id = row[dataset_id_column]
                table_name = row[table_name_column]
                key_columns_str = row[key_columns_column]
                
                # Clean table name - remove .sql extension if present
                if table_name and str(table_name).strip().lower().endswith('.sql'):
                    table_name = str(table_name).strip()[:-4]  # Remove last 4 characters (.sql)
                
                # Validate required fields - skip incomplete rows instead of treating as errors
                if pd.isna(dataset_id) or str(dataset_id).strip() == '':
                    self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Output ID")
                    continue
                
                if pd.isna(table_name) or str(table_name).strip() == '':
                    self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Table Name")
                    continue
                
                if pd.isna(key_columns_str) or str(key_columns_str).strip() == '':
                    self.logger.info(f"⏭️  Skipping row {index + 2}: Empty Key Columns")
                    continue
                
                # Parse key columns (comma-separated)
                key_columns = [col.strip() for col in str(key_columns_str).split(',') if col.strip()]
                
                # Parse optional fields
                sample_size = None
                if sample_size_column and not pd.isna(row.get(sample_size_column)):
                    try:
                        sample_size = int(row[sample_size_column])
                    except (ValueError, TypeError):
                        self.logger.warning(f"⚠️  Row {index + 2}: Invalid sample size, using auto-calculation")
                
                transform_columns = False
                if transform_columns_column and not pd.isna(row.get(transform_columns_column)):
                    transform_value = str(row[transform_columns_column]).lower()
                    transform_columns = transform_value in ['true', '1', 'yes', 'y', 'enabled']
                
                self.logger.info(f"🔄 Comparing dataset {dataset_id} vs table {table_name}")
                
                try:
                    # Generate comparison report
                    report = self.generate_report(
                        domo_dataset_id=str(dataset_id),
                        snowflake_table=str(table_name),
                        key_columns=key_columns,
                        sample_size=sample_size,
                        transform_names=transform_columns,
                        sampling_method=sampling_method
                    )
                    
                    # Check if comparison was successful
                    if report.get('errors'):
                        error_msg = f"Dataset {dataset_id}: Comparison failed with errors"
                        self.logger.error(f"❌ {error_msg}")
                        errors.extend([f"Dataset {dataset_id}: {err['error']}" for err in report['errors']])
                        failed_comparisons.append(str(dataset_id))
                    else:
                        success_msg = f"Dataset {dataset_id}: Comparison completed"
                        if report.get('overall_match'):
                            self.logger.info(f"✅ {success_msg} - Perfect match!")
                        else:
                            self.logger.warning(f"⚠️  {success_msg} - Discrepancies found")
                        successful_comparisons.append(str(dataset_id))
                    
                    # Always update notes in spreadsheet if notes column exists
                    if notes_column:
                        try:
                            # Generate executive summary or error summary based on comparison result
                            if report.get('errors'):
                                # Generate error summary for failed comparisons
                                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                                error_summary = f"❌ COMPARISON FAILED [{timestamp}]\n"
                                error_summary += f"Dataset: {dataset_id} → {table_name}\n"
                                error_summary += f"❌ Errors encountered:\n"
                                for error in report.get('errors', []):
                                    error_summary += f"  • {error.get('error', str(error))}\n"
                                error_summary += f"📄 Check detailed logs for more information"
                                executive_summary = error_summary
                            else:
                                # Generate normal executive summary for successful comparisons
                                comparison_obj = report.get('data_comparison', {}).get('comparison_object')
                                executive_summary = self._generate_executive_summary(report, comparison_obj)
                            
                            # Get current notes content
                            notes_cell_range = f"{sheet_name}!{chr(65 + df.columns.get_loc(notes_column))}{index + 2}"
                            current_notes_result = gsheets_client.read_range(spreadsheet_id, notes_cell_range)
                            current_notes = ""
                            if current_notes_result and len(current_notes_result) > 0 and len(current_notes_result[0]) > 0:
                                current_notes = str(current_notes_result[0][0]).strip()
                            
                            # Append executive summary to existing notes
                            if current_notes:
                                updated_notes = f"{current_notes}\n\n{executive_summary}"
                            else:
                                updated_notes = executive_summary
                            
                            # Update the notes cell
                            gsheets_client.write_range(spreadsheet_id, notes_cell_range, [[updated_notes]])
                            self.logger.info(f"📝 Updated notes for dataset {dataset_id}")
                            
                        except Exception as e:
                            self.logger.warning(f"⚠️  Could not update notes for row {index + 2}: {e}")
                    
                except Exception as e:
                    error_msg = f"Dataset {dataset_id}: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(str(dataset_id))
            
            # Summary
            total_comparisons = len(successful_comparisons) + len(failed_comparisons)
            
            self.logger.info("="*80)
            self.logger.info("📊 SPREADSHEET COMPARISON SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"✅ Successful comparisons: {len(successful_comparisons)}")
            self.logger.info(f"❌ Failed comparisons: {len(failed_comparisons)}")
            self.logger.info(f"📋 Total comparisons: {total_comparisons}")
            
            if successful_comparisons:
                self.logger.info(f"📈 Success rate: {len(successful_comparisons)/total_comparisons*100:.1f}%")
            
            if errors:
                self.logger.error("\n❌ Errors encountered:")
                for error in errors[:10]:  # Show first 10 errors
                    self.logger.error(f"   • {error}")
                if len(errors) > 10:
                    self.logger.error(f"   ... and {len(errors) - 10} more errors")
            
            return {
                "success": len(successful_comparisons),
                "failed": len(failed_comparisons),
                "total": total_comparisons,
                "errors": errors,
                "successful_datasets": successful_comparisons,
                "failed_datasets": failed_comparisons
            }
            
        except Exception as e:
            self.logger.error(f"❌ Spreadsheet comparison failed: {e}")
            return {
                "success": 0,
                "failed": 0,
                "total": 0,
                "errors": [str(e)]
            }
    
    def compare_from_inventory(self, credentials_path: str = None, sampling_method: str = "random") -> Dict[str, Any]:
        """
        Compare datasets from the existing inventory spreadsheet.
        
        Uses the same spreadsheet and sheet as the inventory system with columns:
        - Output ID: Domo dataset ID
        - Model Name: Snowflake table name  
        - Key Columns: Comma-separated list of key columns
        
        Args:
            credentials_path: Path to Google Sheets credentials file
            sampling_method: Sampling method ('random' or 'ordered')
            
        Returns:
            Dictionary with comparison results summary
        """
        try:
            # Get spreadsheet configuration from environment
            from .utils.common import get_env_config
            env_config = get_env_config()
            spreadsheet_id = env_config.get("MIGRATION_SPREADSHEET_ID")
            sheet_name = env_config.get("INTERMEDIATE_MODELS_SHEET_NAME", "Inventory")
            
            if not spreadsheet_id:
                raise Exception("MIGRATION_SPREADSHEET_ID environment variable not set")
            
            self.logger.info(f"🚀 Starting inventory-based comparisons...")
            self.logger.info(f"📋 Using inventory spreadsheet: {spreadsheet_id}")
            self.logger.info(f"📄 Using inventory sheet: {sheet_name}")
            
            # Import GoogleSheets here to avoid circular imports
            from .utils.gsheets import GoogleSheets, READ_WRITE_SCOPES
            
            if not credentials_path:
                credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
            
            if not credentials_path:
                raise Exception("Google Sheets credentials not provided")
            
            if not os.path.exists(credentials_path):
                raise Exception(f"Google Sheets credentials file not found: {credentials_path}")
            
            # Initialize Google Sheets client
            gsheets_client = GoogleSheets(
                credentials_path=credentials_path,
                scopes=READ_WRITE_SCOPES
            )
            
            # Read inventory data
            self.logger.info(f"📖 Reading inventory data from {sheet_name}...")
            data = gsheets_client.read_range(spreadsheet_id, f"{sheet_name}!A:Z")
            
            if not data or len(data) < 2:
                raise Exception(f"No data found in sheet '{sheet_name}' or missing headers")
            
            # Convert to DataFrame
            import pandas as pd
            headers = data[0]
            rows = data[1:]
            
            # Normalize row lengths to match header length
            num_cols = len(headers)
            normalized_rows = []
            for row in rows:
                # Ensure each row has the same number of columns as headers
                normalized_row = row + [''] * (num_cols - len(row)) if len(row) < num_cols else row[:num_cols]
                normalized_rows.append(normalized_row)
            
            df = pd.DataFrame(normalized_rows, columns=headers)
            
            # Remove empty rows
            df = df.dropna(how='all')
            
            self.logger.info(f"📊 Found {len(df)} inventory entries")
            
            # Look for required columns
            dataset_id_column = None
            table_name_column = None
            key_columns_column = None
            
            # Find Output ID column (Domo dataset ID)
            possible_dataset_id_columns = ['Output ID', 'output_id', 'Dataset ID', 'dataset_id', 'Domo Dataset ID']
            for col in possible_dataset_id_columns:
                if col in df.columns:
                    dataset_id_column = col
                    break
            
            # Find Model Name column (Snowflake table name)
            possible_table_columns = ['Model Name', 'model_name', 'Table Name', 'table_name', 'Snowflake Table']
            for col in possible_table_columns:
                if col in df.columns:
                    table_name_column = col
                    break
            
            # Find Key Columns column
            possible_key_columns = ['Key Columns', 'key_columns', 'Keys', 'keys']
            for col in possible_key_columns:
                if col in df.columns:
                    key_columns_column = col
                    break
            
            # Validate required columns
            if dataset_id_column is None:
                raise Exception("Required column 'Output ID' not found in inventory spreadsheet")
            
            if table_name_column is None:
                raise Exception("Required column 'Model Name' not found in inventory spreadsheet")
            
            if key_columns_column is None:
                raise Exception("Required column 'Key Columns' not found in inventory spreadsheet")
            
            # Filter out empty entries
            valid_df = df[
                df[dataset_id_column].notna() & 
                df[table_name_column].notna() & 
                df[key_columns_column].notna() &
                (df[dataset_id_column].astype(str).str.strip() != '') &
                (df[table_name_column].astype(str).str.strip() != '') &
                (df[key_columns_column].astype(str).str.strip() != '')
            ]
            
            self.logger.info(f"📋 Found {len(valid_df)} valid entries for comparison (with Output ID, Model Name, and Key Columns)")
            
            if len(valid_df) == 0:
                self.logger.info("✅ No valid entries found for comparison")
                return {"success": 0, "failed": 0, "total": 0, "errors": []}
            
            # Setup connections once
            if not self.setup_connections():
                raise Exception("Failed to setup connections")
            
            # Process comparisons
            successful_comparisons = []
            failed_comparisons = []
            errors = []
            
            for index, row in valid_df.iterrows():
                dataset_id = str(row[dataset_id_column]).strip()
                table_name = str(row[table_name_column]).strip()
                key_columns_str = str(row[key_columns_column]).strip()
                
                # Clean table name - remove .sql extension if present
                if table_name and table_name.lower().endswith('.sql'):
                    table_name = table_name[:-4]  # Remove last 4 characters (.sql)
                
                # Parse key columns (comma-separated)
                key_columns = [col.strip() for col in key_columns_str.split(',') if col.strip()]
                
                if not key_columns:
                    self.logger.info(f"⏭️  Skipping row {index + 2}: Invalid Key Columns format")
                    continue
                
                self.logger.info(f"🔄 Comparing dataset {dataset_id} vs table {table_name}")
                self.logger.info(f"🔑 Using key columns: {', '.join(key_columns)}")
                
                try:
                    # Generate comparison report
                    report = self.generate_report(
                        domo_dataset_id=dataset_id,
                        snowflake_table=table_name,
                        key_columns=key_columns,
                        sample_size=None,  # Use auto-calculation
                        transform_names=False,  # Don't transform by default for inventory
                        sampling_method=sampling_method
                    )
                    
                    # Check if comparison was successful
                    if report.get('errors'):
                        error_msg = f"Dataset {dataset_id}: Comparison failed with errors"
                        self.logger.error(f"❌ {error_msg}")
                        errors.extend([f"Dataset {dataset_id}: {err['error']}" for err in report['errors']])
                        failed_comparisons.append(dataset_id)
                    else:
                        success_msg = f"Dataset {dataset_id}: Comparison completed"
                        if report.get('overall_match'):
                            self.logger.info(f"✅ {success_msg} - Perfect match!")
                        else:
                            self.logger.warning(f"⚠️  {success_msg} - Discrepancies found")
                        successful_comparisons.append(dataset_id)
                    
                except Exception as e:
                    error_msg = f"Dataset {dataset_id}: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(dataset_id)
            
            # Summary
            total_comparisons = len(successful_comparisons) + len(failed_comparisons)
            
            self.logger.info("="*80)
            self.logger.info("📊 INVENTORY COMPARISON SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"✅ Successful comparisons: {len(successful_comparisons)}")
            self.logger.info(f"❌ Failed comparisons: {len(failed_comparisons)}")
            self.logger.info(f"📋 Total comparisons: {total_comparisons}")
            
            if successful_comparisons:
                self.logger.info(f"📈 Success rate: {len(successful_comparisons)/total_comparisons*100:.1f}%")
            
            if errors:
                self.logger.error("\n❌ Errors encountered:")
                for error in errors[:10]:  # Show first 10 errors
                    self.logger.error(f"   • {error}")
                if len(errors) > 10:
                    self.logger.error(f"   ... and {len(errors) - 10} more errors")
            
            return {
                "success": len(successful_comparisons),
                "failed": len(failed_comparisons),
                "total": total_comparisons,
                "errors": errors,
                "successful_datasets": successful_comparisons,
                "failed_datasets": failed_comparisons
            }
            
        except Exception as e:
            self.logger.error(f"❌ Inventory comparison failed: {e}")
            return {
                "success": 0,
                "failed": 0,
                "total": 0,
                "errors": [str(e)]
            }
    
    def _generate_executive_summary(self, report: Dict[str, Any], comparison: Optional[datacompy.Compare] = None) -> str:
        """
        Generate detailed executive summary for comparison results.
        
        Args:
            report: Complete comparison report dictionary
            comparison: Optional datacompy Compare object for direct data access
            
        Returns:
            String with detailed executive summary or error information
        """
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        dataset_id = report.get('domo_dataset_id', 'N/A')
        table_name = report.get('snowflake_table', 'N/A')
        
        # Get database and schema from environment configuration
        from .utils.common import get_env_config
        env_config = get_env_config()
        database = env_config.get('SNOWFLAKE_DATABASE', 'UNKNOWN_DB')
        schema = env_config.get('SNOWFLAKE_SCHEMA', 'UNKNOWN_SCHEMA')
        
        # Build full table path with real database and schema
        database_schema_table = f"{database.lower()}.{schema.lower()}.{table_name}"
        sampling_method = report.get('data_comparison', {}).get('sampling_method', 'Unknown')
        
        # Track failure reasons for debugging
        failure_reasons = []
        
        # Debug: Check if comparison object is available
        self.logger.info(f"🔍 Executive Summary Debug - Comparison object: {comparison is not None}")
        if comparison:
            self.logger.info(f"🔍 Has column_stats: {hasattr(comparison, 'column_stats')}")
            if hasattr(comparison, 'column_stats'):
                self.logger.info(f"🔍 Column_stats is not None: {comparison.column_stats is not None}")
                if comparison.column_stats is not None:
                    self.logger.info(f"🔍 Column_stats type: {type(comparison.column_stats)}")
                    if isinstance(comparison.column_stats, list):
                        self.logger.info(f"🔍 Column_stats length: {len(comparison.column_stats)}")
                    elif hasattr(comparison.column_stats, 'shape'):
                        self.logger.info(f"🔍 Column_stats shape: {comparison.column_stats.shape}")
        else:
            failure_reasons.append("DataComPy comparison object not available")
        
        # Check for major errors
        if report.get('errors'):
            error_summary = f"[{timestamp}] {sampling_method} - COMPARISON FAILED:\n"
            error_summary += f"{dataset_id} → {database_schema_table}\n"
            error_summary += f"❌ Errors encountered:\n"
            for error in report.get('errors', []):
                error_summary += f"  • {error}\n"
            if failure_reasons:
                error_summary += f"🔍 Debug info:\n"
                for reason in failure_reasons:
                    error_summary += f"  • {reason}\n"
            error_summary += f"📄 Check detailed logs for more information"
            return error_summary
        
        # Overall status
        if report.get('overall_match'):
            status = "PERFECT MATCH"
        else:
            status = "DISCREPANCIES"
        
        # Wrap the entire summary generation in try-catch
        try:
            summary_lines = [
                f"[{timestamp}] {sampling_method} - {status}:",
                f"{dataset_id} → {database_schema_table}"
            ]
            
            # Duplicate keys analysis - check for duplicate keys in match columns
            # This checks if there are duplicate key combinations in the key columns used for matching
            # (e.g., if using 'sku, asin' as keys, this detects multiple rows with same sku+asin combination)
            duplicate_keys_info = ""
            if comparison and hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
                try:
                    # Get join columns from the comparison object
                    join_columns = getattr(comparison, 'join_columns', [])
                    
                    if join_columns:
                        # Check for duplicate keys in Domo data (df1)
                        domo_duplicates = comparison.df1.duplicated(subset=join_columns, keep=False).sum()
                        # Check for duplicate keys in Snowflake data (df2)  
                        sf_duplicates = comparison.df2.duplicated(subset=join_columns, keep=False).sum()
                        
                        if domo_duplicates > 0 or sf_duplicates > 0:
                            # Show detailed duplicate key counts
                            duplicates_detail = []
                            if domo_duplicates > 0:
                                duplicates_detail.append(f"Domo: {domo_duplicates}")
                            if sf_duplicates > 0:
                                duplicates_detail.append(f"Snowflake: {sf_duplicates}")
                            
                            duplicate_keys_info = f"❌ Duplicate keys detected"
                            self.logger.info(f"🔍 Duplicate keys found - Domo: {domo_duplicates}, Snowflake: {sf_duplicates}")
                        else:
                            duplicate_keys_info = "✅ Duplicate keys: None found"
                            
                except Exception as e:
                    self.logger.warning(f"Could not check for duplicate keys: {e}")
                    duplicate_keys_info = "❌ Duplicate keys: Could not determine"
            
            # Add duplicate keys info to summary if available
            if duplicate_keys_info:
                summary_lines.append(duplicate_keys_info)

            # Row count analysis
            rows = report.get('row_count_comparison', {})
            if rows and not rows.get('error'):
                domo_rows = rows.get('domo_rows', 0)
                sf_rows = rows.get('snowflake_rows', 0)
                
                if domo_rows > 0:
                    error_pct = abs(sf_rows - domo_rows) / domo_rows * 100
                    
                    # Add visual indicator based on percentage difference
                    if error_pct <= 1.0:
                        row_indicator = "✅"
                    elif error_pct <= 5.0:
                        row_indicator = "⚠️"
                    else:
                        row_indicator = "❌"
                    
                    summary_lines.append(f"{row_indicator} Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,} ({error_pct:.2f}% difference)")
                else:
                    # Handle zero rows case
                    if sf_rows == 0:
                        summary_lines.append(f"✅ Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,}")
                    else:
                        summary_lines.append(f"❌ Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,}")
            else:
                failure_reasons.append(f"Row count comparison failed: {rows.get('error', 'No row data available') if rows else 'No row comparison data'}")
                summary_lines.append(f"❌ Could not compare row counts")
            
            # Schema analysis - use datacompy comparison object if available, otherwise fallback to schema comparison
            if comparison and hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
                # Extract column information directly from datacompy comparison
                domo_cols = len(comparison.df1.columns)
                sf_cols = len(comparison.df2.columns)
                
                # Get missing and extra columns directly from dataframes first
                domo_cols_set = set(comparison.df1.columns)
                sf_cols_set = set(comparison.df2.columns)
                missing_cols = list(domo_cols_set - sf_cols_set)
                extra_cols = list(sf_cols_set - domo_cols_set)
                
                # Special logic for batch columns (technical metadata columns)
                batch_columns = {'_batch_last_run_', '_batch_id_'}
                missing_cols_set = set(missing_cols)
                
                # Check if missing columns are only batch columns
                only_batch_missing = missing_cols_set.issubset(batch_columns)
                has_non_batch_missing = bool(missing_cols_set - batch_columns)
                
                # Add visual indicator for column count comparison with special logic
                if domo_cols == sf_cols:
                    col_indicator = "✅"
                elif len(missing_cols) <= 2 and only_batch_missing:
                    # Special case: only batch columns missing (≤2)
                    col_indicator = "✅"
                else:
                    col_indicator = "⚠️"
                
                summary_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
                
                # Missing columns with special batch column logic
                if missing_cols:
                    # Choose indicator based on whether non-batch columns are missing
                    missing_indicator = "❌" if has_non_batch_missing else "✅"
                    
                    if len(missing_cols) <= 5:
                        summary_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols)}")
                    else:
                        summary_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
                        summary_lines.append(f"... and {len(missing_cols) - 5} more missing columns")
                
                # Extra columns
                if extra_cols:
                    if len(extra_cols) <= 5:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols)}")
                    else:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols[:5])}")
                        summary_lines.append(f"... and {len(extra_cols) - 5} more extra columns")
            
            else:
                # Fallback to schema comparison from report
                schema = report.get('schema_comparison', {})
                if not schema.get('error'):
                    domo_cols = schema.get('domo_columns', 0)
                    sf_cols = schema.get('snowflake_columns', 0)
                
                # Get missing columns from schema report for fallback logic
                missing_cols = schema.get('missing_in_snowflake', [])
                
                # Special logic for batch columns (technical metadata columns)
                batch_columns = {'_batch_last_run_', '_batch_id_'}
                missing_cols_set = set(missing_cols)
                
                # Check if missing columns are only batch columns
                only_batch_missing = missing_cols_set.issubset(batch_columns)
                has_non_batch_missing = bool(missing_cols_set - batch_columns)
                
                # Add visual indicator for column count comparison with special logic
                if domo_cols == sf_cols:
                    col_indicator = "✅"
                elif len(missing_cols) <= 2 and only_batch_missing:
                    # Special case: only batch columns missing (≤2)
                    col_indicator = "✅"
                else:
                    col_indicator = "⚠️"
                
                if domo_cols > 0:
                    col_error_pct = abs(sf_cols - domo_cols) / domo_cols * 100
                    summary_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols} ({col_error_pct:.2f}% difference)")
                else:
                    summary_lines.append(f"{col_indicator} Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
                
                # Data type errors
                type_mismatches = schema.get('type_mismatches', [])
                if type_mismatches:
                    summary_lines.append(f"❌ Data Type Errors: {len(type_mismatches)} columns")
                    mismatch_details = []
                    for mismatch in type_mismatches[:5]:  # Show first 5
                        col_name = mismatch.get('column', 'unknown')
                        domo_type = mismatch.get('domo_type', 'unknown')
                        sf_type = mismatch.get('snowflake_type', 'unknown')
                        mismatch_details.append(f"{col_name} (Domo: {domo_type} vs SF: {sf_type})")
                    
                    summary_lines.append(f"Type Mismatches: {', '.join(mismatch_details)}")
                    if len(type_mismatches) > 5:
                        summary_lines.append(f"... and {len(type_mismatches) - 5} more type mismatches")
                
                # Missing columns with special batch column logic
                if missing_cols:
                    # Choose indicator based on whether non-batch columns are missing
                    missing_indicator = "❌" if has_non_batch_missing else "✅"
                    
                    if len(missing_cols) <= 5:
                        summary_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols)}")
                    else:
                        summary_lines.append(f"{missing_indicator} Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
                        summary_lines.append(f"... and {len(missing_cols) - 5} more missing columns")
                
                # Extra columns
                extra_cols = schema.get('extra_in_snowflake', [])
                if extra_cols:
                    if len(extra_cols) <= 5:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols)}")
                    else:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols[:5])}")
                        summary_lines.append(f"... and {len(extra_cols) - 5} more extra columns")
            
            # Data comparison analysis - simplified approach
            if comparison:
                try:
                    # Get basic row differences
                    missing_in_sf = len(comparison.df1_unq_rows) if hasattr(comparison, 'df1_unq_rows') else 0
                    extra_in_sf = len(comparison.df2_unq_rows) if hasattr(comparison, 'df2_unq_rows') else 0
                    
                    if missing_in_sf > 0:
                        summary_lines.append(f"❌ Rows Missing in Snowflake: {missing_in_sf}")
                    if extra_in_sf > 0:
                        summary_lines.append(f"⚠️ Extra Rows in Snowflake: {extra_in_sf}")
                    
                    # Get columns with different values - more specific approach
                    columns_with_diffs = []
                    self.logger.info(f"🔍 Starting column difference analysis...")
                    try:
                        # Use column_stats if available, but be more specific about what we count
                        if hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                            self.logger.info(f"🔍 Column_stats available, type: {type(comparison.column_stats)}")
                            if isinstance(comparison.column_stats, list):
                                self.logger.info(f"🔍 Processing list with {len(comparison.column_stats)} elements")
                                
                                # Debug: Show first few entries
                                if len(comparison.column_stats) > 0:
                                    self.logger.info(f"🔍 First column_stat entry: {comparison.column_stats[0]}")
                                
                                # Count all columns with problems (will be overwritten below)
                                pass
                                
                                # Get common columns
                                common_cols = set(comparison.df1.columns) & set(comparison.df2.columns)
                                self.logger.info(f"🔍 Common columns count: {len(common_cols)}")
                                
                                # Only count columns that have actual value differences, not just type mismatches
                                # Filter for columns that have real data differences (not just type differences)
                                columns_with_diffs = []
                                for stat in comparison.column_stats:
                                    col_name = stat.get('column')
                                    unequal_cnt = stat.get('unequal_cnt', 0)
                                    max_diff = stat.get('max_diff', 0)
                                    null_diff = stat.get('null_diff', 0)
                                    
                                    # Only include if:
                                    # 1. Column is in common columns
                                    # 2. Has unequal values > 0 
                                    # 3. AND either has max_diff > 0 OR null_diff > 0 (real value differences)
                                    if (col_name in common_cols and 
                                        unequal_cnt > 0 and 
                                        (max_diff > 0 or null_diff > 0)):
                                        columns_with_diffs.append(col_name)
                                        self.logger.info(f"🔍 Column '{col_name}': unequal={unequal_cnt}, max_diff={max_diff}, null_diff={null_diff}")
                                    elif col_name in common_cols and unequal_cnt > 0:
                                        self.logger.info(f"🔍 SKIPPED '{col_name}': unequal={unequal_cnt}, max_diff={max_diff}, null_diff={null_diff} (type-only difference)")
                                
                                # Log more details about what we found
                                all_unequal_cols = [s for s in comparison.column_stats if s.get('unequal_cnt', 0) > 0]
                                type_only_diffs = len(all_unequal_cols) - len(columns_with_diffs)
                                
                                self.logger.info(f"🔍 SUMMARY:")
                                self.logger.info(f"  📊 Total columns with unequal_cnt > 0: {len(all_unequal_cols)}")
                                self.logger.info(f"  📊 Columns with type-only differences: {type_only_diffs}")
                                self.logger.info(f"  📊 Columns with real value differences: {len(columns_with_diffs)}")
                                self.logger.info(f"🔍 Final filtered columns with differences: {columns_with_diffs}")
                                
                            elif hasattr(comparison.column_stats, 'shape'):  # DataFrame case
                                self.logger.info(f"🔍 Processing DataFrame with shape: {comparison.column_stats.shape}")
                                # Filter for common columns only and real value differences
                                common_cols = set(comparison.df1.columns) & set(comparison.df2.columns)
                                mask = (
                                    (comparison.column_stats['unequal_cnt'] > 0) & 
                                    (comparison.column_stats['column'].isin(common_cols)) &
                                    ((comparison.column_stats['max_diff'] > 0) | (comparison.column_stats['null_diff'] > 0))
                                )
                                columns_with_diffs = comparison.column_stats[mask]['column'].tolist()
                                self.logger.info(f"🔍 DataFrame filtered columns with real differences: {len(columns_with_diffs)}")
                        else:
                            self.logger.info(f"🔍 Column_stats not available or None")
                    except Exception as e:
                        self.logger.error(f"❌ Error analyzing column differences: {e}")
                        import traceback
                        self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
                        pass  # Ignore errors, will use fallback
                    
                    # Show column differences - check case 0 first
                    if len(columns_with_diffs) == 0:
                        # Case when len(columns_with_diffs) == 0
                        summary_lines.append("✅ All values matched")
                    elif len(columns_with_diffs) <= 5:
                        summary_lines.append(f"⚠️ Columns with Different Values: {', '.join(columns_with_diffs)}")
                    else:
                        summary_lines.append(f"⚠️ Columns with Different Values: {', '.join(columns_with_diffs[:5])}")
                        summary_lines.append(f"... and {len(columns_with_diffs) - 5} more columns with differences")
                    
                except Exception as e:
                    failure_reasons.append(f"Error analyzing comparison data: {str(e)}")
                    summary_lines.append(f"❌ Could not analyze data differences: {str(e)}")
            
            # Fallback: use report data if comparison object failed
            if not summary_lines or len([line for line in summary_lines if 'Different Values' in line or 'Missing' in line or 'Extra' in line or 'No column differences' in line]) == 0:
                data = report.get('data_comparison', {})
                if data and not data.get('error'):
                    if data.get('missing_in_snowflake', 0) > 0:
                        summary_lines.append(f"❌ Rows Missing in Snowflake: {data['missing_in_snowflake']}")
                    
                    if data.get('extra_in_snowflake', 0) > 0:
                        summary_lines.append(f"⚠️ Extra Rows in Snowflake: {data['extra_in_snowflake']}")
                    
                    if data.get('rows_with_differences', 0) > 0:
                        summary_lines.append(f"❌ Rows with Different Values: {data['rows_with_differences']} (column details unavailable)")
                
            # Add report file reference
            report_data = report.get('data_comparison', {})
            if report_data.get('report_file'):
                summary_lines.append(f"📄 Detailed Report: {report_data['report_file']}")
            
            return '\n'.join(summary_lines)
        
        except Exception as e:
            # If anything fails during summary generation, return error info
            failure_summary = f"[{timestamp}] {sampling_method} - SUMMARY GENERATION FAILED:\n"
            failure_summary += f"{dataset_id} → {database_schema_table}\n"
            failure_summary += f"💥 Unexpected error: {str(e)}\n"
            failure_summary += f"🔍 Error type: {type(e).__name__}\n"
            if failure_reasons:
                failure_summary += f"⚠️ Previous issues detected:\n"
                for reason in failure_reasons:
                    failure_summary += f"  • {reason}\n"
            failure_summary += f"📊 Comparison object available: {comparison is not None}\n"
            failure_summary += f"📊 Report keys: {list(report.keys()) if report else 'None'}\n"
            failure_summary += f"📄 Check logs and detailed report file for more information"
            
            self.logger.error(f"❌ Executive summary generation failed: {e}")
            return failure_summary
    
    def cleanup(self):
        """Clean up resources."""
        if self.snowflake_handler:
            self.snowflake_handler.cleanup() 