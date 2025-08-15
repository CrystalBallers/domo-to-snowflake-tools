"""
Dataset comparison utilities for validating data migration between Domo and Snowflake.

This module provides the DatasetComparator class which uses datacompy to perform
comprehensive comparisons between Domo datasets and Snowflake tables.
"""

import os
import re
import logging
import math
from typing import List, Dict, Any, Optional
import pandas as pd
import datacompy

from .utils.common import transform_column_name, get_snowflake_table_full_name, setup_dual_connections, get_env_config


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
                self.logger.info("🔄 Applying column name transformation...")
                transformed_domo_columns = {}
                for original_name, col_type in domo_columns.items():
                    transformed_name = transform_column_name(original_name)
                    transformed_domo_columns[transformed_name] = col_type
                domo_columns = transformed_domo_columns
                
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
            else:
                raise Exception("Failed to get Snowflake schema")
                
        except Exception as e:
            self.add_error("Snowflake Schema", "Could not get Snowflake schema", str(e))
            return self._get_error_schema_result(len(domo_columns))
        
        # Compare columns
        domo_cols_set = set(domo_columns.keys())
        sf_cols_set = set(sf_columns.keys())
        
        missing_in_sf = list(domo_cols_set - sf_cols_set)
        extra_in_sf = list(sf_cols_set - domo_cols_set)
        common_cols = domo_cols_set & sf_cols_set
        
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
                           transform_names: bool = False, schema_comparison: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Compare data samples using datacompy.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample (auto-calculated if None)
            transform_names: Whether to apply column name transformation
            schema_comparison: Schema comparison results to include in report
            
        Returns:
            Dictionary with data comparison results
        """
        self.logger.info("🔍 Comparing data samples...")
        
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
        
        # Get Domo sample using DomoHandler (more robust)
        try:
            # Build query for sample
            key_cols_str = ', '.join(key_columns)
            sample_query = f"SELECT * FROM table ORDER BY {key_cols_str} LIMIT {sample_size}"
            
            # Use DomoHandler for data extraction (now returns pandas directly)
            domo_df = self.domo_handler.extract_data(
                dataset_id=domo_dataset_id, 
                query=sample_query,
                enable_auto_type_conversion=True  # Keep original types for comparison
            )
            
            if domo_df is None or domo_df.empty:
                raise Exception("No data returned from Domo")
            
            # Apply column transformation if enabled
            if transform_names:
                original_columns = domo_df.columns.tolist()
                transformed_columns = [transform_column_name(col) for col in original_columns]
                domo_df.columns = transformed_columns
                key_columns = [transform_column_name(col) for col in key_columns]
                
        except Exception as e:
            self.add_error("Domo Sample", "Could not get Domo sample", str(e))
            return self._get_error_data_result(sample_size)
        
        # Get Snowflake sample
        try:
            key_cols_str = ', '.join(key_columns)
            sf_query = f"SELECT * FROM {snowflake_table} ORDER BY {key_cols_str} LIMIT {sample_size}"
            sf_df = self.snowflake_handler.execute_query(sf_query)
            
            if sf_df is None or sf_df.empty:
                raise Exception("Failed to get Snowflake sample")
                
        except Exception as e:
            self.add_error("Snowflake Sample", "Could not get Snowflake sample", str(e))
            return self._get_error_data_result(sample_size, len(domo_df))
        
        # Use datacompy for comparison
        try:
            self.logger.info(f"📊 Domo DataFrame shape: {domo_df.shape}, columns: {list(domo_df.columns)}")
            self.logger.info(f"📊 Snowflake DataFrame shape: {sf_df.shape}, columns: {list(sf_df.columns)}")
            self.logger.info(f"📊 Key columns for comparison: {key_columns}")
            
            # Normalize data types for key columns to ensure compatibility
            for col in key_columns:
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
                    self.logger.warning(f"Key column '{col}' not found in both DataFrames. Domo: {domo_col}, Snowflake: {sf_col}")
            
            comparison = datacompy.Compare(
                domo_df,
                sf_df,
                join_columns=key_columns,
                df1_name='Domo',
                df2_name='Snowflake'
            )
            
            # Save detailed report
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            # Use the provided Snowflake table name as base (now expected to be the Model Name)
            safe_base = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(snowflake_table).strip()) or "report"
            report_filename = f"{safe_base}_{timestamp}.txt"
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(f"COMPARISON REPORT\n")
                f.write(f"Domo Dataset: {domo_dataset_id}\n")
                f.write(f"Snowflake Table: {snowflake_table}\n")
                f.write(f"Key Columns: {', '.join(key_columns)}\n")
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
                'comparison_object': comparison  # Add comparison object for executive summary
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
    
    def generate_report(self, domo_dataset_id: str, snowflake_table: str, 
                       key_columns: List[str], sample_size: Optional[int] = None,
                       transform_names: bool = False) -> Dict[str, Any]:
        """
        Generate complete comparison report.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample
            transform_names: Whether to apply column name transformation
            
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
                                                   key_columns, sample_size, transform_names, schema_comparison)
        
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
    
    def compare_from_spreadsheet(self, spreadsheet_id: str, sheet_name: str = "QA - Test",
                                credentials_path: str = None) -> Dict[str, Any]:
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
            sheet_name: Sheet name containing comparison configurations
            credentials_path: Path to Google Sheets credentials file
            
        Returns:
            Dictionary with comparison results summary
        """
        try:
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
            df = pd.DataFrame(rows, columns=headers)
            
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
                
                # Validate required fields
                if pd.isna(dataset_id) or str(dataset_id).strip() == '':
                    error_msg = f"Row {index + 2}: Empty Output ID"
                    self.logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(f"Row {index + 2}")
                    continue
                
                if pd.isna(table_name) or str(table_name).strip() == '':
                    error_msg = f"Row {index + 2}: Empty Table Name"
                    self.logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(f"Row {index + 2}")
                    continue
                
                if pd.isna(key_columns_str) or str(key_columns_str).strip() == '':
                    error_msg = f"Row {index + 2}: Empty Key Columns"
                    self.logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(f"Row {index + 2}")
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
                        transform_names=transform_columns
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
                        
                        # Update notes in spreadsheet if notes column exists
                        if notes_column:
                            try:
                                # Generate executive summary (try to get comparison object from report)
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
    
    def compare_from_inventory(self, credentials_path: str = None) -> Dict[str, Any]:
        """
        Compare datasets from the existing inventory spreadsheet.
        
        Uses the same spreadsheet and sheet as the inventory system with columns:
        - Output ID: Domo dataset ID
        - Model Name: Snowflake table name  
        - Key Columns: Comma-separated list of key columns
        
        Args:
            credentials_path: Path to Google Sheets credentials file
            
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
            df = pd.DataFrame(rows, columns=headers)
            
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
                
                # Parse key columns (comma-separated)
                key_columns = [col.strip() for col in key_columns_str.split(',') if col.strip()]
                
                if not key_columns:
                    error_msg = f"Row {index + 2}: Invalid Key Columns format"
                    self.logger.warning(f"⚠️  {error_msg}")
                    errors.append(error_msg)
                    failed_comparisons.append(f"Row {index + 2}")
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
                        transform_names=False  # Don't transform by default for inventory
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
            String with detailed executive summary
        """
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        dataset_id = report.get('domo_dataset_id', 'N/A')
        table_name = report.get('snowflake_table', 'N/A')
        
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
        
        # Overall status
        if report.get('errors'):
            status = "❌ ERRORS"
        elif report.get('overall_match'):
            status = "✅ PERFECT MATCH"
        else:
            status = "⚠️ DISCREPANCIES"
        
        summary_lines = [f"[{timestamp}] {status}: {dataset_id} → {table_name}"]
        
        # Row count analysis
        rows = report.get('row_count_comparison', {})
        if rows:
            domo_rows = rows.get('domo_rows', 0)
            sf_rows = rows.get('snowflake_rows', 0)
            
            if domo_rows > 0:
                error_pct = abs(sf_rows - domo_rows) / domo_rows * 100
                summary_lines.append(f"Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,} ({error_pct:.2f}% difference)")
            else:
                summary_lines.append(f"Rows: Domo {domo_rows:,} vs Snowflake {sf_rows:,}")
        
        # Schema analysis - use datacompy comparison object if available, otherwise fallback to schema comparison
        if comparison and hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
            # Extract column information directly from datacompy comparison
            domo_cols = len(comparison.df1.columns)
            sf_cols = len(comparison.df2.columns)
            
            summary_lines.append(f"Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
            
            # Get missing and extra columns directly from dataframes
            domo_cols_set = set(comparison.df1.columns)
            sf_cols_set = set(comparison.df2.columns)
            missing_cols = list(domo_cols_set - sf_cols_set)
            extra_cols = list(sf_cols_set - domo_cols_set)
            
            # Missing columns
            if missing_cols:
                if len(missing_cols) <= 5:
                    summary_lines.append(f"❌ Missing Columns in Snowflake: {', '.join(missing_cols)}")
                else:
                    summary_lines.append(f"❌ Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
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
                
                if domo_cols > 0:
                    col_error_pct = abs(sf_cols - domo_cols) / domo_cols * 100
                    summary_lines.append(f"Columns: Domo {domo_cols} vs Snowflake {sf_cols} ({col_error_pct:.2f}% difference)")
                else:
                    summary_lines.append(f"Columns: Domo {domo_cols} vs Snowflake {sf_cols}")
                
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
                
                # Missing columns
                missing_cols = schema.get('missing_in_snowflake', [])
                if missing_cols:
                    if len(missing_cols) <= 5:
                        summary_lines.append(f"❌ Missing Columns in Snowflake: {', '.join(missing_cols)}")
                    else:
                        summary_lines.append(f"❌ Missing Columns in Snowflake: {', '.join(missing_cols[:5])}")
                        summary_lines.append(f"... and {len(missing_cols) - 5} more missing columns")
                
                # Extra columns
                extra_cols = schema.get('extra_in_snowflake', [])
                if extra_cols:
                    if len(extra_cols) <= 5:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols)}")
                    else:
                        summary_lines.append(f"⚠️ Extra Columns in Snowflake: {', '.join(extra_cols[:5])}")
                        summary_lines.append(f"... and {len(extra_cols) - 5} more extra columns")
        
        # Data comparison analysis - use datacompy comparison object if available
        if comparison:
            # Extract data differences directly from datacompy comparison
            try:
                missing_in_sf = len(comparison.df1_unq_rows) if hasattr(comparison, 'df1_unq_rows') else 0
                extra_in_sf = len(comparison.df2_unq_rows) if hasattr(comparison, 'df2_unq_rows') else 0
                common_rows = len(comparison.intersect_rows) if hasattr(comparison, 'intersect_rows') else 0
                
                # Calculate rows with differences (common rows that don't match)
                total_compared = missing_in_sf + extra_in_sf + common_rows
                matching_rows = common_rows if comparison.matches() else 0
                rows_with_differences = common_rows - matching_rows if not comparison.matches() else 0
                
                # Report differences
                if missing_in_sf > 0:
                    summary_lines.append(f"❌ Rows Missing in Snowflake: {missing_in_sf}")
                
                if extra_in_sf > 0:
                    summary_lines.append(f"⚠️ Extra Rows in Snowflake: {extra_in_sf}")
                
                # Get columns with unequal values using datacompy's column statistics  
                columns_with_diffs = []
                self.logger.info(f"🔍 Extracting columns with unequal values from datacompy comparison...")
                
                try:
                    # Method 1: Parse the comparison report to get accurate match percentages
                    comparison_report = str(comparison.report())
                    lines = comparison_report.split('\n')
                    
                    # Look for lines that show column match percentages
                    for line in lines:
                        if ' / ' in line and '(' in line and '%) match' in line:
                            # Extract column name and percentage
                            # Format: "column_name: X / Y (Z.ZZ%) match"
                            try:
                                parts = line.split(':')
                                if len(parts) >= 2:
                                    col_name = parts[0].strip()
                                    match_info = parts[1].strip()
                                    
                                    # Extract percentage
                                    if '(' in match_info and '%) match' in match_info:
                                        percentage_part = match_info.split('(')[1].split('%)')[0]
                                        percentage = float(percentage_part)
                                        
                                        # Only include columns with less than 100% match
                                        if percentage < 100.0:
                                            columns_with_diffs.append(col_name)
                                            self.logger.info(f"🔍 Column '{col_name}' has {percentage:.2f}% match (< 100%)")
                                        else:
                                            self.logger.debug(f"🔍 Column '{col_name}' has {percentage:.2f}% match (perfect)")
                            except Exception as parse_error:
                                self.logger.debug(f"⚠️ Could not parse line: {line} - {parse_error}")
                    
                    self.logger.info(f"🔍 Found {len(columns_with_diffs)} columns with unequal values: {columns_with_diffs}")
                    
                    # Method 2: Fallback using column_stats if parsing failed
                    if not columns_with_diffs and hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                        try:
                            self.logger.info(f"🔍 Fallback: Using column_stats: {type(comparison.column_stats)}")
                            
                            if isinstance(comparison.column_stats, list):
                                # If column_stats is a list of dictionaries
                                columns_with_diffs = [
                                    stat['column'] for stat in comparison.column_stats 
                                    if stat.get('unequal_cnt', 0) > 0
                                ]
                                self.logger.info(f"🔍 Found {len(columns_with_diffs)} columns with unequal_cnt > 0: {columns_with_diffs}")
                        except Exception as e:
                            self.logger.warning(f"❌ Could not extract differing columns from column_stats: {e}")
                
                except Exception as e:
                    self.logger.warning(f"❌ Could not extract columns with unequal values: {e}")
                    self.logger.warning(f"❌ Exception type: {type(e).__name__}")
                
                # Method 2: Fallback - Parse the comparison report text
                if not columns_with_diffs:
                    try:
                        self.logger.info(f"🔍 Fallback: Parsing comparison report text...")
                        comparison_report = str(comparison.report())
                        self.logger.info(f"🔍 Generated comparison report, length: {len(comparison_report)}")
                        
                        # Look for "Columns with Unequal Values or Types" section
                        if "Columns with Unequal Values or Types" in comparison_report:
                            lines = comparison_report.split('\n')
                            unequal_cols = []
                            in_unequal_section = False
                            
                            for line in lines:
                                if "Columns with Unequal Values or Types" in line:
                                    in_unequal_section = True
                                    continue
                                elif in_unequal_section:
                                    if line.strip() == "" or line.startswith("Sample Rows") or line.startswith("---"):
                                        break
                                    elif line.strip() and not line.startswith("Column ") and not line.startswith("----"):
                                        # Extract column name (first word before space)
                                        parts = line.strip().split()
                                        if parts and not parts[0] in ['Column', '----']:
                                            col_name = parts[0]
                                            if col_name and col_name not in unequal_cols:
                                                unequal_cols.append(col_name)
                            
                            columns_with_diffs = unequal_cols
                            self.logger.info(f"🔍 Extracted columns from report text: {columns_with_diffs}")
                        else:
                            self.logger.warning(f"⚠️ 'Columns with Unequal Values or Types' section not found in report")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Could not parse comparison report: {e}")
                
                # Method 3: Last resort - Use intersect columns if there are differences
                if not columns_with_diffs and rows_with_differences > 0:
                    try:
                        if hasattr(comparison, 'intersect_columns'):
                            intersect_cols = list(comparison.intersect_columns())
                            self.logger.info(f"🔍 Last resort: Using intersect columns: {len(intersect_cols)} columns")
                            columns_with_diffs = intersect_cols[:10]  # Limit to first 10
                        elif hasattr(comparison, 'df1') and hasattr(comparison, 'df2'):
                            # Get common columns manually
                            common_cols = list(set(comparison.df1.columns) & set(comparison.df2.columns))
                            columns_with_diffs = common_cols[:10]  # Limit to first 10
                            self.logger.info(f"🔍 Last resort: Using common columns: {columns_with_diffs}")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Last resort method failed: {e}")
                
                if columns_with_diffs:
                    if len(columns_with_diffs) <= 5:
                        summary_lines.append(f"❌ Columns with Different Values: {', '.join(columns_with_diffs)}")
                    else:
                        summary_lines.append(f"❌ Columns with Different Values: {', '.join(columns_with_diffs[:5])}")
                        summary_lines.append(f"... and {len(columns_with_diffs) - 5} more columns with differences")
                elif rows_with_differences > 0:
                    # Fallback: if we can't get column names but know there are differences
                    self.logger.warning(f"⚠️ Falling back to row count - could not extract column names")
                    summary_lines.append(f"❌ Rows with Different Values: {rows_with_differences} (column details unavailable)")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Could not extract data differences from comparison object: {e}")
                # Fallback to report data
                data = report.get('data_comparison', {})
                if not data.get('error'):
                    if data.get('missing_in_snowflake', 0) > 0:
                        summary_lines.append(f"❌ Rows Missing in Snowflake: {data['missing_in_snowflake']}")
                    
                    if data.get('extra_in_snowflake', 0) > 0:
                        summary_lines.append(f"⚠️ Extra Rows in Snowflake: {data['extra_in_snowflake']}")
                    
                    if data.get('rows_with_differences', 0) > 0:
                        summary_lines.append(f"❌ Rows with Different Values: {data['rows_with_differences']}")
        else:
            # Fallback to data comparison from report
            data = report.get('data_comparison', {})
            if not data.get('error'):
                # Rows with differences
                if data.get('missing_in_snowflake', 0) > 0:
                    summary_lines.append(f"❌ Rows Missing in Snowflake: {data['missing_in_snowflake']}")
                
                if data.get('extra_in_snowflake', 0) > 0:
                    summary_lines.append(f"⚠️ Extra Rows in Snowflake: {data['extra_in_snowflake']}")
                
                # Try to extract column details from report file if available
                if data.get('rows_with_differences', 0) > 0:
                    report_file = data.get('report_file')
                    if report_file:
                        try:
                            # Read the detailed report to extract columns with unequal values
                            with open(report_file, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            # Look for "Columns with Unequal Values or Types" section
                            if "Columns with Unequal Values or Types" in content:
                                lines = content.split('\n')
                                unequal_cols = []
                                in_unequal_section = False
                                
                                for line in lines:
                                    if "Columns with Unequal Values or Types" in line:
                                        in_unequal_section = True
                                        continue
                                    elif in_unequal_section:
                                        if line.strip() == "" or line.startswith("Sample Rows") or line.startswith("---"):
                                            break
                                        elif line.strip() and not line.startswith("Column ") and not line.startswith("----"):
                                            # Extract column name (first word before space)
                                            parts = line.strip().split()
                                            if parts and not parts[0] in ['Column', '----']:
                                                col_name = parts[0]
                                                if col_name and col_name not in unequal_cols:
                                                    unequal_cols.append(col_name)
                                
                                if unequal_cols:
                                    if len(unequal_cols) <= 5:
                                        summary_lines.append(f"❌ Columns with Different Values: {', '.join(unequal_cols)}")
                                    else:
                                        summary_lines.append(f"❌ Columns with Different Values: {', '.join(unequal_cols[:5])}")
                                        summary_lines.append(f"... and {len(unequal_cols) - 5} more columns with differences")
                                else:
                                    # If we can't extract columns but know there are differences
                                    summary_lines.append("❌ Columns with Different Values detected (see detailed report)")
                            else:
                                # If section not found but we know there are differences
                                summary_lines.append("❌ Columns with Different Values detected (see detailed report)")
                        
                        except Exception as e:
                            # If we can't read the file, just note that there are differences
                            summary_lines.append("❌ Columns with Different Values detected (see detailed report)")
                    else:
                        summary_lines.append("❌ Columns with Different Values detected (see detailed report)")
        
        # Add report file reference
        report_data = report.get('data_comparison', {})
        if report_data.get('report_file'):
            summary_lines.append(f"📄 Detailed Report: {report_data['report_file']}")
        
        return '\n'.join(summary_lines)
    
    def cleanup(self):
        """Clean up resources."""
        if self.snowflake_handler:
            self.snowflake_handler.cleanup() 