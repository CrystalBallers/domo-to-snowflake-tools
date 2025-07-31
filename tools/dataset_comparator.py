"""
Dataset comparison utilities for validating data migration between Domo and Snowflake.

This module provides the DatasetComparator class which uses datacompy to perform
comprehensive comparisons between Domo datasets and Snowflake tables.
"""

import os
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
            domo_columns = {col['name']: col['type'] for col in domo_schema['columns']}
            
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
                sf_schema_df = sf_result.to_pandas()
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
                sf_count = int(sf_result.to_pandas().iloc[0]['ROW_COUNT'])
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
                           transform_names: bool = False) -> Dict[str, Any]:
        """
        Compare data samples using datacompy.
        
        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name
            key_columns: List of key columns for comparison
            sample_size: Number of rows to sample (auto-calculated if None)
            transform_names: Whether to apply column name transformation
            
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
            
            # Use DomoHandler for data extraction
            domo_df_polars = self.domo_handler.extract_data(
                dataset_id=domo_dataset_id, 
                query=sample_query,
                enable_auto_type_conversion=False  # Keep original types for comparison
            )
            
            if domo_df_polars is None or domo_df_polars.is_empty():
                raise Exception("No data returned from Domo")
            
            # Convert polars to pandas for datacompy compatibility
            domo_df = domo_df_polars.to_pandas()
            
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
            key_cols_str = ', '.join([f'"{col}"' for col in key_columns])
            sf_query = f"SELECT * FROM {snowflake_table} ORDER BY {key_cols_str} LIMIT {sample_size}"
            sf_result = self.snowflake_handler.execute_query(sf_query)
            
            if sf_result is not None:
                sf_df = sf_result.to_pandas()
            else:
                raise Exception("Failed to get Snowflake sample")
                
        except Exception as e:
            self.add_error("Snowflake Sample", "Could not get Snowflake sample", str(e))
            return self._get_error_data_result(sample_size, len(domo_df))
        
        # Use datacompy for comparison
        try:
            comparison = datacompy.Compare(
                domo_df,
                sf_df,
                join_columns=key_columns,
                df1_name='Domo',
                df2_name='Snowflake'
            )
            
            # Save detailed report
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"comparison_report_{timestamp}.txt"
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(f"COMPARISON REPORT\n")
                f.write(f"Domo Dataset: {domo_dataset_id}\n")
                f.write(f"Snowflake Table: {snowflake_table}\n")
                f.write(f"Key Columns: {', '.join(key_columns)}\n")
                f.write(f"Transform Applied: {transform_names}\n")
                f.write(f"Timestamp: {pd.Timestamp.now().isoformat()}\n")
                f.write("="*80 + "\n")
                f.write(comparison.report())
            
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
                'report_file': report_filename
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
                                                   key_columns, sample_size, transform_names)
        
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
    
    def compare_from_spreadsheet(self, spreadsheet_id: str, sheet_name: str = "Comparison",
                                credentials_path: str = None) -> Dict[str, Any]:
        """
        Compare multiple datasets from Google Sheets configuration.
        
        Expected columns in spreadsheet:
        - Dataset ID: Domo dataset ID
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
            
            # Find Dataset ID column
            possible_dataset_id_columns = ['Dataset ID', 'dataset_id', 'Domo Dataset ID', 'domo_dataset_id', 'ID', 'id']
            for col in possible_dataset_id_columns:
                if col in df.columns:
                    dataset_id_column = col
                    break
            
            # Find Table Name column
            possible_table_columns = ['Table Name', 'table_name', 'Snowflake Table', 'snowflake_table', 'Table', 'table']
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
            
            # Validate required columns
            if dataset_id_column is None:
                raise Exception("Required column 'Dataset ID' not found in spreadsheet")
            
            if table_name_column is None:
                raise Exception("Required column 'Table Name' not found in spreadsheet")
            
            if key_columns_column is None:
                raise Exception("Required column 'Key Columns' not found in spreadsheet")
            
            # Filter rows where Status is not "Completed" (if status column exists)
            if status_column and status_column in df.columns:
                df[status_column] = df[status_column].fillna('Pending')
                df[status_column] = df[status_column].astype(str)
                pending_df = df[~df[status_column].str.contains('Completed|Complete', case=False, na=False)]
                self.logger.info(f"📋 Found {len(pending_df)} comparisons pending (excluding completed)")
            else:
                pending_df = df
                self.logger.info(f"📋 No status column found, processing all {len(pending_df)} comparisons")
            
            if len(pending_df) == 0:
                self.logger.info("✅ No comparisons pending")
                return {"success": 0, "failed": 0, "total": 0, "errors": []}
            
            # Setup connections once
            if not self.setup_connections():
                raise Exception("Failed to setup connections")
            
            # Process comparisons
            successful_comparisons = []
            failed_comparisons = []
            errors = []
            
            for index, row in pending_df.iterrows():
                dataset_id = row[dataset_id_column]
                table_name = row[table_name_column]
                key_columns_str = row[key_columns_column]
                
                # Validate required fields
                if pd.isna(dataset_id) or str(dataset_id).strip() == '':
                    error_msg = f"Row {index + 2}: Empty Dataset ID"
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
                        
                        # Update status in spreadsheet if status column exists
                        if status_column:
                            try:
                                status_text = "Perfect Match" if report.get('overall_match') else "Discrepancies Found"
                                cell_range = f"{sheet_name}!{chr(65 + df.columns.get_loc(status_column))}{index + 2}"
                                gsheets_client.write_range(spreadsheet_id, cell_range, [[status_text]])
                            except Exception as e:
                                self.logger.warning(f"⚠️  Could not update status for row {index + 2}: {e}")
                    
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
            spreadsheet_id = os.getenv("MIGRATION_SPREADSHEET_ID")
            sheet_name = os.getenv("INVENTORY_SHEET_NAME", "Inventory")
            
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
    
    def cleanup(self):
        """Clean up resources."""
        if self.snowflake_handler:
            self.snowflake_handler.cleanup() 