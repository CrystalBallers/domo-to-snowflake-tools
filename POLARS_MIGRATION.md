# Pandas to Polars Migration

This document describes the migration from pandas to polars in the Domo-to-Snowflake migration project.

## Overview

The project has been successfully migrated from pandas to polars for improved performance and memory efficiency. Polars is a fast DataFrame library implemented in Rust that provides better performance than pandas for most operations.

## Changes Made

### 1. Requirements Update
- **File**: `requirements-snowflake.txt`
- **Changes**:
  - Replaced `pandas>=2.0.0` with `polars>=0.20.0`
  - Updated `snowflake-connector-python[pandas]` to `snowflake-connector-python[polars]`

### 2. Snowflake Handler (`utils/snowflake.py`)
- **Imports**: Changed from `import pandas as pd` to `import polars as pl`
- **Snowflake Tools**: Updated from `write_pandas` to `write_polars`
- **DataFrame Operations**:
  - `df.fillna()` → `df.fill_null()`
  - `df.iloc[i:i+batch_size]` → `df.slice(i, batch_size)`
  - `df.values` → `df.iter_rows()`
  - `df.dtypes` → `df.schema`
  - Updated data type mapping for Snowflake schema generation

### 3. Domo Handler (`utils/domo.py`)
- **Imports**: Added polars import while keeping pandas for compatibility
- **DataFrame Operations**:
  - `pd.concat()` → `pl.concat(how="vertical")`
  - `df.dropna()` → `df.drop_nulls()`
  - `df.empty` → `df.is_empty()`
  - Updated data cleaning methods to use polars expressions
  - Added pandas-to-polars conversion: `pl.from_pandas(pandas_df)`

### 4. Data Type Mapping
Updated data type detection and conversion logic to work with polars:
- **Numeric types**: `pl.Int8`, `pl.Int16`, `pl.Int32`, `pl.Int64`, `pl.Float32`, `pl.Float64`
- **String types**: `pl.Utf8`
- **Boolean types**: `pl.Boolean`
- **Datetime types**: `pl.Datetime`

## Benefits of Polars

1. **Performance**: Polars is significantly faster than pandas for most operations
2. **Memory Efficiency**: Better memory usage, especially for large datasets
3. **Lazy Evaluation**: Supports lazy evaluation for complex operations
4. **Type Safety**: Better type system with explicit data types
5. **Modern API**: More intuitive and consistent API design

## Compatibility

- **Pandas Compatibility**: Pandas is still imported in `utils/domo.py` for the `to_dataframe` function from domo_utils
- **Data Conversion**: Pandas DataFrames are converted to polars using `pl.from_pandas()`
- **API Compatibility**: All existing functionality is preserved

## Installation

To install the updated dependencies:

```bash
pip install -r requirements-snowflake.txt
```

## Testing

Run the test script to verify the migration:

```bash
python test_polars_migration.py
```

This will test:
- Polars import and basic functionality
- Snowflake connector with polars support
- Utility module imports
- Pandas compatibility

## Performance Improvements

Expected performance improvements:
- **Data Loading**: 2-5x faster
- **Data Processing**: 3-10x faster for complex operations
- **Memory Usage**: 30-50% reduction
- **Large Dataset Handling**: Better performance with datasets >1GB

## Migration Notes

1. **Data Type Handling**: Polars has stricter type handling than pandas
2. **Null Values**: `fillna()` is now `fill_null()` in polars
3. **Indexing**: Polars uses different indexing methods than pandas
4. **Schema**: Polars uses `schema` instead of `dtypes` for column information

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure polars is installed: `pip install polars`
2. **Snowflake Connector**: Install with polars support: `pip install snowflake-connector-python[polars]`
3. **Data Type Errors**: Check that data types are compatible with polars

### Fallback Options

If polars causes issues, you can temporarily revert to pandas by:
1. Reverting the requirements file
2. Changing imports back to pandas
3. Updating DataFrame operations to use pandas syntax

## Future Considerations

1. **Lazy Evaluation**: Consider implementing lazy evaluation for complex data pipelines
2. **Streaming**: Polars supports streaming operations for very large datasets
3. **GPU Support**: Polars can leverage GPU acceleration for certain operations
4. **Arrow Integration**: Better integration with Apache Arrow for data interchange 