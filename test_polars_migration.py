#!/usr/bin/env python3
"""
Test script to verify polars migration.

This script tests the basic functionality of the migrated code to ensure
polars is working correctly instead of pandas.
"""

import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_polars_import():
    """Test that polars can be imported correctly."""
    try:
        import polars as pl
        logger.info("✅ Polars imported successfully")
        
        # Test basic polars functionality
        df = pl.DataFrame({
            "a": [1, 2, 3],
            "b": ["x", "y", "z"]
        })
        logger.info(f"✅ Created polars DataFrame: {df.shape}")
        
        return True
    except ImportError as e:
        logger.error(f"❌ Failed to import polars: {e}")
        return False

def test_snowflake_connector():
    """Test that snowflake connector with polars is available."""
    try:
        import snowflake.connector
        from snowflake.connector.polars_tools import write_polars
        logger.info("✅ Snowflake connector with polars support imported successfully")
        return True
    except ImportError as e:
        logger.error(f"❌ Failed to import snowflake connector with polars: {e}")
        logger.error("You may need to install: pip install snowflake-connector-python[polars]")
        return False

def test_utils_imports():
    """Test that utility modules can be imported."""
    try:
        from utils.snowflake import SnowflakeHandler
        from utils.domo import DomoHandler
        logger.info("✅ Utility modules imported successfully")
        return True
    except ImportError as e:
        logger.error(f"❌ Failed to import utility modules: {e}")
        return False

def test_pandas_compatibility():
    """Test that pandas is still available for compatibility."""
    try:
        import pandas as pd
        logger.info("✅ Pandas imported successfully (for compatibility)")
        return True
    except ImportError as e:
        logger.error(f"❌ Failed to import pandas: {e}")
        return False

def main():
    """Run all tests."""
    logger.info("🧪 Testing polars migration...")
    
    tests = [
        ("Polars Import", test_polars_import),
        ("Snowflake Connector", test_snowflake_connector),
        ("Utility Modules", test_utils_imports),
        ("Pandas Compatibility", test_pandas_compatibility),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Testing {test_name} ---")
        if test_func():
            passed += 1
        else:
            logger.error(f"❌ {test_name} failed")
    
    logger.info(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Polars migration successful.")
        return 0
    else:
        logger.error("❌ Some tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 