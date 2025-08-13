# 🤝 Contributing Guide

## 📋 Development Standards

### 🎯 File Placement Rules

| File Type | Location | Purpose |
|-----------|----------|---------|
| **Main scripts** | `tools/scripts/` | Reusable utility scripts |
| **Data analysis** | `data_analysis/` | Exploration and analysis scripts |
| **Shared utilities** | `tools/utils/` | Shared modules between scripts |
| **Results/Outputs** | `results/` | ALL generated files |
| **Logs** | `logs/` | Log files |
| **Documentation** | `docs/` | Specialized documentation |

### 🔧 Naming Conventions

#### **Python Scripts**
```python
# ✅ GOOD: Descriptive and action-oriented
generate_stg_files.py
extract_lineage.py  
deduplicate_dataflow_ids.py

# ❌ BAD: Ambiguous or too generic
file.py
script.py
utils.py
```

#### **Result Files**
```bash
# ✅ GOOD: With timestamp and description
results/
├── sql/stg_tables_20240801/
├── sql/translated/batch_20240801/
└── analysis_results_20240801.csv

# ❌ BAD: Without temporal context
output.csv
data.json
```

#### **Functions and Variables**
```python
# ✅ GOOD: Snake_case descriptive
def extract_dataflow_lineage(dataset_id):
    migration_status = "completed"
    dataflow_mapping = {}

# ❌ BAD: CamelCase or ambiguous
def ProcessData():
    status = "ok"
    data = {}
```

## 🚀 Script Development Standards

### **Standard Structure for Scripts**
```python
#!/usr/bin/env python3
"""
Clear description of script purpose.

Usage:
    python script_name.py [options]

Example:
    python generate_stg_files.py --output-dir results/sql/
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main execution function."""
    logger.info("🚀 Starting script execution...")
    # Script logic here
    logger.info("✅ Script completed successfully")

if __name__ == "__main__":
    main()
```

### **Organized Imports**
```python
# 1. Standard library
import os
import sys
import json
from pathlib import Path

# 2. Third party
import pandas as pd
import polars as pl

# 3. Local imports
from tools.utils.domo import DomoHandler
from tools.utils.snowflake import SnowflakeHandler
```

## 📝 Function Documentation

### **Required Docstrings**
```python
def process_dataflow_mapping(dataflow_ids: List[str], output_dir: str) -> bool:
    """
    Process dataflow mapping and generate SQL files.
    
    Args:
        dataflow_ids: List of dataflow IDs to process
        output_dir: Directory to save generated files
        
    Returns:
        bool: True if processing was successful, False otherwise
        
    Raises:
        ValueError: If dataflow_ids is empty
        FileNotFoundError: If output_dir doesn't exist
        
    Example:
        >>> success = process_dataflow_mapping(['123', '456'], 'results/sql/')
        >>> print(success)
        True
    """
```

## 🔍 Error Handling

### **Consistent Logging**
```python
# ✅ GOOD: Emojis and clear context
logger.info("🚀 Starting migration process...")
logger.warning("⚠️  Dataset 123 not found, skipping...")
logger.error("❌ Failed to connect to Snowflake: {error}")
logger.info("✅ Migration completed successfully")

# ❌ BAD: Without context
logger.info("Starting...")
logger.error("Error occurred")
```

### **Exception Handling**
```python
# ✅ GOOD: Specific with recovery
try:
    result = process_dataset(dataset_id)
except DatasetNotFoundError as e:
    logger.warning(f"⚠️  Skipping dataset {dataset_id}: {e}")
    continue
except ConnectionError as e:
    logger.error(f"❌ Connection failed: {e}")
    return False
except Exception as e:
    logger.error(f"❌ Unexpected error processing {dataset_id}: {e}")
    raise
```

## 📦 Dependency Management

### **Import Paths**
```python
# ✅ GOOD: Relative imports from project root
from tools.utils.common import setup_logging
from tools.scripts.extract_lineage import extract_dataset_lineage

# ❌ BAD: Absolute or hardcoded paths
sys.path.append('/Users/user/project/tools')
```

## 🧪 Configuration and Environment

### **Environment Variables**
```python
# ✅ GOOD: With defaults and validation
EXPORT_DIR = os.getenv("EXPORT_DIR", "results/sql/translated")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")

if not DATABASE:
    raise ValueError("SNOWFLAKE_DATABASE environment variable is required")

# ❌ BAD: Without defaults or validation
export_dir = os.getenv("EXPORT_DIR")  # Can be None
```

## 🎨 Code Formatting

### **Recommended Tools**
```bash
# Auto-formatting
black tools/ data_analysis/

# Linting
flake8 tools/ data_analysis/

# Type checking (optional)
mypy tools/
```

### **Configuration in pyproject.toml**
```toml
[tool.black]
line-length = 88
target-version = ['py38']
include = '\.pyi?$'
exclude = '''
/(
    \.eggs
  | \.git
  | \.venv
  | results
  | logs
)/
'''
```

## 🧪 Testing Standards

### **Test Structure**
```python
import pytest
from tools.scripts.generate_stg_files import generate_stg_files_from_dataframe

def test_generate_stg_files_valid_input():
    """Test STG file generation with valid input."""
    # Arrange
    df = create_test_dataframe()
    
    # Act
    result = generate_stg_files_from_dataframe(df)
    
    # Assert
    assert result is True
    assert os.path.exists("sql/stg/test_table.sql")
```

### **Test Naming**
```python
# ✅ GOOD: Descriptive test names
def test_migration_with_empty_dataset_should_skip()
def test_snowflake_connection_with_invalid_credentials_should_fail()
def test_csv_export_with_large_dataset_should_complete()

# ❌ BAD: Generic test names
def test_migration()
def test_connection()
```

## 📋 Pull Request Checklist

### **Before Submitting**
- [ ] ✅ Code follows naming conventions
- [ ] ✅ Functions have proper docstrings
- [ ] ✅ Error handling is implemented
- [ ] ✅ Logging uses emoji conventions
- [ ] ✅ Tests are written and passing
- [ ] ✅ Documentation is updated
- [ ] ✅ No sensitive data in code

### **PR Description Template**
```markdown
## 🎯 Purpose
Brief description of what this PR accomplishes.

## 🔄 Changes Made
- ✅ Added feature X
- ✅ Fixed bug Y
- ✅ Updated documentation Z

## 🧪 Testing
- [ ] Unit tests added/updated
- [ ] Manual testing completed
- [ ] Integration tests pass

## 📋 Checklist
- [ ] Code follows project conventions
- [ ] Documentation updated
- [ ] No breaking changes (or properly documented)
```

## 🚀 Development Workflow

### **Setting Up Development Environment**
```bash
# 1. Fork and clone repository
git clone https://github.com/your-username/Domo-to-snowflake-migration.git
cd Domo-to-snowflake-migration

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# 3. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # If exists

# 4. Install pre-commit hooks
pre-commit install
```

### **Making Changes**
```bash
# 1. Create feature branch
git checkout -b feature/new-awesome-feature

# 2. Make changes and test
python tools/scripts/project_maintenance.py check
python -m pytest tests/

# 3. Commit changes
git add .
git commit -m "✨ Add awesome new feature"

# 4. Push and create PR
git push origin feature/new-awesome-feature
```

## 🔄 Release Process

### **Version Bumping**
```bash
# Update version in setup.py or __init__.py
# Update CHANGELOG.md
# Create release commit
git commit -m "🔖 Release v1.2.0"
git tag v1.2.0
git push origin main --tags
```

## 📞 Getting Help

### **Development Questions**
1. Check existing issues and discussions
2. Review documentation in `docs/`
3. Ask in development channel (if available)
4. Create detailed issue with:
   - Environment details
   - Steps to reproduce
   - Expected vs actual behavior
   - Relevant logs

### **Code Review Guidelines**
- Focus on code clarity and maintainability
- Suggest improvements, don't just point out problems
- Check for security issues and performance implications
- Verify tests cover the main use cases

Thank you for contributing to the Domo to Snowflake Migration project! 🚀