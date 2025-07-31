# Domo to Snowflake Migration Tools

A comprehensive suite of tools for migrating data from Domo to Snowflake, with additional functionalities for managing inventories from Google Sheets.

## 📋 Table of Contents

- [Features](#-features)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Available Commands](#-available-commands)
- [Usage Examples](#-usage-examples)
- [Project Structure](#-project-structure)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

## 🚀 Features

### Inventory Management
- ✅ Extract inventory data from Google Sheets
- ✅ Automatic conversion of dataflows to Snowflake SQL
- ✅ Bulk export of SQL files
- ✅ Placeholder generation when real translation is not available

### Data Migration
- ✅ Individual dataset migration from Domo to Snowflake
- ✅ Batch migration using JSON mapping files
- ✅ Automatic data load validation
- ✅ Connectivity tests for all sources

### Utilities
- ✅ Unified CLI for all operations
- ✅ Detailed logging with emojis for better readability
- ✅ Robust error handling
- ✅ Environment variable support

## 📋 Prerequisites

### Required Software
- Python 3.8 or higher
- pip (Python package manager)
- Internet access for APIs

### Credentials and Access
- **Google Sheets**: Service account with read permissions
- **Domo**: Developer token and instance name
- **Snowflake**: Account credentials with write permissions

## 🛠 Installation

### 1. Clone the Repository
```bash
git clone <repository-url>
cd Domo-to-snowflake-migration
```

### 2. Create Virtual Environment (Recommended)
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Install Additional Dependencies (if needed)
```bash
# For Google Sheets
pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2

# For data processing
pip install pandas polars

# For Snowflake
pip install snowflake-connector-python

# For environment variables
pip install python-dotenv
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Google Sheets
GOOGLE_SHEETS_CREDENTIALS_FILE=/path/to/your/service-account-key.json
MIGRATION_SPREADSHEET_ID=1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA
MIGRATION_SHEET_NAME=Migration
DATASET_SHEET_NAME=Datasets  # Used by datasets export functionality

# Domo API
DOMO_DEVELOPER_TOKEN=your_domo_developer_token
DOMO_INSTANCE=your_domo_instance_name

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role_name  # Optional: Role to assume (e.g., ANALYST, DEVELOPER)

# Optional
EXPORT_DIR=exported_sql  # Default directory for SQL exports
DOMO_TABLE_PREFIX=DOMO_  # Prefix for Snowflake table names (set to empty string to disable)
```

### Google Sheets Setup

1. **Create a Google Cloud Project** (if you don't have one)
2. **Enable Google Sheets API**
3. **Create a Service Account**:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Download the JSON key file
4. **Share your Google Sheet** with the service account email
5. **Set up credentials**:
   ```bash
   export GOOGLE_SHEETS_CREDENTIALS_FILE="/path/to/your/service-account-key.json"
   ```

**Important for Status Updates**: When using spreadsheet migration with automatic status updates, the service account needs **write permissions** to the Google Sheet. Make sure to:
- Share the spreadsheet with the service account email
- Give it "Editor" permissions (not just "Viewer")
- The service account will automatically update the Status column to "Migrated" for successful migrations

### Domo Configuration

1. **Get Developer Token**:
   - Go to your Domo instance
   - Navigate to Admin > Developer
   - Create a new token with necessary permissions

2. **Identify your Instance**:
   - Your instance is the part before `.domo.com` in your URL
   - Example: if your URL is `https://mycompany.domo.com`, your instance is `mycompany`

### Snowflake Configuration

1. **Account Credentials**:
   - Account identifier (format: `account_name.region.cloud_provider`)
   - Username and password with write permissions
   - Warehouse, database and destination schema

2. **Role Configuration** (Optional but Recommended):
   - Set `SNOWFLAKE_ROLE` to specify which role to assume when connecting
   - This is especially important if your user has multiple roles or needs specific privileges
   - Common roles: `ANALYST`, `DEVELOPER`, `SYSADMIN`, `ACCOUNTADMIN`
   - Example: `SNOWFLAKE_ROLE=SYSADMIN`
   
   **Note**: If you're getting "insufficient privileges" errors, make sure:
   - Your role has the necessary permissions to create tables and schemas
   - The role has access to the specified warehouse, database, and schema
   - You're using a role with appropriate privileges for your use case

## 🎯 Usage

The project uses a unified CLI through the `main.py` file:

```bash
python main.py <command> [options]
```

### Available Commands

#### 1. `inventory` - Inventory Management

Extract inventory data from Google Sheets and convert dataflows to SQL.

```bash
python main.py inventory [options]
```

**Options:**
- `--export-dir`: Directory to save SQL files (default: `exported_sql`)
- `--credentials`: Path to Google Sheets credentials file
- `--test-connection`: Test connection and show preview

#### 2. `migrate` - Data Migration

Migrate datasets from Domo to Snowflake.

```bash
python main.py migrate [options]
```

**Options:**
- `--dataset-id`: Domo dataset ID to migrate
- `--target-table`: Target Snowflake table name
- `--batch-file`: JSON file with dataset_id → table mappings
- `--from-spreadsheet`: Migrate datasets from Google Sheets Migration tab
- `--credentials`: Path to Google Sheets credentials file (for spreadsheet migration)
- `--spreadsheet-id`: Google Sheets spreadsheet ID (uses default if not specified)
- `--sheet-name`: Migration sheet tab name (default: Migration)
- `--test-connection`: Test Domo and Snowflake connections

#### 3. `datasets` - Dataset Management

Manage and export Domo datasets.

```bash
python main.py datasets [options]
```

**Options:**
- `--test-connection`: Test Domo connection
- `--export-to-spreadsheet`: Export all Domo datasets to Google Sheets
- `--list-local`: List all Domo datasets locally
- `--credentials`: Path to Google Sheets credentials file
- `--spreadsheet-id`: Google Sheets spreadsheet ID (uses default if not specified)
- `--sheet-name`: Sheet name for datasets (default: DomoDatasets)
- `--batch-size`: Number of datasets to fetch per batch (default: 100)

## 📚 Usage Examples

Examples:
    # Export inventory dataflows to SQL
    python main.py inventory --export-dir exported_sql
    
    # Test Google Sheets connection
    python main.py inventory --test-connection
    
    # Migrate single dataset
    python main.py migrate --dataset-id 12345 --target-table sales_data
    
    # Batch migrate datasets from JSON file
    python main.py migrate --batch-file dataset_mapping.json
    
    # Migrate datasets from Google Sheets Migration tab
    python main.py migrate --from-spreadsheet
    
    # Migrate from spreadsheet with custom credentials
    python main.py migrate --from-spreadsheet --credentials /path/to/creds.json
    
    # Migrate from custom spreadsheet and sheet
    python main.py migrate --from-spreadsheet --spreadsheet-id YOUR_SHEET_ID --sheet-name MyMigration
    
    # Test migration connections
    python main.py migrate --test-connection
    
    # Export all Domo datasets to Google Sheets
    python main.py datasets --export-to-spreadsheet
    
    # List all Domo datasets locally
    python main.py datasets --list-local
    
    # Test Domo connection
    python main.py datasets --test-connection
    
    # Use custom credentials file
    python main.py inventory --credentials /path/to/creds.json --export-dir output

### Migration Spreadsheet Structure

When using `--from-spreadsheet`, the system reads from a Google Sheets tab with the following structure:

| Dataset ID | Name | Status |
|------------|------|--------|
| dataset_001 | sales_monthly | Pending |
| dataset_002 | customer_data | Migrated |
| dataset_003 | inventory_levels | Failed |
| dataset_004 | financial_reports | Pending |

**Required Columns:**
- **Dataset ID**: The Domo dataset identifier
- **Name**: The target table name in Snowflake (will be cleaned: spaces→underscores, lowercase)
- **Status**: Migration status. Only rows where Status ≠ "Migrated" will be processed

**Alternative Column Names:**
- Dataset ID: `dataset_id`, `DatasetID`, `dataset`, `Dataset`, `ID`
- Name: `name`, `table_name`, `Table Name`, `TableName`, `target_table`
- Status: `status`, `migration_status`, `Migration Status`, `state`

## 📝 Project Structure

```
Domo-to-snowflake-migration/
├── main.py                     # Main CLI
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (create)
├── README.md                   # This file
├── tools/                      # Main modules
│   ├── __init__.py
│   ├── inventory_handler.py    # Inventory management
│   ├── domo_to_snowflake.py   # Data migration
│   └── utils/                  # Utilities
│       ├── __init__.py
│       ├── domo.py            # Domo API client
│       ├── snowflake.py       # Snowflake client
│       └── gsheets.py         # Google Sheets client
└── exported_sql/              # Output directory (created automatically)
```

## 🔧 Advanced Configuration

### Customize Spreadsheet Configuration

The system uses environment variables for Google Sheets configuration:

```env
# Default spreadsheet ID (can be overridden)
MIGRATION_SPREADSHEET_ID=1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA

# Default migration sheet name (can be overridden)
MIGRATION_SHEET_NAME=Migration
```

You can also override these at runtime:
```bash
# Use custom spreadsheet
python main.py migrate --from-spreadsheet --spreadsheet-id YOUR_SHEET_ID

# Use custom sheet name
python main.py migrate --from-spreadsheet --sheet-name MyMigrationTab
```

### Customize Dataflow Columns

The system automatically searches for these columns for dataflows:
- "Dataflow ID" (default)
- "dataflow"
- "Dataflow"
- "DataFlow"
- "dataflow_id"
- "Dataflow_ID"

### Control Table Name Prefix

By default, all Snowflake tables are created with a `DOMO_` prefix. You can control this behavior:

**Option 1: Environment Variable**
```bash
# Remove prefix completely
export DOMO_TABLE_PREFIX=""

# Use custom prefix
export DOMO_TABLE_PREFIX="MIGRATED_"

# Use default prefix
export DOMO_TABLE_PREFIX="DOMO_"
```

**Option 2: In .env file**
```env
# Remove prefix
DOMO_TABLE_PREFIX=

# Custom prefix
DOMO_TABLE_PREFIX=MIGRATED_

# Default prefix
DOMO_TABLE_PREFIX=DOMO_
```

**Examples:**
- With prefix: `DOMO_sales_data`
- Without prefix: `sales_data`
- Custom prefix: `MIGRATED_sales_data`

### Configure Timeouts

To adjust API timeouts:

```python
# In tools/inventory_handler.py, line ~200
timeout=60  # Increase if necessary
```

### Using MigrationManager for Custom Migrations

For advanced use cases, you can use the `MigrationManager` class directly:

```python
from tools.domo_to_snowflake import MigrationManager

# Efficient batch migration with connection reuse
with MigrationManager() as manager:
    # All datasets will use the same connections
    success1 = manager.migrate_dataset("dataset_001", "table_1")
    success2 = manager.migrate_dataset("dataset_002", "table_2")
    success3 = manager.migrate_dataset("dataset_003", "table_3")
```

This approach is much more efficient than calling `migrate_dataset()` individually, as it reuses connections instead of re-authenticating for each dataset.

## 🚨 Troubleshooting

### Common Issues

#### 1. Import Error
```
ImportError: cannot import name 'X' from 'tools.Y'
```
**Solution**: Verify all dependencies are installed:
```bash
pip install -r requirements.txt
```

#### 2. Google Sheets Credentials Not Found
```
❌ Credentials file not found
```
**Solution**: 
- Verify the path in `GOOGLE_SHEETS_CREDENTIALS_FILE`
- Ensure the JSON file exists and has read permissions

#### 3. Domo Connection Error
```
❌ Domo connection failed
```
**Solution**:
- Verify `DOMO_DEVELOPER_TOKEN` and `DOMO_INSTANCE`
- Confirm the token has necessary permissions
- Check that the instance is correct

#### 4. Snowflake Connection Error
```
❌ Snowflake connection failed
```
**Solution**:
- Verify all Snowflake variables
- Confirm the user has write permissions
- Check that the warehouse is active

#### 5. Dataflow Column Not Found
```
❌ No dataflow column found
```
**Solution**:
- Verify your Google Sheet has a column with a valid name
- Check the list of supported column names
- Use `--test-connection` to see available columns

### Detailed Logs

To get more information about errors:

```bash
# Enable detailed logging
export PYTHONPATH=.
python -v main.py inventory --test-connection
```

### Verify Configuration

```bash
# Test all connections
python main.py inventory --test-connection
python main.py migrate --test-connection
```

## 📊 Results Interpretation

### Inventory Export

The inventory command generates:
- **Real SQL files**: When translation with argo-utils-cli works
- **Placeholder files**: When translation fails (requires additional configuration)

**Example output:**
```
📊 Export Summary:
   Total dataflows: 25
   ✅ Real translations: 5
   ⚠️  Placeholder files: 20
   📁 Output directory: /path/to/exported_sql
```

### Migration Results

Migrations show:
- **Successful**: Data migrated and verified correctly
- **Failed**: Error in extraction, loading or verification

**Example output:**
```
📊 Batch Migration Summary:
   Total datasets: 10
   ✅ Successful: 8
   ❌ Failed: 2
```

## 🔄 Recommended Workflow

### 1. Initial Setup
```bash
# 1. Configure environment variables
cp .env.example .env
# Edit .env with your credentials

# 2. Test connections
python main.py inventory --test-connection
python main.py migrate --test-connection
```

### 2. Inventory Management
```bash
# 1. Export complete inventory
python main.py inventory --export-dir inventory_$(date +%Y%m%d)

# 2. Review generated files
ls -la inventory_*/
```

### 3. Data Migration
```bash
# 1. Test migration (single dataset)
python main.py migrate --dataset-id TEST_ID --target-table test_table

# 2. Batch migration (production)
python main.py migrate --batch-file production_mapping.json
```

## 🤝 Contributing

### Reporting Issues
1. Describe the problem in detail
2. Include relevant logs
3. Specify your configuration (Python version, OS, etc.)

### Proposing Improvements
1. Fork the repository
2. Create a branch for your feature
3. Implement changes with tests
4. Submit a Pull Request

### Local Development
```bash
# Install development dependencies
pip install -e .

# Run tests
python -m pytest tests/

# Check code
flake8 tools/
black tools/
```

## 📝 License

This project is under the MIT license. See the `LICENSE` file for more details.

## 📞 Support

For technical support:
1. Review this README
2. Check the Troubleshooting section
3. Search existing Issues
4. Create a new Issue if necessary

---

**Note**: This project is under active development. Features may change between versions. Always check the latest documentation before using in production. 

## Migration from Google Sheets

The tool can read migration datasets from a Google Sheets spreadsheet and automatically update the status after successful migrations.

### Spreadsheet Structure

Your Google Sheets should have a tab (default name: "Migration") with the following columns:

| Column Name | Description | Required | Example |
|-------------|-------------|----------|---------|
| Dataset ID | The Domo dataset ID to migrate | Yes | `12345` |
| Name | Dataset name (for reference) | No | `Sales Data` |
| Status | Migration status | No | `Pending`, `Migrated`, `Failed` |

**Note:** The tool is flexible with column names and will try to find columns with similar names (e.g., `dataset_id`, `DatasetID`, etc.).

### Usage

```bash
# Migrate from default spreadsheet (uses environment variables)
python main.py migrate --from-spreadsheet

# Migrate from custom spreadsheet
python main.py migrate --from-spreadsheet \
    --spreadsheet-id "your-spreadsheet-id" \
    --sheet-name "Migration" \
    --credentials "path/to/credentials.json"
```

### Environment Variables

You can set these environment variables to avoid passing parameters:

```bash
export MIGRATION_SPREADSHEET_ID="your-spreadsheet-id"
export MIGRATION_SHEET_NAME="Migration"
export GOOGLE_SHEETS_CREDENTIALS_FILE="path/to/credentials.json"
```

### Automatic Status Updates

When using spreadsheet migration, the tool will:

1. **Read** datasets where Status is not "Migrated"
2. **Migrate** each dataset to Snowflake
3. **Update** the Status column to "Migrated" for successful migrations
4. **Log** detailed progress and any errors

This allows you to:
- Track migration progress visually in the spreadsheet
- Re-run migrations safely (only pending datasets will be processed)
- Maintain a clear audit trail of migration status

### Example Workflow

1. **Prepare your spreadsheet** with Dataset ID, Name, and Status columns
2. **Set Status to "Pending"** for datasets you want to migrate
3. **Run the migration**:
   ```bash
   python main.py migrate --from-spreadsheet
   ```
4. **Check the results** - successful migrations will have Status updated to "Migrated"
5. **Review any errors** in the logs for failed migrations 