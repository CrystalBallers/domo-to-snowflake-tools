# Domo to Snowflake Migration Tools

A comprehensive suite of tools for migrating data from Domo to Snowflake, with additional functionalities for managing inventories from Google Sheets.

## 📋 Table of Contents


- [Features](#-features)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Usage Examples](#-usage-examples)
- [Project Structure](#-project-structure)
- [Troubleshooting](#-troubleshooting)



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
- Python 3.11
- pip (Python package manager)
- Internet access for APIs

### Credentials and Access
- **Google Sheets**: Service account with read permissions
- **Domo**: Developer token and instance name
- **Snowflake**: Account credentials with write permissions

## 🛠 Run it locally

The tool depends on [`argo-utils-cli`](https://github.com/CrystalBallers/argo-utils-cli)
(which provides the `domo_utils` package). Clone it into the project root first — both
the Docker and the virtualenv paths below install it from there:

```bash
git clone <this-repo-url> domo-to-snowflake-tools
cd domo-to-snowflake-tools
git clone https://github.com/CrystalBallers/argo-utils-cli.git argo-utils-cli
cp .env.example .env          # then edit .env with your credentials (see Configuration)
```

### Option A — Docker (recommended)

No local Python needed. The image is Python 3.11 and installs `argo-utils-cli` +
`requirements.txt` for you.

```bash
# 1. Put your Google service-account JSON here (mounted read-only into the container):
mkdir -p secrets && cp /path/to/service-account.json secrets/credentials.json

# 2. Build the image
docker compose build

# 3. Run any command — args after `cli` are passed straight to `python main.py`:
docker compose run --rm cli --help
docker compose run --rm cli inventory --test-connection
docker compose run --rm cli migrate --from-spreadsheet --full-table
docker compose run --rm cli compare --from-inventory
```

Generated SQL / QA reports appear on the host under `./results/` (mounted volume).
In Docker the credentials path is fixed to `/app/secrets/credentials.json`, so leave
`GOOGLE_SHEETS_CREDENTIALS_FILE` in `.env` as-is — `docker-compose.yml` overrides it.

### Option B — Local virtualenv (Python 3.11)

```bash
# If you don't have Python 3.11:
# - macOS (Homebrew): brew install python@3.11
# - Windows: https://www.python.org/downloads/release/python-3110/ ("Add Python to PATH")
python3.11 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e ./argo-utils-cli      # installs the domo_utils package
pip install -r requirements.txt
python main.py --help
```

> **datacompy note:** `requirements.txt` pins `datacompy>=0.11,<0.14`. datacompy 0.14+
> (and 1.0) removed the top-level `Compare` class this project relies on, so newer
> versions break the `compare` command.

## ⚙️ Configuration

All configuration is read from environment variables (loaded from `.env`). Copy
`.env.example` to `.env` and fill in the values — it documents every supported key
(Google Sheets, Domo, Snowflake, and optional settings). The sections below explain
how to obtain each set of credentials.

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

## 🧪 Running the Tests

```bash
pip install -r requirements-test.txt
python -m pytest            # or: python run_tests.py
```

The vendored `argo-utils-cli/` clone is excluded from collection automatically.

## 📄 Offline Template / Fixture

`templates/migration_inventory.template.xlsx` is a **synthetic, client-data-free**
workbook that mirrors the tabs and headers the CLI reads (`Migration`, `QA - Test`,
`Stg Files`, `Inventory`, `All Datasets`, `All Dataflows`). Use it as a reference for
the required sheet/column structure, or upload it to your own Google Sheet to try the
tool without touching live client data. Regenerate it with:

```bash
python tools/scripts/make_inventory_template.py
```

> Real client workbooks (e.g. `Recom Migration Inventory.xlsx`) are git-ignored and
> must never be committed.

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
- `--export-dir`: Directory to save SQL files (default: `results/sql/translated`)
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

#### 4. `generate-stg` - Generate STG Files

Generate staging SQL files with automatic CAST based on Snowflake schema and Google Sheets tracking.

```bash
python main.py generate-stg [options]
```

**Options:**
- `--database`: Snowflake database name (default: from SNOWFLAKE_DATABASE env var)
- `--schema`: Snowflake schema name (default: TEMP_ARGO_RAW)
- `--role`: Snowflake role to use (default: DBT_ROLE)
- `--warehouse`: Snowflake warehouse to use (default: from SNOWFLAKE_WAREHOUSE env var)
- `--output-dir`: Directory to save SQL files (default: sql/stg/)
- `--credentials`: Path to Google Sheets credentials file
- `--spreadsheet-id`: Google Sheets spreadsheet ID
- `--read-only`: Run in read-only mode (don't update Check column)
- `--dry-run`: Show what would be generated without creating files
- `--use-cast`: Use explicit CAST statements in generated SQL (disabled by default)

**Features:**
- ✅ **Smart Skip**: Automatically skips rows where Check = "True"
- ✅ **Optional CAST**: Can generate explicit CAST statements when needed (use --use-cast)
- ✅ **Progress Tracking**: Writes "True" to Check column when files are created successfully
- ✅ **Schema Validation**: Connects to Snowflake to get real column names and types

## 📚 Usage Examples

Examples:
    # Export inventory dataflows to SQL
    python main.py inventory --export-dir results/sql/translated
    
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
    
    # Generate STG files with default configuration
    python main.py generate-stg
    
    # Generate STG files with custom database and schema
    python main.py generate-stg --database DW_REPORTS --schema TEMP_ARGO_RAW
    
    # Dry run - see what would be generated without creating files
    python main.py generate-stg --dry-run
    
    # Read-only mode - don't update Check column in Google Sheets
    python main.py generate-stg --read-only
    
    # Full custom configuration
    python main.py generate-stg --database DW_RAW --schema SRC --role DBT_ROLE --warehouse DBT_WH --output-dir results/sql/stg
    
    # Generate with explicit CAST statements (legacy mode)
    python main.py generate-stg --use-cast

## 📝 Project Structure

```
Domo-to-snowflake-migration/
├── main.py                     # Unified CLI: dispatch + per-command handlers
├── Dockerfile                  # Python 3.11 image
├── docker-compose.yml          # `docker compose run --rm cli <command>`
├── .env.example                # All config keys (copy to .env)
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── templates/                  # Synthetic offline inventory template (no client data)
│   └── migration_inventory.template.xlsx
├── tools/                      # Main modules
│   ├── __init__.py
│   ├── cli/                    # argparse wiring (parser.py), split out of main.py
│   ├── get_all_stg_files.py   # STG files generation (with CLI)
│   ├── inventory_handler.py    # Inventory management
│   ├── domo_to_snowflake.py   # Data migration
│   ├── dataset_comparator.py  # Data comparison/QA
│   ├── scripts/                # Utility scripts
│   │   ├── __init__.py
│   │   ├── make_inventory_template.py  # Regenerate the offline template
│   │   ├── cleanup_project.py
│   │   ├── extract_lineage.py
│   │   ├── maintain_structure.py
│   │   └── project_maintenance.py
│   └── utils/                  # Utilities
│       ├── __init__.py
│       ├── domo.py            # Domo API client
│       ├── snowflake.py       # Snowflake client
│       ├── gsheets.py         # Google Sheets client
│       ├── create_stg_sql_file.py  # STG SQL generation
│       └── create_source.py   # Source file generation
├── results/                    # Output directories (created automatically)
│   ├── sql/
│   │   ├── stg/               # Staging files
│   │   └── translated/        # Inventory exports
│   └── txt/qa/                # QA comparison reports
└── tests/                      # Test files
    ├── __init__.py
    ├── conftest.py
    └── test_*.py
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
   📁 Output directory: /path/to/results/sql/translated
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
