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

### Reporting
- ✅ Reusable, **name-based** "Credit Usage" report that reproduces Domo dataflow 620 for any client (`credit-usage`)
- ✅ Reusable, **name-based** "Runtime Usage" report (dataflow runtimes by dataflow + day) for any client (`runtime-usage`)
- ✅ Configurable time window (last N months, explicit range, or full history)

### Dataset & Card Inventory
- ✅ Export all Domo datasets and their lineage/dataflows to Google Sheets
- ✅ Count Domo cards per dataset (`datasets --count-cards`)
- ✅ List every card and the dataset(s) it uses (`datasets --list-cards`)
- ✅ One-shot `refresh` command that runs the whole Domo → spreadsheet pipeline in order
- ✅ Score dataflow translation difficulty (`weighting`)

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
`Stg Files`, `Intermediate models`, `All Datasets`, `Datasets`, `All Dataflows`). Use it as a reference for
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
- `--export-dir`: Directory to save SQL files (default: `results/translations/sql`)
- `--credentials`: Path to Google Sheets credentials file
- `--test-connection`: Test connection and show preview

#### 2. `dataflow-raw` - Export raw dataflow definitions

Export the **raw** Domo dataflow definitions (tiles/steps) as JSON, before any
translation to SQL. Useful for inspecting the original dataflow structure.

```bash
python main.py dataflow-raw [options]
```

**Options:**
- `--output-dir`: Directory to save raw JSON files (default: `results/translations/raw`)
- `--credentials`: Path to Google Sheets credentials file
- `--dataflow-id`: Fetch a single dataflow by ID instead of reading the inventory sheet

#### 3. `migrate` - Data Migration

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
- `--full-table`: Upload the entire table instead of limiting to the first 1000 rows
- `--auto-chunk-size`: Auto-tune the chunk size for X-Small warehouses based on dataset size
- `--to-stage` / `--from-stage`: Migrate through a Snowflake stage instead of directly to the table
- `--stage-name`: Snowflake stage name for stage-based migration
- `--file-pattern`: File pattern to match in the stage when loading from stage (default: `*.csv`)
- `--if-exists`: What to do if the target table exists — `replace` (default), `append`, or `fail`

#### 4. `stage` - Manage Snowflake stages

Create, inspect and clean Snowflake stages used for staged migrations.

```bash
python main.py stage <action> --stage-name NAME
```

**Actions:**
- `create --stage-name NAME`: Create a new stage
- `list --stage-name NAME`: List files in a stage
- `drop --stage-name NAME`: Drop a stage
- `clean --stage-name NAME [--file-pattern '*']`: Remove files matching a pattern from a stage

#### 5. `datasets` - Dataset Management

Manage and export Domo datasets.

```bash
python main.py datasets [options]
```

**Options:**
- `--test-connection`: Test Domo connection
- `--export-to-spreadsheet`: Export all Domo datasets to Google Sheets
- `--export-dataflows`: Crawl Domo lineage for the datasets in the `All Datasets` tab and write the dataflow table (`Output Dataset ID`, `Dataflow ID`, `Source Dataset IDs`, `All Source Dataset IDs`) to the `All Dataflows` tab
- `--count-cards`: Count Domo cards per dataset (via search) and write a `# Cards` column to the datasets tab, leaving all other columns untouched
- `--list-cards`: List every Domo card and the dataset(s) it uses (one row per card/dataset pair) to a dedicated `Cards per Dataset` tab (cleared and rewritten on every run)
- `--list-local`: List all Domo datasets locally
- `--credentials`: Path to Google Sheets credentials file
- `--spreadsheet-id`: Google Sheets spreadsheet ID (uses default if not specified)
- `--sheet-name`: Sheet name for datasets (default: DomoDatasets)
- `--batch-size`: Number of datasets to fetch per batch (default: 100)

#### 6. `compare` - Data Comparison / QA

Compare a Domo dataset against a Snowflake table or a CSV file to validate a migration.

```bash
python main.py compare [options]
```

**Options:**
- `--domo-dataset-id`: Domo output ID to compare
- `--snowflake-table`: Snowflake table to compare against (required unless using `--csv-file`)
- `--csv-file`: Path to a CSV file to compare against (alternative to a Snowflake table)
- `--key-columns`: One or more key columns to align rows for comparison
- `--sample-size`: Number of rows to sample (default: automatic)
- `--sampling-method`: `random` (default) or `ordered`
- `--transform-columns`: Normalize column names (e.g. `My Column` → `my_column`)
- `--use-schema`: Use the `Schema` column from the spreadsheet to force data types
- `--from-spreadsheet`: Compare datasets listed in the Google Sheets comparison tab
- `--from-inventory`: Compare datasets from the Inventory sheet (uses Output ID, Model Name, Key Columns)
- `--csv-encoding` / `--csv-separator`: CSV parsing options (defaults: `utf-8`, `,`)
- `--credentials` / `--spreadsheet-id` / `--sheet-name`: Google Sheets options (sheet default: `QA - Test`)
- `--test-connection`: Test Domo and Snowflake connections

#### 7. `generate-stg` - Generate STG Files

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
- `--read-only`: Run in read-only mode (don't update the Status column)
- `--dry-run`: Show what would be generated without creating files
- `--use-cast`: Use explicit CAST statements in generated SQL (disabled by default)

**Features:**
- ✅ **Smart Skip**: Automatically skips rows where Status = "Deployed"
- ✅ **Optional CAST**: Can generate explicit CAST statements when needed (use --use-cast)
- ✅ **Progress Tracking**: Sets Status to "Translated" when files are created successfully
- ✅ **Schema Validation**: Connects to Snowflake to get real column names and types

#### 8. `generate-sources` - Generate dbt sources.yml

Generate a dbt `sources.yml` file from the Google Sheets data.

```bash
python main.py generate-sources [options]
```

**Options:**
- `--database`: Snowflake database name (default: from `SNOWFLAKE_DATABASE` env var)
- `--schema`: Snowflake schema name (default: `SRC`)
- `--output`: Output file name (default: `sources_auto.yml`)

#### 9. `refresh` - Run the Domo → spreadsheet pipeline

Orchestrate the export steps (`datasets`, `cards`, `dataflows`, `inventory`) in
dependency order with a single command. By default it runs every step **except**
the slow `score` step (~20 min); add `--with-score` to include it.

```bash
python main.py refresh [options]
```

**Options:**
- `--credentials`: Path to Google Sheets credentials file
- `--spreadsheet-id`: Google Sheets spreadsheet ID (uses default if not specified)
- `--with-score`: Also run the slow `score` step (~20 min)
- `--only`: Comma-separated subset of steps to run (`datasets,cards,dataflows,inventory,score`)
- `--skip`: Comma-separated steps to skip
- `--dry-run`: Print the plan without executing
- `--fail-fast`: Stop at the first failed step (default: continue and report at the end)

#### 10. `credit-usage` - Reusable Credit Usage report (any client)

Reproduces the Domo **"Credit Usage"** dataflow (originally built as dataflow 620 on
`grtfinancial`) for **any** Domo instance. The required sources are Domo *DomoStats*
datasets that share the **same names across instances**, so the command resolves them
**by name** (`Credit Usage | Domostats`, `Governance - Datasets`, `Users`,
`Dataflow Details`) — no client-specific dataset IDs are hardcoded. It joins/aggregates
them in pandas, filters to a time window on the `date` column (default: last 3 months),
and writes the result to a tab (default `Credit Usage`). The full Credit Usage logic is computed
faithfully, but only a focused subset of columns is published to the sheet
(`date, month, entityType, entityId, usageUnit, category, creditsUsed, Dataset ID, Type,
Row Count, Column Count, # Execution by Day`); the tab is cleared before each write.

If a required source dataset cannot be found by name, the command fails with a clear
`❌` error naming what was missing and writes nothing.

```bash
python main.py credit-usage [options]
```

**Options:**
- `--spreadsheet-id`: Google Sheets spreadsheet ID (default: `MIGRATION_SPREADSHEET_ID` env)
- `--sheet-name`: Destination tab name (default: `CREDIT_USAGE_SHEET_NAME` env, or `Credit Usage`)
- `--credentials`: Path to Google Sheets credentials file (default: `GOOGLE_SHEETS_CREDENTIALS_FILE` env)
- `--months`: Size of the trailing time window in months (default: `CREDIT_USAGE_MONTHS` env, or 3)
- `--start-date` / `--end-date`: Explicit window range (`YYYY-MM-DD`); overrides `--months`
- `--all`: Extract the **full** history (no date filter); overrides `--months` and the range
- `--dry-run`: Compute + log row/column counts, the resolved name→id map and the date window, but do **not** write to Sheets
- `--test-connection`: Just verify Domo auth + dataset listing

Window precedence: `--all` > explicit `--start-date`/`--end-date` > `--months` > default (3 months).
When a bounded window is in effect, the date filter is pushed down to the source that owns
the `date` column for an efficient extract.

```bash
# Default: last 3 months → 'CU' tab
python main.py credit-usage

# Prove resolution + computation + window without writing anything
python main.py credit-usage --dry-run

# Full history into a custom tab
python main.py credit-usage --all --sheet-name "CU - All"

# Explicit date range
python main.py credit-usage --start-date 2026-01-01 --end-date 2026-03-31

# Last 6 months
python main.py credit-usage --months 6
```

#### 11. `runtime-usage` - Reusable Runtime Usage report (any client)

Reproduces the Domo **"Runtime Usage"** dataflow (originally built as dataflow 621 on
`grtfinancial`) for **any** Domo instance. Its single source is the **`DataFlow History`**
DomoStats dataset, which shares the **same name across instances**, so the command
resolves it **by name** — no client-specific dataset IDs are hardcoded. It parses the
string `Start Time` / `End Time` columns to compute each run's runtime, then groups into a
single merged table by **(Dataflow ID, calendar day of `Start Time`)** — one row per
dataflow per day (~365/year per dataflow). Each row carries the day's average input/output
rows, average and total runtime, the number of runs considered, and the oldest/newest run
timestamps. Runs with a null/unparseable `End Time` have no usable runtime and are dropped
(the count is logged). The result is filtered to a time window on `Start Time` (default:
last 3 months) and written to a tab (default `Runtime`); the tab is cleared before each write.

If the `DataFlow History` dataset cannot be found by name, the command fails with a clear
`❌` error naming what was missing and writes nothing.

```bash
python main.py runtime-usage [options]
```

**Options:**
- `--spreadsheet-id`: Google Sheets spreadsheet ID (default: `MIGRATION_SPREADSHEET_ID` env)
- `--sheet-name`: Destination tab name (default: `RUNTIME_USAGE_SHEET_NAME` env, or `Runtime`)
- `--credentials`: Path to Google Sheets credentials file (default: `GOOGLE_SHEETS_CREDENTIALS_FILE` env)
- `--months`: Size of the trailing time window in months (default: `RUNTIME_USAGE_MONTHS` env, or 3)
- `--start-date` / `--end-date`: Explicit window range (`YYYY-MM-DD`); overrides `--months`
- `--all`: Extract the **full** history (no date filter); overrides `--months` and the range
- `--dry-run`: Compute + log row/column counts, the resolved name→id and the date window, but do **not** write to Sheets
- `--test-connection`: Just verify Domo auth + dataset listing

Window precedence: `--all` > explicit `--start-date`/`--end-date` > `--months` > default (3 months).
When a bounded window is in effect, the date filter is pushed down to the `Start Time` column
for an efficient extract.

```bash
# Default: last 3 months → 'Runtime' tab
python main.py runtime-usage

# Prove resolution + computation + window without writing anything
python main.py runtime-usage --dry-run

# Full history into a custom tab
python main.py runtime-usage --all --sheet-name "Runtime - All"

# Explicit date range
python main.py runtime-usage --start-date 2026-01-01 --end-date 2026-03-31

# Last 6 months
python main.py runtime-usage --months 6
```

#### 12. `weighting` - Score dataflow translation difficulty

Forwards to the translation-difficulty tool: scores how hard each Domo dataflow is
to translate to Snowflake and writes the results to Google Sheets. Put the tool's
own subcommand and options **after** `weighting`.

```bash
python main.py weighting <subcommand> [options]

# Export the inventory the scorer reads
python main.py weighting export-inventory

# Score dataflows from an Inventory sheet, limiting to 10
python main.py weighting score --from-sheet Inventory --max-dataflows 10
```

Full reference: [docs/TRANSLATION_DIFFICULTY.md](docs/TRANSLATION_DIFFICULTY.md).
The scoring weights live in `translation_difficulty_weights.yaml` at the repo root.

## 📚 Usage Examples

Examples:
    # Export inventory dataflows to SQL
    python main.py inventory --export-dir results/translations/sql
    
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
    
    # Read-only mode - don't update the Status column in Google Sheets
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
│   ├── credit_usage.py        # Reusable name-based "Credit Usage" report (dataflow 620)
│   ├── runtime_usage.py       # Reusable name-based "Runtime Usage" report (dataflow 621)
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
│       ├── domo_export.py     # Shared helpers for name-based reports (resolution, window, sheet writer)
│       ├── create_stg_sql_file.py  # STG SQL generation
│       └── create_source.py   # Source file generation
├── results/                    # Output directories (created automatically)
│   ├── translations/
│   │   ├── sql/               # Inventory SQL exports
│   │   └── raw/               # Raw dataflow JSON exports
│   ├── sql/stg/               # Staging files
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
   📁 Output directory: /path/to/results/translations/sql
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
