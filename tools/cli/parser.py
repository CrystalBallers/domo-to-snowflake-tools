"""argparse definition for the Domo → Snowflake migration CLI.

Kept separate from ``main`` so the parser wiring is isolated from the command
handlers (``tools.cli.commands``). ``main.create_parser`` re-exports this.
"""

import os
import argparse

_EPILOG = """\
Examples:
    python main.py inventory --test-connection
    python main.py dataflow-raw
    python main.py dataflow-raw --dataflow-id 15
    python main.py migrate --from-spreadsheet --full-table
    python main.py migrate --dataset-id 12345 --target-table sales_data
    python main.py compare --domo-dataset-id 12345 --snowflake-table sales_data --key-columns id date
    python main.py compare --from-inventory
    python main.py generate-stg --dry-run
    python main.py generate-sources --database DW_RAW --schema SRC
    python main.py weighting score --from-sheet Inventory --max-dataflows 10
    python main.py credit-usage --dry-run
    python main.py credit-usage --all --sheet-name "Credit Usage"
    python main.py credit-usage --start-date 2026-01-01 --end-date 2026-03-31
    python main.py runtime-usage --dry-run
    python main.py runtime-usage --all --sheet-name "Runtime"
    python main.py runtime-usage --months 6

Configuration is read from environment variables (see .env.example): Google Sheets
(GOOGLE_SHEETS_CREDENTIALS_FILE, MIGRATION_SPREADSHEET_ID, *_SHEET_NAME), Domo
(DOMO_DEVELOPER_TOKEN, DOMO_INSTANCE) and Snowflake (SNOWFLAKE_*). Translation
difficulty options: see docs/TRANSLATION_DIFFICULTY.md.
"""


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        description="Domo to Snowflake Migration Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_EPILOG,
    )
    sub = parser.add_subparsers(dest='command', help='Available commands')

    cred_default = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    sheet_id_default = os.getenv("MIGRATION_SPREADSHEET_ID")

    # inventory
    inv = sub.add_parser('inventory', help='Extract and export inventory data from Google Sheets')
    inv.add_argument("--export-dir", default=os.getenv("EXPORT_DIR", "results/sql/translated"), help="Directory to save SQL files (default: results/sql/translated)")
    inv.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    inv.add_argument("--test-connection", action="store_true", help="Test Google Sheets connection and show inventory preview")

    # dataflow-raw
    raw = sub.add_parser('dataflow-raw', help='Export RAW Domo dataflow definitions (tiles/steps) as JSON, before translation')
    raw.add_argument("--output-dir", default=os.getenv("RAW_EXPORT_DIR", "results/dataflows/raw"), help="Directory to save raw JSON files (default: results/dataflows/raw)")
    raw.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    raw.add_argument("--dataflow-id", help="Fetch a single dataflow by ID instead of reading the inventory sheet")

    # migrate
    mig = sub.add_parser('migrate', help='Migrate datasets from Domo to Snowflake')
    mig.add_argument("--dataset-id", help="Domo dataset ID to migrate")
    mig.add_argument("--target-table", help="Target Snowflake table name")
    mig.add_argument("--batch-file", help="JSON file with dataset ID to table name mappings")
    mig.add_argument("--from-spreadsheet", action="store_true", help="Migrate datasets from Google Sheets Migration tab")
    mig.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    mig.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID (uses default if not specified)")
    mig.add_argument("--sheet-name", default=os.getenv("MIGRATION_SHEET_NAME", "Migration"), help="Migration sheet tab name (default: Migration)")
    mig.add_argument("--test-connection", action="store_true", help="Test Domo and Snowflake connections")
    mig.add_argument("--reload-env", action="store_true", help="Force reload environment variables from .env file")
    mig.add_argument("--full-table", action="store_true", help="Upload the entire table instead of limiting to first 1000 rows (default: False, limits to 1000 rows)")
    mig.add_argument("--auto-chunk-size", action="store_true", help="Automatically determine optimal chunk size for X-Small warehouse based on dataset size (default: False, uses fixed 1000 row chunks)")
    mig.add_argument("--to-stage", action="store_true", help="Migrate data to Snowflake stage instead of directly to table")
    mig.add_argument("--stage-name", help="Snowflake stage name for stage-based migration")
    mig.add_argument("--from-stage", action="store_true", help="Load data from existing stage to table")
    mig.add_argument("--file-pattern", default="*.csv", help="File pattern to match in stage when loading from stage (default: *.csv)")
    mig.add_argument("--if-exists", choices=['replace', 'append', 'fail'], default='replace', help="What to do if target table exists (default: replace)")

    # stage
    stage = sub.add_parser('stage', help='Manage Snowflake stages for data migration')
    stage_sub = stage.add_subparsers(dest='stage_action', help='Stage actions')
    for action, hlp in [('create', 'Create a new stage'), ('list', 'List files in a stage'), ('drop', 'Drop a stage')]:
        p = stage_sub.add_parser(action, help=hlp)
        p.add_argument('--stage-name', required=True, help=f'Name of the stage to {action}')
    clean = stage_sub.add_parser('clean', help='Clean files from a stage')
    clean.add_argument('--stage-name', required=True, help='Name of the stage to clean')
    clean.add_argument('--file-pattern', default='*', help='File pattern to match for cleaning (default: *)')

    # datasets
    ds = sub.add_parser('datasets', help='Manage Domo datasets')
    ds.add_argument("--test-connection", action="store_true", help="Test Domo connection")
    ds.add_argument("--export-to-spreadsheet", action="store_true", help="Export all Domo datasets to a Google Sheets spreadsheet")
    ds.add_argument("--export-dataflows", action="store_true", help="Crawl Domo lineage for the datasets in the 'All Datasets' tab and write the dataflow table to the 'All Dataflows' tab")
    ds.add_argument("--count-cards", action="store_true", help="Count Domo cards per dataset (via search) and write a '# Cards' column to the datasets tab, leaving all other columns untouched")
    ds.add_argument("--list-cards", action="store_true", help="List every Domo card and the dataset(s) it uses (one row per card/dataset pair) to a dedicated 'Cards per Dataset' tab")
    ds.add_argument("--list-local", action="store_true", help="List all Domo datasets locally")
    ds.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    ds.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID to export to (uses default if not specified)")
    ds.add_argument("--sheet-name", default=os.getenv("DATASETS_SHEET_NAME", "Datasets"), help="Sheet name for Domo datasets (default: DomoDatasets)")
    ds.add_argument("--batch-size", type=int, default=100, help="Number of datasets to fetch per batch (default: 100)")

    # compare
    cmp = sub.add_parser('compare', help='Compare a Domo dataset with a Snowflake table or CSV file')
    cmp.add_argument("--domo-dataset-id", help="Domo output ID to compare")
    cmp.add_argument("--snowflake-table", help="Snowflake table name to compare (required if not using --csv-file)")
    cmp.add_argument("--key-columns", nargs='+', help="One or more key columns to use for comparison")
    cmp.add_argument("--sample-size", type=int, help="Number of rows to sample for comparison (default: automatic calculation)")
    cmp.add_argument("--transform-columns", action="store_true", help="Transform column names for comparison (e.g., 'My Column' -> 'my_column')")
    cmp.add_argument("--sampling-method", choices=["random", "ordered"], default="random", help="Sampling method: 'random' (smart random with fallback to ordered) or 'ordered'. Default: random")
    cmp.add_argument("--test-connection", action="store_true", help="Test Domo and Snowflake connections for comparison")
    cmp.add_argument("--from-spreadsheet", action="store_true", help="Compare datasets from Google Sheets Comparison tab")
    cmp.add_argument("--from-inventory", action="store_true", help="Compare datasets from existing Inventory spreadsheet (uses Output ID, Model Name, Key Columns)")
    cmp.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    cmp.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID for comparisons (uses default if not specified)")
    cmp.add_argument("--sheet-name", default=os.getenv("COMPARISON_SHEET_NAME", "QA - Test"), help="Comparison sheet tab name (default: QA - Test)")
    cmp.add_argument("--use-schema", action="store_true", help="Use Schema column from spreadsheet to force data types (prevents pandas type inference)")
    cmp.add_argument("--csv-file", help="Path to CSV file for comparison (alternative to Snowflake table)")
    cmp.add_argument("--csv-encoding", default="utf-8", help="CSV file encoding (default: utf-8)")
    cmp.add_argument("--csv-separator", default=",", help="CSV separator (default: ',')")

    # generate-stg
    stg = sub.add_parser('generate-stg', help='Generate staging SQL files from Google Sheets with Snowflake schema validation')
    stg.add_argument("--database", default=os.getenv("SNOWFLAKE_DATABASE"), help="Snowflake database name (default: from SNOWFLAKE_DATABASE env var)")
    stg.add_argument("--schema", default="TEMP_ARGO_RAW", help="Snowflake schema name (default: TEMP_ARGO_RAW)")
    stg.add_argument("--role", default="DBT_ROLE", help="Snowflake role to use (default: DBT_ROLE)")
    stg.add_argument("--warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE"), help="Snowflake warehouse to use (default: from SNOWFLAKE_WAREHOUSE env var)")
    stg.add_argument("--output-dir", default="sql/stg/", help="Directory to save SQL files (default: sql/stg/)")
    stg.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    stg.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID")
    stg.add_argument("--read-only", action="store_true", help="Run in read-only mode (don't update the Status column in Google Sheets)")
    stg.add_argument("--dry-run", action="store_true", help="Show what would be generated without creating files or updating sheets")
    stg.add_argument("--use-cast", action="store_true", help="Use explicit CAST statements in generated SQL (disabled by default)")

    # generate-sources
    src = sub.add_parser('generate-sources', help='Generate dbt sources.yml file from Google Sheets data')
    src.add_argument("--database", default=os.getenv("SNOWFLAKE_DATABASE"), help="Snowflake database name (default: from SNOWFLAKE_DATABASE env var)")
    src.add_argument("--schema", default="SRC", help="Snowflake schema name (default: SRC)")
    src.add_argument("--output", default="sources_auto.yml", help="Output file name (default: sources_auto.yml)")

    # refresh (orchestrates the Domo → spreadsheet steps in dependency order)
    refresh = sub.add_parser(
        'refresh',
        help="Run the Domo → spreadsheet pipeline (datasets, cards, dataflows, inventory) in order",
        description="Runs the export steps in dependency order. By default runs every step EXCEPT "
                    "the slow 'score' (~20 min); add --with-score to include it.",
    )
    refresh.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file")
    refresh.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID (uses default if not specified)")
    refresh.add_argument("--with-score", action="store_true", help="Also run the slow 'score' step (~20 min)")
    refresh.add_argument("--only", default=None, help="Comma-separated subset of steps to run (datasets,cards,dataflows,inventory,score)")
    refresh.add_argument("--skip", default=None, help="Comma-separated steps to skip")
    refresh.add_argument("--dry-run", action="store_true", help="Print the plan without executing")
    refresh.add_argument("--fail-fast", action="store_true", help="Stop at the first failed step (default: continue and report)")

    # credit-usage (name-based reproduction of dataflow 620 for any client)
    cu = sub.add_parser(
        'credit-usage',
        help="Compute Domo 'Credit Usage' (sources resolved BY NAME) and write a sheet tab",
        description="Resolves the required DomoStats sources by name (not ID), reproduces "
                    "dataflow 620's Credit Usage logic in pandas, filters to a time window "
                    "(default: last 3 months), and writes the result to a tab (default 'CU').",
    )
    cu_sheet_default = os.getenv("CREDIT_USAGE_SHEET_NAME", "Credit Usage")
    cu_months_default = int(os.getenv("CREDIT_USAGE_MONTHS") or 3)
    cu.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID (default: MIGRATION_SPREADSHEET_ID env)")
    cu.add_argument("--sheet-name", default=cu_sheet_default, help="Destination tab name (default: CREDIT_USAGE_SHEET_NAME env or 'Credit Usage')")
    cu.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file (default: GOOGLE_SHEETS_CREDENTIALS_FILE env)")
    cu.add_argument("--months", type=int, default=cu_months_default, help="Size of the trailing time window in months (default: CREDIT_USAGE_MONTHS env or 3)")
    cu.add_argument("--start-date", help="Explicit window start (YYYY-MM-DD); overrides --months")
    cu.add_argument("--end-date", help="Explicit window end (YYYY-MM-DD); overrides --months")
    cu.add_argument("--all", action="store_true", help="Extract the FULL history (no date filter); overrides --months and the range")
    cu.add_argument("--dry-run", action="store_true", help="Compute + log counts, name→id map and the date window, but do NOT write to Sheets")
    cu.add_argument("--test-connection", action="store_true", help="Just verify Domo auth + dataset listing")

    # runtime-usage (name-based reproduction of dataflow 621 for any client)
    ru = sub.add_parser(
        'runtime-usage',
        help="Compute Domo 'Runtime Usage' (source resolved BY NAME) and write a sheet tab",
        description="Resolves the 'DataFlow History' DomoStats by name (not ID), computes a "
                    "merged Runtime Usage table grouped by (Dataflow ID, day) in pandas, filters "
                    "to a time window (default: last 3 months), and writes the result to a tab "
                    "(default 'Runtime').",
    )
    ru_sheet_default = os.getenv("RUNTIME_USAGE_SHEET_NAME", "Runtime")
    ru_months_default = int(os.getenv("RUNTIME_USAGE_MONTHS") or 3)
    ru.add_argument("--spreadsheet-id", default=sheet_id_default, help="Google Sheets spreadsheet ID (default: MIGRATION_SPREADSHEET_ID env)")
    ru.add_argument("--sheet-name", default=ru_sheet_default, help="Destination tab name (default: RUNTIME_USAGE_SHEET_NAME env or 'Runtime')")
    ru.add_argument("--credentials", default=cred_default, help="Path to Google Sheets credentials JSON file (default: GOOGLE_SHEETS_CREDENTIALS_FILE env)")
    ru.add_argument("--months", type=int, default=ru_months_default, help="Size of the trailing time window in months (default: RUNTIME_USAGE_MONTHS env or 3)")
    ru.add_argument("--start-date", help="Explicit window start (YYYY-MM-DD); overrides --months")
    ru.add_argument("--end-date", help="Explicit window end (YYYY-MM-DD); overrides --months")
    ru.add_argument("--all", action="store_true", help="Extract the FULL history (no date filter); overrides --months and the range")
    ru.add_argument("--dry-run", action="store_true", help="Compute + log counts, resolved id and the date window, but do NOT write to Sheets")
    ru.add_argument("--test-connection", action="store_true", help="Just verify Domo auth + dataset listing")

    # weighting (forwards remaining argv to the translation-difficulty CLI)
    weighting = sub.add_parser(
        "weighting",
        help="Score dataflow translation difficulty (Snowflake) and write Google Sheets",
        description="Forwards to the translation-difficulty tool. Put its options and subcommand "
                    "after 'weighting'. Full reference: docs/TRANSLATION_DIFFICULTY.md",
    )
    weighting.add_argument("weighting_argv", nargs=argparse.REMAINDER,
                           help="e.g. export-inventory | score --from-sheet Inventory [--credentials ...]")

    return parser
