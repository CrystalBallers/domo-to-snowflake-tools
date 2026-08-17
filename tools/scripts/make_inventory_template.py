#!/usr/bin/env python3
"""Generate a synthetic, client-data-free migration-inventory workbook.

The real ``Recom Migration Inventory.xlsx`` contains live client data and must
never be committed. This script rebuilds an offline template/fixture that mirrors
only the tabs (and headers) the CLI actually reads, populated with obviously fake
example rows so the tool can be exercised end-to-end without a live Google Sheet.

Usage:
    python tools/scripts/make_inventory_template.py [output.xlsx]
Default output: templates/migration_inventory.template.xlsx
"""

import sys
from pathlib import Path

from openpyxl import Workbook

# One entry per tab the CLI reads. Headers match the names the code detects
# (see possible_*_columns in tools/domo_to_snowflake.py, DATAFLOW_COLUMN_NAME in
# tools/inventory_handler.py, and the 'Stg Files' reader in get_all_stg_files.py).
SHEETS = {
    # migrate --from-spreadsheet
    "Migration": {
        "headers": ["Dataset ID", "Model Name", "Status", "Rows"],
        "rows": [
            ["00000000-0000-0000-0000-000000000001", "example_sales_daily", "Pending", 1000],
            ["00000000-0000-0000-0000-000000000002", "example_customers", "Migrated", 250],
        ],
    },
    # compare --from-spreadsheet
    "QA - Test": {
        "headers": ["Model Name", "Table Name", "Status", "Output ID", "Notes",
                    "Transform Columns", "Key Columns", "Schema"],
        "rows": [
            ["example_sales_daily", "EXAMPLE_SALES_DAILY", "Testing",
             "00000000-0000-0000-0000-000000000001", "", "FALSE", "order_id", ""],
        ],
    },
    # generate-stg
    "Stg Files": {
        "headers": ["Dataset ID", "Check", "Name", "Model"],
        "rows": [
            ["00000000-0000-0000-0000-000000000001", "", "EXAMPLE_SALES_DAILY", "stg_example_sales_daily"],
        ],
    },
    # inventory / compare --from-inventory
    "Inventory": {
        "headers": ["Model Name", "Key Columns", "Output Name", "Dataflow ID", "Status",
                    "Dataflow Name", "Source Names", "Missing Sources", "Notes",
                    "Output ID", "Source IDs", "Priority [Beta]"],
        "rows": [
            ["example_sales_daily", "order_id", "Example Sales Daily",
             "00000000-0000-0000-0000-0000000000aa", "Pending", "Example Sales Flow",
             "raw_orders", "", "", "00000000-0000-0000-0000-000000000001", "src-001", "P1"],
        ],
    },
    # datasets --export-to-spreadsheet (written by the tool; example for reference)
    "All Datasets": {
        "headers": ["Dataset ID", "Name", "Description", "Created", "Last Updated",
                    "Row Count", "Column Count", "Owner", "Priority"],
        "rows": [
            ["00000000-0000-0000-0000-000000000001", "Example Sales Daily",
             "Synthetic example row", "2024-01-01", "2024-01-02", 1000, 5, "example@example.com", ""],
        ],
    },
    "All Dataflows": {
        "headers": ["Output Dataset ID", "Dataflow ID", "Source Dataset IDs", "All Source Dataset IDs"],
        "rows": [
            ["00000000-0000-0000-0000-000000000001",
             "00000000-0000-0000-0000-0000000000aa", "src-001", "src-001"],
        ],
    },
}


def build_workbook() -> Workbook:
    wb = Workbook()
    wb.remove(wb.active)  # drop the default empty sheet
    for title, spec in SHEETS.items():
        ws = wb.create_sheet(title=title)
        ws.append(spec["headers"])
        for row in spec["rows"]:
            ws.append(row)
    return wb


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("templates/migration_inventory.template.xlsx")
    out.parent.mkdir(parents=True, exist_ok=True)
    build_workbook().save(out)
    print(f"✅ Wrote synthetic template with {len(SHEETS)} tabs -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
