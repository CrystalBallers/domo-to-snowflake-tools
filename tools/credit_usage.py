"""Reusable, name-based "Credit Usage" extractor for any Domo client.

This module reproduces Domo dataflow **620 "Credit Usage"** (built on the
``grtfinancial`` instance) in pandas, but resolves its source datasets **by name**
instead of by ID. The sources are Domo *DomoStats* datasets, so they share the same
names across every instance — which is what makes a single command reusable for any
client. No instance-specific dataset IDs are baked into this module.

================================================================================
SPEC — reverse-engineered from dataflow 620 (raw definition + SQL translation)
================================================================================
Authoritative analysis: ``python main.py dataflow-raw --dataflow-id 620`` (the raw
``inputs`` give the datasource_id → datasource_name contract) cross-checked with the
ETL→SQL translation and validated against the live output dataset schema (41 cols).

REQUIRED_SOURCE_NAMES (exact Domo DomoStats names — the resolution contract):
  • "Credit Usage | Domostats"  (the fact table; OWNS the date column)
  • "Governance - Datasets"
  • "Users"
  • "Dataflow Details"
Output dataset name: "Credit Usage".

DATE_COLUMN = "date"  (format ``YYYY-MM-DD``), owned by "Credit Usage | Domostats".
The time-window filter is applied to this column (and pushed down to that source).

Transformation (tiles in dependency order):
  1. Join Data 6  : "Governance - Datasets" LEFT JOIN "Users"
                    ON Governance."Owner ID" = Users."User ID"
                    (drop Users' Name, _BATCH_ID_, _BATCH_LAST_RUN_ before join).
  2. Select Columns (off Join Data 6) → the dataset dimension, keeping:
        Dataset ID, Name→"Dataset Name", Description, Row Count, Column Count,
        Owner ID, Owner Name, Dataset Created Date/Time,
        DataSet Last Touched Date/Time, DataSet Last Updated Date/Time,
        Report Last Run, Type, Display ProcessingType,
        Data Provider ProcessingType, Card Count, PDP Enabled, Email→"Owner Email".
  3. Select Columns 1 (off "Dataflow Details") keeps:
        Dataflow ID, Name→"Dataflow Name", Input Dataset ID, Output Dataset ID.
  4. Group By   (off Select Columns 1): group by "Input Dataset ID",
                COUNT_DISTINCT("Dataflow ID") → "# of dataflows feeds into".
  5. Group By 1 (off Select Columns 1): group by "Output Dataset ID",
                FIRST("Dataflow Name") → "Name of Father Dataflow",
                FIRST("Dataflow ID")   → "ID of Father Dataflow".
  6. Join Data   : "Credit Usage | Domostats" LEFT JOIN <dimension (step 2)>
                   ON fact.entityId = dim."Dataset ID".
  7. Join Data 1 : + step 4  ON "Dataset ID" = "Input Dataset ID" (drop right key).
  8. Join Data 2 : + step 5  ON "Dataset ID" = "Output Dataset ID" (drop right key).
  9. Group By 2  (off Join Data 2): group by
                  [date, skuGroup, usageUnit, entityType, entityId],
                  SUM("usageQuantity") → "# Execution by Day".
 10. Filter Rows (off Group By 2): `entityType` = 'DataSet' AND `usageUnit` = 'Executions'.
 11. Join Data 3 : Join Data 2 LEFT JOIN Filter Rows ON [date, entityId];
                   keep only "# Execution by Day" from the right side.
 12. Add Formula (ExpressionEvaluator), adds:
        "Is used as input of a dataflow?" = IF(`# of dataflows feeds into` > 0, 'True', 'False')
        "short date"                      = DATE(`date`)
        "Is the Output of a dataflow?"    = `Name of Father Dataflow` IS NULL → 'False' else 'True'
        "Avg Credits" =
            CASE WHEN entityType='DataSet' AND usageUnit='Rows'
                      AND skuId='overage-execution-rows'
                 THEN creditsUsed / NULLIF(`# Execution by Day`, 0)
                 WHEN entityType='DataSet' AND usageUnit='Executions'
                 THEN creditsUsed / NULLIF(usageQuantity, 0) END
        "Consumption Type" =
            CASE WHEN skuId IN ('rows-materialized','rows-virtual') THEN 'Storage'
                 WHEN skuId LIKE '%executions%' THEN 'Executions'
                 WHEN skuId LIKE '%overage%'    THEN 'Execution Caps'
                 ELSE 'Other' END
        "Argo_Owned" =
            CASE WHEN `Owner Email` LIKE '%argo%' OR `Owner Email` LIKE '%crystal%'
                 THEN 1 ELSE 0 END
 13. Remove Duplicates (Unique): dedup keeping the first row per
        [date, skuId, entityId, "Dataset ID"].
 14. Publish "Credit Usage": the 41 OUTPUT_COLUMNS below, in order.
================================================================================
"""

import os
import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from tools.utils.domo import DomoHandler
from tools.utils.gsheets import GoogleSheets
from tools.utils import domo_export
from tools.utils.domo_export import apply_time_window, _df_to_values, write_to_sheet

logger = logging.getLogger(__name__)

# ── Source-name contract (resolved by name, never by ID) ─────────────────────
CREDIT_USAGE_DOMOSTATS = "Credit Usage | Domostats"
GOVERNANCE_DATASETS = "Governance - Datasets"
USERS = "Users"
DATAFLOW_DETAILS = "Dataflow Details"
REQUIRED_SOURCE_NAMES: List[str] = [
    CREDIT_USAGE_DOMOSTATS,
    GOVERNANCE_DATASETS,
    USERS,
    DATAFLOW_DETAILS,
]

# The date/timestamp column that drives the time window, and the source that owns it.
DATE_COLUMN = "date"
DATE_SOURCE_NAME = CREDIT_USAGE_DOMOSTATS

# Defaults read from the environment first, falling back to the literal only when the
# env var is unset (so the same configuration drives the CLI and any programmatic call).
DEFAULT_SHEET_NAME = os.getenv("CREDIT_USAGE_SHEET_NAME", "Credit Usage")
DEFAULT_MONTHS = int(os.getenv("CREDIT_USAGE_MONTHS") or 3)

# Final output column list and order (validated against the live "Credit Usage" output).
OUTPUT_COLUMNS: List[str] = [
    "date", "month", "skuGroup", "skuId", "entityType", "entityId", "usageUnit",
    "usageQuantity", "category", "creditsUsed", "instanceId", "domain",
    "_BATCH_ID_", "_BATCH_LAST_RUN_", "Dataset ID", "Dataset Name", "Description",
    "Row Count", "Column Count", "Owner ID", "Owner Name", "Dataset Created Date/Time",
    "DataSet Last Touched Date/Time", "DataSet Last Updated Date/Time", "Report Last Run",
    "Type", "Display ProcessingType", "Data Provider ProcessingType", "Card Count",
    "PDP Enabled", "Owner Email", "# of dataflows feeds into", "Name of Father Dataflow",
    "ID of Father Dataflow", "# Execution by Day", "Is used as input of a dataflow?",
    "short date", "Is the Output of a dataflow?", "Avg Credits", "Consumption Type",
    "Argo_Owned",
]

# Subset (and order) actually written to the sheet. The full 41-column result is
# computed faithfully; only these columns are published — the rest are dropped.
SHEET_OUTPUT_COLUMNS: List[str] = [
    "date", "month", "entityType", "entityId", "usageUnit", "category", "creditsUsed",
    "Dataset ID", "Type", "Row Count", "Column Count", "# Execution by Day",
]

# Columns each source MUST provide for the joins/aggregations/formulas to work.
# Pass-through columns missing from a client's data simply come out blank (reindex).
_REQUIRED_COLUMNS: Dict[str, List[str]] = {
    CREDIT_USAGE_DOMOSTATS: ["date", "skuGroup", "skuId", "entityType", "entityId",
                             "usageUnit", "usageQuantity", "creditsUsed"],
    GOVERNANCE_DATASETS: ["Dataset ID", "Name", "Owner ID"],
    USERS: ["User ID", "Email"],
    DATAFLOW_DETAILS: ["Dataflow ID", "Name", "Input Dataset ID", "Output Dataset ID"],
}


# Source resolution by name, the time-window helpers and the Sheets writer are
# shared with the sibling ``runtime-usage`` command — see ``tools.utils.domo_export``.
# ``apply_time_window``, ``_df_to_values`` and ``write_to_sheet`` are imported above;
# ``resolve_sources`` / ``resolve_window`` are wrapped below to bind this command's
# required source names, DATE_COLUMN and default window.
def resolve_sources(datasets: List[dict], required: List[str] = None):
    """Resolve this command's :data:`REQUIRED_SOURCE_NAMES` (see :mod:`domo_export`)."""
    return domo_export.resolve_sources(datasets, required or REQUIRED_SOURCE_NAMES)


# ── Core computation (faithful pandas port of dataflow 620) ──────────────────
def _require_columns(df: pd.DataFrame, source_name: str, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Source {source_name!r} is missing required column(s): {missing}. "
            f"Found columns: {list(df.columns)}"
        )


def compute_credit_usage(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Reproduce dataflow 620 in pandas from the four resolved source frames.

    Args:
        frames: {source_name -> DataFrame}, keyed by :data:`REQUIRED_SOURCE_NAMES`.

    Returns:
        The Credit Usage DataFrame with exactly :data:`OUTPUT_COLUMNS`, in order.

    Raises:
        ValueError: if any source is missing a column the logic requires.
    """
    for name in REQUIRED_SOURCE_NAMES:
        if name not in frames:
            raise ValueError(f"Missing source frame for {name!r}")
        _require_columns(frames[name], name, _REQUIRED_COLUMNS[name])

    fact = frames[CREDIT_USAGE_DOMOSTATS]
    gov = frames[GOVERNANCE_DATASETS]
    users = frames[USERS]
    flows = frames[DATAFLOW_DETAILS]

    # 1. Join Data 6 — Governance LEFT JOIN Users on Owner ID = User ID.
    #    Users' Name/_BATCH_* are dropped first (Domo schemaModification2).
    users_join = users.drop(
        columns=[c for c in ("Name", "_BATCH_ID_", "_BATCH_LAST_RUN_") if c in users.columns]
    )
    join6 = gov.merge(users_join, how="left", left_on="Owner ID", right_on="User ID")

    # 2. Select Columns — the dataset dimension (rename Name/Email).
    select_map = [
        ("Dataset ID", "Dataset ID"), ("Name", "Dataset Name"), ("Description", "Description"),
        ("Row Count", "Row Count"), ("Column Count", "Column Count"), ("Owner ID", "Owner ID"),
        ("Owner Name", "Owner Name"), ("Dataset Created Date/Time", "Dataset Created Date/Time"),
        ("DataSet Last Touched Date/Time", "DataSet Last Touched Date/Time"),
        ("DataSet Last Updated Date/Time", "DataSet Last Updated Date/Time"),
        ("Report Last Run", "Report Last Run"), ("Type", "Type"),
        ("Display ProcessingType", "Display ProcessingType"),
        ("Data Provider ProcessingType", "Data Provider ProcessingType"),
        ("Card Count", "Card Count"), ("PDP Enabled", "PDP Enabled"),
        ("Email", "Owner Email"),
    ]
    dim = pd.DataFrame(index=join6.index)
    for src, dst in select_map:
        dim[dst] = join6[src] if src in join6.columns else np.nan

    # 3. Select Columns 1 — off Dataflow Details.
    sc1 = pd.DataFrame({
        "Dataflow ID": flows["Dataflow ID"],
        "Dataflow Name": flows["Name"],
        "Input Dataset ID": flows["Input Dataset ID"],
        "Output Dataset ID": flows["Output Dataset ID"],
    })

    # 4. Group By — # of dataflows feeds into = COUNT_DISTINCT(Dataflow ID) per input.
    group_by = (
        sc1.groupby("Input Dataset ID", dropna=False)["Dataflow ID"]
        .nunique()
        .reset_index(name="# of dataflows feeds into")
    )

    # 5. Group By 1 — FIRST name/id of the dataflow per output dataset.
    group_by1 = (
        sc1.groupby("Output Dataset ID", dropna=False)
        .agg(**{
            "Name of Father Dataflow": ("Dataflow Name", "first"),
            "ID of Father Dataflow": ("Dataflow ID", "first"),
        })
        .reset_index()
    )

    # 6. Join Data — fact LEFT JOIN dim on entityId = Dataset ID.
    join_data = fact.merge(dim, how="left", left_on="entityId", right_on="Dataset ID")

    # 7. Join Data 1 — + # of dataflows feeds into (drop the right join key).
    join1 = join_data.merge(
        group_by, how="left", left_on="Dataset ID", right_on="Input Dataset ID"
    ).drop(columns=["Input Dataset ID"])

    # 8. Join Data 2 — + Name/ID of Father Dataflow (drop the right join key).
    join2 = join1.merge(
        group_by1, how="left", left_on="Dataset ID", right_on="Output Dataset ID"
    ).drop(columns=["Output Dataset ID"])

    # 9. Group By 2 — SUM(usageQuantity) per (date, skuGroup, usageUnit, entityType, entityId).
    tmp = join2.copy()
    tmp["__uq"] = pd.to_numeric(tmp["usageQuantity"], errors="coerce")
    group_by2 = (
        tmp.groupby(["date", "skuGroup", "usageUnit", "entityType", "entityId"], dropna=False)["__uq"]
        .sum()
        .reset_index(name="# Execution by Day")
    )

    # 10. Filter Rows — DataSet / Executions only.
    filter_rows = group_by2[
        (group_by2["entityType"] == "DataSet") & (group_by2["usageUnit"] == "Executions")
    ]

    # 11. Join Data 3 — bring "# Execution by Day" back onto the main stream by (date, entityId).
    fr_keys = filter_rows[["date", "entityId", "# Execution by Day"]]
    join3 = join2.merge(fr_keys, how="left", on=["date", "entityId"])

    # 12. Add Formula.
    df = join3
    feeds = pd.to_numeric(df["# of dataflows feeds into"], errors="coerce")
    df["Is used as input of a dataflow?"] = np.where(feeds > 0, "True", "False")
    df["short date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Is the Output of a dataflow?"] = np.where(
        df["Name of Father Dataflow"].notna(), "True", "False"
    )

    credits = pd.to_numeric(df["creditsUsed"], errors="coerce")
    usage_qty = pd.to_numeric(df["usageQuantity"], errors="coerce")
    exec_day = pd.to_numeric(df["# Execution by Day"], errors="coerce")
    is_dataset = df["entityType"] == "DataSet"
    cond_rows = is_dataset & (df["usageUnit"] == "Rows") & (df["skuId"] == "overage-execution-rows")
    cond_exec = is_dataset & (df["usageUnit"] == "Executions")
    avg = pd.Series(np.nan, index=df.index, dtype="float64")
    avg[cond_rows] = (credits / exec_day.replace(0, np.nan))[cond_rows]
    avg[cond_exec] = (credits / usage_qty.replace(0, np.nan))[cond_exec]
    df["Avg Credits"] = avg

    sku = df["skuId"].astype("string")
    df["Consumption Type"] = np.select(
        [
            df["skuId"].isin(["rows-materialized", "rows-virtual"]),
            sku.str.contains("executions", case=False, na=False),
            sku.str.contains("overage", case=False, na=False),
        ],
        ["Storage", "Executions", "Execution Caps"],
        default="Other",
    )

    email = df["Owner Email"].astype("string")
    df["Argo_Owned"] = np.where(
        email.str.contains("argo", case=False, na=False)
        | email.str.contains("crystal", case=False, na=False),
        1, 0,
    )

    # 13. Remove Duplicates — first row per (date, skuId, entityId, Dataset ID).
    df = df.drop_duplicates(subset=["date", "skuId", "entityId", "Dataset ID"], keep="first")

    # 14. Publish — exactly the 41 output columns, in order.
    return df.reindex(columns=OUTPUT_COLUMNS).reset_index(drop=True)


# ── Time window ──────────────────────────────────────────────────────────────
def resolve_window(args, today=None) -> domo_export.WindowSpec:
    """Resolve the time window from CLI args, bound to this command's date column.

    Precedence: ``--all`` > explicit ``--start-date``/``--end-date`` > ``--months`` >
    default (last :data:`DEFAULT_MONTHS` months). See :func:`domo_export.resolve_window`.
    """
    return domo_export.resolve_window(args, DATE_COLUMN, DEFAULT_MONTHS, today=today)


# ── Orchestrator (called by the CLI handler) ─────────────────────────────────
def run_credit_usage(args) -> int:
    """Resolve sources by name, compute Credit Usage, window it, write it. 0/1 exit code."""
    spreadsheet_id = getattr(args, "spreadsheet_id", None) or os.getenv("MIGRATION_SPREADSHEET_ID")
    sheet_name = getattr(args, "sheet_name", None) or DEFAULT_SHEET_NAME
    credentials_path = getattr(args, "credentials", None) or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")

    # 1. Auth to Domo.
    handler = DomoHandler()
    if not handler.setup_auth():
        logger.error("❌ Failed to authenticate with Domo (check DOMO_DEVELOPER_TOKEN / DOMO_INSTANCE)")
        return 1

    # --test-connection: just verify auth + dataset listing.
    if getattr(args, "test_connection", False):
        logger.info("🧪 Verifying Domo dataset listing...")
        datasets = handler.get_all_datasets()
        if not datasets:
            logger.error("❌ Authenticated but no datasets were returned")
            return 1
        logger.info("✅ Domo OK — %s dataset(s) visible", len(datasets))
        return 0

    # 2. Resolve the time window.
    window = resolve_window(args)
    logger.info("🗓️  Time window: %s", window.describe())

    # 3. List datasets and resolve every required source BY NAME.
    logger.info("🔎 Listing Domo datasets to resolve sources by name...")
    datasets = handler.get_all_datasets()
    if not datasets:
        logger.error("❌ No datasets returned from Domo")
        return 1

    resolved, missing, collisions = resolve_sources(datasets)
    if missing:
        logger.error("❌ Could not find required source dataset(s) by name — nothing written:")
        for name in missing:
            logger.error("   • %s", name)
        logger.error("   Required: %s", REQUIRED_SOURCE_NAMES)
        return 1
    for name, (chosen, others) in collisions.items():
        logger.warning(
            "⚠️  Name collision for %r — %s datasets share this name. Chose id=%s "
            "(highest row_count, newest). Other ids: %s", name, len(others) + 1, chosen, others)
    for name, dsid in resolved.items():
        logger.info("   ✓ %r → %s", name, dsid)

    # 4. Extract each source (push the date filter down to the fact table when bounded).
    frames: Dict[str, pd.DataFrame] = {}
    for name, dsid in resolved.items():
        query = None
        if name == DATE_SOURCE_NAME and not window.is_all and window.has_bounds():
            query = window.pushdown_query()
            logger.info("⏬ Pushing date filter down to %r: %s", name, query)
        df = handler.extract_data(dsid, query=query)
        if df is None:
            logger.error("❌ Failed to extract data for %r (id=%s)", name, dsid)
            return 1
        logger.info("   📥 %r: %s rows × %s cols", name, len(df), len(df.columns))
        frames[name] = df

    # 5. Compute Credit Usage.
    try:
        result = compute_credit_usage(frames)
    except (ValueError, KeyError) as e:
        logger.error("❌ Failed to compute Credit Usage: %s", e)
        return 1

    # 6. Apply the time window on DATE_COLUMN (idempotent if it was pushed down).
    if not window.is_all:
        result, kept, dropped = apply_time_window(result, window)
        logger.info("🪟 Window on %r — kept %s row(s), dropped %s", DATE_COLUMN, kept, dropped)
    logger.info("📊 Credit Usage result: %s row(s) × %s column(s) (computed)",
                len(result), len(result.columns))

    # Project to the columns we actually publish; drop the rest before the sheet.
    result = result.reindex(columns=SHEET_OUTPUT_COLUMNS)
    logger.info("✂️  Writing %s of the computed columns: %s",
                len(SHEET_OUTPUT_COLUMNS), SHEET_OUTPUT_COLUMNS)

    # 7a. --dry-run: prove resolution + computation + window without touching Sheets.
    if getattr(args, "dry_run", False):
        logger.info("🧪 Dry-run — NOT writing to Google Sheets.")
        logger.info("   Resolved sources: %s", {n: i for n, i in resolved.items()})
        logger.info("   Would write %s row(s) to spreadsheet=%s tab=%r",
                    len(result), spreadsheet_id, sheet_name)
        return 0

    # 7b. Write to the output sheet.
    if not spreadsheet_id:
        logger.error("❌ No spreadsheet ID (use --spreadsheet-id or set MIGRATION_SPREADSHEET_ID)")
        return 1
    if not credentials_path:
        logger.error("❌ No Google Sheets credentials (use --credentials or set GOOGLE_SHEETS_CREDENTIALS_FILE)")
        return 1

    logger.info("📝 Writing Credit Usage to spreadsheet=%s tab=%r ...", spreadsheet_id, sheet_name)
    try:
        n = write_to_sheet(result, spreadsheet_id, sheet_name, credentials_path,
                           client_cls=GoogleSheets, columns=SHEET_OUTPUT_COLUMNS)
    except Exception as e:  # noqa: BLE001
        logger.error("❌ Failed to write to Google Sheets: %s", e)
        return 1
    logger.info("🎉 Wrote %s row(s) to %r", n, sheet_name)
    return 0
