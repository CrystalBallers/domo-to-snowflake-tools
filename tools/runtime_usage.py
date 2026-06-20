"""Reusable, name-based "Runtime Usage" report for any Domo client.

This is the sibling of :mod:`tools.credit_usage`. It reproduces a Domo dataflow
(built as **dataflow 621** on the ``grtfinancial`` instance) that summarises how long
each dataflow takes to run, but resolves its single source **by name** instead of by a
client-specific id — so the same command works on any instance.

================================================================================
SPEC
================================================================================
Single source (a Domo *DomoStats* dataset, same name across instances):

    REQUIRED_SOURCE_NAMES = ["DataFlow History"]   (matched tolerant of case/whitespace)

"DataFlow History" columns:
    Dataflow ID, Display Name, Link, Type, Inputs, Outputs,
    Input Row Count, Output Row Count, Start Time, End Time
``Start Time`` / ``End Time`` are STRINGS in ``%Y-%m-%d %H:%M:%S``. They are parsed
defensively to datetime; ``runtime_seconds = End Time − Start Time``. Rows whose
``End Time`` (or ``Start Time``) is null/unparseable have no usable runtime and are
dropped from the aggregation entirely (the dropped count is logged).

DATE_COLUMN for the time window = ``Start Time``.

Merged transformation — ONE table, regrouped by (Dataflow ID, calendar day of
``Start Time``). One row per dataflow per day. Canonical SQL (implemented in pandas):

    SELECT
        `Dataflow ID`                                              AS dataflow_id,
        CAST(`Start Time` AS DATE)                                 AS run_date,
        MAX(`Display Name`)                                        AS display_name,
        AVG(`Input Row Count`)                                     AS avg_input_rows,
        AVG(`Output Row Count`)                                    AS avg_output_rows,
        AVG(TIMESTAMPDIFF(SECOND, `Start Time`, `End Time`))       AS avg_runtime_seconds,
        SUM(TIMESTAMPDIFF(SECOND, `Start Time`, `End Time`))/3600  AS total_runtime_hours,
        COUNT(*)                                                   AS runs_considered,
        MIN(`Start Time`)                                          AS oldest_run,
        MAX(`Start Time`)                                          AS newest_run
    FROM `dataflow_history`
    WHERE `Start Time` >= <window_start>   -- default: today − 3 months (inclusive)
    GROUP BY `Dataflow ID`, CAST(`Start Time` AS DATE)
    ORDER BY `Dataflow ID`, run_date;
================================================================================
"""

import os
import logging
from typing import List

import pandas as pd

from tools.utils.domo import DomoHandler
from tools.utils.gsheets import GoogleSheets
from tools.utils import domo_export
from tools.utils.domo_export import apply_time_window, write_to_sheet

logger = logging.getLogger(__name__)

# ── Source-name contract (resolved by name, never by ID) ─────────────────────
DATAFLOW_HISTORY = "DataFlow History"
REQUIRED_SOURCE_NAMES: List[str] = [DATAFLOW_HISTORY]

# The timestamp column that drives the time window. ``Start Time`` is a string in
# the format below; the window is pushed down to the source as a lexical comparison.
DATE_COLUMN = "Start Time"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Defaults read from the environment first, falling back to the literal only when the
# env var is unset (so the same configuration drives the CLI and any programmatic call).
DEFAULT_SHEET_NAME = os.getenv("RUNTIME_USAGE_SHEET_NAME", "Runtime")
DEFAULT_MONTHS = int(os.getenv("RUNTIME_USAGE_MONTHS") or 3)

# Final output columns, in order.
OUTPUT_COLUMNS: List[str] = [
    "dataflow_id", "run_date", "display_name", "avg_input_rows", "avg_output_rows",
    "avg_runtime_seconds", "total_runtime_hours", "runs_considered",
    "oldest_run", "newest_run",
]

# Columns the source MUST provide for the aggregation to work.
_REQUIRED_COLUMNS: List[str] = [
    "Dataflow ID", "Display Name", "Input Row Count", "Output Row Count",
    "Start Time", "End Time",
]


def resolve_sources(datasets: List[dict], required: List[str] = None):
    """Resolve this command's :data:`REQUIRED_SOURCE_NAMES` (see :mod:`domo_export`)."""
    return domo_export.resolve_sources(datasets, required or REQUIRED_SOURCE_NAMES)


def resolve_window(args, today=None) -> domo_export.WindowSpec:
    """Resolve the time window from CLI args, bound to ``Start Time``.

    Precedence: ``--all`` > explicit ``--start-date``/``--end-date`` > ``--months`` >
    default (last :data:`DEFAULT_MONTHS` months). See :func:`domo_export.resolve_window`.
    """
    return domo_export.resolve_window(args, DATE_COLUMN, DEFAULT_MONTHS, today=today)


# ── Core computation (the merged dataflow-621 spec, in pandas) ───────────────
def _require_columns(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Source {DATAFLOW_HISTORY!r} is missing required column(s): {missing}. "
            f"Found columns: {list(df.columns)}"
        )


def _parse_times(series: pd.Series) -> pd.Series:
    """Parse a Start/End Time string column to datetime, defensively.

    Tries the known ``%Y-%m-%d %H:%M:%S`` format first, then falls back to pandas'
    flexible parser for any value the strict format couldn't read. Unparseable /
    blank values become ``NaT``.
    """
    s = series.astype("string")
    parsed = pd.to_datetime(s, format=TIME_FORMAT, errors="coerce")
    retry = parsed.isna() & s.notna() & (s.str.strip() != "")
    if retry.any():
        parsed.loc[retry] = pd.to_datetime(s[retry], errors="coerce")
    return parsed


def compute_runtime_usage(df: pd.DataFrame) -> pd.DataFrame:
    """Reproduce dataflow 621 in pandas from the "DataFlow History" frame.

    Groups by (Dataflow ID, calendar day of Start Time) into exactly
    :data:`OUTPUT_COLUMNS`. Rows with a null/unparseable Start or End Time are dropped
    from the aggregation (the count is logged).

    Raises:
        ValueError: if a required column is missing.
    """
    _require_columns(df, _REQUIRED_COLUMNS)
    work = df.copy()

    start = _parse_times(work["Start Time"])
    end = _parse_times(work["End Time"])
    runtime = (end - start).dt.total_seconds()

    valid = start.notna() & end.notna() & runtime.notna()
    dropped = int((~valid).sum())
    if dropped:
        logger.info("🧹 Dropped %s run(s) with null/unparseable Start/End Time "
                    "(no usable runtime)", dropped)

    work = work.loc[valid].copy()
    work["__start"] = start[valid]
    work["__runtime"] = runtime[valid]
    work["__run_date"] = work["__start"].dt.strftime("%Y-%m-%d")
    work["__in"] = pd.to_numeric(work["Input Row Count"], errors="coerce")
    work["__out"] = pd.to_numeric(work["Output Row Count"], errors="coerce")
    work["__name"] = work["Display Name"].astype("string")

    if work.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    grouped = work.groupby(["Dataflow ID", "__run_date"], dropna=False).agg(
        display_name=("__name", "max"),
        avg_input_rows=("__in", "mean"),
        avg_output_rows=("__out", "mean"),
        avg_runtime_seconds=("__runtime", "mean"),
        total_runtime_seconds=("__runtime", "sum"),
        runs_considered=("__runtime", "size"),
        oldest_run=("__start", "min"),
        newest_run=("__start", "max"),
    ).reset_index()

    grouped = grouped.rename(columns={"Dataflow ID": "dataflow_id", "__run_date": "run_date"})
    grouped["total_runtime_hours"] = grouped.pop("total_runtime_seconds") / 3600.0
    grouped["runs_considered"] = grouped["runs_considered"].astype(int)
    grouped["oldest_run"] = grouped["oldest_run"].dt.strftime(TIME_FORMAT)
    grouped["newest_run"] = grouped["newest_run"].dt.strftime(TIME_FORMAT)

    grouped = grouped.sort_values(["dataflow_id", "run_date"]).reset_index(drop=True)
    return grouped.reindex(columns=OUTPUT_COLUMNS)


# ── Orchestrator (called by the CLI handler) ─────────────────────────────────
def run_runtime_usage(args) -> int:
    """Resolve "DataFlow History" by name, compute Runtime Usage, window it, write it."""
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

    # 3. List datasets and resolve the required source BY NAME.
    logger.info("🔎 Listing Domo datasets to resolve %r by name...", DATAFLOW_HISTORY)
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

    dataset_id = resolved[DATAFLOW_HISTORY]

    # 4. Extract (push the date filter down to Start Time when bounded).
    query = None
    if not window.is_all and window.has_bounds():
        query = window.pushdown_query()
        logger.info("⏬ Pushing date filter down to %r: %s", DATAFLOW_HISTORY, query)
    df = handler.extract_data(dataset_id, query=query)
    if df is None:
        logger.error("❌ Failed to extract data for %r (id=%s)", DATAFLOW_HISTORY, dataset_id)
        return 1
    logger.info("   📥 %r: %s rows × %s cols", DATAFLOW_HISTORY, len(df), len(df.columns))

    # 5. Apply the time window on Start Time (idempotent if it was pushed down).
    if not window.is_all:
        df, kept, dropped = apply_time_window(df, window)
        logger.info("🪟 Window on %r — kept %s row(s), dropped %s", DATE_COLUMN, kept, dropped)

    # 6. Compute the merged Runtime Usage table.
    try:
        result = compute_runtime_usage(df)
    except (ValueError, KeyError) as e:
        logger.error("❌ Failed to compute Runtime Usage: %s", e)
        return 1
    logger.info("📊 Runtime Usage result: %s row(s) × %s column(s) (one row per dataflow per day)",
                len(result), len(result.columns))

    # 7a. --dry-run: prove resolution + computation + window without touching Sheets.
    if getattr(args, "dry_run", False):
        logger.info("🧪 Dry-run — NOT writing to Google Sheets.")
        logger.info("   Resolved source: %r → %s", DATAFLOW_HISTORY, dataset_id)
        logger.info("   Resolved window: %s", window.describe())
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

    logger.info("📝 Writing Runtime Usage to spreadsheet=%s tab=%r ...", spreadsheet_id, sheet_name)
    try:
        n = write_to_sheet(result, spreadsheet_id, sheet_name, credentials_path,
                           client_cls=GoogleSheets, columns=OUTPUT_COLUMNS)
    except Exception as e:  # noqa: BLE001
        logger.error("❌ Failed to write to Google Sheets: %s", e)
        return 1
    logger.info("🎉 Wrote %s row(s) to %r", n, sheet_name)
    return 0
