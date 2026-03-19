"""
Domo API pagination, translation, and Google Sheets I/O for difficulty scoring.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from .scoring import StepScore, aggregate_row, load_weights, score_steps

logger = logging.getLogger(__name__)

DATAFLOW_COL = "Dataflow ID"


def _load_gsheets_module() -> ModuleType:
    """Load tools/utils/gsheets.py without importing tools.utils package __init__."""
    gs_path = Path(__file__).resolve().parent.parent / "gsheets.py"
    name = "tools_migration_gsheets_standalone"
    spec = importlib.util.spec_from_file_location(name, gs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load gsheets from {gs_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _try_import_domo():
    try:
        from domo_utils import get_dataflow_api, get_dataset_api
        from domo_utils.auth import DeveloperTokenAuth
        from domo_utils.services.etl_translator import DataflowTranslator
        from domo_utils.services.etl_translator.core.translator_utils import (
            TranslatorUtils,
        )
        from domo_utils.services.etl_translator.models.sql_fragment import SQLDialect

        return (
            DeveloperTokenAuth,
            get_dataflow_api,
            get_dataset_api,
            DataflowTranslator,
            TranslatorUtils,
            SQLDialect,
        )
    except ImportError as e:
        raise ImportError(
            "domo_utils is required. Install argo-utils-cli in the same environment, e.g.\n"
            "  pip install -e ../argo-utils-cli\n"
            f"Original error: {e}"
        ) from e


def connect_domo():
    """Return (dataflow_api, dataset_api) using DOMO_DEVELOPER_TOKEN and DOMO_INSTANCE."""
    (
        DeveloperTokenAuth,
        get_dataflow_api,
        get_dataset_api,
        _DataflowTranslator,
        _TranslatorUtils,
        _SQLDialect,
    ) = _try_import_domo()
    token = os.getenv("DOMO_DEVELOPER_TOKEN")
    instance = os.getenv("DOMO_INSTANCE")
    if not token or not instance:
        raise ValueError(
            "Set DOMO_DEVELOPER_TOKEN and DOMO_INSTANCE environment variables."
        )
    auth = DeveloperTokenAuth(token=token, instance_id=instance)
    auth.connect()
    return get_dataflow_api(auth), get_dataset_api(auth)


def list_all_dataflows(dataflow_api, page_size: int = 50) -> List[Any]:
    """Paginate Domo list endpoint until a short page."""
    out: List[Any] = []
    offset = 0
    while True:
        batch = dataflow_api.list(limit=page_size, offset=offset)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    logger.info("Listed %s dataflows from Domo API", len(out))
    return out


def dataflows_to_inventory_df(dataflows: List[Any]) -> pd.DataFrame:
    rows = []
    for df in dataflows:
        rows.append(
            {
                DATAFLOW_COL: str(df.id),
                "Name": getattr(df, "name", "") or "",
                "Enabled": getattr(df, "enabled", ""),
                "Draft": getattr(df, "draft", ""),
                "NumOutputs": getattr(df, "num_outputs", ""),
            }
        )
    return pd.DataFrame(rows)


def read_dataflow_ids_from_sheet(
    gsheets,
    spreadsheet_id: str,
    sheet_name: str,
    column: str = DATAFLOW_COL,
) -> List[str]:
    df = gsheets.read_to_dataframe(
        spreadsheet_id, f"{sheet_name}!A:ZZ", header=True
    )
    if df.empty or column not in df.columns:
        raise ValueError(
            f"No column {column!r} in sheet {sheet_name!r}. Columns: {list(df.columns)}"
        )
    s = df[column].dropna().astype(str).str.strip()
    s = s[s != ""]
    return s.unique().tolist()


def read_existing_output_ids(
    gsheets,
    spreadsheet_id: str,
    sheet_name: str,
) -> Set[str]:
    df = gsheets.read_to_dataframe(
        spreadsheet_id, f"{sheet_name}!A:ZZ", header=True
    )
    if df.empty or DATAFLOW_COL not in df.columns:
        return set()
    s = df[DATAFLOW_COL].dropna().astype(str).str.strip()
    return set(s.tolist())


def _column_name_case_insensitive(df: pd.DataFrame, want: str) -> Optional[str]:
    lower_want = want.lower().strip()
    for c in df.columns:
        if str(c).lower().strip() == lower_want:
            return str(c)
    return None


def read_mdaas_exclude_dataflow_ids(
    gsheets,
    spreadsheet_id: str,
    sheet_name: str,
) -> Set[str]:
    """
    Dataflow IDs to omit from scoring, from the MDAAS Tasks tab (or configured name).

    Returns an empty set if the sheet is missing, empty, or has no Dataflow ID column.
    """
    df = gsheets.read_to_dataframe(
        spreadsheet_id, f"{sheet_name}!A:ZZ", header=True
    )
    if df.empty:
        logger.info(
            "Sheet %r missing or empty; no MDAAS-task exclusions applied", sheet_name
        )
        return set()
    col = _column_name_case_insensitive(df, DATAFLOW_COL)
    if not col:
        logger.warning(
            "Sheet %r has no %r column (case-insensitive); skipping MDAAS exclusions",
            sheet_name,
            DATAFLOW_COL,
        )
        return set()
    s = df[col].dropna().astype(str).str.strip()
    s = s[s != ""]
    return set(s.tolist())


def score_one_dataflow(
    dataflow_api,
    dataset_api,
    dataflow_id: str,
    weights_cfg: Dict[str, Any],
    dialect: Any,
    translator_cls: Any,
    translator_utils: Any,
) -> Tuple[List[StepScore], Dict[str, Any], bool]:
    """
    Fetch one dataflow, translate to Snowflake SQL, score steps.

    Returns:
        step_scores, summary_row dict, translation_overall_success
    """
    df = dataflow_api.get(int(dataflow_id))
    actions = df.actions or []
    ordered = translator_utils.get_execution_order(actions)
    translator = translator_cls(dialect=dialect)
    result = translator.translate_dataflow(df, dataset_api)
    step_scores, step_points = score_steps(
        str(df.id), ordered, result.action_results, weights_cfg, dialect
    )
    summary = aggregate_row(str(df.id), step_points, len(ordered))
    return step_scores, summary, result.success


def run_export_inventory(
    spreadsheet_id: str,
    credentials_path: str,
    inventory_sheet: str,
    page_size: int,
) -> None:
    _gs = _load_gsheets_module()
    GoogleSheets = _gs.GoogleSheets
    READ_WRITE_SCOPES = _gs.READ_WRITE_SCOPES

    dataflow_api, _ = connect_domo()
    flows = list_all_dataflows(dataflow_api, page_size=page_size)
    inv_df = dataflows_to_inventory_df(flows)
    client = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
    client.write_dataframe(
        inv_df, spreadsheet_id, f"{inventory_sheet}!A1", include_header=True
    )
    logger.info("Wrote %s rows to %s", len(inv_df), inventory_sheet)


def run_score(
    spreadsheet_id: str,
    credentials_path: str,
    output_sheet: str,
    detail_sheet: str,
    weights_path: Optional[Path],
    page_size: int,
    max_dataflows: Optional[int],
    skip_existing: bool,
    ids: Optional[List[str]],
    from_sheet: Optional[str],
    from_api_list: bool,
    apply_mdaas_exclusions: bool = True,
    mdaas_tasks_sheet: Optional[str] = None,
) -> None:
    _gs = _load_gsheets_module()
    GoogleSheets = _gs.GoogleSheets
    READ_WRITE_SCOPES = _gs.READ_WRITE_SCOPES

    sheets_client = GoogleSheets(
        credentials_path=credentials_path, scopes=READ_WRITE_SCOPES
    )

    weights_cfg = load_weights(weights_path)
    (
        _DevAuth,
        _gdfa,
        _gdsa,
        DataflowTranslator,
        translator_utils,
        SQLDialect,
    ) = _try_import_domo()
    dataflow_api, dataset_api = connect_domo()
    dialect = SQLDialect.SNOWFLAKE

    if from_api_list:
        flows = list_all_dataflows(dataflow_api, page_size=page_size)
        id_list = [str(f.id) for f in flows]
    elif from_sheet:
        id_list = read_dataflow_ids_from_sheet(
            sheets_client, spreadsheet_id, from_sheet
        )
    elif ids:
        id_list = [str(i).strip() for i in ids if str(i).strip()]
    else:
        raise ValueError(
            "Provide one of: --from-api-list, --from-sheet SHEET, or --ids ..."
        )

    mdaas_tab = mdaas_tasks_sheet
    if mdaas_tab is None:
        mdaas_tab = os.getenv("TRANSLATION_DIFFICULTY_MDAAS_TASKS_SHEET", "MDAAS Tasks")

    if apply_mdaas_exclusions and mdaas_tab.strip():
        excluded = read_mdaas_exclude_dataflow_ids(
            sheets_client, spreadsheet_id, mdaas_tab.strip()
        )
        if excluded:
            before = len(id_list)
            id_list = [i for i in id_list if str(i).strip() not in excluded]
            logger.info(
                "MDAAS exclusions (%s): removed %s id(s), %s remaining",
                mdaas_tab,
                before - len(id_list),
                len(id_list),
            )
    elif apply_mdaas_exclusions and not mdaas_tab.strip():
        logger.info("MDAAS tasks sheet name empty; no exclusions applied")

    if max_dataflows is not None:
        id_list = id_list[: max_dataflows]

    if skip_existing:
        existing = read_existing_output_ids(sheets_client, spreadsheet_id, output_sheet)
        before = len(id_list)
        id_list = [i for i in id_list if i not in existing]
        logger.info("Skip-existing: %s -> %s ids", before, len(id_list))

    if not id_list:
        logger.warning("No dataflow IDs left to score after filters; nothing to write")
        return

    summaries: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []

    for i, did in enumerate(id_list):
        logger.info("Scoring dataflow %s (%s/%s)", did, i + 1, len(id_list))
        try:
            step_scores, summary, ok = score_one_dataflow(
                dataflow_api,
                dataset_api,
                did,
                weights_cfg,
                dialect,
                DataflowTranslator,
                translator_utils,
            )
            summary["TranslationOK"] = ok
            summaries.append(summary)
            for s in step_scores:
                detail_rows.append(
                    {
                        "dataflow_id": s.dataflow_id,
                        "action_id": s.action_id,
                        "action_type": s.action_type,
                        "base_minutes": s.base_minutes,
                        "length_minutes": s.length_minutes,
                        "total_minutes": s.total_minutes,
                        "sql_chars": s.sql_chars,
                        "success": s.success,
                    }
                )
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed dataflow %s: %s", did, e)
            summaries.append(
                {
                    "filename": f"{did}.sql",
                    "Dataflow ID": did,
                    "Step Points": "",
                    "Total Tiles": "",
                    "Subtotal Points": "",
                    "Total Points": "",
                    "TranslationOK": False,
                    "Error": str(e)[:500],
                }
            )

    main_columns = [
        "filename",
        "Dataflow ID",
        "Step Points",
        "Total Tiles",
        "Subtotal Points",
        "Total Points",
    ]
    summary_df = pd.DataFrame(summaries)
    for c in main_columns:
        if c not in summary_df.columns:
            summary_df[c] = ""
    summary_df = summary_df[main_columns]

    try:
        sheets_client.create_sheet(spreadsheet_id, output_sheet)
    except Exception:  # noqa: BLE001
        logger.debug("Sheet %s may already exist", output_sheet)

    sheets_client.write_dataframe(
        summary_df, spreadsheet_id, f"{output_sheet}!A1", include_header=True
    )
    logger.info("Wrote %s rows to sheet %s", len(summary_df), output_sheet)

    detail_df = pd.DataFrame(detail_rows)
    if not detail_df.empty:
        try:
            sheets_client.create_sheet(spreadsheet_id, detail_sheet)
        except Exception:  # noqa: BLE001
            pass
        sheets_client.write_dataframe(
            detail_df, spreadsheet_id, f"{detail_sheet}!A1", include_header=True
        )
        logger.info("Wrote %s detail rows to %s", len(detail_df), detail_sheet)
