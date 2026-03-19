"""
CLI for translation difficulty scoring.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .runner import run_export_inventory, run_score

logger = logging.getLogger(__name__)


def _default_spreadsheet_id() -> str:
    return os.getenv("MIGRATION_SPREADSHEET_ID", "")


def _default_credentials() -> str:
    return os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Weight Domo dataflow translation difficulty (Snowflake SQL)."
    )
    p.add_argument(
        "--credentials",
        default=None,
        help="Service account JSON path (default: GOOGLE_SHEETS_CREDENTIALS_FILE).",
    )
    p.add_argument(
        "--spreadsheet-id",
        default=None,
        help="Google spreadsheet id (default: MIGRATION_SPREADSHEET_ID).",
    )
    p.add_argument(
        "--weights",
        type=Path,
        default=None,
        help="Override path to weights.yaml",
    )
    p.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Domo list API page size (default 50).",
    )

    sub = p.add_subparsers(dest="command", required=True)

    inv = sub.add_parser(
        "export-inventory",
        help="List all dataflows from Domo and write an Inventory sheet tab.",
    )
    inv.add_argument(
        "--sheet",
        default=os.getenv("INTERMEDIATE_MODELS_SHEET_NAME", "Inventory"),
        help="Target sheet tab name (default Inventory or INTERMEDIATE_MODELS_SHEET_NAME).",
    )

    score = sub.add_parser(
        "score",
        help="Translate each dataflow to Snowflake SQL and write difficulty scores.",
    )
    score.add_argument(
        "--output-sheet",
        default=os.getenv("TRANSLATION_DIFFICULTY_OUTPUT_SHEET", "CTE Points Analysis"),
        help="Main results tab (matches cte_points_analysis CSV columns).",
    )
    score.add_argument(
        "--detail-sheet",
        default=os.getenv(
            "TRANSLATION_DIFFICULTY_DETAIL_SHEET", "Translation difficulty detail"
        ),
        help="Per-step detail tab.",
    )
    src = score.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--from-api-list",
        action="store_true",
        help="Score every dataflow returned by Domo list (paginated).",
    )
    src.add_argument(
        "--from-sheet",
        metavar="TAB_NAME",
        help="Read Dataflow ID column from this spreadsheet tab.",
    )
    src.add_argument(
        "--ids",
        metavar="ID",
        nargs="+",
        help="Explicit dataflow ids to score.",
    )
    score.add_argument(
        "--max-dataflows",
        type=int,
        default=None,
        help="Process at most this many dataflows (after ordering / filtering).",
    )
    score.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip ids already present in the output sheet.",
    )
    score.add_argument(
        "--no-mdaas-exclude",
        action="store_true",
        help=(
            "Do not exclude Dataflow IDs listed on the MDAAS Tasks sheet "
            "(default: exclude when that tab exists and has a Dataflow ID column)."
        ),
    )
    score.add_argument(
        "--mdaas-tasks-sheet",
        default=None,
        metavar="TAB_NAME",
        help=(
            "Tab name for MDAAS task exclusions "
            "(default: MDAAS Tasks or TRANSLATION_DIFFICULTY_MDAAS_TASKS_SHEET)."
        ),
    )

    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    args = build_parser().parse_args(argv)

    creds = args.credentials or _default_credentials()
    if not creds or not Path(creds).is_file():
        logger.error("Missing or invalid --credentials / GOOGLE_SHEETS_CREDENTIALS_FILE")
        return 1

    sid = args.spreadsheet_id or _default_spreadsheet_id()
    if not sid:
        logger.error("Missing --spreadsheet-id / MIGRATION_SPREADSHEET_ID")
        return 1

    try:
        if args.command == "export-inventory":
            run_export_inventory(
                spreadsheet_id=sid,
                credentials_path=creds,
                inventory_sheet=args.sheet,
                page_size=args.page_size,
            )
        elif args.command == "score":
            run_score(
                spreadsheet_id=sid,
                credentials_path=creds,
                output_sheet=args.output_sheet,
                detail_sheet=args.detail_sheet,
                weights_path=args.weights,
                page_size=args.page_size,
                max_dataflows=args.max_dataflows,
                skip_existing=args.skip_existing,
                ids=args.ids,
                from_sheet=args.from_sheet,
                from_api_list=args.from_api_list,
                apply_mdaas_exclusions=not args.no_mdaas_exclude,
                mdaas_tasks_sheet=args.mdaas_tasks_sheet,
            )
        else:
            return 1
    except Exception as e:  # noqa: BLE001
        logger.error("%s", e)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
