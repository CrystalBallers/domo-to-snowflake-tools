"""Shared building blocks for name-based Domo → Google Sheets reports.

The ``credit-usage`` and ``runtime-usage`` commands are siblings: both authenticate
to Domo, resolve their source DomoStats datasets **by name** (never by a client-specific
id), filter to a configurable time window, and publish the result to a Google Sheets
tab. This module factors out everything those two share so each command module only
holds its own transformation:

  * :func:`resolve_sources` — map required source NAMES → dataset ids, with the
    "missing source" and "name collision → deterministic pick" rules.
  * :class:`WindowSpec` / :func:`resolve_window` / :func:`apply_time_window` — the
    ``--months`` / ``--start-date`` / ``--end-date`` / ``--all`` time-window parsing,
    pushdown-query generation and post-filter, parametrised by the date column.
  * :func:`write_to_sheet` — create the tab if absent, then clear + write header + rows.

Each command module imports ``DomoHandler`` / ``GoogleSheets`` into *its own* namespace
(so tests can patch them per-command) and passes the ``GoogleSheets`` class into
:func:`write_to_sheet` via ``client_cls``.
"""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

from tools.utils.gsheets import READ_WRITE_SCOPES

logger = logging.getLogger(__name__)


# ── Source resolution by name ────────────────────────────────────────────────
def _norm(name) -> str:
    """Normalise a dataset name for tolerant matching (whitespace + case)."""
    return str(name).strip().casefold()


def _pick_winner(candidates: List[dict]) -> dict:
    """Deterministically choose among datasets sharing a name.

    Highest ``row_count``, tie-break on newest ``last_updated``, then id (so the
    pick is stable regardless of listing order).
    """
    def sort_key(c: dict):
        rc = c.get("row_count") or 0
        try:
            rc = int(rc)
        except (TypeError, ValueError):
            rc = 0
        return (rc, str(c.get("last_updated") or ""), str(c.get("id")))

    return max(candidates, key=sort_key)


def resolve_sources(
    datasets: List[dict], required: List[str]
) -> Tuple[Dict[str, str], List[str], Dict[str, Tuple[str, List[str]]]]:
    """Map every required source NAME to a dataset id.

    Args:
        datasets: list of dicts with at least ``id`` and ``name`` (as returned by
            :meth:`DomoHandler.get_all_datasets`).
        required: the source names to resolve.

    Returns:
        ``(resolved, missing, collisions)`` where
          * ``resolved``  : {name -> dataset_id} for everything found,
          * ``missing``   : names with no match (a hard error for the caller),
          * ``collisions``: {name -> (chosen_id, [other_ids])} for names that
            matched more than one dataset.
    """
    index: Dict[str, List[dict]] = {}
    for d in datasets:
        index.setdefault(_norm(d.get("name")), []).append(d)

    resolved: Dict[str, str] = {}
    missing: List[str] = []
    collisions: Dict[str, Tuple[str, List[str]]] = {}

    for name in required:
        candidates = index.get(_norm(name), [])
        if not candidates:
            missing.append(name)
            continue
        if len(candidates) == 1:
            resolved[name] = str(candidates[0]["id"])
            continue
        winner = _pick_winner(candidates)
        winner_id = str(winner["id"])
        resolved[name] = winner_id
        others = [str(c["id"]) for c in candidates if str(c["id"]) != winner_id]
        collisions[name] = (winner_id, others)

    return resolved, missing, collisions


# ── Time window ──────────────────────────────────────────────────────────────
def _parse_date(value: str) -> pd.Timestamp:
    try:
        return pd.Timestamp(value).normalize()
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"Invalid date {value!r}; expected YYYY-MM-DD ({e})") from e


class WindowSpec:
    """A resolved time window for a named date column."""

    def __init__(self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp],
                 is_all: bool, label: str, date_column: str):
        self.start = start
        self.end = end
        self.is_all = is_all
        self.label = label
        self.date_column = date_column

    def has_bounds(self) -> bool:
        return self.start is not None or self.end is not None

    def pushdown_query(self) -> Optional[str]:
        """A Domo query that pushes the window down to the source, or None for ALL.

        Compares against ``YYYY-MM-DD``; this is correct for both ``YYYY-MM-DD`` date
        columns and ``YYYY-MM-DD HH:MM:SS`` timestamp strings (lexicographic order
        agrees with chronological order for that fixed-width format).
        """
        if self.is_all or not self.has_bounds():
            return None
        clauses = []
        if self.start is not None:
            clauses.append(f"`{self.date_column}` >= '{self.start.strftime('%Y-%m-%d')}'")
        if self.end is not None:
            clauses.append(f"`{self.date_column}` <= '{self.end.strftime('%Y-%m-%d')}'")
        return f"SELECT * FROM table WHERE {' AND '.join(clauses)}"

    def describe(self) -> str:
        return self.label


def resolve_window(args, date_column: str, default_months: int,
                   today: Optional[pd.Timestamp] = None) -> WindowSpec:
    """Resolve the time window from CLI args for ``date_column``.

    Precedence: ``--all`` > explicit ``--start-date``/``--end-date`` > ``--months`` >
    default (last ``default_months`` months).
    """
    if bool(getattr(args, "all", False)):
        return WindowSpec(None, None, True, "ALL history (no date filter)", date_column)

    start_s = getattr(args, "start_date", None)
    end_s = getattr(args, "end_date", None)
    if start_s or end_s:
        start = _parse_date(start_s) if start_s else None
        end = _parse_date(end_s) if end_s else None
        lo = start.date() if start is not None else "(open)"
        hi = end.date() if end is not None else "(open)"
        return WindowSpec(start, end, False, f"explicit range {lo} .. {hi}", date_column)

    months = getattr(args, "months", None)
    months = default_months if months is None else int(months)
    today = today if today is not None else pd.Timestamp.today().normalize()
    start = (today - pd.DateOffset(months=months)).normalize()
    return WindowSpec(start, None, False,
                      f"last {months} month(s): {date_column} >= {start.date()}", date_column)


def apply_time_window(df: pd.DataFrame, window: WindowSpec) -> Tuple[pd.DataFrame, int, int]:
    """Filter ``df`` to ``window`` on its date column. Returns (df, kept, dropped).

    Parses the date column defensively; unparseable rows are dropped when a bound is
    in effect. A no-op (everything kept) for ``--all`` or an unbounded window.
    """
    col = window.date_column
    if window.is_all or not window.has_bounds() or col not in df.columns:
        return df.reset_index(drop=True), len(df), 0

    parsed = pd.to_datetime(df[col], errors="coerce")
    mask = pd.Series(True, index=df.index)
    if window.start is not None:
        mask &= parsed >= window.start
    if window.end is not None:
        # End bound is inclusive of the whole day for timestamp columns.
        mask &= parsed < (window.end + pd.Timedelta(days=1))
    mask = mask.fillna(False)
    kept = int(mask.sum())
    return df[mask].reset_index(drop=True), kept, int(len(df) - kept)


# ── Sheet output ─────────────────────────────────────────────────────────────
def _df_to_values(df: pd.DataFrame) -> List[List]:
    """DataFrame → list-of-rows for Sheets; NaN/NaT/None → "" (USER_ENTERED parses numbers).

    Per-cell ``pd.isna`` (not ``df.where(..., None)``) so float ``NaN`` becomes "" instead
    of the literal text "nan" — a float column can't actually hold ``None``.
    """
    return [["" if pd.isna(v) else str(v) for v in rec]
            for rec in df.itertuples(index=False, name=None)]


def _quote_sheet(name: str) -> str:
    """Quote a tab name for A1 notation (required when it contains spaces, etc.).

    Single-quote the title and escape any internal single quote by doubling it, per
    Google Sheets A1 rules. Without this, ranges like ``Runtime Usage!A1:ZZ100000``
    are mis-parsed and a clear silently no-ops, leaving stale columns behind.
    """
    return "'" + str(name).replace("'", "''") + "'"


def _ensure_sheet(client, spreadsheet_id: str, sheet_name: str) -> None:
    """Create ``sheet_name`` if it isn't already a tab in the spreadsheet."""
    try:
        props = client.get_sheet_properties(spreadsheet_id)
        titles = [s["properties"]["title"] for s in props.get("sheets", [])]
    except Exception:  # noqa: BLE001
        titles = []
    if sheet_name not in titles:
        logger.info("📄 Tab %r not found; creating it...", sheet_name)
        try:
            client.create_sheet(spreadsheet_id, sheet_name)
        except Exception:  # noqa: BLE001
            pass


def write_to_sheet(df: pd.DataFrame, spreadsheet_id: str, sheet_name: str,
                   credentials_path: str, client_cls,
                   columns: Optional[List[str]] = None) -> int:
    """Clear ``sheet_name`` and write the header + rows of ``df``. Returns row count.

    ``columns`` is the canonical column order to publish. When given, ``df`` is
    reindexed to exactly those columns, in that order, **before** writing — so the
    sheet's column order never depends on whatever order the DataFrame happened to be
    built in (a missing column comes out blank, an extra one is dropped). Callers that
    care about column order should always pass it; this is what keeps every report's
    layout stable and reproducible across runs.

    ``client_cls`` is the ``GoogleSheets`` class (passed in by each command module so
    tests can patch it in the command's own namespace).
    """
    if columns is not None:
        df = df.reindex(columns=columns)
    client = client_cls(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
    _ensure_sheet(client, spreadsheet_id, sheet_name)
    values = [list(df.columns)] + _df_to_values(df)
    qname = _quote_sheet(sheet_name)
    client.clear_range(spreadsheet_id, f"{qname}!A1:ZZ100000")
    client.write_range(spreadsheet_id, f"{qname}!A1", values)
    return len(df)
