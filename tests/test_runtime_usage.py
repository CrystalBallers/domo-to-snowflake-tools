"""Tests for the name-based Runtime Usage command (tools/runtime_usage.py).

Covers source resolution by name (present / missing / collision), the pandas
reproduction of dataflow 621 (group by dataflow + day, runtime from string
Start/End times, null End Time dropped), the time-window precedence and filtering,
and the end-to-end orchestration with Domo + Google Sheets mocked.
"""

import argparse

import pandas as pd
import pytest
from unittest.mock import patch

import tools.runtime_usage as ru
from tools.runtime_usage import (
    resolve_sources, compute_runtime_usage, resolve_window, apply_time_window,
    run_runtime_usage, OUTPUT_COLUMNS, REQUIRED_SOURCE_NAMES, DATE_COLUMN,
)
from tools.utils.domo_export import write_to_sheet


# ── Fixtures: synthetic "DataFlow History" with a known, hand-computable result ─
def _history_frame():
    """DataFlow History — runs for two dataflows across two days.

    f1 / 2026-06-01: two runs (60s and 120s) → avg 90s, total 180s.
    f1 / 2026-06-02: one run, 30s, but a NULL End Time row that must be dropped.
    f2 / 2026-01-01: one run, 10s — outside the default 3-month window.
    """
    rows = [
        # Dataflow ID, Display Name, Input, Output, Start Time, End Time
        ("f1", "Flow One", 100, 200, "2026-06-01 00:00:00", "2026-06-01 00:01:00"),  # 60s
        ("f1", "Flow One", 300, 400, "2026-06-01 12:00:00", "2026-06-01 12:02:00"),  # 120s
        ("f1", "Flow One", 500, 600, "2026-06-02 00:00:00", "2026-06-02 00:00:30"),  # 30s
        ("f1", "Flow One", 700, 800, "2026-06-02 09:00:00", None),                   # dropped
        ("f2", "Flow Two", 10, 20, "2026-01-01 00:00:00", "2026-01-01 00:00:10"),    # 10s
    ]
    df = pd.DataFrame(rows, columns=[
        "Dataflow ID", "Display Name", "Input Row Count", "Output Row Count",
        "Start Time", "End Time"])
    # Pass-through columns of the real source (ignored by the aggregation).
    df["Link"] = "http://x"
    df["Type"] = "MAGIC"
    df["Inputs"] = "a"
    df["Outputs"] = "b"
    return df


def _datasets(extra=None):
    base = [{"id": "idDFH", "name": ru.DATAFLOW_HISTORY, "row_count": 100,
             "last_updated": "2026-06-01"}]
    if extra:
        base.extend(extra)
    return base


def _args(**overrides):
    defaults = dict(
        spreadsheet_id="sheet123", sheet_name="RU", credentials="/fake/creds.json",
        months=3, start_date=None, end_date=None, all=False, dry_run=False,
        test_connection=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class _FakeHandler:
    """Stand-in for DomoHandler: returns a synthetic frame keyed by dataset id."""

    def __init__(self, datasets, frames_by_id, auth=True):
        self._datasets = datasets
        self._frames = frames_by_id
        self._auth = auth
        self.queries = {}

    def setup_auth(self):
        return self._auth

    def get_all_datasets(self, batch_size=500):
        return self._datasets

    def extract_data(self, dataset_id, query=None, chunk_size=None,
                     enable_auto_type_conversion=False):
        self.queries[dataset_id] = query
        return self._frames[dataset_id].copy()


def _frames_by_id():
    return {"idDFH": _history_frame()}


# ── resolve_sources ──────────────────────────────────────────────────────────
class TestResolveSources:
    def test_present(self):
        resolved, missing, collisions = resolve_sources(_datasets())
        assert missing == []
        assert collisions == {}
        assert resolved[ru.DATAFLOW_HISTORY] == "idDFH"

    def test_tolerates_whitespace_and_case(self):
        datasets = [{"id": "z", "name": "  dataflow history ", "row_count": 1}]
        resolved, missing, _ = resolve_sources(datasets)
        assert missing == []
        assert resolved[ru.DATAFLOW_HISTORY] == "z"

    def test_missing_source_reported(self):
        resolved, missing, _ = resolve_sources([{"id": "x", "name": "Something Else"}])
        assert missing == [ru.DATAFLOW_HISTORY]
        assert ru.DATAFLOW_HISTORY not in resolved

    def test_collision_picks_highest_row_count(self):
        extra = [{"id": "idDFH2", "name": ru.DATAFLOW_HISTORY, "row_count": 999,
                  "last_updated": "2026-01-01"}]
        resolved, missing, collisions = resolve_sources(_datasets(extra))
        assert missing == []
        assert resolved[ru.DATAFLOW_HISTORY] == "idDFH2"
        assert collisions[ru.DATAFLOW_HISTORY][0] == "idDFH2"
        assert "idDFH" in collisions[ru.DATAFLOW_HISTORY][1]


# ── compute_runtime_usage ────────────────────────────────────────────────────
class TestComputeRuntimeUsage:
    def test_output_shape_and_columns(self):
        out = compute_runtime_usage(_history_frame())
        assert list(out.columns) == OUTPUT_COLUMNS
        # Three groups: (f1, 06-01), (f1, 06-02), (f2, 01-01). The null End Time row drops.
        assert len(out) == 3

    def test_grouped_by_dataflow_and_day(self):
        out = compute_runtime_usage(_history_frame()).set_index(["dataflow_id", "run_date"])

        g1 = out.loc[("f1", "2026-06-01")]
        assert int(g1["runs_considered"]) == 2
        assert g1["avg_runtime_seconds"] == pytest.approx(90.0)      # (60 + 120) / 2
        assert g1["total_runtime_hours"] == pytest.approx(180 / 3600)
        assert g1["avg_input_rows"] == pytest.approx(200.0)          # (100 + 300) / 2
        assert g1["avg_output_rows"] == pytest.approx(300.0)         # (200 + 400) / 2
        assert g1["display_name"] == "Flow One"
        assert g1["oldest_run"] == "2026-06-01 00:00:00"
        assert g1["newest_run"] == "2026-06-01 12:00:00"

    def test_null_end_time_row_dropped(self):
        out = compute_runtime_usage(_history_frame()).set_index(["dataflow_id", "run_date"])
        # f1 / 2026-06-02 had a 30s run plus a NULL-End-Time run; only the 30s counts.
        g2 = out.loc[("f1", "2026-06-02")]
        assert int(g2["runs_considered"]) == 1
        assert g2["avg_runtime_seconds"] == pytest.approx(30.0)

    def test_missing_required_column_raises(self):
        df = _history_frame().drop(columns=["End Time"])
        with pytest.raises(ValueError, match="DataFlow History.*End Time"):
            compute_runtime_usage(df)


# ── write_to_sheet defensive column reordering (shared writer) ───────────────
class TestWriteColumnOrder:
    def _client_cls(self):
        from unittest.mock import MagicMock
        gs = MagicMock()
        gs.get_sheet_properties.return_value = {"sheets": [{"properties": {"title": "RU"}}]}
        return MagicMock(return_value=gs), gs

    def test_scrambled_columns_written_in_canonical_order(self):
        # A DataFrame whose columns are shuffled and missing one of the canonical
        # columns must still be written in exactly OUTPUT_COLUMNS order.
        scrambled = pd.DataFrame({
            "newest_run": ["2026-06-01 00:01:00"],
            "dataflow_id": ["f1"],
            "run_date": ["2026-06-01"],
            "runs_considered": [1],
        })
        client_cls, gs = self._client_cls()
        write_to_sheet(scrambled, "sheet", "RU", "/creds", client_cls,
                       columns=OUTPUT_COLUMNS)
        header, *_ = gs.write_range.call_args.args[2]
        assert header == OUTPUT_COLUMNS                 # canonical order, not df order

    def test_extra_columns_dropped(self):
        df = pd.DataFrame({c: ["x"] for c in OUTPUT_COLUMNS})
        df["__internal"] = ["leak"]                     # not in the canonical list
        client_cls, gs = self._client_cls()
        write_to_sheet(df, "sheet", "RU", "/creds", client_cls, columns=OUTPUT_COLUMNS)
        header, *_ = gs.write_range.call_args.args[2]
        assert "__internal" not in header
        assert header == OUTPUT_COLUMNS


# ── resolve_window precedence ────────────────────────────────────────────────
class TestResolveWindow:
    TODAY = pd.Timestamp("2026-06-19")

    def test_default_is_three_months(self):
        w = resolve_window(_args(months=None), today=self.TODAY)
        assert w.is_all is False
        assert w.start == pd.Timestamp("2026-03-19")
        assert w.end is None

    def test_months_override(self):
        w = resolve_window(_args(months=6), today=self.TODAY)
        assert w.start == pd.Timestamp("2025-12-19")

    def test_explicit_range_overrides_months(self):
        w = resolve_window(_args(months=6, start_date="2026-01-01", end_date="2026-03-31"),
                           today=self.TODAY)
        assert w.start == pd.Timestamp("2026-01-01")
        assert w.end == pd.Timestamp("2026-03-31")

    def test_all_overrides_everything(self):
        w = resolve_window(_args(all=True, start_date="2026-01-01", months=6), today=self.TODAY)
        assert w.is_all is True
        assert w.pushdown_query() is None

    def test_pushdown_query_uses_start_time(self):
        w = resolve_window(_args(start_date="2026-01-01", end_date="2026-03-31"), today=self.TODAY)
        q = w.pushdown_query()
        assert "`Start Time` >= '2026-01-01'" in q
        assert "`Start Time` <= '2026-03-31'" in q


# ── apply_time_window (on raw Start Time strings) ────────────────────────────
class TestApplyTimeWindow:
    def test_default_window_drops_old_rows(self):
        df = _history_frame()
        w = resolve_window(_args(months=3), today=pd.Timestamp("2026-06-19"))
        filtered, kept, dropped = apply_time_window(df, w)
        # The 2026-01-01 (f2) row falls outside the 3-month window.
        assert dropped == 1
        assert kept == len(df) - 1
        assert "f2" not in set(filtered["Dataflow ID"])

    def test_explicit_range_inclusive_of_end_day(self):
        df = _history_frame()
        w = resolve_window(_args(start_date="2026-06-01", end_date="2026-06-02"))
        filtered, kept, dropped = apply_time_window(df, w)
        # Both 2026-06-02 runs (incl. 09:00) survive — end bound covers the whole day.
        assert kept == 4
        assert set(filtered["Dataflow ID"]) == {"f1"}

    def test_all_keeps_full_history(self):
        df = _history_frame()
        w = resolve_window(_args(all=True))
        filtered, kept, dropped = apply_time_window(df, w)
        assert dropped == 0 and kept == len(df)


# ── run_runtime_usage orchestration ──────────────────────────────────────────
class TestRunRuntimeUsage:
    def _patched_run(self, args, handler):
        with patch("tools.runtime_usage.DomoHandler", return_value=handler), \
             patch("tools.runtime_usage.GoogleSheets") as MockGS:
            gs = MockGS.return_value
            gs.get_sheet_properties.return_value = {"sheets": [{"properties": {"title": "RU"}}]}
            rc = run_runtime_usage(args)
            return rc, gs

    def test_source_present_writes_grouped_output(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(all=True), handler)
        assert rc == 0
        assert gs.write_range.called
        header, *rows = gs.write_range.call_args.args[2]
        assert header == OUTPUT_COLUMNS
        assert len(rows) == 3                           # full history, grouped by dataflow+day

    def test_missing_source_errors_and_writes_nothing(self):
        handler = _FakeHandler([{"id": "x", "name": "Other"}], _frames_by_id())
        rc, gs = self._patched_run(_args(), handler)
        assert rc == 1
        assert not gs.write_range.called

    def test_collision_picks_deterministically_and_proceeds(self):
        extra = [{"id": "idDFH2", "name": ru.DATAFLOW_HISTORY, "row_count": 999,
                  "last_updated": "2026-01-01"}]
        frames = _frames_by_id()
        frames["idDFH2"] = _history_frame()             # the chosen (higher-row_count) id
        handler = _FakeHandler(_datasets(extra), frames)
        rc, gs = self._patched_run(_args(all=True), handler)
        assert rc == 0
        assert "idDFH2" in handler.queries              # the winner was extracted
        assert "idDFH" not in handler.queries           # the loser was not

    def test_default_three_month_window_applied(self):
        # Dates relative to "today" so the assertion is stable whenever the test runs:
        # one run inside the default 3-month window, one well outside it.
        today = pd.Timestamp.today().normalize()
        recent = (today - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        old = (today - pd.Timedelta(days=200)).strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "Dataflow ID": ["f1", "f2"],
            "Display Name": ["Flow One", "Flow Two"],
            "Input Row Count": [100, 10],
            "Output Row Count": [200, 20],
            "Start Time": [f"{recent} 00:00:00", f"{old} 00:00:00"],
            "End Time": [f"{recent} 00:01:00", f"{old} 00:00:30"],
        })
        handler = _FakeHandler(_datasets(), {"idDFH": df})
        rc, gs = self._patched_run(_args(), handler)        # default months=3
        assert rc == 0
        _, *rows = gs.write_range.call_args.args[2]
        assert len(rows) == 1                                # only the recent run survives

    def test_explicit_range_override_pushed_down(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(start_date="2026-06-01", end_date="2026-06-02"), handler)
        assert rc == 0
        assert handler.queries["idDFH"] == (
            "SELECT * FROM table WHERE `Start Time` >= '2026-06-01' "
            "AND `Start Time` <= '2026-06-02'")
        _, *rows = gs.write_range.call_args.args[2]
        # Only f1's two days remain (f2's January run is excluded by the range filter).
        assert len(rows) == 2

    def test_all_is_not_pushed_down(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(all=True), handler)
        assert rc == 0
        assert handler.queries["idDFH"] is None         # full history, no filter

    def test_dry_run_writes_nothing(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(dry_run=True), handler)
        assert rc == 0
        assert not gs.write_range.called

    def test_auth_failure_returns_one(self):
        handler = _FakeHandler(_datasets(), _frames_by_id(), auth=False)
        rc, gs = self._patched_run(_args(), handler)
        assert rc == 1
        assert not gs.write_range.called

    def test_test_connection_only_lists(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(test_connection=True), handler)
        assert rc == 0
        assert not gs.write_range.called
