"""Tests for the name-based Credit Usage command (tools/credit_usage.py).

Covers source resolution by name (present / missing / collision), the pandas
reproduction of dataflow 620, the time-window precedence and filtering, and the
end-to-end orchestration with Domo + Google Sheets mocked.
"""

import argparse

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

import tools.credit_usage as cu
from tools.credit_usage import (
    resolve_sources, compute_credit_usage, resolve_window, apply_time_window,
    run_credit_usage, _df_to_values, OUTPUT_COLUMNS, SHEET_OUTPUT_COLUMNS,
    REQUIRED_SOURCE_NAMES, DATE_COLUMN,
)


# ── Fixtures: synthetic source frames with a known, hand-computable result ────
def _fact_frame():
    """Credit Usage | Domostats — 4 rows across two datasets and two dates."""
    rows = [
        # date, entityId, entityType, usageUnit, skuId, usageQuantity, creditsUsed, skuGroup
        ("2026-06-01", "ds1", "DataSet", "Executions", "executions-dataflow-magic-v1", 4, 8.0, "g1"),
        ("2026-01-01", "ds1", "DataSet", "Executions", "executions-dataflow-magic-v1", 2, 2.0, "g1"),
        ("2026-06-02", "ds2", "DataSet", "Rows", "overage-execution-rows", 100, 10.0, "g2"),
        ("2026-06-02", "ds2", "DataSet", "Executions", "executions-dataflow-magic-v1", 5, 5.0, "g2"),
    ]
    df = pd.DataFrame(rows, columns=[
        "date", "entityId", "entityType", "usageUnit", "skuId", "usageQuantity",
        "creditsUsed", "skuGroup"])
    # Remaining pass-through columns of the source.
    df["month"] = "2026-06"
    df["category"] = "cat"
    df["instanceId"] = "inst"
    df["domain"] = "dom"
    df["_BATCH_ID_"] = "b"
    df["_BATCH_LAST_RUN_"] = "r"
    return df


def _gov_frame():
    return pd.DataFrame({
        "Dataset ID": ["ds1", "ds2"],
        "Name": ["Dataset One", "Dataset Two"],
        "Owner ID": ["u1", "u2"],
        "Owner Name": ["Alice", "Bob"],
        "_BATCH_ID_": ["b", "b"],
        "_BATCH_LAST_RUN_": ["r", "r"],
    })


def _users_frame():
    return pd.DataFrame({
        "User ID": ["u1", "u2"],
        "Name": ["Alice U", "Bob U"],
        "Email": ["alice@argo.com", "bob@example.com"],
        "_BATCH_ID_": ["b", "b"],
        "_BATCH_LAST_RUN_": ["r", "r"],
    })


def _flows_frame():
    return pd.DataFrame({
        "Dataflow ID": ["f1", "f2"],
        "Name": ["Flow A", "Flow B"],
        "Input Dataset ID": ["ds1", "ds2"],
        "Output Dataset ID": ["ds2", "dsX"],
    })


def _frames():
    return {
        cu.CREDIT_USAGE_DOMOSTATS: _fact_frame(),
        cu.GOVERNANCE_DATASETS: _gov_frame(),
        cu.USERS: _users_frame(),
        cu.DATAFLOW_DETAILS: _flows_frame(),
    }


def _datasets(extra=None):
    """A Domo get_all_datasets() result mapping each required name to one id."""
    base = [
        {"id": "idCU", "name": cu.CREDIT_USAGE_DOMOSTATS, "row_count": 100, "last_updated": "2026-06-01"},
        {"id": "idG", "name": cu.GOVERNANCE_DATASETS, "row_count": 100, "last_updated": "2026-06-01"},
        {"id": "idU", "name": cu.USERS, "row_count": 100, "last_updated": "2026-06-01"},
        {"id": "idF", "name": cu.DATAFLOW_DETAILS, "row_count": 100, "last_updated": "2026-06-01"},
    ]
    if extra:
        base.extend(extra)
    return base


def _args(**overrides):
    defaults = dict(
        spreadsheet_id="sheet123", sheet_name="CU", credentials="/fake/creds.json",
        months=3, start_date=None, end_date=None, all=False, dry_run=False,
        test_connection=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class _FakeHandler:
    """Stand-in for DomoHandler: returns synthetic frames keyed by dataset id."""

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
    return {"idCU": _fact_frame(), "idG": _gov_frame(), "idU": _users_frame(), "idF": _flows_frame()}


# ── resolve_sources ──────────────────────────────────────────────────────────
class TestResolveSources:
    def test_all_present(self):
        resolved, missing, collisions = resolve_sources(_datasets())
        assert missing == []
        assert collisions == {}
        assert set(resolved) == set(REQUIRED_SOURCE_NAMES)
        assert resolved[cu.USERS] == "idU"

    def test_tolerates_whitespace_and_case(self):
        datasets = [
            {"id": "a", "name": "  credit usage | DOMOSTATS ", "row_count": 1},
            {"id": "b", "name": "Governance - Datasets", "row_count": 1},
            {"id": "c", "name": "users", "row_count": 1},
            {"id": "d", "name": "Dataflow Details", "row_count": 1},
        ]
        resolved, missing, _ = resolve_sources(datasets)
        assert missing == []
        assert resolved[cu.CREDIT_USAGE_DOMOSTATS] == "a"
        assert resolved[cu.USERS] == "c"

    def test_missing_source_reported(self):
        datasets = [d for d in _datasets() if d["name"] != cu.USERS]
        resolved, missing, _ = resolve_sources(datasets)
        assert missing == [cu.USERS]
        assert cu.USERS not in resolved

    def test_collision_picks_highest_row_count(self):
        extra = [{"id": "idU2", "name": cu.USERS, "row_count": 999, "last_updated": "2026-01-01"}]
        resolved, missing, collisions = resolve_sources(_datasets(extra))
        assert missing == []
        assert resolved[cu.USERS] == "idU2"          # higher row_count wins
        assert collisions[cu.USERS][0] == "idU2"
        assert "idU" in collisions[cu.USERS][1]

    def test_collision_tiebreak_on_last_updated(self):
        extra = [{"id": "idU2", "name": cu.USERS, "row_count": 100, "last_updated": "2030-01-01"}]
        resolved, _, collisions = resolve_sources(_datasets(extra))
        assert resolved[cu.USERS] == "idU2"          # equal row_count → newest wins
        assert collisions[cu.USERS][0] == "idU2"


# ── compute_credit_usage ─────────────────────────────────────────────────────
class TestComputeCreditUsage:
    def test_output_shape_and_columns(self):
        out = compute_credit_usage(_frames())
        assert list(out.columns) == OUTPUT_COLUMNS
        assert len(out) == 4                         # all 4 dedup keys distinct

    def test_formula_values(self):
        out = compute_credit_usage(_frames()).set_index(["date", "entityId", "usageUnit"])

        r1 = out.loc[("2026-06-01", "ds1", "Executions")]
        assert r1["Avg Credits"] == pytest.approx(2.0)        # 8 / usageQuantity(4)
        assert r1["Consumption Type"] == "Executions"
        assert int(r1["Argo_Owned"]) == 1                     # alice@argo.com
        assert r1["Dataset Name"] == "Dataset One"
        assert r1["Owner Email"] == "alice@argo.com"
        assert float(r1["# Execution by Day"]) == 4.0
        assert r1["Is used as input of a dataflow?"] == "True"   # ds1 feeds f1
        assert r1["Is the Output of a dataflow?"] == "False"     # ds1 not an output

        r3 = out.loc[("2026-06-02", "ds2", "Rows")]
        assert r3["Consumption Type"] == "Execution Caps"     # overage-*
        assert float(r3["# Execution by Day"]) == 5.0          # exec sum for (date,ds2)
        assert r3["Avg Credits"] == pytest.approx(2.0)        # 10 / 5
        assert r3["Is the Output of a dataflow?"] == "True"   # ds2 is output of f1

        r4 = out.loc[("2026-06-02", "ds2", "Executions")]
        assert r4["Avg Credits"] == pytest.approx(1.0)        # 5 / 5
        assert int(r4["Argo_Owned"]) == 0                     # bob@example.com

    def test_sheet_columns_are_a_valid_subset(self):
        assert set(SHEET_OUTPUT_COLUMNS).issubset(OUTPUT_COLUMNS)
        out = compute_credit_usage(_frames())
        # Every published column is producible from the full result.
        assert list(out.reindex(columns=SHEET_OUTPUT_COLUMNS).columns) == SHEET_OUTPUT_COLUMNS

    def test_nan_cells_render_blank_not_text_nan(self):
        # Float NaN (e.g. unmatched "# of dataflows feeds into") must become "" in the
        # Sheets payload, never the literal string "nan".
        df = pd.DataFrame({"a": [1.0, np.nan], "b": ["x", None]})
        values = _df_to_values(df)
        assert values == [["1.0", "x"], ["", ""]]

    def test_missing_required_column_raises(self):
        frames = _frames()
        frames[cu.USERS] = frames[cu.USERS].drop(columns=["Email"])
        with pytest.raises(ValueError, match="Users.*Email"):
            compute_credit_usage(frames)


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

    def test_pushdown_query(self):
        w = resolve_window(_args(start_date="2026-01-01", end_date="2026-03-31"), today=self.TODAY)
        q = w.pushdown_query()
        assert "`date` >= '2026-01-01'" in q
        assert "`date` <= '2026-03-31'" in q


# ── apply_time_window ────────────────────────────────────────────────────────
class TestApplyTimeWindow:
    def test_default_window_drops_old_rows(self):
        out = compute_credit_usage(_frames())
        w = resolve_window(_args(months=3), today=pd.Timestamp("2026-06-19"))
        filtered, kept, dropped = apply_time_window(out, w)
        assert kept == 3 and dropped == 1            # the 2026-01-01 row drops
        assert "2026-01-01" not in set(filtered[DATE_COLUMN])

    def test_explicit_range_respected(self):
        out = compute_credit_usage(_frames())
        w = resolve_window(_args(start_date="2026-06-02", end_date="2026-06-30"))
        filtered, kept, dropped = apply_time_window(out, w)
        assert set(filtered[DATE_COLUMN]) == {"2026-06-02"}
        assert kept == 2

    def test_all_keeps_full_history(self):
        out = compute_credit_usage(_frames())
        w = resolve_window(_args(all=True))
        filtered, kept, dropped = apply_time_window(out, w)
        assert dropped == 0 and kept == len(out) == 4


# ── run_credit_usage orchestration ───────────────────────────────────────────
class TestRunCreditUsage:
    def _patched_run(self, args, handler):
        with patch("tools.credit_usage.DomoHandler", return_value=handler), \
             patch("tools.credit_usage.GoogleSheets") as MockGS:
            gs = MockGS.return_value
            gs.get_sheet_properties.return_value = {"sheets": [{"properties": {"title": "CU"}}]}
            rc = run_credit_usage(args)
            return rc, gs

    def test_all_sources_present_writes_output(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(all=True), handler)
        assert rc == 0
        assert gs.write_range.called
        header, *rows = gs.write_range.call_args.args[2]
        assert header == SHEET_OUTPUT_COLUMNS          # only the published subset
        assert len(rows) == 4                          # full history

    def test_missing_source_errors_and_writes_nothing(self):
        datasets = [d for d in _datasets() if d["name"] != cu.DATAFLOW_DETAILS]
        handler = _FakeHandler(datasets, _frames_by_id())
        rc, gs = self._patched_run(_args(), handler)
        assert rc == 1
        assert not gs.write_range.called               # nothing written

    def test_collision_picks_deterministically_and_proceeds(self):
        extra = [{"id": "idU2", "name": cu.USERS, "row_count": 999, "last_updated": "2026-01-01"}]
        frames = _frames_by_id()
        frames["idU2"] = _users_frame()                # the chosen (higher-row_count) id
        handler = _FakeHandler(_datasets(extra), frames)
        rc, gs = self._patched_run(_args(all=True), handler)
        assert rc == 0
        assert handler.queries.get("idU2") is None     # idU2 was the one extracted
        assert "idU" not in handler.queries            # the loser was not extracted

    def test_default_three_month_window_applied(self):
        # Build fact dates relative to "today" so the assertion is stable whenever the
        # test runs: one row inside the default 3-month window, one well outside it.
        today = pd.Timestamp.today().normalize()
        recent = (today - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        old = (today - pd.Timedelta(days=200)).strftime("%Y-%m-%d")
        fact = _fact_frame()
        fact.loc[0, "date"] = recent          # ds1 Executions → kept
        fact.loc[1, "date"] = old             # ds1 Executions (older skuId date) → dropped
        fact = fact.iloc[:2]                   # just the two ds1 rows
        frames = _frames_by_id()
        frames["idCU"] = fact
        handler = _FakeHandler(_datasets(), frames)

        rc, gs = self._patched_run(_args(), handler)   # default months=3
        assert rc == 0
        _, *rows = gs.write_range.call_args.args[2]
        assert len(rows) == 1                  # only the recent row survives the window

    def test_explicit_range_override_respected(self):
        handler = _FakeHandler(_datasets(), _frames_by_id())
        rc, gs = self._patched_run(_args(start_date="2026-06-02", end_date="2026-06-30"), handler)
        assert rc == 0
        # Fact pushdown query carries the explicit bounds.
        assert handler.queries["idCU"] == "SELECT * FROM table WHERE `date` >= '2026-06-02' AND `date` <= '2026-06-30'"
        _, *rows = gs.write_range.call_args.args[2]
        assert len(rows) == 2                           # only 2026-06-02 rows

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
