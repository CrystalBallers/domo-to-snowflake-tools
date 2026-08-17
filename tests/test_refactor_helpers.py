"""Characterization tests locking the behavior extracted during the refactor.

These cover the small, pure helpers introduced when splitting the CLI parser out
of main.py and de-duplicating the spreadsheet logic in domo_to_snowflake.py, plus
the gsheets scope alias. They are intentionally dependency-free and fast.
"""

import pandas as pd
import pytest

import main
from tools.cli import commands
from tools import domo_to_snowflake as d2s
from tools.cli.parser import create_parser as parser_create_parser
from tools.utils import gsheets


class _Args:
    """Minimal stand-in for an argparse Namespace."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


# ───────────────────────── main.py: chunk-size resolver ──────────────────────
class TestResolveChunkSize:
    def test_auto_chunk_size_wins(self):
        args = _Args(auto_chunk_size=True, full_table=True)
        assert commands._resolve_chunk_size(args) == "auto"

    def test_full_table_returns_none(self):
        args = _Args(auto_chunk_size=False, full_table=True)
        assert commands._resolve_chunk_size(args) is None

    def test_default_limits_to_1000(self):
        args = _Args(auto_chunk_size=False, full_table=False)
        assert commands._resolve_chunk_size(args) == 1000


# ───────────────────────── main.py: parser re-export ─────────────────────────
class TestParserReexport:
    def test_main_create_parser_is_module_parser(self):
        assert main.create_parser is parser_create_parser

    def test_all_subcommands_present(self):
        parser = main.create_parser()
        # Parse each subcommand's no-arg form to confirm it is wired up.
        for cmd in ["inventory", "migrate", "datasets", "compare",
                    "generate-stg", "generate-sources", "stage", "weighting"]:
            args = parser.parse_args([cmd])
            assert args.command == cmd


# ──────────────────── domo_to_snowflake.py: sheet helpers ────────────────────
class TestMigrationSheetHelpers:
    def test_find_column_first_match(self):
        df = pd.DataFrame(columns=["X", "Dataset ID", "Status"])
        assert d2s._find_column(df, ["dataset_id", "Dataset ID"]) == "Dataset ID"
        assert d2s._find_column(df, ["nope", "missing"]) is None

    def test_resolve_columns_finds_existing(self):
        df = pd.DataFrame({"Dataset ID": ["a"], "Model Name": ["m"], "Status": ["Pending"]})
        assert d2s._resolve_migration_columns(df) == ("Dataset ID", "Model Name", "Status")

    def test_resolve_columns_adds_defaults_when_missing(self):
        df = pd.DataFrame({"Unrelated": [1]})
        id_col, name_col, status_col = d2s._resolve_migration_columns(df)
        assert (id_col, name_col, status_col) == ("Dataset ID", "Name", "Status")
        # Defaults were added in place
        assert {"Dataset ID", "Name", "Status"}.issubset(set(df.columns))
        assert df["Name"].iloc[0] == "Unknown"
        assert df["Status"].iloc[0] == "Pending"

    def test_filter_pending_excludes_migrated_case_insensitive(self):
        df = pd.DataFrame({"Status": ["Pending", "Migrated", "migrated", None, "Failed"]})
        pending = d2s._filter_pending_rows(df, "Status")
        # 'Pending', NaN->'Pending', 'Failed' kept; both 'Migrated' variants dropped
        assert len(pending) == 3
        assert not pending["Status"].str.contains("Migrated", case=False).any()

    @pytest.mark.parametrize("full_table,auto,expected", [
        (True, False, None),
        (True, True, None),     # full_table takes precedence
        (False, True, "auto"),
        (False, False, 1000),
    ])
    def test_resolve_spreadsheet_chunk_size(self, full_table, auto, expected):
        assert d2s._resolve_spreadsheet_chunk_size(full_table, auto) == expected


# ────────────────────────── gsheets scope alias ──────────────────────────────
class TestGsheetsScopes:
    def test_read_only_alias_matches_default(self):
        assert gsheets.READ_ONLY_SCOPES == gsheets.DEFAULT_SCOPES
        assert all("readonly" in s for s in gsheets.READ_ONLY_SCOPES)

    def test_read_write_scope_is_not_read_only(self):
        assert gsheets.READ_WRITE_SCOPES != gsheets.READ_ONLY_SCOPES
