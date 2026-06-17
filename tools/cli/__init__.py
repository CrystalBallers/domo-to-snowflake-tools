"""Command-line interface for the Domo → Snowflake migration tools.

The argparse definition lives in :mod:`tools.cli.parser`; the command handlers
stay in :mod:`main` so they remain individually testable/patchable.
"""

from .parser import create_parser

__all__ = ["create_parser"]
