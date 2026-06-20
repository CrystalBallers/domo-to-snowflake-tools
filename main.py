#!/usr/bin/env python3
"""Unified CLI entry point for the Domo → Snowflake migration tools.

This module only wires things together: argument parsing lives in
``tools.cli.parser`` and the command logic in ``tools.cli.commands``.
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from tools.cli.parser import create_parser
    from tools.cli.commands import (
        handle_inventory_command, handle_dataflow_raw_command, handle_migrate_command,
        handle_stage_command, handle_datasets_command, handle_compare_command,
        handle_generate_stg_command, handle_generate_sources_command, handle_weighting_command,
        handle_refresh_command, handle_credit_usage_command, handle_runtime_usage_command,
    )
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    sys.exit(1)

# command name -> handler. Handlers are looked up here so that monkeypatching
# main.handle_* in tests still takes effect.
_HANDLERS = {
    'inventory': 'handle_inventory_command',
    'dataflow-raw': 'handle_dataflow_raw_command',
    'migrate': 'handle_migrate_command',
    'stage': 'handle_stage_command',
    'datasets': 'handle_datasets_command',
    'compare': 'handle_compare_command',
    'generate-stg': 'handle_generate_stg_command',
    'generate-sources': 'handle_generate_sources_command',
    'weighting': 'handle_weighting_command',
    'refresh': 'handle_refresh_command',
    'credit-usage': 'handle_credit_usage_command',
    'runtime-usage': 'handle_runtime_usage_command',
}


def main() -> int:
    """Parse arguments and dispatch to the matching command handler."""
    parser = create_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1
    return globals()[_HANDLERS[args.command]](args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)
