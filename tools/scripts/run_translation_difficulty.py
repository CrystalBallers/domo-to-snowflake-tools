#!/usr/bin/env python3
"""Run from repo root: python tools/scripts/run_translation_difficulty.py ..."""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from tools.utils.translation_difficulty.cli import main

if __name__ == "__main__":
    sys.exit(main())
