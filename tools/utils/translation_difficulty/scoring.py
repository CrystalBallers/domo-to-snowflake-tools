"""
Score dataflow translation difficulty from action types and generated SQL size.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

SELECT_VALUES_TYPE = "SelectValues"
_WEIGHTS_PATH = Path(__file__).resolve().parent / "weights.yaml"


@dataclass
class StepScore:
    dataflow_id: str
    action_id: str
    action_type: str
    base_minutes: float
    length_minutes: float
    total_minutes: float
    sql_chars: int
    success: bool


def load_weights(path: Optional[Path] = None) -> Dict[str, Any]:
    p = path or _WEIGHTS_PATH
    with open(p, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return cfg.get("defaults") or {}


def base_minutes_for_type(action_type: str, cfg: Dict[str, Any]) -> float:
    if action_type == SELECT_VALUES_TYPE:
        return 1.0
    d = _defaults(cfg)
    unknown = float(d.get("unknown_type_minutes", 20))
    mapping = cfg.get("type_base_minutes") or {}
    if action_type in mapping:
        return float(mapping[action_type])
    logger.debug("Unknown action type %r, using default %s minutes", action_type, unknown)
    return unknown


def _length_minutes(
    action_type: str,
    sql_chars: int,
    fallback_chars: int,
    cfg: Dict[str, Any],
) -> float:
    if action_type == SELECT_VALUES_TYPE:
        return 0.0
    d = _defaults(cfg)
    bucket = int(d.get("bucket_chars", 500))
    per = float(d.get("minutes_per_bucket", 0.75))
    cap = float(d.get("length_addon_cap", 15))
    chars = sql_chars
    if chars <= 0 and fallback_chars > 0:
        fb_bucket = int(d.get("fallback_bucket_chars", 800))
        fb_per = float(d.get("fallback_minutes_per_bucket", 0.5))
        fb_cap = float(d.get("fallback_length_cap", 12))
        extra = (fallback_chars // fb_bucket) * fb_per
        return min(extra, fb_cap)
    extra = (chars // bucket) * per
    return min(extra, cap)


def score_steps(
    dataflow_id: str,
    actions_ordered: List[Any],
    action_results: List[Any],
    cfg: Dict[str, Any],
    dialect: Any,
) -> Tuple[List[StepScore], float]:
    """
    Pair actions with translation results in order (same contract as DataflowTranslator).

    Args:
        dataflow_id: Domo dataflow id as string
        actions_ordered: List of DataflowAction in execution order
        action_results: List of TranslationResult in processing order
        cfg: Loaded weights YAML
        dialect: SQLDialect enum value for query.render()
    """
    if len(actions_ordered) != len(action_results):
        logger.warning(
            "Action count mismatch for dataflow %s: %s actions vs %s results",
            dataflow_id,
            len(actions_ordered),
            len(action_results),
        )

    step_scores: List[StepScore] = []
    total = 0.0
    pairs = zip(actions_ordered, action_results)
    for action, tr in pairs:
        atype = action.type
        base = base_minutes_for_type(atype, cfg)
        try:
            rendered = tr.query.render(dialect) if tr.query else ""
        except Exception as e:  # noqa: BLE001
            logger.debug("render() failed for action %s: %s", action.id, e)
            rendered = ""
        sql_chars = len(rendered)
        try:
            fallback_chars = len(
                json.dumps(action.model_dump(mode="json"), default=str)
            )
        except Exception:  # noqa: BLE001
            fallback_chars = 0
        length = _length_minutes(atype, sql_chars, fallback_chars, cfg)
        step_total = base + length
        total += step_total
        step_scores.append(
            StepScore(
                dataflow_id=dataflow_id,
                action_id=action.id,
                action_type=atype,
                base_minutes=base,
                length_minutes=length,
                total_minutes=step_total,
                sql_chars=sql_chars,
                success=bool(getattr(tr, "success", False)),
            )
        )

    return step_scores, total


def aggregate_row(
    dataflow_id: str,
    step_points: float,
    total_tiles: int,
) -> Dict[str, Any]:
    subtotal = step_points + total_tiles
    return {
        "filename": f"{dataflow_id}.sql",
        "Dataflow ID": dataflow_id,
        "Step Points": round(step_points),
        "Total Tiles": total_tiles,
        "Subtotal Points": round(subtotal),
        "Total Points": "",
    }
