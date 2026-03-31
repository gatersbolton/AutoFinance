from __future__ import annotations

from typing import Any, Dict, Sequence


def build_review_decision_summary(
    applied_rows: Sequence[Dict[str, Any]],
    rejected_rows: Sequence[Dict[str, Any]],
    touched_files: Sequence[str],
) -> Dict[str, Any]:
    return {
        "applied_total": len(applied_rows),
        "rejected_total": len(rejected_rows),
        "touched_files": list(touched_files),
        "applied_action_types": count_by_key(applied_rows, "action_type"),
        "rejected_reasons": count_by_key(rejected_rows, "reject_reason"),
    }


def count_by_key(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "")).strip()
        counter[value] = counter.get(value, 0) + 1
    return counter
