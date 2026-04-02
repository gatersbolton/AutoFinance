from __future__ import annotations

from typing import Any, Dict, Sequence


def build_promotion_audit_summary(
    applied_rows: Sequence[Dict[str, Any]],
    rejected_rows: Sequence[Dict[str, Any]],
    touched_files: Sequence[str],
) -> Dict[str, Any]:
    return {
        "run_id": "",
        "applied_total": len(applied_rows),
        "rejected_total": len(rejected_rows),
        "touched_files": list(touched_files),
    }


def build_shadow_promotion_audit_summary(
    selected_rows: Sequence[Dict[str, Any]],
    audit_rows: Sequence[Dict[str, Any]],
    *,
    applied_total: int,
    baseline_run_id: str,
) -> Dict[str, Any]:
    return {
        "run_id": "",
        "baseline_run_id": baseline_run_id,
        "candidate_total": len(audit_rows),
        "selected_total": len(selected_rows),
        "applied_total": int(applied_total),
        "selection_reason_breakdown": count_by_key(audit_rows, "selection_reason"),
    }


def count_by_key(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "")).strip()
        counter[value] = counter.get(value, 0) + 1
    return counter
