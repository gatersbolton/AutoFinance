from __future__ import annotations

from typing import Any, Dict, List


STAGE8_COMPARE_METRICS = [
    "mapped_facts_ratio",
    "amount_coverage_ratio",
    "exportable_facts_total",
    "target_missing_total",
    "benchmark_missing_true_total",
    "target_mapped_ratio",
    "target_amount_coverage_ratio",
    "unmapped_total",
    "main_target_review_total",
]


def build_stage8_delta(before: Dict[str, Any] | None, after: Dict[str, Any] | None) -> Dict[str, Any]:
    before = before or {}
    after = after or {}
    rows: List[Dict[str, Any]] = []
    for metric in STAGE8_COMPARE_METRICS:
        before_value = before.get(metric, 0)
        after_value = after.get(metric, 0)
        rows.append(
            {
                "metric": metric,
                "before": before_value,
                "after": after_value,
                "delta": round(float(after_value or 0.0) - float(before_value or 0.0), 6),
            }
        )
    return {
        "run_id": after.get("run_id", ""),
        "baseline_run_id": before.get("run_id", ""),
        "promoted_run_id": after.get("run_id", ""),
        "metrics": rows,
        "safe_auto_promotions_applied": int(after.get("shadow_applied_total", 0)),
    }

