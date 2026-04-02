from __future__ import annotations

from typing import Any, Dict, List


PROMOTION_METRICS = [
    "target_missing_total",
    "target_mapped_ratio",
    "target_amount_coverage_ratio",
    "exportable_facts_total",
    "benchmark_missing_true_total",
]


def build_promotion_delta(before: Dict[str, Any] | None, after: Dict[str, Any] | None) -> Dict[str, Any]:
    before = before or {}
    after = after or {}
    rows: List[Dict[str, Any]] = []
    for metric in PROMOTION_METRICS:
        rows.append(
            {
                "metric": metric,
                "before": before.get(metric, 0),
                "after": after.get(metric, 0),
                "delta": round(float(after.get(metric, 0) or 0) - float(before.get(metric, 0) or 0), 6),
            }
        )
    summary = {
        "run_id": after.get("run_id", ""),
        "metrics": rows,
        "promoted_alias_total": int(after.get("promoted_alias_total", 0)),
        "promoted_formula_total": int(after.get("promoted_formula_total", 0)),
    }
    return {"summary": summary, "rows": rows}
