from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from ..models import FactRecord, ReviewQueueRecord


def build_stage6_kpis(
    run_summary: Dict[str, Any],
    facts: Sequence[FactRecord],
    review_items: Sequence[ReviewQueueRecord],
    actionable_review_rows: Sequence[Dict[str, Any]],
    reocr_tasks_total: int,
    actionable_reocr_tasks_total: int,
    benchmark_payload: Dict[str, Any] | None = None,
    baseline_summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    benchmark_payload = benchmark_payload or {}
    baseline_summary = baseline_summary or {}
    active_facts = [fact for fact in facts if fact.status != "suppressed"]
    unknown_statement_type_total = sum(1 for fact in active_facts if fact.statement_type == "unknown")
    unknown_period_role_export_blocking_total = sum(
        1 for fact in active_facts
        if fact.mapping_code and fact.value_num is not None and fact.report_date_norm not in {"", "unknown_date"} and (fact.period_role_norm or "") == "unknown"
    )
    unmapped_value_bearing_total = sum(1 for fact in active_facts if not fact.mapping_code and fact.value_num is not None and abs(float(fact.value_num or 0.0)) > 0)
    summary = {
        "run_id": run_summary.get("run_id", ""),
        "mapped_facts_ratio": run_summary.get("mapped_facts_ratio", 0.0),
        "amount_coverage_ratio": run_summary.get("amount_coverage_ratio", 0.0),
        "exportable_facts_total": run_summary.get("exportable_facts_total", 0),
        "unknown_statement_type_total": unknown_statement_type_total,
        "unknown_period_role_export_blocking_total": unknown_period_role_export_blocking_total,
        "unmapped_value_bearing_total": unmapped_value_bearing_total,
        "review_total": len(review_items),
        "actionable_review_total": len(actionable_review_rows),
        "reocr_tasks_total": reocr_tasks_total,
        "actionable_reocr_tasks_total": actionable_reocr_tasks_total,
        "benchmark_missing_in_auto": benchmark_payload.get("summary", {}).get("missing_in_auto", 0),
        "benchmark_value_diff_cells": benchmark_payload.get("summary", {}).get("value_diff_cells", 0),
    }
    if baseline_summary:
        for key in ["mapped_facts_ratio", "amount_coverage_ratio", "exportable_facts_total", "review_total"]:
            summary[f"{key}_delta"] = round(float(summary.get(key, 0) or 0) - float(baseline_summary.get(key, 0) or 0), 6)
    return summary


def build_statement_coverage_rows(facts: Sequence[FactRecord]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, float]] = defaultdict(lambda: {"facts_total": 0, "mapped_total": 0, "exportable_total": 0, "amount_total": 0.0, "mapped_amount_total": 0.0})
    for fact in facts:
        if fact.status == "suppressed":
            continue
        bucket = grouped[fact.statement_type]
        bucket["facts_total"] += 1
        if fact.value_num is not None:
            bucket["amount_total"] += abs(float(fact.value_num or 0.0))
        if fact.mapping_code:
            bucket["mapped_total"] += 1
            if fact.value_num is not None and fact.status not in {"review", "conflict"}:
                bucket["mapped_amount_total"] += abs(float(fact.value_num or 0.0))
        if fact.mapping_code and fact.value_num is not None and fact.report_date_norm not in {"", "unknown_date"} and (fact.period_role_norm or "") != "unknown" and not fact.unplaced_reason:
            bucket["exportable_total"] += 1
    return [
        {
            "statement_type": statement_type,
            "facts_total": int(bucket["facts_total"]),
            "mapped_total": int(bucket["mapped_total"]),
            "exportable_total": int(bucket["exportable_total"]),
            "mapped_ratio": safe_ratio(bucket["mapped_total"], bucket["facts_total"]),
            "amount_coverage_ratio": safe_ratio(bucket["mapped_amount_total"], bucket["amount_total"]),
        }
        for statement_type, bucket in sorted(grouped.items())
    ]


def build_benchmark_recall_rows(benchmark_payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    subject_counts = Counter()
    subject_match = Counter()
    period_counts = Counter()
    period_match = Counter()
    for row in benchmark_payload.get("cell_rows", []):
        mapping_code = row.get("mapping_code", "")
        period_key = row.get("aligned_period_key", "") or row.get("benchmark_header", "")
        subject_counts[mapping_code] += 1
        period_counts[period_key] += 1
        if row.get("status") == "match":
            subject_match[mapping_code] += 1
            period_match[period_key] += 1
    subject_rows = [
        {"mapping_code": code, "cells_total": total, "matched_cells": subject_match.get(code, 0), "recall": safe_ratio(subject_match.get(code, 0), total)}
        for code, total in subject_counts.items()
    ]
    period_rows = [
        {"period_key": period, "cells_total": total, "matched_cells": period_match.get(period, 0), "recall": safe_ratio(period_match.get(period, 0), total)}
        for period, total in period_counts.items()
    ]
    return sorted(period_rows, key=lambda row: row["period_key"]), sorted(subject_rows, key=lambda row: row["mapping_code"])


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
