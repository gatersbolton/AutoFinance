from __future__ import annotations

from typing import Any, Dict, List, Sequence

from ..models import FactRecord


def build_target_kpis(
    facts: Sequence[FactRecord],
    benchmark_missing_true_rows: Sequence[Dict[str, Any]],
    main_target_review_rows: Sequence[Dict[str, Any]],
    note_detail_review_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    target_facts = [fact for fact in facts if fact.status != "suppressed" and fact.target_scope in {"main_export_target", "derived_target"}]
    amount_total = sum(abs(float(fact.value_num or 0.0)) for fact in target_facts if fact.value_num is not None)
    mapped_amount = sum(
        abs(float(fact.value_num or 0.0))
        for fact in target_facts
        if fact.mapping_code and fact.value_num is not None and not fact.unplaced_reason
    )
    mapped_total = sum(1 for fact in target_facts if fact.mapping_code)
    summary = {
        "run_id": "",
        "target_missing_total": len(benchmark_missing_true_rows),
        "target_facts_total": len(target_facts),
        "target_mapped_ratio": safe_ratio(mapped_total, len(target_facts)),
        "target_amount_coverage_ratio": safe_ratio(mapped_amount, amount_total),
        "target_review_total": len(main_target_review_rows),
        "note_detail_review_total": len(note_detail_review_rows),
    }
    return summary


def build_stage7_kpis(
    run_summary: Dict[str, Any],
    target_summary: Dict[str, Any],
    benchmark_alignment_summary: Dict[str, Any],
    promotion_summary: Dict[str, Any],
    no_source_summary: Dict[str, Any],
    actionable_reocr_tasks_total: int,
    baseline_stage7: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    baseline_stage7 = baseline_stage7 or {}
    summary = {
        "run_id": run_summary.get("run_id", ""),
        "target_missing_total": int(target_summary.get("target_missing_total", 0)),
        "target_mapped_ratio": float(target_summary.get("target_mapped_ratio", 0.0)),
        "target_amount_coverage_ratio": float(target_summary.get("target_amount_coverage_ratio", 0.0)),
        "benchmark_missing_true_total": int(benchmark_alignment_summary.get("missing_in_auto_true", 0)),
        "alignment_only_gap_total": int(benchmark_alignment_summary.get("alignment_only_gap_total", 0)),
        "promoted_alias_total": int(promotion_summary.get("promoted_alias_total", 0)),
        "promoted_formula_total": int(promotion_summary.get("promoted_formula_total", 0)),
        "no_source_true_total": int(no_source_summary.get("truly_no_source_total", 0)),
        "main_target_review_total": int(target_summary.get("target_review_total", 0)),
        "note_detail_review_total": int(target_summary.get("note_detail_review_total", 0)),
        "actionable_reocr_tasks_total": int(actionable_reocr_tasks_total),
    }
    for key in ("target_missing_total", "target_mapped_ratio", "target_amount_coverage_ratio", "main_target_review_total"):
        summary[f"{key}_delta"] = round(float(summary.get(key, 0) or 0) - float(baseline_stage7.get(key, 0) or 0), 6)
    return summary


def build_stage8_kpis(
    run_summary: Dict[str, Any],
    target_summary: Dict[str, Any],
    benchmark_summary: Dict[str, Any],
    stage7_summary: Dict[str, Any],
    shadow_promotion_summary: Dict[str, Any],
    kpi_contract_summary: Dict[str, Any],
    source_backed_summary: Dict[str, Any],
    backfill_summary: Dict[str, Any],
    *,
    mode: str,
    baseline_run_id: str = "",
) -> Dict[str, Any]:
    return {
        "run_id": run_summary.get("run_id", ""),
        "mode": mode,
        "baseline_run_id": baseline_run_id,
        "target_missing_total": int(target_summary.get("target_missing_total", 0)),
        "target_mapped_ratio": float(target_summary.get("target_mapped_ratio", 0.0)),
        "target_amount_coverage_ratio": float(target_summary.get("target_amount_coverage_ratio", 0.0)),
        "benchmark_missing_true_total": int(benchmark_summary.get("missing_in_auto_true", benchmark_summary.get("benchmark_missing_true_total", 0))),
        "benchmark_precision_ratio": float(benchmark_summary.get("benchmark_precision_ratio", benchmark_summary.get("precision_against_benchmark", 0.0))),
        "benchmark_recall_ratio": float(benchmark_summary.get("benchmark_recall_ratio", benchmark_summary.get("recall_against_benchmark", 0.0))),
        "main_target_review_total": int(stage7_summary.get("main_target_review_total", target_summary.get("target_review_total", 0))),
        "shadow_selected_total": int(shadow_promotion_summary.get("selected_total", 0)),
        "shadow_applied_total": int(shadow_promotion_summary.get("applied_total", 0)),
        "shadow_alias_total": int(shadow_promotion_summary.get("selected_alias_total", 0)),
        "shadow_formula_total": int(shadow_promotion_summary.get("selected_formula_total", 0)),
        "shadow_placement_total": int(shadow_promotion_summary.get("selected_placement_total", 0)),
        "shadow_period_total": int(shadow_promotion_summary.get("selected_period_total", 0)),
        "source_backed_closure_candidates_total": int(source_backed_summary.get("closure_candidates_total", 0)),
        "source_backed_auto_close_total": int(source_backed_summary.get("safe_to_auto_close_total", 0)),
        "truly_no_source_backfill_total": int(backfill_summary.get("tasks_total", 0)),
        "kpi_contract_fail_total": int(kpi_contract_summary.get("contract_fail_total", 0)),
    }


def build_target_gap_delta_summary(before: Dict[str, Any] | None, after: Dict[str, Any] | None) -> Dict[str, Any]:
    before = before or {}
    after = after or {}
    before_breakdown = before.get("cause_breakdown", {}) or {}
    after_breakdown = after.get("cause_breakdown", {}) or {}
    keys = sorted(set(before_breakdown) | set(after_breakdown) | {"target_gap_total", "truly_no_source_total"})
    metrics: List[Dict[str, Any]] = []
    for key in keys:
        if key in {"target_gap_total", "truly_no_source_total"}:
            before_value = before.get(key, 0)
            after_value = after.get(key, 0)
        else:
            before_value = before_breakdown.get(key, 0)
            after_value = after_breakdown.get(key, 0)
        metrics.append(
            {
                "metric": key,
                "before": before_value,
                "after": after_value,
                "delta": round(float(after_value or 0.0) - float(before_value or 0.0), 6),
            }
        )
    return {
        "run_id": after.get("run_id", ""),
        "baseline_run_id": before.get("run_id", ""),
        "promoted_run_id": after.get("run_id", ""),
        "metrics": metrics,
    }


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
