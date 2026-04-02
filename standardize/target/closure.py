from __future__ import annotations

from collections import Counter
import json
from typing import Any, Dict, List, Sequence, Tuple

from ..models import FactRecord, IssueRecord, ReviewQueueRecord
from ..stable_ids import stable_id


def investigate_no_source_gaps(
    benchmark_missing_true_rows: Sequence[Dict[str, Any]],
    facts_raw: Sequence[FactRecord],
    facts_deduped: Sequence[FactRecord],
    unplaced_rows: Sequence[Dict[str, Any]],
    derived_facts: Sequence[FactRecord],
    review_items: Sequence[ReviewQueueRecord],
    issues: Sequence[IssueRecord],
) -> Dict[str, Any]:
    investigation_rows: List[Dict[str, Any]] = []
    backfill_rows: List[Dict[str, Any]] = []
    cause_counter = Counter()
    for row in benchmark_missing_true_rows:
        result = investigate_single_gap(
            row=row,
            facts_raw=facts_raw,
            facts_deduped=facts_deduped,
            unplaced_rows=unplaced_rows,
            derived_facts=derived_facts,
            review_items=review_items,
            issues=issues,
        )
        investigation_rows.append(result)
        cause_counter[result["gap_cause"]] += 1
        if result["gap_cause"] == "truly_no_source":
            backfill_rows.append(
                {
                    "run_id": "",
                    "mapping_code": row.get("mapping_code", ""),
                    "mapping_name": row.get("mapping_name", ""),
                    "aligned_period_key": row.get("aligned_period_key", ""),
                    "benchmark_value": row.get("benchmark_value"),
                    "task_type": "target_backfill",
                    "evidence": result.get("evidence_refs", ""),
                    "suggested_action": "investigate_source_tables",
                }
            )
    summary = {
        "run_id": "",
        "gaps_total": len(investigation_rows),
        "truly_no_source_total": cause_counter.get("truly_no_source", 0),
        "cause_breakdown": dict(cause_counter),
    }
    backlog_rows = sorted(
        [
            {
                "run_id": "",
                "mapping_code": row.get("mapping_code", ""),
                "mapping_name": row.get("mapping_name", ""),
                "aligned_period_key": row.get("aligned_period_key", ""),
                "benchmark_value": row.get("benchmark_value"),
                "gap_cause": row.get("gap_cause", ""),
                "priority_score": abs(float(row.get("benchmark_value") or 0.0)) if _is_numeric(row.get("benchmark_value")) else 0.0,
                "evidence_refs": row.get("evidence_refs", ""),
            }
            for row in investigation_rows
        ],
        key=lambda item: float(item.get("priority_score", 0) or 0.0),
        reverse=True,
    )
    return {
        "rows": investigation_rows,
        "summary": summary,
        "backfill_rows": backfill_rows,
        "backfill_summary": {"run_id": "", "tasks_total": len(backfill_rows)},
        "target_gap_backlog_rows": backlog_rows,
        "target_gap_summary": {
            "run_id": "",
            "target_gap_total": len(backlog_rows),
            "cause_breakdown": dict(cause_counter),
        },
    }


def investigate_single_gap(
    row: Dict[str, Any],
    facts_raw: Sequence[FactRecord],
    facts_deduped: Sequence[FactRecord],
    unplaced_rows: Sequence[Dict[str, Any]],
    derived_facts: Sequence[FactRecord],
    review_items: Sequence[ReviewQueueRecord],
    issues: Sequence[IssueRecord],
) -> Dict[str, Any]:
    mapping_code = row.get("mapping_code", "")
    period_key = row.get("aligned_period_key", "")
    benchmark_value = row.get("benchmark_value")

    deduped_same_code = [fact for fact in facts_deduped if fact.mapping_code == mapping_code]
    same_period = [fact for fact in deduped_same_code if fact.period_key == period_key]
    if same_period:
        filtered = [fact for fact in same_period if fact.target_scope not in {"main_export_target", "derived_target"}]
        if filtered:
            return build_result(row, "source_exists_but_target_scope_filtered", filtered[:3], "target_scope_filtered")
        with_unplaced = [fact for fact in same_period if fact.unplaced_reason]
        if with_unplaced:
            return build_result(row, "source_exists_but_unplaced", with_unplaced[:3], "deduped_unplaced")

    unplaced_matches = [item for item in unplaced_rows if item.get("mapping_code", "") == mapping_code and item.get("period_key", "") == period_key]
    if unplaced_matches:
        return build_result(row, "source_exists_but_unplaced", unplaced_matches[:3], "unplaced_facts")

    misaligned = [fact for fact in deduped_same_code if fact.period_key != period_key]
    if misaligned:
        return build_result(row, "source_exists_but_period_misaligned", misaligned[:3], "mapped_other_period")

    derived_matches = [fact for fact in derived_facts if fact.mapping_code == mapping_code and fact.period_key == period_key]
    if derived_matches:
        return build_result(row, "candidate_formula_possible", derived_matches[:3], "derived_fact")

    raw_unmapped = [
        fact
        for fact in facts_raw
        if not fact.mapping_code
        and fact.period_key == period_key
        and _amount_close(fact.value_num, benchmark_value)
    ]
    if raw_unmapped:
        return build_result(row, "source_exists_but_unmapped", raw_unmapped[:3], "facts_raw_amount_match")

    review_matches = [
        item
        for item in review_items
        if item.period_key == period_key and _amount_close(item.value_num, benchmark_value)
    ]
    if review_matches:
        return build_result(row, "source_exists_but_unmapped", review_matches[:3], "review_queue_amount_match")

    issue_matches = [issue for issue in issues if mapping_code and mapping_code in (issue.message or "")]
    if issue_matches:
        return build_result(row, "truly_no_source", issue_matches[:3], "issues_only")

    return build_result(row, "truly_no_source", [], "no_candidate_found")


def build_result(row: Dict[str, Any], cause: str, evidence_items: Sequence[Any], source: str) -> Dict[str, Any]:
    evidence_refs = []
    for item in evidence_items:
        if isinstance(item, dict):
            evidence_refs.append(item.get("fact_id") or item.get("review_id") or item.get("source_cell_ref") or "")
        else:
            evidence_refs.append(getattr(item, "fact_id", "") or getattr(item, "review_id", "") or getattr(item, "source_cell_ref", ""))
    return {
        "run_id": "",
        "mapping_code": row.get("mapping_code", ""),
        "mapping_name": row.get("mapping_name", ""),
        "aligned_period_key": row.get("aligned_period_key", ""),
        "benchmark_value": row.get("benchmark_value"),
        "gap_cause": cause,
        "evidence_source": source,
        "evidence_refs": ";".join(value for value in evidence_refs if value),
    }


def _amount_close(left: Any, right: Any, tolerance: float = 0.01) -> bool:
    if left in (None, "") or right in (None, ""):
        return False
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return False


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def build_source_backed_gap_closure(
    benchmark_missing_true_rows: Sequence[Dict[str, Any]],
    investigation_rows: Sequence[Dict[str, Any]],
    facts_raw: Sequence[FactRecord],
    facts_deduped: Sequence[FactRecord],
    review_items: Sequence[ReviewQueueRecord],
    rules: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    rules = rules or {}
    investigation_by_key = {
        (row.get("mapping_code", ""), row.get("aligned_period_key", ""), str(row.get("benchmark_value", ""))): row
        for row in investigation_rows
    }
    raw_by_fact_id = {fact.fact_id: fact for fact in facts_raw if fact.fact_id}
    deduped_by_fact_id = {fact.fact_id: fact for fact in facts_deduped if fact.fact_id}
    review_by_id = {item.review_id: item for item in review_items if item.review_id}

    closure_rows: List[Dict[str, Any]] = []
    closure_counter = Counter()
    auto_closable_total = 0
    for row in benchmark_missing_true_rows:
        mapping_code = row.get("mapping_code", "")
        aligned_period_key = row.get("aligned_period_key", "")
        benchmark_value = row.get("benchmark_value")
        investigation = investigation_by_key.get((mapping_code, aligned_period_key, str(benchmark_value)), {})
        gap_cause = str(investigation.get("gap_cause", "")).strip()
        if gap_cause not in {
            "source_exists_but_unmapped",
            "source_exists_but_unplaced",
            "source_exists_but_period_misaligned",
        }:
            continue
        evidence_refs = [
            value.strip()
            for value in str(investigation.get("evidence_refs", "")).split(";")
            if value.strip()
        ]
        gap_id = stable_id(
            "GAP_",
            [
                mapping_code,
                aligned_period_key,
                benchmark_value,
                gap_cause,
                sorted(evidence_refs),
            ],
        )
        candidate = _build_closure_candidate(
            gap_id=gap_id,
            benchmark_row=row,
            gap_cause=gap_cause,
            evidence_refs=evidence_refs,
            raw_by_fact_id=raw_by_fact_id,
            deduped_by_fact_id=deduped_by_fact_id,
            review_by_id=review_by_id,
            rules=rules,
        )
        if not candidate:
            continue
        closure_counter[candidate["closure_type"]] += 1
        auto_closable_total += 1 if candidate.get("safe_to_auto_close") else 0
        closure_rows.append(candidate)

    summary = {
        "run_id": "",
        "closure_candidates_total": len(closure_rows),
        "safe_to_auto_close_total": auto_closable_total,
        "closure_type_breakdown": dict(closure_counter),
    }
    return {
        "rows": closure_rows,
        "summary": summary,
    }


def _build_closure_candidate(
    gap_id: str,
    benchmark_row: Dict[str, Any],
    gap_cause: str,
    evidence_refs: Sequence[str],
    raw_by_fact_id: Dict[str, FactRecord],
    deduped_by_fact_id: Dict[str, FactRecord],
    review_by_id: Dict[str, ReviewQueueRecord],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    benchmark_value = benchmark_row.get("benchmark_value")
    mapping_code = str(benchmark_row.get("mapping_code", "")).strip()
    mapping_name = str(benchmark_row.get("mapping_name", "")).strip()
    aligned_period_key = str(benchmark_row.get("aligned_period_key", "")).strip()
    candidate: Dict[str, Any] = {
        "run_id": "",
        "gap_id": gap_id,
        "mapping_code": mapping_code,
        "mapping_name": mapping_name,
        "aligned_period_key": aligned_period_key,
        "benchmark_value": benchmark_value,
        "gap_cause": gap_cause,
        "source_fact_ids": ";".join(evidence_refs),
        "closure_type": "",
        "recommended_action": "",
        "safe_to_auto_close": False,
        "payload_json": "",
        "reason": "",
    }

    if gap_cause == "source_exists_but_unmapped":
        fact_candidates = [raw_by_fact_id[fact_id] for fact_id in evidence_refs if fact_id in raw_by_fact_id]
        review_candidates = [review_by_id[item_id] for item_id in evidence_refs if item_id in review_by_id]
        alias_labels = {
            (fact.row_label_canonical_candidate or fact.row_label_norm or fact.row_label_std or fact.row_label_raw).strip()
            for fact in fact_candidates
            if (fact.row_label_canonical_candidate or fact.row_label_norm or fact.row_label_std or fact.row_label_raw).strip()
        }
        alias_labels.update(
            (item.row_label_std or item.row_label_raw).strip()
            for item in review_candidates
            if (item.row_label_std or item.row_label_raw).strip()
        )
        if len(alias_labels) != 1:
            candidate["closure_type"] = "alias_promotion_candidate"
            candidate["recommended_action"] = "review_alias_promotion"
            candidate["reason"] = "multiple_alias_labels"
            return candidate
        alias_value = sorted(alias_labels)[0]
        statement_types = {
            fact.statement_type
            for fact in fact_candidates
            if fact.statement_type
        }
        statement_types.update(item.statement_type for item in review_candidates if item.statement_type)
        payload = {
            "promotion_kind": "alias",
            "alias": alias_value,
            "canonical_code": mapping_code,
            "canonical_name": mapping_name,
            "statement_types": sorted(statement_types),
            "gap_id": gap_id,
            "source_run_id": "",
            "promotion_id": stable_id("SHADOW_ALIAS_", [gap_id, mapping_code, alias_value]),
        }
        candidate["closure_type"] = "alias_promotion_candidate"
        candidate["recommended_action"] = "shadow_alias_promotion"
        candidate["safe_to_auto_close"] = True
        candidate["payload_json"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        candidate["reason"] = "single_source_backed_alias_label"
        return candidate

    if gap_cause == "source_exists_but_unplaced":
        fact_candidates = [deduped_by_fact_id[fact_id] for fact_id in evidence_refs if fact_id in deduped_by_fact_id]
        numeric_candidates = [fact for fact in fact_candidates if _amount_close(fact.value_num, benchmark_value)]
        if len(numeric_candidates) != 1:
            candidate["closure_type"] = "placement_preference_candidate"
            candidate["recommended_action"] = "review_shadow_placement"
            candidate["reason"] = "multiple_matching_unplaced_facts"
            return candidate
        preferred = numeric_candidates[0]
        payload = {
            "promotion_kind": "placement",
            "fact_id": preferred.fact_id,
            "mapping_code": preferred.mapping_code,
            "period_key": preferred.period_key,
            "statement_type": preferred.statement_type,
            "gap_id": gap_id,
            "promotion_id": stable_id("SHADOW_PLACE_", [gap_id, preferred.fact_id]),
        }
        candidate["closure_type"] = "placement_preference_candidate"
        candidate["recommended_action"] = "shadow_placement_preference"
        candidate["safe_to_auto_close"] = True
        candidate["payload_json"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        candidate["reason"] = "single_unplaced_fact_matches_benchmark_amount"
        return candidate

    if gap_cause == "source_exists_but_period_misaligned":
        fact_candidates = [deduped_by_fact_id[fact_id] for fact_id in evidence_refs if fact_id in deduped_by_fact_id]
        numeric_candidates = [fact for fact in fact_candidates if _amount_close(fact.value_num, benchmark_value)]
        unique_periods = sorted({fact.period_key for fact in numeric_candidates if fact.period_key})
        if len(unique_periods) != 1:
            candidate["closure_type"] = "period_override_candidate"
            candidate["recommended_action"] = "review_shadow_period_override"
            candidate["reason"] = "multiple_candidate_periods"
            return candidate
        payload = {
            "promotion_kind": "period",
            "mapping_code": mapping_code,
            "mapping_name": mapping_name,
            "benchmark_header": benchmark_row.get("benchmark_header", ""),
            "aligned_period_key": unique_periods[0],
            "gap_id": gap_id,
            "promotion_id": stable_id("SHADOW_PERIOD_", [gap_id, mapping_code, unique_periods[0]]),
        }
        candidate["closure_type"] = "period_override_candidate"
        candidate["recommended_action"] = "shadow_period_override"
        candidate["safe_to_auto_close"] = True
        candidate["payload_json"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        candidate["reason"] = "single_source_period_matches_benchmark_amount"
        return candidate

    return {}
