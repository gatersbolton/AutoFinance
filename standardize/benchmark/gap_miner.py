from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence

from ..models import ConflictRecord, FactRecord, MappingCandidateRecord, ReviewQueueRecord, ValidationResultRecord


def explain_benchmark_gaps(
    benchmark_missing_rows: Sequence[Dict[str, Any]],
    facts: Sequence[FactRecord],
    unplaced_rows: Sequence[Dict[str, Any]],
    conflicts: Sequence[ConflictRecord],
    validations: Sequence[ValidationResultRecord],
    mapping_candidates: Sequence[MappingCandidateRecord],
    derived_facts: Sequence[FactRecord],
) -> Dict[str, Any]:
    facts_by_mapping_period = defaultdict(list)
    unmapped_numeric_facts = []
    for fact in facts:
        facts_by_mapping_period[(fact.mapping_code, fact.period_key)].append(fact)
        if not fact.mapping_code and fact.value_num is not None:
            unmapped_numeric_facts.append(fact)

    unplaced_by_mapping_period = defaultdict(list)
    for row in unplaced_rows:
        unplaced_by_mapping_period[(row.get("mapping_code", ""), row.get("period_key", ""))].append(row)

    conflict_keys = {(conflict.row_label_std, conflict.period_key): conflict for conflict in conflicts if conflict.decision in {"review_required", "unresolved"}}
    validation_refs = {ref for result in validations if result.status == "fail" for ref in result.evidence_fact_refs}
    candidate_by_code = defaultdict(list)
    for candidate in mapping_candidates:
        candidate_by_code[candidate.candidate_code].append(candidate)

    explanation_rows: List[Dict[str, Any]] = []
    alias_suggestions: List[Dict[str, Any]] = []
    formula_suggestions: List[Dict[str, Any]] = []
    priority_rows: List[Dict[str, Any]] = []
    cause_counter = Counter()

    for row in benchmark_missing_rows:
        mapping_code = row.get("mapping_code", "")
        aligned_period_key = row.get("aligned_period_key", "")
        benchmark_value = row.get("benchmark_value")
        matching_facts = facts_by_mapping_period.get((mapping_code, aligned_period_key), [])
        cause = "no_source_fact_found"
        detail = ""
        if row.get("reason") == "ambiguous_period_alignment":
            cause = "ambiguous_period_alignment"
            detail = "benchmark legacy column aligned to multiple export periods"
        elif matching_facts:
            if any(fact.unplaced_reason for fact in matching_facts):
                cause = "source_fact_exists_but_unplaced"
                detail = ",".join(sorted({fact.unplaced_reason for fact in matching_facts if fact.unplaced_reason}))
            elif any(fact.source_cell_ref in validation_refs for fact in matching_facts):
                cause = "blocked_by_validation"
                detail = "related fact participates in validation fail"
            else:
                cause = "statement_type_mismatch"
                detail = "mapped fact exists but export cell still blank"
        else:
            alias_candidate = find_alias_candidate(mapping_code, aligned_period_key, benchmark_value, unmapped_numeric_facts, candidate_by_code)
            if alias_candidate:
                cause = "likely_alias_missing"
                detail = alias_candidate["row_label_std"]
                alias_suggestions.append(alias_candidate)
            else:
                formula_candidate = find_formula_candidate(mapping_code, aligned_period_key, benchmark_value, derived_facts)
                if formula_candidate:
                    cause = "candidate_formula_possible"
                    detail = formula_candidate["rule_id"]
                    formula_suggestions.append(formula_candidate)

        cause_counter[cause] += 1
        explanation = {
            "run_id": "",
            "mapping_code": mapping_code,
            "mapping_name": row.get("mapping_name", ""),
            "aligned_period_key": aligned_period_key,
            "benchmark_value": benchmark_value,
            "gap_cause": cause,
            "detail": detail,
        }
        explanation_rows.append(explanation)
        priority_rows.append(
            {
                "run_id": "",
                "mapping_code": mapping_code,
                "mapping_name": row.get("mapping_name", ""),
                "aligned_period_key": aligned_period_key,
                "benchmark_value": benchmark_value,
                "gap_cause": cause,
                "priority_score": abs(float(benchmark_value or 0.0)) if _is_numeric(benchmark_value) else 0.0,
            }
        )

    priority_rows.sort(key=lambda item: float(item.get("priority_score", 0) or 0.0), reverse=True)
    summary = {
        "run_id": "",
        "gaps_total": len(explanation_rows),
        "cause_breakdown": dict(cause_counter),
        "alias_suggestions_total": len(alias_suggestions),
        "formula_suggestions_total": len(formula_suggestions),
    }
    return {
        "explanations": explanation_rows,
        "summary": summary,
        "alias_suggestions": alias_suggestions,
        "formula_suggestions": formula_suggestions,
        "priority_rows": priority_rows[:100],
    }


def find_alias_candidate(
    mapping_code: str,
    period_key: str,
    benchmark_value: Any,
    unmapped_facts: Sequence[FactRecord],
    candidate_by_code: Dict[str, List[MappingCandidateRecord]],
) -> Dict[str, Any] | None:
    if not _is_numeric(benchmark_value):
        return None
    for fact in unmapped_facts:
        if fact.period_key != period_key:
            continue
        if abs(float(fact.value_num or 0.0) - float(benchmark_value)) > 0.01:
            continue
        matching_candidates = [candidate for candidate in candidate_by_code.get(mapping_code, []) if candidate.source_cell_ref == fact.source_cell_ref]
        return {
            "run_id": "",
            "mapping_code": mapping_code,
            "row_label_std": fact.row_label_std,
            "period_key": period_key,
            "benchmark_value": benchmark_value,
            "fact_id": fact.fact_id,
            "candidate_method": getattr(matching_candidates[0], "candidate_method", "benchmark_amount_match") if matching_candidates else "benchmark_amount_match",
        }
    return None


def find_formula_candidate(mapping_code: str, period_key: str, benchmark_value: Any, derived_facts: Sequence[FactRecord]) -> Dict[str, Any] | None:
    if not _is_numeric(benchmark_value):
        return None
    for fact in derived_facts:
        if fact.mapping_code != mapping_code or fact.period_key != period_key:
            continue
        if fact.value_num is None:
            continue
        if abs(float(fact.value_num) - float(benchmark_value)) > 0.01:
            continue
        return {
            "run_id": "",
            "rule_id": fact.source_cell_ref.split(":")[3] if ":" in fact.source_cell_ref else fact.source_cell_ref,
            "mapping_code": mapping_code,
            "period_key": period_key,
            "benchmark_value": benchmark_value,
            "derived_fact_id": fact.fact_id,
        }
    return None


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False
