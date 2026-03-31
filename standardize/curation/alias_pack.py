from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from ..models import FactRecord, MappingCandidateRecord


def split_unmapped_facts(facts: Sequence[FactRecord]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    value_rows: List[Dict[str, Any]] = []
    blank_rows: List[Dict[str, Any]] = []
    amount_by_label = Counter()
    amount_by_statement = Counter()
    for fact in facts:
        if fact.status == "suppressed" or fact.mapping_code:
            continue
        row = {
            "fact_id": fact.fact_id,
            "doc_id": fact.doc_id,
            "page_no": fact.page_no,
            "statement_type": fact.statement_type,
            "row_label_raw": fact.row_label_raw,
            "row_label_std": fact.row_label_std,
            "row_label_norm": fact.row_label_norm,
            "row_label_canonical_candidate": fact.row_label_canonical_candidate,
            "period_key": fact.period_key,
            "value_raw": fact.value_raw,
            "value_num": fact.value_num,
            "source_cell_ref": fact.source_cell_ref,
        }
        if fact.value_num is not None and abs(float(fact.value_num or 0.0)) > 0:
            value_rows.append(row)
            label = fact.row_label_canonical_candidate or fact.row_label_norm or fact.row_label_std or fact.row_label_raw
            amount_by_label[label] += abs(float(fact.value_num or 0.0))
            amount_by_statement[fact.statement_type] += abs(float(fact.value_num or 0.0))
        else:
            blank_rows.append(row)
    summary = {
        "unmapped_value_bearing_total": len(value_rows),
        "unmapped_blank_or_non_numeric_total": len(blank_rows),
        "amount_opportunity_by_label": dict(amount_by_label.most_common(50)),
        "amount_opportunity_by_statement_type": dict(amount_by_statement),
    }
    return value_rows, blank_rows, summary


def build_alias_acceptance_candidates(
    value_bearing_rows: Sequence[Dict[str, Any]],
    facts: Sequence[FactRecord],
    mapping_candidates: Sequence[MappingCandidateRecord],
    benchmark_missing_rows: Sequence[Dict[str, Any]],
    alias_rules: Dict[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    alias_rules = alias_rules or {}
    fact_map = {fact.fact_id: fact for fact in facts}
    benchmark_support = build_benchmark_support(benchmark_missing_rows)
    candidates_by_source = defaultdict(list)
    for candidate in mapping_candidates:
        candidates_by_source[candidate.source_cell_ref].append(candidate)

    grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in value_bearing_rows:
        fact = fact_map.get(row["fact_id"])
        if fact is None:
            continue
        source_candidates = sorted(
            candidates_by_source.get(fact.source_cell_ref, []),
            key=lambda item: (item.candidate_rank, -item.candidate_score),
        )
        if not source_candidates:
            continue
        top = source_candidates[0]
        alias_label = fact.row_label_canonical_candidate or fact.row_label_norm or fact.row_label_std or fact.row_label_raw
        key = (alias_label, top.candidate_code, fact.statement_type)
        bucket = grouped.setdefault(
            key,
            {
                "candidate_alias": alias_label,
                "canonical_code": top.candidate_code,
                "canonical_name": top.candidate_name,
                "statement_type": fact.statement_type,
                "evidence_count": 0,
                "amount_coverage_gain": 0.0,
                "benchmark_support": 0,
                "candidate_methods": Counter(),
                "candidate_scores": [],
                "conflicting_codes": set(),
            },
        )
        bucket["evidence_count"] += 1
        bucket["amount_coverage_gain"] += abs(float(fact.value_num or 0.0))
        bucket["candidate_methods"][top.candidate_method] += 1
        bucket["candidate_scores"].append(float(top.candidate_score))
        bucket["benchmark_support"] += benchmark_support.get((top.candidate_code, fact.period_key, round(float(fact.value_num or 0.0), 6)), 0)
        bucket["conflicting_codes"].update(candidate.candidate_code for candidate in source_candidates[:3])

    safe_methods = set(alias_rules.get("safe_methods", ["exact_normalized_match", "alias_table"]))
    min_evidence = int(alias_rules.get("safe_min_evidence_count", 2))
    rows: List[Dict[str, Any]] = []
    for bucket in grouped.values():
        best_method = bucket["candidate_methods"].most_common(1)[0][0]
        avg_score = sum(bucket["candidate_scores"]) / max(len(bucket["candidate_scores"]), 1)
        safe_to_auto_accept = (
            best_method in safe_methods
            and len(bucket["conflicting_codes"]) == 1
            and (bucket["benchmark_support"] > 0 or bucket["evidence_count"] >= min_evidence)
        )
        rows.append(
            {
                "candidate_alias": bucket["candidate_alias"],
                "canonical_code": bucket["canonical_code"],
                "canonical_name": bucket["canonical_name"],
                "statement_type": bucket["statement_type"],
                "evidence_count": bucket["evidence_count"],
                "amount_coverage_gain": round(bucket["amount_coverage_gain"], 6),
                "benchmark_support": bucket["benchmark_support"],
                "safe_to_auto_accept": safe_to_auto_accept,
                "review_required": not safe_to_auto_accept,
                "candidate_method": best_method,
                "average_candidate_score": round(avg_score, 6),
                "conflicting_target_count": len(bucket["conflicting_codes"]),
            }
        )
    rows.sort(
        key=lambda row: (
            not row["safe_to_auto_accept"],
            -row["benchmark_support"],
            -row["amount_coverage_gain"],
            -row["evidence_count"],
            row["canonical_code"],
        )
    )
    summary = {
        "candidates_total": len(rows),
        "safe_to_auto_accept_total": sum(1 for row in rows if row["safe_to_auto_accept"]),
        "benchmark_supported_total": sum(1 for row in rows if row["benchmark_support"] > 0),
    }
    return rows, summary


def build_benchmark_support(benchmark_missing_rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str, float], int]:
    support = Counter()
    for row in benchmark_missing_rows:
        try:
            amount = round(float(row.get("benchmark_value", 0) or 0), 6)
        except (TypeError, ValueError):
            continue
        support[(row.get("mapping_code", ""), row.get("aligned_period_key", ""), amount)] += 1
    return support
