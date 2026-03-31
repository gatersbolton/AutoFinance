from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List

from .models import CellRecord, ConflictRecord, DuplicateRecord, FactRecord, ProviderComparisonRecord, RunSummaryRecord, ValidationResultRecord


def build_run_summary(
    docs_total: int,
    pages_total: int,
    pages_with_tables: int,
    pages_skipped_as_non_table: int,
    tables_total: int,
    cells: List[CellRecord],
    facts_raw: List[FactRecord],
    facts_deduped: List[FactRecord],
    duplicates: List[DuplicateRecord],
    provider_comparisons: List[ProviderComparisonRecord],
    validations: List[ValidationResultRecord],
    conflicts: List[ConflictRecord] | None = None,
    mapping_stats: Dict[str, Any] | None = None,
    review_summary: Dict[str, Any] | None = None,
    integrity_summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    mapping_stats = mapping_stats or {}
    review_summary = review_summary or {}
    integrity_summary = integrity_summary or {}

    active_facts = [fact for fact in facts_deduped if fact.status != "suppressed"]
    mapped_facts_total = sum(1 for fact in active_facts if fact.mapping_code)
    unknown_date_total = sum(1 for fact in active_facts if fact.report_date_norm == "unknown_date")
    unknown_statement_type_total = sum(1 for fact in active_facts if fact.statement_type == "unknown")
    unknown_period_role_total = sum(1 for fact in active_facts if (fact.period_role_norm or "") == "unknown")
    suspicious_cells_total = sum(1 for cell in cells if cell.is_suspicious)
    repaired_facts_total = sum(1 for fact in active_facts if fact.status == "repaired" or "repaired_numeric" in fact.issue_flags)
    review_facts_total = sum(1 for fact in active_facts if fact.status == "review")
    validation_status = Counter(result.status for result in validations)
    comparison_totals = {
        "provider_compared_pairs": sum(record.compared_pairs for record in provider_comparisons),
        "provider_equal_pairs": sum(record.equal_pairs for record in provider_comparisons),
        "provider_conflict_pairs": sum(record.conflict_pairs for record in provider_comparisons),
    }
    statement_type_breakdown = Counter(fact.statement_type for fact in active_facts)
    period_key_breakdown = Counter(fact.period_key for fact in active_facts)
    review_reason_breakdown = Counter()
    for fact in active_facts:
        if fact.status == "review":
            review_reason_breakdown.update(fact.issue_flags or ["review"])

    amount_total = sum(abs(float(fact.value_num or 0.0)) for fact in active_facts if fact.value_num is not None)
    mapped_amount_total = sum(
        abs(float(fact.value_num or 0.0))
        for fact in active_facts
        if fact.value_num is not None and fact.mapping_code and fact.status not in {"review", "conflict"}
    )
    conflict_decision_breakdown = Counter(conflict.decision for conflict in conflicts or [])
    unmapped_total = sum(1 for fact in active_facts if not fact.mapping_code)

    summary = RunSummaryRecord(
        docs_total=docs_total,
        pages_total=pages_total,
        pages_with_tables=pages_with_tables,
        pages_skipped_as_non_table=pages_skipped_as_non_table,
        tables_total=tables_total,
        cells_total=len(cells),
        facts_raw_total=len(facts_raw),
        facts_deduped_total=len(facts_deduped),
        mapped_facts_total=mapped_facts_total,
        mapped_facts_ratio=safe_ratio(mapped_facts_total, len(active_facts)),
        unknown_date_total=unknown_date_total,
        unknown_date_ratio=safe_ratio(unknown_date_total, len(active_facts)),
        suspicious_cells_total=suspicious_cells_total,
        repaired_facts_total=repaired_facts_total,
        review_facts_total=review_facts_total,
        duplicates_total=len(duplicates),
        duplicate_groups_total=len({record.duplicate_group_id for record in duplicates}),
        provider_compared_pairs=comparison_totals["provider_compared_pairs"],
        provider_equal_pairs=comparison_totals["provider_equal_pairs"],
        provider_conflict_pairs=comparison_totals["provider_conflict_pairs"],
        validation_total=len(validations),
        validation_pass_total=validation_status.get("pass", 0),
        validation_fail_total=validation_status.get("fail", 0),
    )
    summary_dict = {
        **summary.__dict__,
        "statement_type_breakdown": dict(statement_type_breakdown),
        "period_key_breakdown": dict(period_key_breakdown),
        "review_reason_breakdown": dict(review_reason_breakdown),
        "validation_reason_breakdown": dict(Counter(result.rule_name for result in validations)),
        "mapped_by_exact": int(mapping_stats.get("mapped_by_exact", 0)),
        "mapped_by_alias": int(mapping_stats.get("mapped_by_alias", 0)),
        "mapped_by_relation": int(mapping_stats.get("mapped_by_relation", 0)),
        "unmapped_total": unmapped_total,
        "unmapped_ratio": safe_ratio(unmapped_total, len(active_facts)),
        "amount_coverage_ratio": safe_ratio(mapped_amount_total, amount_total),
        "conflict_decision_breakdown": dict(conflict_decision_breakdown),
        "review_total": int(review_summary.get("review_total", review_facts_total)),
        "integrity_fail_total": int(integrity_summary.get("integrity_fail_total", 0)),
        "suppressed_total": sum(1 for fact in facts_deduped if fact.status == "suppressed"),
        "unknown_statement_type_total": unknown_statement_type_total,
        "unknown_period_role_total": unknown_period_role_total,
    }
    return summary_dict


def build_top_unknown_labels(facts: List[FactRecord], limit: int = 20) -> List[Dict[str, Any]]:
    counter = Counter(fact.mapping_name or fact.row_label_std or fact.row_label_raw for fact in facts if fact.report_date_norm == "unknown_date")
    return [
        {"label": label, "count": count}
        for label, count in counter.most_common(limit)
        if label
    ]


def build_top_suspicious_values(cells: List[CellRecord], limit: int = 20) -> List[Dict[str, Any]]:
    counter = Counter((cell.text_raw, cell.suspicious_reason) for cell in cells if cell.is_suspicious)
    return [
        {"text_raw": text_raw, "reason": reason, "count": count}
        for (text_raw, reason), count in counter.most_common(limit)
        if text_raw
    ]


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
