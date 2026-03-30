from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from .models import DuplicateRecord, FactRecord, compact_json


def assign_fact_ids(facts: List[FactRecord]) -> List[FactRecord]:
    for index, fact in enumerate(facts, start=1):
        if not fact.fact_id:
            fact.fact_id = f"F{index:06d}"
    return facts


def dedupe_facts(facts: List[FactRecord], provider_priority: List[str]) -> Tuple[List[FactRecord], List[DuplicateRecord]]:
    priority_index = {provider: index for index, provider in enumerate(provider_priority)}
    duplicates: List[DuplicateRecord] = []
    dropped_ids = set()

    strict_groups: Dict[Tuple[str, str, str, str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        canonical_key = fact.mapping_code or fact.row_label_std
        if not canonical_key:
            continue
        strict_groups[
            (
                fact.doc_id,
                fact.statement_group_key or fact.table_semantic_key,
                canonical_key,
                fact.period_key,
                fact.value_type,
            )
        ].append(fact)

    duplicate_group_index = 0
    for key, items in strict_groups.items():
        if len(items) <= 1:
            continue
        duplicate_group_index += 1
        duplicate_group_id = f"DUP{duplicate_group_index:05d}"
        kept, group_duplicates = resolve_duplicate_group(items, duplicate_group_id, priority_index, exact_period=True)
        duplicates.extend(group_duplicates)
        for item in items:
            item.duplicate_group_id = duplicate_group_id
            item.kept_fact_id = kept.fact_id
            if item is not kept and any(record.dropped_fact_id == item.fact_id and record.decision == "dropped" for record in group_duplicates):
                dropped_ids.add(item.fact_id)

    approx_groups: Dict[Tuple[str, str, str, str, str, object], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        canonical_key = fact.mapping_code or fact.row_label_std
        if not canonical_key or fact.fact_id in dropped_ids:
            continue
        normalized_value = normalize_fact_value(fact)
        if normalized_value is None:
            continue
        approx_groups[
            (
                fact.doc_id,
                fact.statement_group_key or fact.table_semantic_key,
                canonical_key,
                fact.period_role_norm or fact.period_role_raw,
                fact.value_type,
                normalized_value,
            )
        ].append(fact)

    for key, items in approx_groups.items():
        if len(items) <= 1:
            continue
        explicit = [item for item in items if item.report_date_norm and item.report_date_norm != "unknown_date"]
        unknown = [item for item in items if not item.report_date_norm or item.report_date_norm == "unknown_date"]
        if not explicit or not unknown:
            continue
        duplicate_group_index += 1
        duplicate_group_id = f"DUP{duplicate_group_index:05d}"
        kept = sorted(explicit, key=lambda item: score_fact(item, priority_index), reverse=True)[0]
        for item in items:
            item.duplicate_group_id = duplicate_group_id
            item.kept_fact_id = kept.fact_id
        for item in unknown:
            if item.fact_id == kept.fact_id:
                continue
            dropped_ids.add(item.fact_id)
            duplicates.append(
                DuplicateRecord(
                    duplicate_group_id=duplicate_group_id,
                    doc_id=item.doc_id,
                    statement_type=item.statement_type,
                    statement_group_key=item.statement_group_key or item.table_semantic_key,
                    period_key=item.period_key,
                    canonical_key=key[2],
                    kept_fact_id=kept.fact_id,
                    dropped_fact_id=item.fact_id,
                    kept_provider=kept.provider,
                    dropped_provider=item.provider,
                    kept_source_cell_ref=kept.source_cell_ref,
                    dropped_source_cell_ref=item.source_cell_ref,
                    dedupe_reason="explicit_date_preferred_over_unknown_date",
                    decision="dropped",
                    meta_json=compact_json(
                        {
                            "kept_period_key": kept.period_key,
                            "dropped_period_key": item.period_key,
                            "normalized_value": normalize_fact_value(item),
                        }
                    ),
                )
            )

    deduped = [fact for fact in facts if fact.fact_id not in dropped_ids]
    return deduped, duplicates


def resolve_duplicate_group(
    items: List[FactRecord],
    duplicate_group_id: str,
    priority_index: Dict[str, int],
    exact_period: bool,
) -> Tuple[FactRecord, List[DuplicateRecord]]:
    scored = sorted(items, key=lambda item: score_fact(item, priority_index), reverse=True)
    kept = scored[0]
    normalized_values = {normalize_fact_value(item) for item in items}
    duplicates: List[DuplicateRecord] = []
    decision = "dropped"
    reason = "strict_duplicate_same_period"
    if len(normalized_values) > 1:
        decision = "review_kept_both"
        reason = "duplicate_candidates_with_value_mismatch"

    for item in items:
        item.duplicate_group_id = duplicate_group_id
        item.kept_fact_id = kept.fact_id
        if item is kept:
            continue
        duplicates.append(
            DuplicateRecord(
                duplicate_group_id=duplicate_group_id,
                doc_id=item.doc_id,
                statement_type=item.statement_type,
                statement_group_key=item.statement_group_key or item.table_semantic_key,
                period_key=item.period_key,
                canonical_key=item.mapping_code or item.row_label_std,
                kept_fact_id=kept.fact_id,
                dropped_fact_id=item.fact_id,
                kept_provider=kept.provider,
                dropped_provider=item.provider,
                kept_source_cell_ref=kept.source_cell_ref,
                dropped_source_cell_ref=item.source_cell_ref,
                dedupe_reason=reason,
                decision=decision,
                meta_json=compact_json(
                    {
                        "exact_period": exact_period,
                        "kept_period_key": kept.period_key,
                        "dropped_period_key": item.period_key,
                        "kept_value": normalize_fact_value(kept),
                        "dropped_value": normalize_fact_value(item),
                    }
                ),
            )
        )
    return kept, duplicates


def score_fact(fact: FactRecord, priority_index: Dict[str, int]) -> Tuple[int, ...]:
    return (
        date_score(fact.report_date_norm),
        statement_score(fact.statement_type),
        1 if fact.mapping_code else 0,
        status_score(fact.status),
        source_kind_score(fact.source_kind),
        1 if fact.value_num is not None else 0,
        len(fact.col_header_path or []),
        1 if fact.table_semantic_key else 0,
        -priority_index.get(fact.provider, 999),
        1 if fact.comparison_status in {"accepted", "equal", "single_provider"} else 0,
    )


def date_score(report_date_norm: str) -> int:
    if not report_date_norm or report_date_norm == "unknown_date":
        return 0
    if len(report_date_norm) == 10 and report_date_norm[4] == "-" and report_date_norm[7] == "-":
        return 3
    if report_date_norm.endswith("月"):
        return 2
    if report_date_norm.endswith("年度"):
        return 1
    return 1


def statement_score(statement_type: str) -> int:
    return 0 if statement_type == "unknown" else 1


def status_score(status: str) -> int:
    order = {
        "observed": 5,
        "repaired": 4,
        "inferred": 3,
        "blank": 2,
        "review": 1,
        "conflict": 0,
    }
    return order.get(status, 0)


def source_kind_score(source_kind: str) -> int:
    return 0 if source_kind == "xlsx_fallback" else 1


def normalize_fact_value(fact: FactRecord):
    if fact.value_num is not None:
        return round(float(fact.value_num), 8)
    if fact.value_raw:
        return fact.value_raw.strip()
    return None
