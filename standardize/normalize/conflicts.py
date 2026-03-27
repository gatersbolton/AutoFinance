from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from ..models import ConflictRecord, FactRecord, compact_json


def resolve_conflicts(
    facts: List[FactRecord],
    provider_priority: List[str],
    enabled: bool,
) -> Tuple[List[FactRecord], List[ConflictRecord]]:
    if not enabled:
        return facts, []

    grouped: Dict[Tuple[str, int, str, str, str], List[FactRecord]] = defaultdict(list)
    for fact in facts:
        key = (
            fact.doc_id,
            fact.page_no,
            fact.table_semantic_key,
            fact.row_label_std,
            fact.column_semantic_key,
        )
        grouped[key].append(fact)

    conflicts: List[ConflictRecord] = []
    priority_index = {provider: index for index, provider in enumerate(provider_priority)}

    for key, items in grouped.items():
        providers = {item.provider for item in items}
        if len(providers) <= 1:
            continue

        comparable_values = {normalize_fact_value(item) for item in items}
        comparable_values.discard(None)
        if len(comparable_values) <= 1:
            continue

        chosen = choose_fact(items, priority_index)
        provider_values = {
            item.provider: {
                "logical_subtable_id": item.logical_subtable_id,
                "table_semantic_key": item.table_semantic_key,
                "column_semantic_key": item.column_semantic_key,
                "value_raw": item.value_raw,
                "value_num": item.value_num,
                "status": item.status,
                "issue_flags": item.issue_flags,
            }
            for item in items
        }

        if chosen is None:
            for item in items:
                item.status = "review"
                if "provider_conflict" not in item.issue_flags:
                    item.issue_flags.append("provider_conflict")
            decision = "review"
            accepted_provider = ""
            reason = "unable_to_decide"
        else:
            for item in items:
                if item is chosen:
                    if item.status in {"conflict", "review"}:
                        item.status = "observed"
                    continue
                item.status = "conflict"
                if "provider_conflict" not in item.issue_flags:
                    item.issue_flags.append("provider_conflict")
            decision = "accepted"
            accepted_provider = chosen.provider
            reason = "highest_quality_numeric" if chosen.value_num is not None else "priority_fallback"

        conflicts.append(
            ConflictRecord(
                doc_id=key[0],
                page_no=key[1],
                logical_subtable_id=items[0].logical_subtable_id,
                table_semantic_key=key[2],
                statement_type=items[0].statement_type,
                row_label_std=key[3],
                column_semantic_key=key[4],
                period_key=items[0].period_key,
                provider_values_json=compact_json(provider_values),
                decision=decision,
                accepted_provider=accepted_provider,
                reason=reason,
                meta_json=compact_json(
                    {
                        "providers": sorted(providers),
                        "provider_logical_subtable_ids": {
                            item.provider: item.logical_subtable_id for item in items
                        },
                    }
                ),
            )
        )

    return facts, conflicts


def choose_fact(items: List[FactRecord], priority_index: Dict[str, int]) -> FactRecord | None:
    legal_numeric = [
        item
        for item in items
        if item.value_num is not None and not any("noise" in flag for flag in item.issue_flags)
    ]
    if legal_numeric:
        return sorted(legal_numeric, key=lambda item: priority_index.get(item.provider, 999))[0]

    parseable = [item for item in items if item.value_num is not None]
    if parseable:
        return sorted(parseable, key=lambda item: priority_index.get(item.provider, 999))[0]
    return None


def normalize_fact_value(fact: FactRecord):
    if fact.value_num is not None:
        return round(float(fact.value_num), 8)
    if fact.value_raw:
        return fact.value_raw.strip()
    return None
