from __future__ import annotations

from typing import Dict, List, Sequence

from ..models import FactRecord, ReviewQueueRecord


def apply_suppression_overrides(facts: List[FactRecord], entries: Sequence[Dict[str, object]]) -> List[FactRecord]:
    by_fact_id = {str(entry.get("fact_id", "")).strip(): entry for entry in entries if str(entry.get("fact_id", "")).strip()}
    by_source_ref = {str(entry.get("source_cell_ref", "")).strip(): entry for entry in entries if str(entry.get("source_cell_ref", "")).strip()}
    for fact in facts:
        entry = by_fact_id.get(fact.fact_id) or by_source_ref.get(fact.source_cell_ref)
        if entry is None:
            continue
        action_type = str(entry.get("action_type", "")).strip() or "suppress_false_positive"
        fact.status = "suppressed"
        fact.suppression_reason = str(entry.get("note", action_type)).strip() or action_type
        fact.unplaced_reason = fact.unplaced_reason or action_type
        fact.override_source = "manual_override"
    return facts


def filter_review_items_by_placement(review_items: List[ReviewQueueRecord], entries: Sequence[Dict[str, object]]) -> List[ReviewQueueRecord]:
    suppress_review_ids = {
        str(entry.get("review_id", "")).strip()
        for entry in entries
        if str(entry.get("action_type", "")).strip() in {"ignore", "accept_relation_review_only"}
    }
    return [item for item in review_items if item.review_id not in suppress_review_ids]
