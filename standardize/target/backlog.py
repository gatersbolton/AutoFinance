from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple

from ..curation.backlog import categorize_review_item
from ..models import FactRecord, ReviewQueueRecord
from ..models import dataclass_row


def build_target_review_backlogs(
    review_items: Sequence[ReviewQueueRecord],
    facts: Sequence[FactRecord],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    fact_scope = {fact.fact_id: fact.target_scope or "" for fact in facts}
    main_rows: List[Dict[str, Any]] = []
    note_rows: List[Dict[str, Any]] = []
    suppressed_rows: List[Dict[str, Any]] = []
    scope_counter = Counter()
    for item in review_items:
        scope = infer_review_scope(item, fact_scope)
        row = dataclass_row(item)
        row["run_id"] = ""
        row["target_scope"] = scope
        row["review_category"] = categorize_review_item(item)
        scope_counter[scope] += 1
        if scope in {"main_export_target", "derived_target"}:
            main_rows.append(row)
        elif scope in {"note_detail", "note_aggregation"}:
            note_rows.append(row)
            if row["review_category"] in {"mapping_only", "header_or_structure_noise"}:
                suppressed_rows.append(row)
        else:
            note_rows.append(row)
    summary = {
        "run_id": "",
        "main_target_review_total": len(main_rows),
        "note_detail_review_total": len(note_rows),
        "suppressed_note_detail_total": len(suppressed_rows),
        "scope_breakdown": dict(scope_counter),
    }
    return main_rows, note_rows, suppressed_rows, summary


def infer_review_scope(item: ReviewQueueRecord, fact_scope: Dict[str, str]) -> str:
    for fact_id in item.related_fact_ids:
        scope = fact_scope.get(fact_id, "")
        if scope:
            return scope
    if item.statement_type == "note":
        return "note_detail"
    if item.statement_type in {"balance_sheet", "income_statement", "cash_flow", "changes_in_equity"}:
        return "main_export_target"
    return "non_target_noise"
