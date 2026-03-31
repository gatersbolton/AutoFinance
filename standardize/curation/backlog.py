from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from ..models import ReOCRTaskRecord, ReviewQueueRecord


def build_actionable_backlog(
    review_items: Sequence[ReviewQueueRecord],
    stage6_targets: Dict[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    actionable_rows: List[Dict[str, Any]] = []
    nonactionable_rows: List[Dict[str, Any]] = []
    for item in review_items:
        category = categorize_review_item(item)
        row = {
            "review_id": item.review_id,
            "doc_id": item.doc_id,
            "page_no": item.page_no,
            "statement_type": item.statement_type,
            "row_label_std": item.row_label_std,
            "period_key": item.period_key,
            "value_num": item.value_num,
            "priority_score": item.priority_score,
            "reason_codes": json.dumps(item.reason_codes, ensure_ascii=False),
            "category": category,
        }
        if category in {"provider_conflict", "likely_ocr_numeric_error", "validation_sensitive"}:
            actionable_rows.append(row)
        elif category == "mapping_only" and item.value_num not in (None, 0, 0.0):
            actionable_rows.append(row)
        else:
            nonactionable_rows.append(row)
    summary = {
        "review_total": len(review_items),
        "actionable_review_total": len(actionable_rows),
        "nonactionable_review_total": len(nonactionable_rows),
        "category_breakdown": count_by_key(actionable_rows + nonactionable_rows, "category"),
    }
    return actionable_rows, nonactionable_rows, summary


def prune_reocr_tasks(
    tasks: Sequence[ReOCRTaskRecord],
    review_items: Sequence[ReviewQueueRecord],
    stage6_targets: Dict[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    review_map = {item.review_id: item for item in review_items}
    kept: List[Dict[str, Any]] = []
    dropped_mapping_only = 0
    for task in tasks:
        review_item = review_map.get(task.source_review_id)
        category = categorize_review_item(review_item) if review_item else "unknown"
        if category == "mapping_only":
            dropped_mapping_only += 1
            continue
        kept.append(
            {
                "task_id": task.task_id,
                "granularity": task.granularity,
                "doc_id": task.doc_id,
                "page_no": task.page_no,
                "table_id": task.table_id,
                "logical_subtable_id": task.logical_subtable_id,
                "bbox": task.bbox,
                "reason_codes": json.dumps(task.reason_codes, ensure_ascii=False),
                "suggested_provider": task.suggested_provider,
                "priority_score": task.priority_score,
                "expected_benefit": task.expected_benefit,
                "source_review_id": task.source_review_id,
                "category": category,
            }
        )
    kept = drop_redundant_page_tasks(kept)
    return kept, {
        "reocr_tasks_total_before": len(tasks),
        "reocr_tasks_total_after": len(kept),
        "dropped_mapping_only_total": dropped_mapping_only,
        "page_level_total_after": sum(1 for row in kept if row["granularity"] == "page"),
        "category_breakdown": count_by_key(kept, "category"),
    }


def categorize_review_item(item: ReviewQueueRecord | None) -> str:
    if item is None:
        return "unknown"
    reasons = set(item.reason_codes)
    if any(reason.startswith("conflict:") for reason in reasons):
        return "provider_conflict"
    if any(reason.startswith("validation:") for reason in reasons):
        return "validation_sensitive"
    if "quality:suspicious_numeric" in reasons or "issue:suspicious_value" in reasons:
        return "likely_ocr_numeric_error"
    if "mapping:unmapped" in reasons and len(reasons) == 1:
        return "mapping_only"
    if "source:xlsx_fallback" in reasons:
        return "header_or_structure_noise"
    return "mapping_only" if "mapping:unmapped" in reasons else "header_or_structure_noise"


def drop_redundant_page_tasks(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    non_page_keys = {(row["doc_id"], row["page_no"]) for row in rows if row["granularity"] in {"cell", "row", "table"}}
    return [row for row in rows if not (row["granularity"] == "page" and (row["doc_id"], row["page_no"]) in non_page_keys)]


def count_by_key(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        counts[value] = counts.get(value, 0) + 1
    return counts
