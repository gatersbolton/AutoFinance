from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from ..models import ReOCRTaskRecord, ReviewQueueRecord
from ..stable_ids import stable_id


def build_actionable_backlog(
    review_items: Sequence[ReviewQueueRecord],
    stage6_targets: Dict[str, Any] | None = None,
    fact_scope_map: Dict[str, str] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    fact_scope_map = fact_scope_map or {}
    actionable_rows: List[Dict[str, Any]] = []
    nonactionable_rows: List[Dict[str, Any]] = []
    for item in review_items:
        category = categorize_review_item(item)
        target_scope = infer_target_scope(item, fact_scope_map)
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
            "target_scope": target_scope,
        }
        if target_scope in {"note_detail", "note_aggregation", "non_target_noise"} and category in {"mapping_only", "header_or_structure_noise"}:
            nonactionable_rows.append(row)
        elif category in {"provider_conflict", "likely_ocr_numeric_error", "validation_sensitive"}:
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
    fact_scope_map: Dict[str, str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    fact_scope_map = fact_scope_map or {}
    review_map = {item.review_id: item for item in review_items}
    kept: List[Dict[str, Any]] = []
    dropped_mapping_only = 0
    dropped_note_detail = 0
    for task in tasks:
        review_item = review_map.get(task.source_review_id)
        category = categorize_review_item(review_item) if review_item else "unknown"
        target_scope = infer_target_scope(review_item, fact_scope_map) if review_item else ""
        if category == "mapping_only":
            dropped_mapping_only += 1
            continue
        if target_scope in {"note_detail", "note_aggregation", "non_target_noise"} and category in {"mapping_only", "header_or_structure_noise"}:
            dropped_note_detail += 1
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
                "target_scope": target_scope,
            }
        )
    kept = drop_redundant_page_tasks(kept)
    dedupe_input_total = len(kept)
    duplicate_groups_before = count_duplicate_groups(kept)
    kept = dedupe_reocr_rows(kept)
    duplicate_groups_after = count_duplicate_groups(kept)
    return kept, {
        "reocr_tasks_total_before": len(tasks),
        "reocr_tasks_total_after": len(kept),
        "dropped_mapping_only_total": dropped_mapping_only,
        "dropped_note_detail_total": dropped_note_detail,
        "page_level_total_after": sum(1 for row in kept if row["granularity"] == "page"),
        "category_breakdown": count_by_key(kept, "category"),
        "duplicate_groups_before": duplicate_groups_before,
        "duplicate_groups_after": duplicate_groups_after,
        "merged_task_count": max(dedupe_input_total - len(kept), 0),
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


def dedupe_reocr_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, int, str, str, str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        cluster_key = (
            str(row.get("doc_id", "")).strip(),
            int(row.get("page_no", 0) or 0),
            normalize_bbox(row.get("bbox", "")),
            str(row.get("logical_subtable_id", "") or row.get("table_id", "")).strip(),
            str(row.get("category", "")).strip(),
            str(row.get("expected_benefit", "")).strip(),
        )
        grouped.setdefault(cluster_key, []).append(dict(row))

    deduped: List[Dict[str, Any]] = []
    for cluster_key, cluster_rows in grouped.items():
        ordered = sorted(
            cluster_rows,
            key=lambda row: (
                granularity_rank(str(row.get("granularity", "")).strip()),
                -float(row.get("priority_score", 0.0) or 0.0),
                str(row.get("task_id", "")),
            ),
        )
        selected = dict(ordered[0])
        merged_task_ids = [str(row.get("task_id", "")).strip() for row in ordered]
        merged_review_ids = [str(row.get("source_review_id", "")).strip() for row in ordered if str(row.get("source_review_id", "")).strip()]
        selected["cluster_id"] = stable_id("REOCR_CLUSTER_", list(cluster_key) + merged_task_ids)
        selected["merged_task_ids"] = json.dumps(merged_task_ids, ensure_ascii=False)
        selected["merged_review_ids"] = json.dumps(sorted(set(merged_review_ids)), ensure_ascii=False)
        selected["merged_task_count"] = len(merged_task_ids) - 1
        selected["bbox_normalized"] = cluster_key[2]
        deduped.append(selected)

    deduped.sort(
        key=lambda row: (
            -float(row.get("priority_score", 0.0) or 0.0),
            str(row.get("doc_id", "")),
            int(row.get("page_no", 0) or 0),
            str(row.get("task_id", "")),
        )
    )
    return deduped


def count_duplicate_groups(rows: Sequence[Dict[str, Any]]) -> int:
    counter: Dict[Tuple[str, int, str, str, str, str], int] = {}
    for row in rows:
        cluster_key = (
            str(row.get("doc_id", "")).strip(),
            int(row.get("page_no", 0) or 0),
            normalize_bbox(row.get("bbox", "")),
            str(row.get("logical_subtable_id", "") or row.get("table_id", "")).strip(),
            str(row.get("category", "")).strip(),
            str(row.get("expected_benefit", "")).strip(),
        )
        counter[cluster_key] = counter.get(cluster_key, 0) + 1
    return sum(1 for count in counter.values() if count > 1)


def normalize_bbox(value: Any) -> str:
    if isinstance(value, list):
        parts = value
    else:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            parts = json.loads(text)
        except json.JSONDecodeError:
            return text
    if not isinstance(parts, list):
        return str(parts)
    normalized = []
    for item in parts[:4]:
        try:
            normalized.append(str(int(round(float(item)))))
        except (TypeError, ValueError):
            normalized.append(str(item))
    return ",".join(normalized)


def granularity_rank(value: str) -> int:
    order = {"cell": 0, "row": 1, "table": 2, "page": 3}
    return order.get(value, 9)


def count_by_key(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        counts[value] = counts.get(value, 0) + 1
    return counts


def infer_target_scope(item: ReviewQueueRecord | None, fact_scope_map: Dict[str, str]) -> str:
    if item is None:
        return ""
    for fact_id in item.related_fact_ids:
        scope = fact_scope_map.get(fact_id, "")
        if scope:
            return scope
    if item.statement_type == "note":
        return "note_detail"
    if item.statement_type in {"balance_sheet", "income_statement", "cash_flow", "changes_in_equity"}:
        return "main_export_target"
    return "non_target_noise"
