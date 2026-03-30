from __future__ import annotations

from pathlib import Path
from typing import Iterable

from openpyxl import Workbook

from ..models import ReviewQueueRecord


def export_review_workbook(path: Path, review_items: Iterable[ReviewQueueRecord]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "review_queue"
    headers = [
        "review_id",
        "priority_score",
        "reason_codes",
        "doc_id",
        "page_no",
        "statement_type",
        "row_label_raw",
        "row_label_std",
        "period_key",
        "value_raw",
        "value_num",
        "provider",
        "source_file",
        "bbox",
        "related_fact_ids",
        "related_conflict_ids",
        "related_validation_ids",
        "mapping_candidates",
        "evidence_cell_path",
        "evidence_row_path",
        "evidence_table_path",
        "review_action",
        "review_note",
    ]
    worksheet.append(headers)
    for item in review_items:
        worksheet.append(
            [
                item.review_id,
                item.priority_score,
                ",".join(item.reason_codes),
                item.doc_id,
                item.page_no,
                item.statement_type,
                item.row_label_raw,
                item.row_label_std,
                item.period_key,
                item.value_raw,
                item.value_num,
                item.provider,
                item.source_file,
                item.bbox,
                ",".join(item.related_fact_ids),
                ",".join(item.related_conflict_ids),
                ",".join(item.related_validation_ids),
                item.mapping_candidates,
                item.evidence_cell_path,
                item.evidence_row_path,
                item.evidence_table_path,
                "",
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
