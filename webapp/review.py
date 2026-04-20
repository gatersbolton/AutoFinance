from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from standardize.feedback.actions import SUPPORTED_ACTIONS as BACKEND_SUPPORTED_ACTIONS
from standardize.feedback.template import TEMPLATE_HEADERS

from .config import WebAppSettings
from .db import list_review_actions, upsert_review_action, utc_now_iso
from .jobs import _repo_relative_or_absolute
from .models import (
    REVIEW_ACTION_TYPES,
    REVIEW_STATUS_DEFERRED,
    REVIEW_STATUS_IGNORED,
    REVIEW_STATUS_REOCR_REQUESTED,
    REVIEW_STATUS_RESOLVED,
    REVIEW_STATUS_UNRESOLVED,
    JobRecord,
    ReviewActionRecord,
    ReviewItemRecord,
    ReviewSourceArtifact,
)


HIGH_PRIORITY_THRESHOLD = 5.0

REVIEW_SOURCE_DEFS = (
    ("review_queue", "复核队列", "review_queue.csv"),
    ("issues", "问题清单", "issues.csv"),
    ("validation_results", "校验结果", "validation_results.csv"),
    ("conflicts_enriched", "冲突明细", "conflicts_enriched.csv"),
    ("conflict_decision_audit", "冲突决策审计", "conflict_decision_audit.csv"),
    ("unplaced_facts", "未落位事实", "unplaced_facts.csv"),
    ("mapping_candidates", "科目映射候选", "mapping_candidates.csv"),
    ("benchmark_gap_explanations", "基准差异说明", "benchmark_gap_explanations.csv"),
    ("source_backed_gap_closure", "来源支撑缺口闭环", "source_backed_gap_closure.csv"),
    ("review_workbook", "复核工作簿", "review_workbook.xlsx"),
)

EXPORT_EXTRA_HEADERS = [
    "review_item_id",
    "source_ref",
    "created_at",
    "original_action_type",
    "compatibility_status",
    "compatibility_note",
]
EXPORT_HEADERS = [*TEMPLATE_HEADERS, *EXPORT_EXTRA_HEADERS]


def get_review_dir(job: JobRecord) -> Path:
    return Path(job.output_dir).resolve().parent / "review"


def get_allowed_review_roots(job: JobRecord) -> tuple[Path, ...]:
    return (
        Path(job.output_dir).resolve().parent,
        Path(job.result_dir).resolve(),
    )


def review_source_artifacts(job: JobRecord) -> list[ReviewSourceArtifact]:
    output_dir = Path(job.output_dir)
    artifacts: list[ReviewSourceArtifact] = []
    for slug, label, filename in REVIEW_SOURCE_DEFS:
        path = output_dir / filename
        row_count = count_csv_rows(path) if path.suffix.lower() == ".csv" and path.exists() else 0
        artifacts.append(
            ReviewSourceArtifact(
                slug=slug,
                label=label,
                path=str(path),
                relative_path=_repo_relative_or_absolute(path),
                exists=path.exists(),
                row_count=row_count,
            )
        )
    return artifacts


def load_review_items(settings: WebAppSettings, job: JobRecord) -> tuple[list[ReviewItemRecord], list[ReviewSourceArtifact]]:
    output_dir = Path(job.output_dir)
    items: list[ReviewItemRecord] = []

    review_queue_path = output_dir / "review_queue.csv"
    if review_queue_path.exists():
        items.extend(_load_review_queue_items(review_queue_path))

    issues_path = output_dir / "issues.csv"
    if issues_path.exists():
        items.extend(_load_issue_items(issues_path))

    validation_path = output_dir / "validation_results.csv"
    if validation_path.exists():
        items.extend(_load_validation_items(validation_path))

    conflict_path = output_dir / "conflicts_enriched.csv"
    if conflict_path.exists():
        items.extend(_load_conflict_items(conflict_path))

    unplaced_path = output_dir / "unplaced_facts.csv"
    if unplaced_path.exists():
        items.extend(_load_unplaced_fact_items(unplaced_path))

    mapping_candidates_path = output_dir / "mapping_candidates.csv"
    if mapping_candidates_path.exists():
        items.extend(_load_mapping_candidate_items(mapping_candidates_path))

    _apply_saved_actions(items, list_review_actions(settings, job.job_id))
    items.sort(key=_default_sort_key)
    return items, review_source_artifacts(job)


def filter_review_items(
    items: Iterable[ReviewItemRecord],
    *,
    status: str = "",
    source_type: str = "",
    reason_code: str = "",
    page_no: str = "",
    statement_type: str = "",
    provider: str = "",
    search: str = "",
    only_high_priority: bool = False,
    sort_by: str = "priority_desc",
) -> list[ReviewItemRecord]:
    search_text = search.strip().lower()
    normalized_reason = reason_code.strip()
    normalized_page = page_no.strip()
    filtered: list[ReviewItemRecord] = []
    for item in items:
        if status and item.current_status != status:
            continue
        if source_type and item.source_type != source_type:
            continue
        if normalized_reason and normalized_reason not in item.reason_codes and normalized_reason != item.reason_code:
            continue
        if normalized_page:
            try:
                if item.page_no != int(normalized_page):
                    continue
            except ValueError:
                continue
        if statement_type and item.statement_type != statement_type:
            continue
        if provider and item.provider != provider:
            continue
        if only_high_priority and item.priority_score < HIGH_PRIORITY_THRESHOLD:
            continue
        if search_text:
            haystack = " ".join(
                [
                    item.row_label_raw,
                    item.row_label_std,
                    item.reason_label_zh,
                    item.doc_id,
                    item.mapping_name,
                    item.mapping_code,
                ]
            ).lower()
            if search_text not in haystack:
                continue
        filtered.append(item)
    filtered.sort(key=_sort_key(sort_by))
    return filtered


def build_review_dashboard_summary(items: Iterable[ReviewItemRecord]) -> dict[str, Any]:
    rows = list(items)
    source_breakdown = Counter(item.source_type for item in rows)
    reason_breakdown = Counter(item.reason_label_zh for item in rows)
    return {
        "total_review_items": len(rows),
        "unresolved_review_items": sum(1 for item in rows if item.current_status == REVIEW_STATUS_UNRESOLVED),
        "high_priority_items": sum(1 for item in rows if item.priority_score >= HIGH_PRIORITY_THRESHOLD),
        "validation_fail_count": sum(1 for item in rows if item.source_type == "validation"),
        "mapping_unmapped_count": sum(
            1
            for item in rows
            if item.source_type in {"mapping_candidate", "unplaced_fact"}
            or any(code.startswith("mapping:") for code in item.reason_codes)
        ),
        "suspicious_numeric_count": sum(
            1
            for item in rows
            if any(code == "quality:suspicious_numeric" for code in item.reason_codes)
        ),
        "ocr_suspicious_count": sum(
            1
            for item in rows
            if any(code == "issue:suspicious_value" or code.startswith("source:") for code in item.reason_codes)
        ),
        "provider_conflict_count": sum(
            1 for item in rows if item.source_type == "conflict" or any(code.startswith("conflict:") for code in item.reason_codes)
        ),
        "resolved_count": sum(1 for item in rows if item.current_status == REVIEW_STATUS_RESOLVED),
        "deferred_count": sum(1 for item in rows if item.current_status == REVIEW_STATUS_DEFERRED),
        "source_type_breakdown": dict(sorted(source_breakdown.items())),
        "reason_label_breakdown": dict(sorted(reason_breakdown.items())),
    }


def build_review_filters(items: Iterable[ReviewItemRecord]) -> dict[str, list[str]]:
    rows = list(items)
    return {
        "statuses": sorted({item.current_status for item in rows}),
        "source_types": sorted({item.source_type for item in rows}),
        "reason_codes": sorted({code for item in rows for code in item.reason_codes}),
        "statement_types": sorted({item.statement_type for item in rows if item.statement_type}),
        "providers": sorted({item.provider for item in rows if item.provider}),
    }


def save_review_action(
    settings: WebAppSettings,
    job: JobRecord,
    item: ReviewItemRecord,
    *,
    action_type: str,
    action_value: str,
    reviewer_note: str,
    reviewer_name: str,
) -> ReviewActionRecord:
    if action_type not in REVIEW_ACTION_TYPES:
        raise ValueError(f"不支持的复核动作: {action_type}")
    now = utc_now_iso()
    action = ReviewActionRecord(
        job_id=job.job_id,
        review_item_id=item.review_item_id,
        action_type=action_type.strip(),
        action_value=action_value.strip(),
        reviewer_note=reviewer_note.strip(),
        reviewer_name=reviewer_name.strip(),
        review_status=_derive_review_status(action_type),
        source_type=item.source_type,
        source_ref=item.source_ref or item.source_cell_ref or item.review_item_id,
        created_at=now,
        updated_at=now,
    )
    return upsert_review_action(settings, action)


def export_review_actions(settings: WebAppSettings, job: JobRecord) -> dict[str, Any]:
    items, _ = load_review_items(settings, job)
    item_by_id = {item.review_item_id: item for item in items}
    actions = list_review_actions(settings, job.job_id)
    export_rows: list[dict[str, Any]] = []
    compatibility_notes: list[str] = []
    backend_ready_total = 0
    backend_partial_total = 0
    missing_review_id_total = 0
    request_reocr_fallback_total = 0
    action_type_mapping_total = 0

    for action in actions:
        item = item_by_id.get(action.review_item_id)
        if item is None:
            continue
        row, note_bundle = _build_export_row(item, action)
        export_rows.append(row)
        if note_bundle["compatibility_status"] == "ready":
            backend_ready_total += 1
        else:
            backend_partial_total += 1
        if note_bundle["compatibility_note"]:
            compatibility_notes.append(note_bundle["compatibility_note"])
        if not row["review_id"]:
            missing_review_id_total += 1
        if note_bundle["used_reocr_fallback"]:
            request_reocr_fallback_total += 1
        if note_bundle["action_type_mapped"]:
            action_type_mapping_total += 1

    review_dir = get_review_dir(job)
    review_dir.mkdir(parents=True, exist_ok=True)
    csv_path = review_dir / "review_actions_filled.csv"
    xlsx_path = review_dir / "review_actions_filled.xlsx"
    summary_path = review_dir / "review_action_export_summary.json"

    _write_export_csv(csv_path, export_rows)
    _write_export_workbook(xlsx_path, export_rows)

    summary = {
        "job_id": job.job_id,
        "exported_at": utc_now_iso(),
        "actions_total": len(export_rows),
        "backend_ready_total": backend_ready_total,
        "backend_partial_total": backend_partial_total,
        "missing_review_id_total": missing_review_id_total,
        "request_reocr_fallback_total": request_reocr_fallback_total,
        "mapped_action_type_total": action_type_mapping_total,
        "compatibility_notes": sorted({note for note in compatibility_notes if note}),
        "csv_path": _repo_relative_or_absolute(csv_path),
        "xlsx_path": _repo_relative_or_absolute(xlsx_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "csv_path": csv_path,
        "xlsx_path": xlsx_path,
        "summary_path": summary_path,
        "summary": summary,
        "actions_total": len(export_rows),
    }


def resolve_evidence_file(job: JobRecord, item: ReviewItemRecord, evidence_kind: str) -> Path | None:
    candidate = ""
    if evidence_kind == "cell":
        candidate = item.meta.get("evidence_cell_path", "") or item.evidence_path
    elif evidence_kind == "row":
        candidate = item.meta.get("evidence_row_path", "")
    elif evidence_kind == "table":
        candidate = item.meta.get("evidence_table_path", "")
    if not candidate:
        return None

    path = Path(candidate)
    if not path.is_absolute():
        path = Path(job.output_dir).resolve().parent / path
    resolved = path.resolve()
    for root in get_allowed_review_roots(job):
        try:
            resolved.relative_to(root)
            return resolved if resolved.exists() and resolved.is_file() else None
        except ValueError:
            continue
    return None


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _load_review_queue_items(path: Path) -> list[ReviewItemRecord]:
    rows: list[ReviewItemRecord] = []
    for row in _read_csv_rows(path):
        reason_codes = _parse_json_list(row.get("reason_codes", ""))
        meta = _parse_json_object(row.get("meta_json", ""))
        mapping_code, mapping_name = _parse_mapping_candidate_text(row.get("mapping_candidates", ""))
        evidence_path, evidence_label = _first_evidence_path(row)
        rows.append(
            ReviewItemRecord(
                review_item_id=row.get("review_id", "").strip(),
                review_id=row.get("review_id", "").strip(),
                source_file=row.get("source_file", "").strip(),
                source_type="review_queue",
                priority_score=_to_float(row.get("priority_score"), 0.0),
                reason_code=reason_codes[0] if reason_codes else "",
                reason_codes=reason_codes,
                reason_label_zh=_reason_label_zh(reason_codes[0] if reason_codes else ""),
                doc_id=row.get("doc_id", "").strip(),
                page_no=_to_int(row.get("page_no")),
                statement_type=row.get("statement_type", "").strip(),
                row_label_raw=row.get("row_label_raw", "").strip(),
                row_label_std=row.get("row_label_std", "").strip(),
                mapping_code=mapping_code,
                mapping_name=mapping_name,
                period_key=row.get("period_key", "").strip(),
                value_raw=row.get("value_raw", "").strip(),
                value_num=_to_float_or_none(row.get("value_num")),
                provider=row.get("provider", "").strip(),
                source_cell_ref=str(meta.get("source_cell_ref", "")).strip(),
                evidence_path=evidence_path,
                evidence_label=evidence_label,
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=str(meta.get("source_cell_ref", "")).strip() or row.get("review_id", "").strip(),
                related_conflict_ids=_parse_json_list(row.get("related_conflict_ids", "")),
                related_validation_ids=_parse_json_list(row.get("related_validation_ids", "")),
                related_fact_ids=_parse_json_list(row.get("related_fact_ids", "")),
                meta={
                    "evidence_cell_path": row.get("evidence_cell_path", "").strip(),
                    "evidence_row_path": row.get("evidence_row_path", "").strip(),
                    "evidence_table_path": row.get("evidence_table_path", "").strip(),
                    **meta,
                },
            )
        )
    return rows


def _load_issue_items(path: Path) -> list[ReviewItemRecord]:
    rows: list[ReviewItemRecord] = []
    for row in _read_csv_rows(path):
        issue_type = row.get("issue_type", "").strip()
        item_id = _stable_review_item_id(
            "issue",
            row.get("doc_id", ""),
            row.get("page_no", ""),
            row.get("source_cell_ref", ""),
            issue_type,
            row.get("message", ""),
        )
        meta = _parse_json_object(row.get("meta_json", ""))
        reason_code = f"issue:{issue_type}" if issue_type else "issue"
        raw_label = row.get("text_clean", "").strip() or row.get("text_raw", "").strip() or row.get("message", "").strip()
        rows.append(
            ReviewItemRecord(
                review_item_id=item_id,
                review_id="",
                source_file=row.get("source_file", "").strip(),
                source_type="issue",
                priority_score=_priority_from_issue(row.get("severity", "").strip(), issue_type),
                reason_code=reason_code,
                reason_codes=[reason_code],
                reason_label_zh=_reason_label_zh(reason_code),
                doc_id=row.get("doc_id", "").strip(),
                page_no=_to_int(row.get("page_no")),
                row_label_raw=raw_label,
                provider=row.get("provider", "").strip(),
                source_cell_ref=row.get("source_cell_ref", "").strip(),
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=row.get("source_cell_ref", "").strip() or item_id,
                meta=meta,
            )
        )
    return rows


def _load_validation_items(path: Path) -> list[ReviewItemRecord]:
    rows: list[ReviewItemRecord] = []
    for row in _read_csv_rows(path):
        status = row.get("status", "").strip()
        if status not in {"fail", "review"}:
            continue
        refs = _parse_json_list(row.get("evidence_fact_refs", ""))
        first_ref = refs[0] if refs else ""
        ref_meta = _parse_source_cell_ref(first_ref)
        rule_name = row.get("rule_name", "").strip()
        reason_code = f"validation:{rule_name}:{status}" if rule_name else f"validation:{status}"
        rows.append(
            ReviewItemRecord(
                review_item_id=row.get("validation_id", "").strip() or _stable_review_item_id("validation", rule_name, first_ref),
                review_id="",
                source_file="",
                source_type="validation",
                priority_score=5.0 if status == "fail" else 4.0,
                reason_code=reason_code,
                reason_codes=[reason_code],
                reason_label_zh=_reason_label_zh(reason_code),
                doc_id=row.get("doc_id", "").strip() or ref_meta.get("doc_id", ""),
                page_no=ref_meta.get("page_no"),
                statement_type=row.get("statement_type", "").strip(),
                row_label_raw=row.get("message", "").strip(),
                row_label_std=rule_name,
                period_key=row.get("period_key", "").strip(),
                provider=ref_meta.get("provider", ""),
                source_cell_ref=first_ref,
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=first_ref or row.get("validation_id", "").strip(),
                related_validation_ids=[row.get("validation_id", "").strip()],
                meta=_parse_json_object(row.get("meta_json", "")),
            )
        )
    return rows


def _load_conflict_items(path: Path) -> list[ReviewItemRecord]:
    rows: list[ReviewItemRecord] = []
    for row in _read_csv_rows(path):
        decision = row.get("decision", "").strip()
        needs_review = str(row.get("needs_review", "")).strip().lower() in {"1", "true", "yes"}
        if decision not in {"review_required", "unresolved"} and not needs_review:
            continue
        providers = row.get("providers", "").strip()
        candidate_fact_id = _first_fact_id_from_json(row.get("candidate_values_json", "")) or row.get("accepted_fact_id", "").strip()
        reason_code = f"conflict:{decision or 'review_required'}"
        rows.append(
            ReviewItemRecord(
                review_item_id=row.get("conflict_id", "").strip() or _stable_review_item_id("conflict", providers, row.get("row_label_std", "")),
                review_id="",
                source_file="",
                source_type="conflict",
                priority_score=5.0 if decision == "review_required" or needs_review else 4.0,
                reason_code=reason_code,
                reason_codes=[reason_code],
                reason_label_zh=_reason_label_zh(reason_code),
                doc_id=row.get("doc_id", "").strip(),
                page_no=_to_int(row.get("page_no")),
                statement_type=row.get("statement_type", "").strip(),
                row_label_std=row.get("row_label_std", "").strip(),
                period_key=row.get("period_key", "").strip(),
                provider=providers or row.get("accepted_provider", "").strip(),
                value_raw=row.get("validation_delta", "").strip(),
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=row.get("conflict_id", "").strip() or providers,
                related_conflict_ids=[row.get("conflict_id", "").strip()] if row.get("conflict_id", "").strip() else [],
                candidate_conflict_fact_id=candidate_fact_id,
                meta=_parse_json_object(row.get("meta_json", "")),
            )
        )
    return rows


def _load_unplaced_fact_items(path: Path) -> list[ReviewItemRecord]:
    rows: list[ReviewItemRecord] = []
    for row in _read_csv_rows(path):
        reason = row.get("unplaced_reason", "").strip() or "unplaced"
        reason_code = f"unplaced:{reason}"
        rows.append(
            ReviewItemRecord(
                review_item_id=row.get("fact_id", "").strip() or _stable_review_item_id("unplaced", row.get("source_cell_ref", "")),
                review_id="",
                source_file="",
                source_type="unplaced_fact",
                priority_score=3.5 if reason == "unmapped" else 2.5,
                reason_code=reason_code,
                reason_codes=[reason_code],
                reason_label_zh=_reason_label_zh(reason_code),
                doc_id=row.get("doc_id", "").strip(),
                page_no=_to_int(row.get("page_no")),
                statement_type=row.get("statement_type", "").strip(),
                row_label_raw=row.get("row_label_raw", "").strip(),
                row_label_std=row.get("row_label_std", "").strip(),
                mapping_code=row.get("mapping_code", "").strip(),
                mapping_name=row.get("mapping_name", "").strip(),
                period_key=row.get("period_key", "").strip(),
                value_raw=row.get("value_raw", "").strip(),
                value_num=_to_float_or_none(row.get("value_num")),
                provider=row.get("provider", "").strip(),
                source_cell_ref=row.get("source_cell_ref", "").strip(),
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=row.get("source_cell_ref", "").strip() or row.get("fact_id", "").strip(),
                related_fact_ids=[row.get("fact_id", "").strip()] if row.get("fact_id", "").strip() else [],
            )
        )
    return rows


def _load_mapping_candidate_items(path: Path) -> list[ReviewItemRecord]:
    grouped: dict[str, dict[str, str]] = {}
    for row in _read_csv_rows(path):
        group_key = "|".join(
            [
                row.get("doc_id", "").strip(),
                row.get("page_no", "").strip(),
                row.get("source_cell_ref", "").strip(),
                row.get("row_label_std", "").strip() or row.get("row_label_raw", "").strip(),
            ]
        )
        current = grouped.get(group_key)
        if current is None or _candidate_sort_tuple(row) < _candidate_sort_tuple(current):
            grouped[group_key] = row

    rows: list[ReviewItemRecord] = []
    for row in grouped.values():
        review_required = str(row.get("review_required", "")).strip().lower() in {"1", "true", "yes"}
        reason_code = "mapping:candidate_review" if review_required else "mapping:candidate"
        candidate_code = row.get("candidate_code", "").strip()
        candidate_name = row.get("candidate_name", "").strip()
        rows.append(
            ReviewItemRecord(
                review_item_id=_stable_review_item_id("mapping", row.get("source_cell_ref", ""), candidate_code, candidate_name),
                review_id="",
                source_file="",
                source_type="mapping_candidate",
                priority_score=round(2.0 + _to_float(row.get("candidate_score"), 0.0), 3),
                reason_code=reason_code,
                reason_codes=[reason_code],
                reason_label_zh=_reason_label_zh(reason_code),
                doc_id=row.get("doc_id", "").strip(),
                page_no=_to_int(row.get("page_no")),
                statement_type=row.get("statement_type", "").strip(),
                row_label_raw=row.get("row_label_raw", "").strip(),
                row_label_std=row.get("row_label_std", "").strip(),
                mapping_code=candidate_code,
                mapping_name=candidate_name,
                provider=row.get("provider", "").strip(),
                source_cell_ref=row.get("source_cell_ref", "").strip(),
                current_status=REVIEW_STATUS_UNRESOLVED,
                source_ref=row.get("source_cell_ref", "").strip() or candidate_code,
                meta=_parse_json_object(row.get("meta_json", "")),
            )
        )
    return rows


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [{str(key).strip(): ("" if value is None else str(value).strip()) for key, value in row.items()} for row in csv.DictReader(handle)]


def _apply_saved_actions(items: Iterable[ReviewItemRecord], actions: Iterable[ReviewActionRecord]) -> None:
    action_map = {item.review_item_id: item for item in actions}
    for item in items:
        action = action_map.get(item.review_item_id)
        if action is None:
            continue
        item.action_type = action.action_type
        item.action_value = action.action_value
        item.reviewer_note = action.reviewer_note
        item.reviewer_name = action.reviewer_name
        item.current_status = action.review_status or _derive_review_status(action.action_type)


def _build_export_row(item: ReviewItemRecord, action: ReviewActionRecord) -> tuple[dict[str, Any], dict[str, Any]]:
    export_action_type = action.action_type
    action_type_mapped = False
    action_value = action.action_value
    compatibility_notes: list[str] = []
    used_reocr_fallback = False

    if export_action_type == "accept_mapping_candidate":
        export_action_type = "accept_mapping_alias"
        action_type_mapped = True
        action_value = action_value or item.mapping_code
        compatibility_notes.append("accept_mapping_candidate 已映射为 accept_mapping_alias 以兼容现有 backend。")

    if export_action_type == "request_reocr" and not (action_value or item.suggested_reocr_task_id):
        action_value = f"web_reocr_request:{item.review_item_id}"
        used_reocr_fallback = True
        compatibility_notes.append("request_reocr 未提供后端任务 ID，已回退为 Web 侧占位 task_id。")

    if export_action_type == "set_mapping_override":
        action_value = action_value or item.mapping_code
    if export_action_type == "set_conflict_winner":
        action_value = action_value or item.candidate_conflict_fact_id

    row = {header: "" for header in EXPORT_HEADERS}
    row.update(
        {
            "review_id": item.review_id,
            "priority_score": item.priority_score,
            "source_type": item.source_type,
            "fact_id": item.related_fact_ids[0] if item.related_fact_ids else "",
            "source_cell_ref": item.source_cell_ref,
            "related_conflict_ids": ",".join(item.related_conflict_ids),
            "doc_id": item.doc_id,
            "page_no": item.page_no if item.page_no is not None else "",
            "statement_type": item.statement_type,
            "row_label_raw": item.row_label_raw,
            "row_label_std": item.row_label_std,
            "period_key": item.period_key,
            "value_raw": item.value_raw,
            "value_num": item.value_num if item.value_num is not None else "",
            "provider": item.provider,
            "candidate_mapping_code": item.mapping_code,
            "candidate_mapping_name": item.mapping_name,
            "candidate_conflict_fact_id": item.candidate_conflict_fact_id,
            "candidate_period_override": item.candidate_period_override,
            "suggested_reocr_task_id": item.suggested_reocr_task_id or (action_value if export_action_type == "request_reocr" else ""),
            "action_type": export_action_type,
            "action_value": action_value,
            "reviewer_note": action.reviewer_note,
            "reviewer_name": action.reviewer_name,
            "review_status": action.review_status,
            "applied_status": "",
            "apply_message": "",
            "review_item_id": item.review_item_id,
            "source_ref": action.source_ref or item.source_ref,
            "created_at": action.created_at,
            "original_action_type": action.action_type,
        }
    )

    compatibility_status = "ready"
    if not item.review_id:
        compatibility_status = "partial"
        compatibility_notes.append("该条目不是 review_queue 原生 review_id，直接用于现有 apply-review-actions 时需要补齐 review_id。")
    if export_action_type not in BACKEND_SUPPORTED_ACTIONS:
        compatibility_status = "partial"
        compatibility_notes.append("导出 action_type 仍未被现有 backend 支持。")
    if export_action_type in {"accept_mapping_alias", "set_mapping_override"} and not row["candidate_mapping_code"] and not row["action_value"]:
        compatibility_status = "partial"
        compatibility_notes.append("映射动作缺少 candidate_mapping_code/action_value。")
    if export_action_type == "set_conflict_winner" and not row["candidate_conflict_fact_id"] and not row["action_value"]:
        compatibility_status = "partial"
        compatibility_notes.append("冲突动作缺少 candidate_conflict_fact_id/action_value。")

    row["compatibility_status"] = compatibility_status
    row["compatibility_note"] = "；".join(dict.fromkeys(note for note in compatibility_notes if note))
    return row, {
        "compatibility_status": compatibility_status,
        "compatibility_note": row["compatibility_note"],
        "used_reocr_fallback": used_reocr_fallback,
        "action_type_mapped": action_type_mapped,
    }


def _write_export_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: _serialize_value(row.get(header, "")) for header in EXPORT_HEADERS})


def _write_export_workbook(path: Path, rows: list[dict[str, Any]]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Actions"
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = f"A1:{get_column_letter(len(EXPORT_HEADERS))}1"
    worksheet.append(EXPORT_HEADERS)
    for row in rows:
        worksheet.append([_serialize_value(row.get(header, "")) for header in EXPORT_HEADERS])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return value if value is not None else ""


def _sort_key(sort_by: str):
    if sort_by == "page_asc":
        return lambda item: (item.page_no if item.page_no is not None else 10**9, -(item.priority_score or 0.0), item.source_type, item.review_item_id)
    if sort_by == "source_type":
        return lambda item: (item.source_type, -(item.priority_score or 0.0), item.page_no if item.page_no is not None else 10**9, item.review_item_id)
    return _default_sort_key


def _default_sort_key(item: ReviewItemRecord):
    return (-(item.priority_score or 0.0), item.page_no if item.page_no is not None else 10**9, item.source_type, item.review_item_id)


def _derive_review_status(action_type: str) -> str:
    if action_type == "ignore":
        return REVIEW_STATUS_IGNORED
    if action_type == "defer":
        return REVIEW_STATUS_DEFERRED
    if action_type == "request_reocr":
        return REVIEW_STATUS_REOCR_REQUESTED
    if action_type:
        return REVIEW_STATUS_RESOLVED
    return REVIEW_STATUS_UNRESOLVED


def _stable_review_item_id(prefix: str, *parts: object) -> str:
    payload = "|".join(str(part or "").strip() for part in parts)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _parse_json_list(raw: str) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = [value.strip() for value in raw.split(",") if value.strip()]
    if isinstance(parsed, list):
        return [str(value).strip() for value in parsed if str(value).strip()]
    return [str(parsed).strip()] if str(parsed).strip() else []


def _parse_json_object(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_source_cell_ref(value: str) -> dict[str, Any]:
    parts = [part.strip() for part in value.split(":")]
    if len(parts) < 3:
        return {"doc_id": "", "page_no": None, "provider": ""}
    page_no = _to_int(parts[1])
    return {"doc_id": parts[0], "page_no": page_no, "provider": parts[2] if len(parts) > 2 else ""}


def _parse_mapping_candidate_text(raw: str) -> tuple[str, str]:
    text = raw.strip()
    if not text:
        return "", ""
    first = text.split(";", 1)[0].strip()
    if " " not in first:
        return first, ""
    code, remainder = first.split(" ", 1)
    name = remainder.rsplit("(", 1)[0].strip()
    return code.strip(), name


def _first_evidence_path(row: dict[str, str]) -> tuple[str, str]:
    for key, label in (
        ("evidence_cell_path", "单元格证据"),
        ("evidence_row_path", "行证据"),
        ("evidence_table_path", "表格证据"),
    ):
        value = row.get(key, "").strip()
        if value:
            return value, label
    return "", ""


def _priority_from_issue(severity: str, issue_type: str) -> float:
    base = {"error": 5.0, "warning": 3.5, "info": 2.0}.get(severity, 2.5)
    if issue_type == "suspicious_value":
        base += 0.5
    return base


def _reason_label_zh(reason_code: str) -> str:
    if reason_code.startswith("validation:"):
        return "校验失败"
    if reason_code.startswith("conflict:"):
        return "供应商识别冲突"
    if reason_code.startswith("mapping:"):
        return "科目映射缺失"
    if reason_code == "quality:suspicious_numeric":
        return "金额异常"
    if reason_code == "issue:suspicious_value" or reason_code.startswith("source:"):
        return "OCR 可疑"
    if reason_code.startswith("unplaced:"):
        return "待定位事实"
    if reason_code.startswith("issue:"):
        return "问题项"
    return "待复核"


def _candidate_sort_tuple(row: dict[str, str]) -> tuple[int, float]:
    return (_to_int(row.get("candidate_rank"), 999), -_to_float(row.get("candidate_score"), 0.0))


def _first_fact_id_from_json(raw: str) -> str:
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if isinstance(parsed, dict):
        values = parsed.values()
    elif isinstance(parsed, list):
        values = [parsed]
    else:
        return ""
    for entries in values:
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and str(entry.get("fact_id", "")).strip():
                return str(entry["fact_id"]).strip()
    return ""


def _to_int(value: object, default: int | None = None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def _to_float(value: object, default: float) -> float:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _to_float_or_none(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
