from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from fastapi import UploadFile

from .config import WebAppSettings
from .db import (
    add_document_batch_item,
    create_document_batch,
    get_document_batch,
    get_document_batch_item_by_index,
    get_job,
    list_document_batch_items,
    list_document_batches,
    queue_jobs_atomically,
    update_document_batch,
    utc_now_iso,
)
from .document_library import (
    MISSING_OCR_CREDENTIALS_MESSAGE,
    document_root,
    document_to_job,
    load_document,
    save_uploaded_document,
    update_document_status,
)
from .document_models import (
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_PARTIAL_FAILED,
    BATCH_STATUS_QUEUED,
    BATCH_STATUS_RUNNING,
    BATCH_STATUS_UPLOADING,
    STATUS_QUEUED,
    DocumentBatchItemRecord,
    DocumentBatchRecord,
)
from .models import (
    JOB_MODE_DOCUMENT_PIPELINE,
    JOB_STATUS_CANCELLED,
    JOB_STATUS_FAILED,
    JOB_STATUS_QUEUED,
    SUCCESS_LIKE_JOB_STATUSES,
    JobRecord,
)
from .ocr_runtime import upload_provider_runtime_ready
from .quality import describe_job_status


def generate_batch_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"batch_{timestamp}_{uuid.uuid4().hex[:8]}"


def create_new_document_batch(
    settings: WebAppSettings,
    *,
    expected_files: int,
) -> DocumentBatchRecord:
    if expected_files < 1:
        raise ValueError("请至少选择一个 PDF 文件。")
    if expected_files > settings.max_upload_batch_files:
        raise ValueError(f"每批最多上传 {settings.max_upload_batch_files} 个 PDF 文件。")
    now = utc_now_iso()
    return create_document_batch(
        settings,
        DocumentBatchRecord(
            batch_id=generate_batch_id(),
            status=BATCH_STATUS_UPLOADING,
            expected_files=expected_files,
            created_at=now,
            updated_at=now,
        ),
    )


async def add_uploaded_file_to_batch(
    settings: WebAppSettings,
    *,
    batch_id: str,
    upload_index: int,
    upload: UploadFile,
) -> tuple[DocumentBatchItemRecord, bool]:
    batch = get_document_batch(settings, batch_id)
    if batch is None:
        raise KeyError(batch_id)
    if upload_index < 0 or upload_index >= batch.expected_files:
        raise ValueError("上传序号超出批次范围。")
    existing = get_document_batch_item_by_index(settings, batch_id, upload_index)
    if existing is not None:
        if existing.original_filename != str(upload.filename or ""):
            raise ValueError("该上传序号已被另一个文件占用。")
        await upload.close()
        return existing, False

    document = await save_uploaded_document(settings, upload)
    item = DocumentBatchItemRecord(
        batch_id=batch_id,
        doc_id=document.doc_id,
        upload_index=upload_index,
        original_filename=document.original_filename,
        job_id="",
        created_at=utc_now_iso(),
    )
    try:
        add_document_batch_item(settings, item)
    except Exception:
        target = document_root(settings, document.doc_id).resolve()
        library_root = settings.library_root.resolve()
        if target != library_root and target.parent == library_root:
            shutil.rmtree(target, ignore_errors=True)
        raise
    return item, True


def ensure_document_pipeline_ready(settings: WebAppSettings) -> None:
    runtime = upload_provider_runtime_ready(settings, "cloud_first")
    if not runtime["runtime_ready"]:
        raise ValueError(MISSING_OCR_CREDENTIALS_MESSAGE)


def enqueue_document_pipeline(
    settings: WebAppSettings,
    doc_id: str,
) -> JobRecord:
    queued_job = _build_queued_document_job(settings, doc_id)
    persisted = queue_jobs_atomically(settings, [queued_job])[0]
    _mark_document_pipeline_queued(settings, doc_id, persisted.job_id)
    return persisted


def _build_queued_document_job(
    settings: WebAppSettings,
    doc_id: str,
) -> JobRecord:
    document = load_document(settings, doc_id, refresh=False)
    base_job = document_to_job(settings, document)
    now = utc_now_iso()
    return replace(
        base_job,
        mode=JOB_MODE_DOCUMENT_PIPELINE,
        provider_mode="cloud_first",
        status=JOB_STATUS_QUEUED,
        current_stage="queued",
        progress_summary="已进入处理队列，等待单 Worker 串行处理。",
        updated_at=now,
        started_at="",
        finished_at="",
        error_message="",
        raw_error_message="",
        user_friendly_error="",
        recommended_action="",
        command_executed="",
        exit_code=None,
    )


def _mark_document_pipeline_queued(
    settings: WebAppSettings,
    doc_id: str,
    job_id: str,
) -> None:
    update_document_status(
        settings,
        doc_id,
        ocr_status=STATUS_QUEUED,
        raw_metrics_status=STATUS_QUEUED,
        standard_metrics_status=STATUS_QUEUED,
        latest_job_id=job_id,
        error_message="",
    )


def queue_document_batch(
    settings: WebAppSettings,
    batch_id: str,
) -> dict[str, Any]:
    batch = get_document_batch(settings, batch_id)
    if batch is None:
        raise KeyError(batch_id)
    if batch.status != BATCH_STATUS_UPLOADING:
        return build_document_batch_summary(settings, batch_id)
    items = list_document_batch_items(settings, batch_id)
    if len(items) != batch.expected_files:
        raise ValueError(
            f"批次计划上传 {batch.expected_files} 个文件，当前仅收到 {len(items)} 个。"
        )
    ensure_document_pipeline_ready(settings)
    queued_jobs = [
        _build_queued_document_job(settings, item.doc_id)
        for item in items
    ]
    persisted_jobs = queue_jobs_atomically(
        settings,
        queued_jobs,
        batch_id=batch_id,
    )
    for job in persisted_jobs:
        _mark_document_pipeline_queued(settings, job.job_id, job.job_id)
    return build_document_batch_summary(settings, batch_id)


def build_document_batch_summary(
    settings: WebAppSettings,
    batch_id: str,
) -> dict[str, Any]:
    batch = get_document_batch(settings, batch_id)
    if batch is None:
        raise KeyError(batch_id)
    items = list_document_batch_items(settings, batch_id)
    item_payloads: list[dict[str, Any]] = []
    counts = {
        "uploaded": len(items),
        "queued": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
    }
    for item in items:
        job = get_job(settings, item.job_id) if item.job_id else None
        try:
            document = load_document(settings, item.doc_id)
        except KeyError:
            document = None
        job_status = job.status if job is not None else "uploaded"
        if job_status in SUCCESS_LIKE_JOB_STATUSES:
            counts["completed"] += 1
        elif job_status in {JOB_STATUS_FAILED, JOB_STATUS_CANCELLED}:
            counts["failed"] += 1
        elif job_status == "running":
            counts["running"] += 1
        elif job_status in {"queued", "created"}:
            counts["queued"] += 1
        item_payloads.append(
            {
                **item.as_dict(),
                "display_name": document.display_name if document is not None else item.original_filename,
                "document_exists": document is not None,
                "document_status": document.document_status_label_zh if document is not None else "已删除",
                "job_status": job_status,
                "job_status_label_zh": (
                    describe_job_status(job_status) if job is not None else "已上传"
                ),
                "job_updated_at": job.updated_at if job is not None else "",
                "current_stage": job.current_stage if job is not None else "",
                "progress_summary": job.progress_summary if job is not None else "等待批次入队。",
                "continue_url": f"/documents/{item.doc_id}/continue",
            }
        )

    resolved_status = batch.status
    finished_at = batch.finished_at
    if batch.queued_at:
        if counts["running"]:
            resolved_status = BATCH_STATUS_RUNNING
            finished_at = ""
        elif counts["queued"]:
            resolved_status = BATCH_STATUS_QUEUED
            finished_at = ""
        elif items and counts["completed"] == len(items):
            resolved_status = BATCH_STATUS_COMPLETED
            finished_at = finished_at or utc_now_iso()
        elif items and counts["failed"] == len(items):
            resolved_status = BATCH_STATUS_FAILED
            finished_at = finished_at or utc_now_iso()
        elif items and counts["completed"] + counts["failed"] == len(items):
            resolved_status = BATCH_STATUS_PARTIAL_FAILED
            finished_at = finished_at or utc_now_iso()
    if resolved_status != batch.status or finished_at != batch.finished_at:
        batch = update_document_batch(
            settings,
            batch_id,
            status=resolved_status,
            finished_at=finished_at,
        )

    state_payload = {
        "status": batch.status,
        "counts": counts,
        "items": [
            {
                "doc_id": item["doc_id"],
                "job_status": item["job_status"],
                "current_stage": item["current_stage"],
                "job_updated_at": item["job_updated_at"],
            }
            for item in item_payloads
        ],
    }
    state_token = hashlib.sha256(
        json.dumps(
            state_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:20]
    return {
        **batch.as_dict(),
        "status_label_zh": batch.status_label_zh,
        "counts": counts,
        "items": item_payloads,
        "state_token": state_token,
        "active": batch.status in {
            BATCH_STATUS_UPLOADING,
            BATCH_STATUS_QUEUED,
            BATCH_STATUS_RUNNING,
        },
    }


def list_recent_document_batch_summaries(
    settings: WebAppSettings,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    return [
        build_document_batch_summary(settings, batch.batch_id)
        for batch in list_document_batches(settings, limit=limit)
    ]
