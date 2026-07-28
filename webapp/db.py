from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator

from .config import WebAppSettings
from .document_models import DocumentBatchItemRecord, DocumentBatchRecord
from .models import (
    ACTIVE_OPERATION_STATUSES,
    OPERATION_STATUS_CANCELLED,
    OPERATION_STATUS_QUEUED,
    OPERATION_STATUS_RUNNING,
    ReviewActionRecord,
    ReviewOperationRecord,
    JobRecord,
)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    mode TEXT NOT NULL,
    provider_mode TEXT NOT NULL,
    input_path TEXT NOT NULL,
    source_image_dir TEXT NOT NULL DEFAULT '',
    upload_dir TEXT NOT NULL DEFAULT '',
    ocr_output_dir TEXT NOT NULL DEFAULT '',
    template_path TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    result_dir TEXT NOT NULL,
    log_dir TEXT NOT NULL,
    provider_priority TEXT NOT NULL,
    status TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    progress_summary TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT NOT NULL DEFAULT '',
    raw_error_message TEXT NOT NULL DEFAULT '',
    user_friendly_error TEXT NOT NULL DEFAULT '',
    recommended_action TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    command_executed TEXT NOT NULL DEFAULT '',
    exit_code INTEGER,
    timeout_seconds INTEGER NOT NULL,
    requested_stage TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at);
CREATE TABLE IF NOT EXISTS review_actions (
    job_id TEXT NOT NULL,
    review_item_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    action_value TEXT NOT NULL DEFAULT '',
    reviewer_note TEXT NOT NULL DEFAULT '',
    reviewer_name TEXT NOT NULL DEFAULT '',
    review_status TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT '',
    source_ref TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, review_item_id)
);
CREATE INDEX IF NOT EXISTS idx_review_actions_job_created_at ON review_actions(job_id, created_at);
CREATE TABLE IF NOT EXISTS review_operations (
    operation_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    queue_backend TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    duration_seconds REAL NOT NULL DEFAULT 0,
    progress_stage TEXT NOT NULL DEFAULT '',
    progress_message_zh TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    user_friendly_error_zh TEXT NOT NULL DEFAULT '',
    log_paths_json TEXT NOT NULL DEFAULT '[]',
    result_paths_json TEXT NOT NULL DEFAULT '[]',
    operation_dir TEXT NOT NULL DEFAULT '',
    summary_path TEXT NOT NULL DEFAULT '',
    timeline_path TEXT NOT NULL DEFAULT '',
    retry_of_operation_id TEXT NOT NULL DEFAULT '',
    retry_count INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    cancel_acknowledged INTEGER NOT NULL DEFAULT 0,
    queue_job_id TEXT NOT NULL DEFAULT '',
    extra_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_review_operations_job_created_at ON review_operations(job_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_operations_status_created_at ON review_operations(status, created_at);
CREATE TABLE IF NOT EXISTS document_batches (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    expected_files INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    queued_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_document_batches_created_at ON document_batches(created_at DESC);
CREATE TABLE IF NOT EXISTS document_batch_items (
    batch_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    upload_index INTEGER NOT NULL,
    original_filename TEXT NOT NULL,
    job_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, doc_id),
    UNIQUE (batch_id, upload_index)
);
CREATE INDEX IF NOT EXISTS idx_document_batch_items_batch_index
ON document_batch_items(batch_id, upload_index);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def _connect(settings: WebAppSettings) -> Iterator[sqlite3.Connection]:
    settings.ensure_directories()
    conn = sqlite3.connect(str(settings.db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        conn.close()


def init_db(settings: WebAppSettings) -> None:
    with _connect(settings) as conn:
        conn.executescript(SCHEMA_SQL)
        _ensure_column(conn, "jobs", "raw_error_message", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "jobs", "user_friendly_error", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "jobs", "recommended_action", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "jobs", "requested_stage", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "reviewer_note", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "reviewer_name", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "review_status", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "source_type", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "source_ref", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_actions", "updated_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "started_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "finished_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "duration_seconds", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "review_operations", "progress_stage", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "progress_message_zh", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "error_message", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "user_friendly_error_zh", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "log_paths_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "review_operations", "result_paths_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "review_operations", "operation_dir", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "summary_path", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "timeline_path", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "retry_of_operation_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "retry_count", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "review_operations", "cancel_requested", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "review_operations", "cancel_acknowledged", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "review_operations", "queue_job_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "review_operations", "extra_json", "TEXT NOT NULL DEFAULT '{}'")


def create_job(settings: WebAppSettings, job: JobRecord) -> JobRecord:
    with _connect(settings) as conn:
        _insert_job(conn, job)
    return job


_JOB_COLUMNS = (
    "job_id",
    "display_name",
    "mode",
    "provider_mode",
    "input_path",
    "source_image_dir",
    "upload_dir",
    "ocr_output_dir",
    "template_path",
    "output_dir",
    "result_dir",
    "log_dir",
    "provider_priority",
    "status",
    "current_stage",
    "progress_summary",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "error_message",
    "raw_error_message",
    "user_friendly_error",
    "recommended_action",
    "run_id",
    "command_executed",
    "exit_code",
    "timeout_seconds",
    "requested_stage",
)


def _insert_job(conn: sqlite3.Connection, job: JobRecord) -> None:
    columns = ", ".join(_JOB_COLUMNS)
    placeholders = ", ".join(f":{column}" for column in _JOB_COLUMNS)
    conn.execute(
        f"INSERT INTO jobs ({columns}) VALUES ({placeholders})",
        job.as_dict(),
    )


def queue_jobs_atomically(
    settings: WebAppSettings,
    jobs: Iterable[JobRecord],
    *,
    batch_id: str = "",
) -> list[JobRecord]:
    queued_jobs = list(jobs)
    if not queued_jobs:
        return []
    job_ids = [job.job_id for job in queued_jobs]
    if len(set(job_ids)) != len(job_ids):
        raise ValueError("待入队任务中存在重复文件。")
    if any(job.status != "queued" for job in queued_jobs):
        raise ValueError("只能原子写入 queued 状态的任务。")

    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if batch_id:
                batch = conn.execute(
                    "SELECT * FROM document_batches WHERE batch_id = ?",
                    (batch_id,),
                ).fetchone()
                if batch is None:
                    raise KeyError(batch_id)
                if str(batch["status"]) != "uploading":
                    raise ValueError("该批次已结束上传，不能重复入队。")
                items = conn.execute(
                    """
                    SELECT doc_id FROM document_batch_items
                    WHERE batch_id = ?
                    ORDER BY upload_index ASC
                    """,
                    (batch_id,),
                ).fetchall()
                item_doc_ids = [str(item["doc_id"]) for item in items]
                if len(item_doc_ids) != int(batch["expected_files"]):
                    raise ValueError(
                        f"批次计划上传 {int(batch['expected_files'])} 个文件，"
                        f"当前仅收到 {len(item_doc_ids)} 个。"
                    )
                if set(item_doc_ids) != set(job_ids):
                    raise ValueError("批次文件与待入队任务不一致。")

            placeholders = ", ".join("?" for _ in job_ids)
            running = conn.execute(
                f"""
                SELECT job_id FROM jobs
                WHERE status = 'running' AND job_id IN ({placeholders})
                LIMIT 1
                """,
                job_ids,
            ).fetchone()
            if running is not None:
                raise ValueError("该文件正在处理中，不能重复入队。")

            assignments = ", ".join(
                f"{column} = :{column}"
                for column in _JOB_COLUMNS
                if column != "job_id"
            )
            for job in queued_jobs:
                exists = conn.execute(
                    "SELECT 1 FROM jobs WHERE job_id = ?",
                    (job.job_id,),
                ).fetchone()
                if exists is None:
                    _insert_job(conn, job)
                else:
                    conn.execute(
                        f"UPDATE jobs SET {assignments} WHERE job_id = :job_id",
                        job.as_dict(),
                    )

            if batch_id:
                for job in queued_jobs:
                    updated = conn.execute(
                        """
                        UPDATE document_batch_items
                        SET job_id = ?
                        WHERE batch_id = ? AND doc_id = ?
                        """,
                        (job.job_id, batch_id, job.job_id),
                    ).rowcount
                    if updated != 1:
                        raise KeyError((batch_id, job.job_id))
                now = utc_now_iso()
                conn.execute(
                    """
                    UPDATE document_batches
                    SET status = 'queued',
                        queued_at = ?,
                        finished_at = '',
                        error_message = '',
                        updated_at = ?
                    WHERE batch_id = ?
                    """,
                    (now, now, batch_id),
                )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    return [
        persisted
        for job_id in job_ids
        if (persisted := get_job(settings, job_id)) is not None
    ]


def get_job(settings: WebAppSettings, job_id: str) -> JobRecord | None:
    with _connect(settings) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    return JobRecord.from_row(row) if row else None


def list_jobs(settings: WebAppSettings, limit: int = 100) -> list[JobRecord]:
    with _connect(settings) as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [JobRecord.from_row(row) for row in rows]


def update_job(settings: WebAppSettings, job_id: str, **fields: object) -> JobRecord:
    if not fields:
        job = get_job(settings, job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    payload = dict(fields)
    payload["updated_at"] = utc_now_iso()
    assignments = ", ".join(f"{key} = :{key}" for key in payload)
    payload["job_id"] = job_id
    with _connect(settings) as conn:
        conn.execute(f"UPDATE jobs SET {assignments} WHERE job_id = :job_id", payload)
    job = get_job(settings, job_id)
    if job is None:
        raise KeyError(job_id)
    return job


def claim_next_queued_job(settings: WebAppSettings) -> JobRecord | None:
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        running = conn.execute(
            "SELECT job_id FROM jobs WHERE status = 'running' LIMIT 1"
        ).fetchone()
        if running is not None:
            conn.execute("COMMIT")
            return None
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC, rowid ASC LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        now = utc_now_iso()
        updated = conn.execute(
            """
            UPDATE jobs
            SET status = ?, current_stage = ?, progress_summary = ?, started_at = COALESCE(NULLIF(started_at, ''), ?), updated_at = ?
            WHERE job_id = ? AND status = 'queued'
            """,
            ("running", "worker_claimed", "Worker 已领取任务。", now, now, row["job_id"]),
        ).rowcount
        conn.execute("COMMIT")
        if not updated:
            return None
    return get_job(settings, str(row["job_id"]))


def recover_abandoned_running_jobs(
    settings: WebAppSettings,
    *,
    now: datetime | None = None,
) -> list[JobRecord]:
    current = now or datetime.now(timezone.utc)
    recovered_ids: list[str] = []
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'running' ORDER BY updated_at ASC"
        ).fetchall()
        for row in rows:
            try:
                updated_at = datetime.fromisoformat(
                    str(row["updated_at"]).replace("Z", "+00:00")
                )
            except ValueError:
                updated_at = current - timedelta(days=1)
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            stale_after = timedelta(
                seconds=max(
                    int(row["timeout_seconds"]),
                    1,
                )
                + max(settings.worker_stale_job_grace_seconds, 0)
            )
            if current - updated_at <= stale_after:
                continue
            message = "Worker 在任务时限内未完成，系统已将遗留运行任务标记为失败，可重新入队。"
            conn.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    progress_summary = ?,
                    finished_at = ?,
                    error_message = ?,
                    raw_error_message = ?,
                    user_friendly_error = ?,
                    recommended_action = ?,
                    exit_code = -1,
                    updated_at = ?
                WHERE job_id = ? AND status = 'running'
                """,
                (
                    message,
                    current.isoformat(),
                    message,
                    message,
                    message,
                    "确认服务器 Worker 正常后重新入队。",
                    current.isoformat(),
                    row["job_id"],
                ),
            )
            recovered_ids.append(str(row["job_id"]))
        conn.execute("COMMIT")
    return [
        job
        for job_id in recovered_ids
        if (job := get_job(settings, job_id)) is not None
    ]


def cancel_job(settings: WebAppSettings, job_id: str) -> JobRecord | None:
    job = get_job(settings, job_id)
    if job is None:
        return None
    if job.status not in {"created", "queued"}:
        return job
    return update_job(
        settings,
        job_id,
        status="cancelled",
        current_stage="cancelled",
        finished_at=utc_now_iso(),
        progress_summary="任务已取消。",
    )


def requeue_job(settings: WebAppSettings, job_id: str) -> JobRecord | None:
    job = get_job(settings, job_id)
    if job is None:
        return None
    if job.status not in {"created", "failed", "cancelled"}:
        return job
    return update_job(
        settings,
        job_id,
        status="queued",
        current_stage="queued",
        finished_at="",
        error_message="",
        raw_error_message="",
        user_friendly_error="",
        recommended_action="",
        command_executed="",
        exit_code=None,
        progress_summary="任务已重新入队，等待 worker。",
    )


def iter_jobs(settings: WebAppSettings) -> Iterable[JobRecord]:
    return list_jobs(settings, limit=1000)


def create_document_batch(
    settings: WebAppSettings,
    batch: DocumentBatchRecord,
) -> DocumentBatchRecord:
    with _connect(settings) as conn:
        conn.execute(
            """
            INSERT INTO document_batches (
                batch_id, status, expected_files, created_at, updated_at,
                queued_at, finished_at, error_message
            ) VALUES (
                :batch_id, :status, :expected_files, :created_at, :updated_at,
                :queued_at, :finished_at, :error_message
            )
            """,
            batch.as_dict(),
        )
    return batch


def get_document_batch(
    settings: WebAppSettings,
    batch_id: str,
) -> DocumentBatchRecord | None:
    with _connect(settings) as conn:
        row = conn.execute(
            "SELECT * FROM document_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
    return DocumentBatchRecord.from_row(row) if row else None


def list_document_batches(
    settings: WebAppSettings,
    *,
    limit: int = 50,
) -> list[DocumentBatchRecord]:
    with _connect(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM document_batches ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [DocumentBatchRecord.from_row(row) for row in rows]


def update_document_batch(
    settings: WebAppSettings,
    batch_id: str,
    **fields: object,
) -> DocumentBatchRecord:
    allowed = {
        "status",
        "expected_files",
        "queued_at",
        "finished_at",
        "error_message",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"Unsupported document batch fields: {sorted(unknown)}")
    if fields:
        payload = dict(fields)
        payload["updated_at"] = utc_now_iso()
        payload["batch_id"] = batch_id
        assignments = ", ".join(f"{key} = :{key}" for key in payload if key != "batch_id")
        with _connect(settings) as conn:
            conn.execute(
                f"UPDATE document_batches SET {assignments} WHERE batch_id = :batch_id",
                payload,
            )
    batch = get_document_batch(settings, batch_id)
    if batch is None:
        raise KeyError(batch_id)
    return batch


def get_document_batch_item_by_index(
    settings: WebAppSettings,
    batch_id: str,
    upload_index: int,
) -> DocumentBatchItemRecord | None:
    with _connect(settings) as conn:
        row = conn.execute(
            """
            SELECT * FROM document_batch_items
            WHERE batch_id = ? AND upload_index = ?
            """,
            (batch_id, upload_index),
        ).fetchone()
    return DocumentBatchItemRecord.from_row(row) if row else None


def list_document_batch_items(
    settings: WebAppSettings,
    batch_id: str,
) -> list[DocumentBatchItemRecord]:
    with _connect(settings) as conn:
        rows = conn.execute(
            """
            SELECT * FROM document_batch_items
            WHERE batch_id = ?
            ORDER BY upload_index ASC
            """,
            (batch_id,),
        ).fetchall()
    return [DocumentBatchItemRecord.from_row(row) for row in rows]


def add_document_batch_item(
    settings: WebAppSettings,
    item: DocumentBatchItemRecord,
) -> DocumentBatchItemRecord:
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        batch = conn.execute(
            "SELECT * FROM document_batches WHERE batch_id = ?",
            (item.batch_id,),
        ).fetchone()
        if batch is None:
            conn.execute("ROLLBACK")
            raise KeyError(item.batch_id)
        if str(batch["status"]) != "uploading":
            conn.execute("ROLLBACK")
            raise ValueError("该批次已结束上传，不能再添加文件。")
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM document_batch_items WHERE batch_id = ?",
                (item.batch_id,),
            ).fetchone()[0]
        )
        if count >= int(batch["expected_files"]):
            conn.execute("ROLLBACK")
            raise ValueError("该批次文件数量已达到预期值。")
        conn.execute(
            """
            INSERT INTO document_batch_items (
                batch_id, doc_id, upload_index, original_filename, job_id, created_at
            ) VALUES (
                :batch_id, :doc_id, :upload_index, :original_filename, :job_id, :created_at
            )
            """,
            item.as_dict(),
        )
        conn.execute(
            "UPDATE document_batches SET updated_at = ? WHERE batch_id = ?",
            (utc_now_iso(), item.batch_id),
        )
        conn.execute("COMMIT")
    return item


def upsert_review_action(settings: WebAppSettings, action: ReviewActionRecord) -> ReviewActionRecord:
    with _connect(settings) as conn:
        conn.execute(
            """
            INSERT INTO review_actions (
                job_id, review_item_id, action_type, action_value, reviewer_note, reviewer_name,
                review_status, source_type, source_ref, created_at, updated_at
            ) VALUES (
                :job_id, :review_item_id, :action_type, :action_value, :reviewer_note, :reviewer_name,
                :review_status, :source_type, :source_ref, :created_at, :updated_at
            )
            ON CONFLICT(job_id, review_item_id) DO UPDATE SET
                action_type = excluded.action_type,
                action_value = excluded.action_value,
                reviewer_note = excluded.reviewer_note,
                reviewer_name = excluded.reviewer_name,
                review_status = excluded.review_status,
                source_type = excluded.source_type,
                source_ref = excluded.source_ref,
                updated_at = excluded.updated_at
            """,
            action.as_dict(),
        )
    stored = get_review_action(settings, action.job_id, action.review_item_id)
    if stored is None:
        raise KeyError(f"Failed to save review action for {action.job_id}:{action.review_item_id}")
    return stored


def get_review_action(settings: WebAppSettings, job_id: str, review_item_id: str) -> ReviewActionRecord | None:
    with _connect(settings) as conn:
        row = conn.execute(
            "SELECT * FROM review_actions WHERE job_id = ? AND review_item_id = ?",
            (job_id, review_item_id),
        ).fetchone()
    return ReviewActionRecord.from_row(row) if row else None


def list_review_actions(settings: WebAppSettings, job_id: str) -> list[ReviewActionRecord]:
    with _connect(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM review_actions WHERE job_id = ? ORDER BY created_at ASC, review_item_id ASC",
            (job_id,),
        ).fetchall()
    return [ReviewActionRecord.from_row(row) for row in rows]


def create_review_operation(settings: WebAppSettings, operation: ReviewOperationRecord) -> ReviewOperationRecord:
    with _connect(settings) as conn:
        conn.execute(
            """
            INSERT INTO review_operations (
                operation_id, job_id, operation_type, queue_backend, status, created_at, updated_at,
                started_at, finished_at, duration_seconds, progress_stage, progress_message_zh,
                error_message, user_friendly_error_zh, log_paths_json, result_paths_json,
                operation_dir, summary_path, timeline_path, retry_of_operation_id, retry_count,
                cancel_requested, cancel_acknowledged, queue_job_id, extra_json
            ) VALUES (
                :operation_id, :job_id, :operation_type, :queue_backend, :status, :created_at, :updated_at,
                :started_at, :finished_at, :duration_seconds, :progress_stage, :progress_message_zh,
                :error_message, :user_friendly_error_zh, :log_paths_json, :result_paths_json,
                :operation_dir, :summary_path, :timeline_path, :retry_of_operation_id, :retry_count,
                :cancel_requested, :cancel_acknowledged, :queue_job_id, :extra_json
            )
            """,
            operation.as_db_dict(),
        )
    return operation


def get_review_operation(settings: WebAppSettings, operation_id: str) -> ReviewOperationRecord | None:
    with _connect(settings) as conn:
        row = conn.execute("SELECT * FROM review_operations WHERE operation_id = ?", (operation_id,)).fetchone()
    return ReviewOperationRecord.from_row(row) if row else None


def list_review_operations(settings: WebAppSettings, job_id: str, limit: int = 50) -> list[ReviewOperationRecord]:
    with _connect(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM review_operations WHERE job_id = ? ORDER BY created_at DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
    return [ReviewOperationRecord.from_row(row) for row in rows]


def update_review_operation(settings: WebAppSettings, operation_id: str, **fields: object) -> ReviewOperationRecord:
    if not fields:
        operation = get_review_operation(settings, operation_id)
        if operation is None:
            raise KeyError(operation_id)
        return operation

    payload = dict(fields)
    if "log_paths" in payload:
        operation = get_review_operation(settings, operation_id)
        if operation is None:
            raise KeyError(operation_id)
        updated_operation = ReviewOperationRecord(
            **{
                **operation.as_dict(),
                **payload,
            }
        )
        payload = updated_operation.as_db_dict()
    else:
        if "result_paths" in payload or "extra" in payload or "cancel_requested" in payload or "cancel_acknowledged" in payload:
            operation = get_review_operation(settings, operation_id)
            if operation is None:
                raise KeyError(operation_id)
            updated_operation = ReviewOperationRecord(
                **{
                    **operation.as_dict(),
                    **payload,
                }
            )
            payload = updated_operation.as_db_dict()

    payload["updated_at"] = utc_now_iso()
    assignments = ", ".join(f"{key} = :{key}" for key in payload)
    payload["operation_id"] = operation_id
    with _connect(settings) as conn:
        conn.execute(f"UPDATE review_operations SET {assignments} WHERE operation_id = :operation_id", payload)
    operation = get_review_operation(settings, operation_id)
    if operation is None:
        raise KeyError(operation_id)
    return operation


def create_review_operation_if_unlocked(
    settings: WebAppSettings,
    operation: ReviewOperationRecord,
    *,
    blocking_operation_types: Iterable[str],
) -> tuple[ReviewOperationRecord | None, ReviewOperationRecord | None]:
    blocking_types = list(dict.fromkeys(str(value).strip() for value in blocking_operation_types if str(value).strip()))
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        blocked_row = None
        if blocking_types:
            placeholders = ", ".join("?" for _ in blocking_types)
            query = (
                "SELECT * FROM review_operations "
                f"WHERE job_id = ? AND status IN ({', '.join('?' for _ in ACTIVE_OPERATION_STATUSES)}) "
                f"AND operation_type IN ({placeholders}) "
                "ORDER BY created_at DESC LIMIT 1"
            )
            params = [operation.job_id, *ACTIVE_OPERATION_STATUSES, *blocking_types]
            blocked_row = conn.execute(query, params).fetchone()
        if blocked_row is not None:
            conn.execute("COMMIT")
            return None, ReviewOperationRecord.from_row(blocked_row)
        conn.execute(
            """
            INSERT INTO review_operations (
                operation_id, job_id, operation_type, queue_backend, status, created_at, updated_at,
                started_at, finished_at, duration_seconds, progress_stage, progress_message_zh,
                error_message, user_friendly_error_zh, log_paths_json, result_paths_json,
                operation_dir, summary_path, timeline_path, retry_of_operation_id, retry_count,
                cancel_requested, cancel_acknowledged, queue_job_id, extra_json
            ) VALUES (
                :operation_id, :job_id, :operation_type, :queue_backend, :status, :created_at, :updated_at,
                :started_at, :finished_at, :duration_seconds, :progress_stage, :progress_message_zh,
                :error_message, :user_friendly_error_zh, :log_paths_json, :result_paths_json,
                :operation_dir, :summary_path, :timeline_path, :retry_of_operation_id, :retry_count,
                :cancel_requested, :cancel_acknowledged, :queue_job_id, :extra_json
            )
            """,
            operation.as_db_dict(),
        )
        conn.execute("COMMIT")
    return operation, None


def claim_next_queued_review_operation(settings: WebAppSettings) -> ReviewOperationRecord | None:
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM review_operations WHERE status = ? AND cancel_requested = 0 ORDER BY created_at ASC LIMIT 1",
            (OPERATION_STATUS_QUEUED,),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        now = utc_now_iso()
        updated = conn.execute(
            """
            UPDATE review_operations
            SET status = ?, started_at = COALESCE(NULLIF(started_at, ''), ?), updated_at = ?
            WHERE operation_id = ? AND status = ?
            """,
            (OPERATION_STATUS_RUNNING, now, now, row["operation_id"], OPERATION_STATUS_QUEUED),
        ).rowcount
        conn.execute("COMMIT")
        if not updated:
            return None
    return get_review_operation(settings, str(row["operation_id"]))


def get_active_review_operation(settings: WebAppSettings, job_id: str) -> ReviewOperationRecord | None:
    with _connect(settings) as conn:
        row = conn.execute(
            f"SELECT * FROM review_operations WHERE job_id = ? AND status IN ({', '.join('?' for _ in ACTIVE_OPERATION_STATUSES)}) "
            "ORDER BY created_at DESC LIMIT 1",
            (job_id, *ACTIVE_OPERATION_STATUSES),
        ).fetchone()
    return ReviewOperationRecord.from_row(row) if row else None


def request_cancel_review_operation(settings: WebAppSettings, operation_id: str) -> ReviewOperationRecord | None:
    operation = get_review_operation(settings, operation_id)
    if operation is None:
        return None
    now = utc_now_iso()
    with _connect(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM review_operations WHERE operation_id = ?", (operation_id,)).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        status = str(row["status"])
        if status in {OPERATION_STATUS_CANCELLED, "failed", "succeeded"}:
            conn.execute("COMMIT")
            return ReviewOperationRecord.from_row(row)
        if status in {"created", "queued"}:
            conn.execute(
                """
                UPDATE review_operations
                SET status = ?, cancel_requested = 1, cancel_acknowledged = 1, finished_at = ?, updated_at = ?
                WHERE operation_id = ?
                """,
                (OPERATION_STATUS_CANCELLED, now, now, operation_id),
            )
        else:
            conn.execute(
                """
                UPDATE review_operations
                SET cancel_requested = 1, updated_at = ?
                WHERE operation_id = ?
                """,
                (now, operation_id),
            )
        conn.execute("COMMIT")
    return get_review_operation(settings, operation_id)


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    if any(str(row["name"]) == column_name for row in rows):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
