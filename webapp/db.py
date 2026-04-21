from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .config import WebAppSettings
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
    timeout_seconds INTEGER NOT NULL
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
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect(settings: WebAppSettings) -> sqlite3.Connection:
    settings.ensure_directories()
    conn = sqlite3.connect(str(settings.db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(settings: WebAppSettings) -> None:
    with _connect(settings) as conn:
        conn.executescript(SCHEMA_SQL)
        _ensure_column(conn, "jobs", "raw_error_message", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "jobs", "user_friendly_error", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "jobs", "recommended_action", "TEXT NOT NULL DEFAULT ''")
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
        conn.execute(
            """
            INSERT INTO jobs (
                job_id, display_name, mode, provider_mode, input_path, source_image_dir,
                upload_dir, ocr_output_dir, template_path, output_dir, result_dir, log_dir,
                provider_priority, status, current_stage, progress_summary, created_at, updated_at,
                started_at, finished_at, error_message, raw_error_message, user_friendly_error,
                recommended_action, run_id, command_executed, exit_code, timeout_seconds
            ) VALUES (
                :job_id, :display_name, :mode, :provider_mode, :input_path, :source_image_dir,
                :upload_dir, :ocr_output_dir, :template_path, :output_dir, :result_dir, :log_dir,
                :provider_priority, :status, :current_stage, :progress_summary, :created_at, :updated_at,
                :started_at, :finished_at, :error_message, :raw_error_message, :user_friendly_error,
                :recommended_action, :run_id, :command_executed, :exit_code, :timeout_seconds
            )
            """,
            job.as_dict(),
        )
    return job


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
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
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
