from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .config import WebAppSettings
from .models import JobRecord


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


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    if any(str(row["name"]) == column_name for row in rows):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
