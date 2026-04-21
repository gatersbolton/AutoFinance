from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from project_paths import REPO_ROOT

from .config import WebAppSettings, load_settings
from .db import (
    claim_next_queued_review_operation,
    create_review_operation_if_unlocked,
    get_review_operation,
    list_review_operations as db_list_review_operations,
    request_cancel_review_operation,
    update_review_operation,
    utc_now_iso,
)
from .jobs import _repo_relative_or_absolute, require_job
from .labels import operation_status_label_zh, operation_type_label_zh
from .models import (
    ACTIVE_OPERATION_STATUSES,
    OPERATION_STATUS_CANCELLED,
    OPERATION_STATUS_CREATED,
    OPERATION_STATUS_FAILED,
    OPERATION_STATUS_QUEUED,
    OPERATION_STATUS_RUNNING,
    OPERATION_STATUS_SUCCEEDED,
    OPERATION_TYPE_APPLY_AND_RERUN,
    OPERATION_TYPE_APPLY_REVIEW_ACTIONS,
    OPERATION_TYPE_RERUN_ONLY,
    REVIEW_OPERATION_TYPES,
    ReviewOperationRecord,
)
from .review import (
    ReviewOperationCancelled,
    _duration_seconds,
    _write_json_file,
    apply_and_rerun_review_actions_from_web,
    apply_review_actions_from_web,
    get_review_dir,
    get_review_operations_dir,
    rerun_only_from_web,
)


OPERATION_BLOCKING_TYPES = {
    OPERATION_TYPE_APPLY_REVIEW_ACTIONS,
    OPERATION_TYPE_APPLY_AND_RERUN,
    OPERATION_TYPE_RERUN_ONLY,
}
OPERATION_TERMINAL_STATUSES = {
    OPERATION_STATUS_SUCCEEDED,
    OPERATION_STATUS_FAILED,
    OPERATION_STATUS_CANCELLED,
}


class DuplicateOperationError(RuntimeError):
    def __init__(self, existing_operation: ReviewOperationRecord):
        super().__init__(f"operation_locked:{existing_operation.operation_id}")
        self.existing_operation = existing_operation


def _operation_allowed_roots(job) -> tuple[Path, ...]:
    return (
        get_review_dir(job),
        get_review_operations_dir(job),
        Path(job.output_dir).resolve().parent / "reruns",
        Path(job.result_dir).resolve(),
        Path(job.log_dir).resolve(),
    )


def _resolve_allowed_operation_file(job, raw_path: str) -> Path | None:
    candidate = Path(str(raw_path or "").strip())
    if not candidate:
        return None
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()
    for root in _operation_allowed_roots(job):
        try:
            candidate.relative_to(root.resolve())
        except ValueError:
            continue
        return candidate if candidate.exists() and candidate.is_file() else None
    return None


def _tail_text(path: Path, limit_chars: int) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit_chars:
        return text
    return text[-limit_chars:]


def _elapsed_seconds(operation: ReviewOperationRecord) -> float:
    if operation.started_at and operation.finished_at:
        return max(float(operation.duration_seconds or 0.0), 0.0)
    if not operation.started_at:
        return 0.0
    try:
        started = datetime.fromisoformat(operation.started_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    current = datetime.now(timezone.utc)
    return max(round((current - started).total_seconds(), 3), 0.0)


def _operation_summary_payload(
    settings: WebAppSettings,
    job,
    operation: ReviewOperationRecord,
    *,
    include_log_tail: bool,
) -> dict[str, Any]:
    payload = operation.as_dict()
    payload["status_label"] = operation_status_label_zh(operation.status)
    payload["status_label_zh"] = operation_status_label_zh(operation.status)
    payload["operation_type_label_zh"] = operation_type_label_zh(operation.operation_type)
    payload["elapsed_seconds"] = _elapsed_seconds(operation)
    payload["can_cancel"] = operation.status in ACTIVE_OPERATION_STATUSES and not operation.cancel_requested
    payload["can_retry"] = operation.status in {OPERATION_STATUS_FAILED, OPERATION_STATUS_CANCELLED}
    payload["cancel_supported"] = True
    payload["retry_supported"] = True
    payload["queue_backend"] = operation.queue_backend
    payload["summary_path"] = operation.summary_path
    payload["timeline_path"] = operation.timeline_path
    if include_log_tail:
        payload["log_tails"] = get_operation_log_tails(settings, job, operation)
    return payload


def _operation_timeline_path(job, operation_id: str) -> Path:
    return get_review_operations_dir(job) / operation_id / "operation_stage_timeline.json"


def _write_operation_timeline(
    job,
    operation: ReviewOperationRecord,
    *,
    stage: str,
    message_zh: str,
    timestamp: str | None = None,
    status: str | None = None,
) -> None:
    timeline_path = _operation_timeline_path(job, operation.operation_id)
    payload = {}
    if timeline_path.exists():
        try:
            payload = json.loads(timeline_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
    payload.setdefault("job_id", job.job_id)
    payload.setdefault("operation_id", operation.operation_id)
    payload.setdefault("operation_type", operation.operation_type)
    payload.setdefault("events", [])
    payload["events"].append(
        {
            "timestamp": timestamp or utc_now_iso(),
            "stage": stage,
            "status": status or operation.status,
            "status_label_zh": operation_status_label_zh(status or operation.status),
            "message_zh": message_zh,
        }
    )
    payload["pass"] = operation.status == OPERATION_STATUS_SUCCEEDED
    _write_json_file(timeline_path, payload)
    _write_json_file(get_review_dir(job) / "operation_stage_timeline.json", payload)


def _sync_operation_files(settings: WebAppSettings, job, operation: ReviewOperationRecord) -> ReviewOperationRecord:
    payload = _operation_summary_payload(settings, job, operation, include_log_tail=False)
    summary_path = Path(operation.summary_path)
    if not summary_path.is_absolute():
        summary_path = (REPO_ROOT / summary_path).resolve()
    _write_json_file(summary_path, payload)
    _write_json_file(get_review_dir(job) / "review_operation_summary.json", payload)
    return operation


def _write_operation_lock_summary(
    job,
    *,
    requested_operation_type: str,
    blocked: bool,
    blocked_by_operation: ReviewOperationRecord | None,
    created_operation: ReviewOperationRecord | None,
) -> dict[str, Any]:
    summary = {
        "job_id": job.job_id,
        "written_at": utc_now_iso(),
        "requested_operation_type": requested_operation_type,
        "requested_operation_type_label_zh": operation_type_label_zh(requested_operation_type),
        "policy": "reject",
        "blocked": blocked,
        "blocked_by_operation_id": blocked_by_operation.operation_id if blocked_by_operation else "",
        "blocked_by_operation_type": blocked_by_operation.operation_type if blocked_by_operation else "",
        "blocked_by_operation_type_label_zh": operation_type_label_zh(blocked_by_operation.operation_type) if blocked_by_operation else "",
        "blocked_by_status": blocked_by_operation.status if blocked_by_operation else "",
        "blocked_by_status_label_zh": operation_status_label_zh(blocked_by_operation.status) if blocked_by_operation else "",
        "created_operation_id": created_operation.operation_id if created_operation else "",
        "pass": not blocked,
    }
    _write_json_file(get_review_dir(job) / "operation_lock_summary.json", summary)
    return summary


def _write_operation_retry_summary(job, *, source_operation: ReviewOperationRecord, new_operation: ReviewOperationRecord) -> dict[str, Any]:
    summary = {
        "job_id": job.job_id,
        "retried_at": utc_now_iso(),
        "source_operation_id": source_operation.operation_id,
        "source_operation_type": source_operation.operation_type,
        "source_operation_type_label_zh": operation_type_label_zh(source_operation.operation_type),
        "source_status": source_operation.status,
        "source_status_label_zh": operation_status_label_zh(source_operation.status),
        "new_operation_id": new_operation.operation_id,
        "new_operation_type": new_operation.operation_type,
        "new_operation_type_label_zh": operation_type_label_zh(new_operation.operation_type),
        "pass": True,
    }
    operation_dir = get_review_operations_dir(job) / new_operation.operation_id
    _write_json_file(get_review_dir(job) / "operation_retry_summary.json", summary)
    _write_json_file(operation_dir / "operation_retry_summary.json", summary)
    return summary


def _make_operation_record(settings: WebAppSettings, job, operation_type: str, *, retry_of_operation_id: str = "") -> ReviewOperationRecord:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    operation_id = f"operation_{timestamp}"
    operation_dir = get_review_operations_dir(job) / operation_id
    summary_path = operation_dir / "review_operation_summary.json"
    timeline_path = operation_dir / "operation_stage_timeline.json"
    now = utc_now_iso()
    return ReviewOperationRecord(
        operation_id=operation_id,
        job_id=job.job_id,
        operation_type=operation_type,
        queue_backend=settings.queue_backend,
        status=OPERATION_STATUS_CREATED,
        created_at=now,
        updated_at=now,
        started_at="",
        finished_at="",
        duration_seconds=0.0,
        progress_stage="created",
        progress_message_zh="操作已创建，等待进入执行队列。",
        error_message="",
        user_friendly_error_zh="",
        log_paths=[_repo_relative_or_absolute(operation_dir / "operation.log")],
        result_paths=[],
        operation_dir=_repo_relative_or_absolute(operation_dir),
        summary_path=_repo_relative_or_absolute(summary_path),
        timeline_path=_repo_relative_or_absolute(timeline_path),
        retry_of_operation_id=retry_of_operation_id,
        retry_count=1 if retry_of_operation_id else 0,
        cancel_requested=False,
        cancel_acknowledged=False,
        queue_job_id="",
        extra={},
    )


def _queue_operation_local(settings: WebAppSettings, operation: ReviewOperationRecord) -> ReviewOperationRecord:
    return update_review_operation(
        settings,
        operation.operation_id,
        status=OPERATION_STATUS_QUEUED,
        progress_stage="queued",
        progress_message_zh="操作已入队，等待后台 worker 执行。",
    )


def _queue_operation_rq(settings: WebAppSettings, operation: ReviewOperationRecord) -> ReviewOperationRecord:
    try:
        from redis import Redis
        from rq import Queue
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("当前环境未安装 rq/redis，无法使用 RQ 队列后端。") from exc

    connection = Redis.from_url(settings.redis_url)
    queue = Queue("review_operations", connection=connection)
    rq_job = queue.enqueue(process_operation_from_queue, operation.operation_id, job_timeout=settings.operation_timeout_seconds)
    return update_review_operation(
        settings,
        operation.operation_id,
        status=OPERATION_STATUS_QUEUED,
        progress_stage="queued",
        progress_message_zh="操作已提交到 RQ 队列，等待 worker 执行。",
        queue_job_id=str(rq_job.id),
    )


def enqueue_review_operation(
    settings: WebAppSettings,
    job,
    operation_type: str,
    *,
    retry_of_operation_id: str = "",
) -> ReviewOperationRecord:
    if operation_type not in REVIEW_OPERATION_TYPES:
        raise ValueError(f"不支持的复核操作类型: {operation_type}")

    draft = _make_operation_record(settings, job, operation_type, retry_of_operation_id=retry_of_operation_id)
    created_operation, blocked_operation = create_review_operation_if_unlocked(
        settings,
        draft,
        blocking_operation_types=OPERATION_BLOCKING_TYPES,
    )
    if blocked_operation is not None:
        _write_operation_lock_summary(
            job,
            requested_operation_type=operation_type,
            blocked=True,
            blocked_by_operation=blocked_operation,
            created_operation=None,
        )
        raise DuplicateOperationError(blocked_operation)

    assert created_operation is not None
    operation_dir = get_review_operations_dir(job) / created_operation.operation_id
    operation_dir.mkdir(parents=True, exist_ok=True)
    _sync_operation_files(settings, job, created_operation)
    _write_operation_timeline(job, created_operation, stage="created", message_zh="操作已创建。")

    if settings.queue_backend == "rq":
        created_operation = _queue_operation_rq(settings, created_operation)
    else:
        created_operation = _queue_operation_local(settings, created_operation)
    _sync_operation_files(settings, job, created_operation)
    _write_operation_timeline(job, created_operation, stage="queued", message_zh=created_operation.progress_message_zh)
    _write_operation_lock_summary(
        job,
        requested_operation_type=operation_type,
        blocked=False,
        blocked_by_operation=None,
        created_operation=created_operation,
    )
    if retry_of_operation_id:
        source_operation = get_review_operation(settings, retry_of_operation_id)
        if source_operation is not None:
            _write_operation_retry_summary(job, source_operation=source_operation, new_operation=created_operation)
    return created_operation


def retry_review_operation(settings: WebAppSettings, job, operation_id: str) -> ReviewOperationRecord:
    source_operation = get_review_operation(settings, operation_id)
    if source_operation is None or source_operation.job_id != job.job_id:
        raise KeyError(operation_id)
    if source_operation.status not in {OPERATION_STATUS_FAILED, OPERATION_STATUS_CANCELLED}:
        raise ValueError("只有失败或已取消的操作才允许重试。")
    return enqueue_review_operation(settings, job, source_operation.operation_type, retry_of_operation_id=source_operation.operation_id)


def cancel_review_operation(settings: WebAppSettings, job, operation_id: str) -> ReviewOperationRecord:
    operation = request_cancel_review_operation(settings, operation_id)
    if operation is None or operation.job_id != job.job_id:
        raise KeyError(operation_id)
    if operation.status == OPERATION_STATUS_CANCELLED:
        operation = update_review_operation(
            settings,
            operation.operation_id,
            progress_stage="cancelled",
            progress_message_zh="操作已取消。",
            finished_at=operation.finished_at or utc_now_iso(),
            duration_seconds=_duration_seconds(operation.started_at, operation.finished_at or utc_now_iso())
            if operation.started_at
            else 0.0,
        )
    else:
        operation = update_review_operation(
            settings,
            operation.operation_id,
            progress_stage="cancel_requested",
            progress_message_zh="已请求取消；当前阶段会在安全点尽快停止。",
        )
    _sync_operation_files(settings, job, operation)
    _write_operation_timeline(job, operation, stage=operation.progress_stage, message_zh=operation.progress_message_zh)
    return operation


class _OperationRuntime:
    def __init__(self, settings: WebAppSettings, job, operation: ReviewOperationRecord):
        self.settings = settings
        self.job = job
        self.operation = operation
        self.operation_log_path = get_review_operations_dir(job) / operation.operation_id / "operation.log"
        self.operation_log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.operation_log_path.exists():
            self.operation_log_path.write_text("", encoding="utf-8")

    def refresh(self) -> ReviewOperationRecord:
        operation = get_review_operation(self.settings, self.operation.operation_id)
        if operation is None:
            raise KeyError(self.operation.operation_id)
        self.operation = operation
        return operation

    def cancel_requested(self) -> bool:
        return bool(self.refresh().cancel_requested)

    def log(self, message: str) -> None:
        line = f"[{utc_now_iso()}] {message}\n"
        with self.operation_log_path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    def progress(self, stage: str, message_zh: str) -> None:
        self.operation = update_review_operation(
            self.settings,
            self.operation.operation_id,
            progress_stage=stage,
            progress_message_zh=message_zh,
        )
        _sync_operation_files(self.settings, self.job, self.operation)
        _write_operation_timeline(self.job, self.operation, stage=stage, message_zh=message_zh)

    def finish(
        self,
        *,
        status: str,
        result_paths: Iterable[str] = (),
        error_message: str = "",
        user_friendly_error_zh: str = "",
    ) -> ReviewOperationRecord:
        finished_at = utc_now_iso()
        duration_seconds = _duration_seconds(self.operation.started_at or finished_at, finished_at) if self.operation.started_at else 0.0
        self.operation = update_review_operation(
            self.settings,
            self.operation.operation_id,
            status=status,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            result_paths=list(dict.fromkeys(str(path) for path in result_paths if str(path).strip())),
            error_message=error_message,
            user_friendly_error_zh=user_friendly_error_zh,
            cancel_acknowledged=(status == OPERATION_STATUS_CANCELLED),
            progress_stage=status,
            progress_message_zh={
                OPERATION_STATUS_SUCCEEDED: "操作已完成。",
                OPERATION_STATUS_FAILED: user_friendly_error_zh or "操作执行失败。",
                OPERATION_STATUS_CANCELLED: "操作已取消。",
            }.get(status, self.operation.progress_message_zh),
        )
        _sync_operation_files(self.settings, self.job, self.operation)
        _write_operation_timeline(
            self.job,
            self.operation,
            stage=self.operation.progress_stage,
            message_zh=self.operation.progress_message_zh,
            status=status,
        )
        _write_operation_lock_summary(
            self.job,
            requested_operation_type=self.operation.operation_type,
            blocked=False,
            blocked_by_operation=None,
            created_operation=self.operation,
        )
        return self.operation


def _result_paths_from_summary(operation_type: str, summary: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    if operation_type == OPERATION_TYPE_APPLY_REVIEW_ACTIONS:
        for key in (
            "review_apply_summary_path",
            "applied_review_actions_path",
            "rejected_review_actions_path",
            "override_audit_path",
            "review_decision_summary_path",
        ):
            value = str(summary.get(key, "")).strip()
            if value:
                paths.append(value)
    elif operation_type == OPERATION_TYPE_APPLY_AND_RERUN:
        for key in (
            "review_apply_and_rerun_summary_path",
            "review_rerun_summary_path",
            "review_rerun_delta_path",
            "review_rerun_delta_explained_path",
            "review_apply_summary_path",
        ):
            value = str(summary.get(key, "")).strip()
            if value:
                paths.append(value)
    else:
        for key in (
            "review_rerun_only_summary_path",
            "review_rerun_summary_path",
            "review_rerun_delta_path",
            "review_rerun_delta_explained_path",
        ):
            value = str(summary.get(key, "")).strip()
            if value:
                paths.append(value)
    return list(dict.fromkeys(paths))


def execute_review_operation(settings: WebAppSettings, operation_id: str, *, already_claimed: bool = False) -> ReviewOperationRecord:
    operation = get_review_operation(settings, operation_id)
    if operation is None:
        raise KeyError(operation_id)
    job = require_job(settings, operation.job_id)
    runtime = _OperationRuntime(settings, job, operation)

    if not already_claimed:
        if operation.status != OPERATION_STATUS_QUEUED:
            return operation
        operation = update_review_operation(
            settings,
            operation.operation_id,
            status=OPERATION_STATUS_RUNNING,
            started_at=operation.started_at or utc_now_iso(),
        )
        runtime.operation = operation
        _sync_operation_files(settings, job, operation)
        _write_operation_timeline(job, operation, stage="running", message_zh="后台 worker 已开始执行。")
    else:
        runtime.log("worker claimed operation")
        runtime.progress("running", "后台 worker 已开始执行。")

    try:
        if runtime.cancel_requested():
            raise ReviewOperationCancelled("operation_cancelled")

        if operation.operation_type == OPERATION_TYPE_APPLY_REVIEW_ACTIONS:
            summary = apply_review_actions_from_web(
                settings,
                job,
                operation_id=operation.operation_id,
                progress_callback=runtime.progress,
                log_callback=runtime.log,
                cancel_requested=runtime.cancel_requested,
            )
        elif operation.operation_type == OPERATION_TYPE_APPLY_AND_RERUN:
            summary = apply_and_rerun_review_actions_from_web(
                settings,
                job,
                operation_id=operation.operation_id,
                progress_callback=runtime.progress,
                log_callback=runtime.log,
                cancel_requested=runtime.cancel_requested,
            )
        elif operation.operation_type == OPERATION_TYPE_RERUN_ONLY:
            summary = rerun_only_from_web(
                settings,
                job,
                operation_id=operation.operation_id,
                progress_callback=runtime.progress,
                log_callback=runtime.log,
                cancel_requested=runtime.cancel_requested,
            )
        else:
            raise ValueError(f"不支持的复核操作类型: {operation.operation_type}")

        result_paths = _result_paths_from_summary(operation.operation_type, summary)
        if summary.get("pass") is False:
            runtime.finish(
                status=OPERATION_STATUS_FAILED,
                result_paths=result_paths,
                error_message=str(summary.get("rerun_status") or summary.get("status") or "failed"),
                user_friendly_error_zh=str(summary.get("rerun_status_label") or summary.get("status_label") or "后台操作执行失败。"),
            )
        else:
            runtime.finish(status=OPERATION_STATUS_SUCCEEDED, result_paths=result_paths)
    except ReviewOperationCancelled:
        runtime.log("operation cancelled")
        runtime.finish(status=OPERATION_STATUS_CANCELLED)
    except Exception as exc:
        runtime.log(f"operation failed: {exc}")
        runtime.finish(
            status=OPERATION_STATUS_FAILED,
            error_message=str(exc),
            user_friendly_error_zh=str(exc) if isinstance(exc, (ValueError, RuntimeError, subprocess.TimeoutExpired)) else "后台操作执行失败，请查看日志。",
        )
    return runtime.operation


def run_review_operation_once(settings: WebAppSettings) -> ReviewOperationRecord | None:
    operation = claim_next_queued_review_operation(settings)
    if operation is None:
        return None
    return execute_review_operation(settings, operation.operation_id, already_claimed=True)


def process_operation_from_queue(operation_id: str) -> str:
    settings = load_settings()
    settings.ensure_directories()
    execute_review_operation(settings, operation_id, already_claimed=False)
    return operation_id


def get_latest_review_operation_summary(job) -> dict[str, Any]:
    summary_path = get_review_dir(job) / "review_operation_summary.json"
    if summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))
    operations = sorted((path for path in get_review_operations_dir(job).glob("operation_*") if path.is_dir()), key=lambda path: path.name)
    if not operations:
        return {}
    return json.loads((operations[-1] / "review_operation_summary.json").read_text(encoding="utf-8"))


def build_operation_status_payload(settings: WebAppSettings, job, operation: ReviewOperationRecord) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "operation": _operation_summary_payload(settings, job, operation, include_log_tail=True),
    }


def list_review_operations_payload(settings: WebAppSettings, job) -> list[dict[str, Any]]:
    return [
        _operation_summary_payload(settings, job, operation, include_log_tail=False)
        for operation in db_list_review_operations(settings, job.job_id, limit=50)
    ]


def get_operation_log_tails(settings: WebAppSettings, job, operation: ReviewOperationRecord) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_path in operation.log_paths[:5]:
        resolved = _resolve_allowed_operation_file(job, raw_path)
        if resolved is None:
            continue
        rows.append(
            {
                "path": _repo_relative_or_absolute(resolved),
                "tail": _tail_text(resolved, settings.operation_log_tail_chars),
            }
        )
    return rows


def resolve_operation_artifact(job, operation: ReviewOperationRecord, kind: str, index: int) -> Path | None:
    entries = operation.log_paths if kind == "log" else operation.result_paths if kind == "result" else []
    if index < 0 or index >= len(entries):
        return None
    return _resolve_allowed_operation_file(job, entries[index])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AutoFinance review-operation helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_one = subparsers.add_parser("run-once", help="Run one queued review operation.")
    run_one.set_defaults(command="run-once")

    run_op = subparsers.add_parser("run-operation", help="Run a specific review operation.")
    run_op.add_argument("--operation-id", required=True, help="Operation id to execute.")

    run_rq = subparsers.add_parser("run-rq-worker", help="Run an RQ worker for review operations.")
    run_rq.set_defaults(command="run-rq-worker")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings()
    settings.ensure_directories()

    if args.command == "run-once":
        run_review_operation_once(settings)
        return 0
    if args.command == "run-operation":
        execute_review_operation(settings, args.operation_id, already_claimed=False)
        return 0
    if args.command == "run-rq-worker":
        try:
            from redis import Redis
            from rq import Connection, Worker
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("当前环境未安装 rq/redis，无法启动 RQ worker。") from exc

        if not settings.redis_url.strip():
            raise RuntimeError("REDIS_URL 为空，无法启动 RQ worker。")
        connection = Redis.from_url(settings.redis_url)
        with Connection(connection):
            worker = Worker(["review_operations"])
            worker.work()
        return 0
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
