from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Sequence

from project_paths import REPO_ROOT

from .config import WebAppSettings, load_settings
from .db import claim_next_queued_job, get_job, init_db, update_job, utc_now_iso
from .jobs import discover_output_files, ensure_job_workspace, write_output_manifest
from .models import JOB_MODE_UPLOAD, JOB_STATUS_FAILED, SUCCESS_LIKE_JOB_STATUSES, JobRecord
from .operations import run_review_operation_once
from .quality import build_job_quality_summary, load_json


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _tail_text(path: Path, limit_chars: int = 6000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit_chars:
        return text
    return text[-limit_chars:]


def _command_text(command: Sequence[str]) -> str:
    return subprocess.list2cmdline([str(part) for part in command])


def _extract_run_id(output_dir: Path) -> str:
    for filename in ("run_summary.json", "summary.json", "pipeline_completion_summary.json"):
        payload = load_json(output_dir / filename)
        run_id = str(payload.get("run_id", "") or "")
        if run_id:
            return run_id
    return ""


def _write_log_bundle(job: JobRecord) -> dict[str, object]:
    log_dir = Path(job.log_dir)
    bundle_path = Path(job.result_dir) / "job_log_bundle.json"
    payload = {"job_id": job.job_id, "log_files": []}
    for path in sorted(log_dir.glob("*.txt")):
        payload["log_files"].append(
            {
                "name": path.name,
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "tail": _tail_text(path),
            }
        )
    _write_json(bundle_path, payload)
    return payload


def _write_quality_summary(
    job: JobRecord,
    *,
    exit_code: int | None,
    raw_error_message: str = "",
    user_friendly_error: str = "",
    recommended_action: str = "",
) -> dict[str, object]:
    payload = build_job_quality_summary(
        job,
        command_exit_code=exit_code,
        raw_error_message=raw_error_message,
        user_friendly_error=user_friendly_error,
        recommended_action=recommended_action,
    )
    _write_json(Path(job.result_dir) / "job_quality_summary.json", payload)
    return payload


def _write_job_summary(
    job: JobRecord,
    *,
    commands_executed: list[str],
    exit_code: int | None,
    duration_seconds: float,
    error_message: str = "",
    quality_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    output_files = [artifact.as_dict() for artifact in discover_output_files(job)]
    generated_files = [item["relative_path"] for item in output_files if item["exists"]]
    payload = {
        "job_id": job.job_id,
        "display_name": job.display_name,
        "job_status": job.status,
        "mode": job.mode,
        "input_path": job.input_path,
        "ocr_output_dir": job.ocr_output_dir,
        "output_dir": job.output_dir,
        "commands_executed": commands_executed,
        "command_executed": commands_executed[-1] if commands_executed else "",
        "exit_code": exit_code,
        "generated_files": generated_files,
        "run_id_if_available": job.run_id,
        "duration_seconds": round(duration_seconds, 3),
        "pass": job.status in SUCCESS_LIKE_JOB_STATUSES,
        "error_message": error_message,
        "raw_error_message": job.raw_error_message,
        "user_friendly_error": job.user_friendly_error,
        "recommended_action": job.recommended_action,
        "quality_summary": quality_summary or {},
    }
    _write_json(Path(job.result_dir) / "job_summary.json", payload)
    return payload


def _run_subprocess(*, command: Sequence[str], stdout_path: Path, stderr_path: Path, timeout_seconds: int) -> subprocess.CompletedProcess[bytes]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        return subprocess.run(
            list(command),
            cwd=str(REPO_ROOT),
            stdout=stdout_handle,
            stderr=stderr_handle,
            timeout=timeout_seconds,
            check=False,
        )


def _build_standardize_command(job: JobRecord, settings: WebAppSettings, input_dir: Path) -> list[str]:
    command = [
        settings.python_executable,
        "-m",
        "standardize.cli",
        "--input-dir",
        str(input_dir),
        "--template",
        job.template_path,
        "--output-dir",
        job.output_dir,
        "--output-run-subdir",
        "none",
        "--provider-priority",
        job.provider_priority,
    ]
    if job.source_image_dir and Path(job.source_image_dir).exists():
        command.extend(["--source-image-dir", job.source_image_dir])
    command.extend(settings.standardize_flags)
    return command


def _build_ocr_command(job: JobRecord, settings: WebAppSettings) -> list[str]:
    return [
        settings.python_executable,
        "OCR.py",
        "--method",
        settings.upload_ocr_method,
        "--input",
        job.upload_dir,
        "--output",
        job.ocr_output_dir,
    ]


def _stderr_tail(log_dir: str, filename: str) -> str:
    path = Path(log_dir) / filename
    if not path.exists():
        return ""
    return _tail_text(path, limit_chars=4000)


def _mark_failed(
    job_id: str,
    settings: WebAppSettings,
    *,
    raw_error_message: str,
    exit_code: int | None,
    commands: list[str],
    started_at_monotonic: float,
) -> JobRecord:
    provisional_job = update_job(
        settings,
        job_id,
        status=JOB_STATUS_FAILED,
        current_stage="failed",
        finished_at=utc_now_iso(),
        error_message="",
        raw_error_message=raw_error_message,
        user_friendly_error="",
        recommended_action="",
        exit_code=exit_code,
        command_executed="\n".join(commands),
        progress_summary="任务执行失败，正在整理错误摘要。",
    )
    quality_summary = _write_quality_summary(
        provisional_job,
        exit_code=exit_code,
        raw_error_message=raw_error_message,
    )
    job = update_job(
        settings,
        job_id,
        status=str(quality_summary.get("final_job_status", JOB_STATUS_FAILED)),
        error_message=str(quality_summary.get("user_friendly_error", "")),
        raw_error_message=str(quality_summary.get("raw_error_message", raw_error_message)),
        user_friendly_error=str(quality_summary.get("user_friendly_error", "")),
        recommended_action=str(quality_summary.get("recommended_user_action", "")),
        progress_summary=str(quality_summary.get("user_friendly_error", "")),
    )
    _write_log_bundle(job)
    _write_job_summary(
        job,
        commands_executed=commands,
        exit_code=exit_code,
        duration_seconds=time.perf_counter() - started_at_monotonic,
        error_message=job.error_message,
        quality_summary=quality_summary,
    )
    write_output_manifest(job)
    return job


def execute_job(settings: WebAppSettings, job_id: str) -> JobRecord:
    job = get_job(settings, job_id)
    if job is None:
        raise KeyError(job_id)
    ensure_job_workspace(settings, job.job_id)

    started_at_monotonic = time.perf_counter()
    commands: list[str] = []

    try:
        if job.mode == JOB_MODE_UPLOAD:
            if not settings.auto_run_upload_ocr:
                return _mark_failed(
                    job.job_id,
                    settings,
                    raw_error_message="上传任务已保存，但当前未启用自动 OCR。请设置 WEBAPP_AUTO_RUN_UPLOAD_OCR=1 后重新入队。",
                    exit_code=None,
                    commands=commands,
                    started_at_monotonic=started_at_monotonic,
                )
            credentials = settings.detect_ocr_credentials()
            if not bool(credentials.get("active_upload_method_ready", False)):
                return _mark_failed(
                    job.job_id,
                    settings,
                    raw_error_message=f"上传任务缺少 {settings.upload_ocr_method} 所需云 OCR 凭据，未执行 OCR。",
                    exit_code=None,
                    commands=commands,
                    started_at_monotonic=started_at_monotonic,
                )

            ocr_command = _build_ocr_command(job, settings)
            commands.append(_command_text(ocr_command))
            update_job(
                settings,
                job.job_id,
                current_stage="ocr",
                progress_summary="正在执行 OCR。",
                command_executed="\n".join(commands),
            )
            ocr_result = _run_subprocess(
                command=ocr_command,
                stdout_path=Path(job.log_dir) / "ocr_stdout.txt",
                stderr_path=Path(job.log_dir) / "ocr_stderr.txt",
                timeout_seconds=job.timeout_seconds,
            )
            if ocr_result.returncode != 0:
                return _mark_failed(
                    job.job_id,
                    settings,
                    raw_error_message=_stderr_tail(job.log_dir, "ocr_stderr.txt") or "OCR 子进程执行失败。",
                    exit_code=ocr_result.returncode,
                    commands=commands,
                    started_at_monotonic=started_at_monotonic,
                )
            input_dir = Path(job.ocr_output_dir)
        else:
            input_dir = Path(job.input_path)

        standardize_command = _build_standardize_command(job, settings, input_dir)
        commands.append(_command_text(standardize_command))
        update_job(
            settings,
            job.job_id,
            current_stage="standardize",
            progress_summary="正在执行标准化流水线。",
            command_executed="\n".join(commands),
        )
        standardize_result = _run_subprocess(
            command=standardize_command,
            stdout_path=Path(job.log_dir) / "standardize_stdout.txt",
            stderr_path=Path(job.log_dir) / "standardize_stderr.txt",
            timeout_seconds=job.timeout_seconds,
        )
        if standardize_result.returncode != 0:
            return _mark_failed(
                job.job_id,
                settings,
                raw_error_message=_stderr_tail(job.log_dir, "standardize_stderr.txt") or "标准化子进程执行失败。",
                exit_code=standardize_result.returncode,
                commands=commands,
                started_at_monotonic=started_at_monotonic,
            )
    except subprocess.TimeoutExpired:
        return _mark_failed(
            job.job_id,
            settings,
            raw_error_message=f"任务超时，超过 {job.timeout_seconds} 秒。",
            exit_code=-1,
            commands=commands,
            started_at_monotonic=started_at_monotonic,
        )
    except Exception as exc:
        return _mark_failed(
            job.job_id,
            settings,
            raw_error_message=str(exc),
            exit_code=-1,
            commands=commands,
            started_at_monotonic=started_at_monotonic,
        )

    run_id = _extract_run_id(Path(job.output_dir))
    provisional_job = update_job(
        settings,
        job.job_id,
        current_stage="completed",
        finished_at=utc_now_iso(),
        error_message="",
        raw_error_message="",
        user_friendly_error="",
        recommended_action="",
        run_id=run_id,
        exit_code=0,
        command_executed="\n".join(commands),
        progress_summary="任务执行完成，正在生成质量摘要。",
    )
    quality_summary = _write_quality_summary(provisional_job, exit_code=0)
    generated_output_count = sum(1 for artifact in discover_output_files(provisional_job) if artifact.exists)
    if str(quality_summary.get("final_job_status")) == "succeeded":
        progress_summary = f"任务执行完成，发现 {generated_output_count} 个输出文件。"
    else:
        progress_summary = str(quality_summary.get("user_friendly_error", "")) or f"任务执行完成，发现 {generated_output_count} 个输出文件。"
    job = update_job(
        settings,
        job.job_id,
        status=str(quality_summary.get("final_job_status", provisional_job.status)),
        error_message=str(quality_summary.get("user_friendly_error", "")),
        raw_error_message=str(quality_summary.get("raw_error_message", "")),
        user_friendly_error=str(quality_summary.get("user_friendly_error", "")),
        recommended_action=str(quality_summary.get("recommended_user_action", "")),
        progress_summary=progress_summary,
    )
    _write_log_bundle(job)
    _write_job_summary(
        job,
        commands_executed=commands,
        exit_code=0,
        duration_seconds=time.perf_counter() - started_at_monotonic,
        error_message=job.error_message,
        quality_summary=quality_summary,
    )
    write_output_manifest(job)
    return job


def run_worker_once(settings: WebAppSettings) -> JobRecord | None:
    init_db(settings)
    operation = run_review_operation_once(settings)
    if operation is not None:
        return None
    job = claim_next_queued_job(settings)
    if job is None:
        return None
    return execute_job(settings, job.job_id)


def run_worker_forever(settings: WebAppSettings, stop_event: threading.Event | None = None) -> None:
    init_db(settings)
    while stop_event is None or not stop_event.is_set():
        job = run_worker_once(settings)
        if job is None:
            if stop_event is not None:
                stop_event.wait(settings.worker_poll_seconds)
            else:  # pragma: no cover
                time.sleep(settings.worker_poll_seconds)


class LocalWorkerThread:
    def __init__(self, settings: WebAppSettings):
        self.settings = settings
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=run_worker_forever,
            kwargs={"settings": settings, "stop_event": self._stop_event},
            name="autofinance-web-worker",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(self.settings.worker_poll_seconds + 1, 2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AutoFinance web worker utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("run-worker", help="Run the local web worker.")
    worker_parser.add_argument("--once", action="store_true", help="Run at most one queued job and exit.")

    run_job_parser = subparsers.add_parser("run-job", help="Run a specific job immediately.")
    run_job_parser.add_argument("--job-id", required=True, help="Job id to execute.")

    subparsers.add_parser("healthcheck", help="Validate worker runtime configuration.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings()
    settings.ensure_directories()
    init_db(settings)

    if args.command == "run-worker":
        if args.once:
            run_worker_once(settings)
            return 0
        run_worker_forever(settings)
        return 0
    if args.command == "run-job":
        execute_job(settings, args.job_id)
        return 0
    if args.command == "healthcheck":
        settings.validate_runtime_configuration()
        init_db(settings)
        return 0
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
