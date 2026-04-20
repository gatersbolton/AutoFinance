from __future__ import annotations

import json
import platform
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from fastapi import HTTPException, UploadFile

from project_paths import REPO_ROOT

from .config import WebAppSettings
from .db import create_job, get_job, utc_now_iso
from .models import (
    JOB_MODE_EXISTING,
    JOB_MODE_UPLOAD,
    JOB_STATUS_CREATED,
    JOB_STATUS_QUEUED,
    JobRecord,
    OutputArtifact,
    SystemStatusRecord,
)
from .quality import (
    describe_job_status,
    load_json,
    summarize_for_operator,
)


COMMON_OUTPUT_DEFS = (
    ("filled_workbook", "填充结果工作簿", "会计报表_填充结果.xlsx"),
    ("run_summary", "运行摘要", "run_summary.json"),
    ("artifact_integrity", "完整性检查", "artifact_integrity.json"),
    ("review_workbook", "复核工作簿", "review_workbook.xlsx"),
    ("review_queue", "复核队列", "review_queue.csv"),
    ("issues", "问题清单", "issues.csv"),
    ("validation_results", "校验结果", "validation_results.csv"),
    ("conflicts_enriched", "冲突明细", "conflicts_enriched.csv"),
    ("conflict_decision_audit", "冲突决策审计", "conflict_decision_audit.csv"),
    ("unplaced_facts", "未落位事实", "unplaced_facts.csv"),
    ("mapping_candidates", "科目映射候选", "mapping_candidates.csv"),
    ("benchmark_gap_explanations", "基准差异说明", "benchmark_gap_explanations.csv"),
    ("source_backed_gap_closure", "来源支撑缺口闭环", "source_backed_gap_closure.csv"),
)


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _repo_relative_or_absolute(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _sanitize_filename(filename: str) -> str:
    candidate = Path(filename or "").name.strip()
    if not candidate:
        candidate = "upload.pdf"
    candidate = re.sub(r"[^\w.\-()\u4e00-\u9fff]+", "_", candidate)
    candidate = candidate.strip("._") or "upload.pdf"
    return candidate


def _default_display_name(mode: str, source_name: str) -> str:
    base = source_name.strip() if source_name else ""
    if base:
        return base[:120]
    return "标准化任务" if mode == JOB_MODE_EXISTING else "上传 OCR 任务"


def generate_job_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"job_{timestamp}_{uuid.uuid4().hex[:8]}"


def ensure_job_workspace(settings: WebAppSettings, job_id: str) -> dict[str, Path]:
    upload_dir = settings.uploads_root / job_id
    job_root = settings.jobs_root / job_id
    output_dir = job_root / "standardize"
    ocr_output_dir = job_root / "ocr_outputs"
    result_dir = settings.results_root / job_id
    log_dir = settings.logs_root / job_id
    for path in (upload_dir, job_root, output_dir, ocr_output_dir, result_dir, log_dir):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "upload_dir": upload_dir,
        "job_root": job_root,
        "output_dir": output_dir,
        "ocr_output_dir": ocr_output_dir,
        "result_dir": result_dir,
        "log_dir": log_dir,
    }


def resolve_user_path(raw_path: str) -> Path:
    candidate = Path((raw_path or "").strip())
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate.resolve()


def validate_existing_ocr_path(settings: WebAppSettings, raw_path: str) -> Path:
    path = resolve_user_path(raw_path)
    if not path.exists() or not path.is_dir():
        raise ValueError(f"OCR 输出目录不存在: {path}")
    if not any(_is_within(path, root) for root in settings.allowed_existing_input_roots()):
        raise ValueError("只允许使用 data/corpus/ 或 data/generated/web/ 下的 OCR 输出目录。")
    if not any(child.is_dir() for child in path.iterdir()):
        raise ValueError("OCR 输出目录为空，无法创建任务。")
    return path


def infer_source_image_dir(input_path: Path) -> Path | None:
    sibling = input_path.parent / "input"
    return sibling if sibling.exists() and sibling.is_dir() else None


async def save_uploaded_files(settings: WebAppSettings, upload_dir: Path, files: Sequence[UploadFile]) -> list[Path]:
    saved_files: list[Path] = []
    if not files:
        raise ValueError("请至少上传一个 PDF 文件。")
    for index, upload in enumerate(files, start=1):
        filename = _sanitize_filename(upload.filename or f"upload_{index}.pdf")
        suffix = Path(filename).suffix.lower()
        if suffix not in settings.allowed_upload_extensions:
            raise ValueError(f"不支持的上传文件类型: {filename}")
        target_path = upload_dir / filename
        counter = 1
        while target_path.exists():
            target_path = upload_dir / f"{target_path.stem}_{counter}{target_path.suffix}"
            counter += 1
        total_bytes = 0
        with target_path.open("wb") as handle:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > settings.max_upload_bytes:
                    handle.close()
                    target_path.unlink(missing_ok=True)
                    raise ValueError(f"{filename} 超过上传大小限制 {settings.max_upload_bytes} 字节。")
                handle.write(chunk)
        await upload.close()
        saved_files.append(target_path)
    return saved_files


def create_existing_ocr_job(settings: WebAppSettings, *, display_name: str, raw_input_path: str) -> JobRecord:
    input_path = validate_existing_ocr_path(settings, raw_input_path)
    job_id = generate_job_id()
    workspace = ensure_job_workspace(settings, job_id)
    now = utc_now_iso()
    source_image_dir = infer_source_image_dir(input_path)
    job = JobRecord(
        job_id=job_id,
        display_name=_default_display_name(JOB_MODE_EXISTING, display_name or input_path.parent.name),
        mode=JOB_MODE_EXISTING,
        provider_mode="cloud_first",
        input_path=str(input_path),
        source_image_dir=str(source_image_dir or ""),
        upload_dir="",
        ocr_output_dir="",
        template_path=str(settings.template_path),
        output_dir=str(workspace["output_dir"]),
        result_dir=str(workspace["result_dir"]),
        log_dir=str(workspace["log_dir"]),
        provider_priority=settings.provider_priority,
        status=JOB_STATUS_QUEUED,
        current_stage="queued",
        progress_summary="标准化任务已入队，等待 worker 执行。",
        created_at=now,
        updated_at=now,
        started_at="",
        finished_at="",
        error_message="",
        raw_error_message="",
        user_friendly_error="",
        recommended_action="",
        run_id="",
        command_executed="",
        exit_code=None,
        timeout_seconds=settings.job_timeout_seconds,
    )
    return create_job(settings, job)


async def create_upload_job(settings: WebAppSettings, *, display_name: str, files: Sequence[UploadFile]) -> JobRecord:
    job_id = generate_job_id()
    workspace = ensure_job_workspace(settings, job_id)
    saved_files = await save_uploaded_files(settings, workspace["upload_dir"], files)
    now = utc_now_iso()
    credentials = settings.detect_ocr_credentials()
    auto_queue = settings.auto_run_upload_ocr and bool(credentials.get("active_upload_method_ready", False))
    job = JobRecord(
        job_id=job_id,
        display_name=_default_display_name(JOB_MODE_UPLOAD, display_name or saved_files[0].stem),
        mode=JOB_MODE_UPLOAD,
        provider_mode="cloud_first",
        input_path=str(workspace["upload_dir"]),
        source_image_dir=str(workspace["upload_dir"]),
        upload_dir=str(workspace["upload_dir"]),
        ocr_output_dir=str(workspace["ocr_output_dir"]),
        template_path=str(settings.template_path),
        output_dir=str(workspace["output_dir"]),
        result_dir=str(workspace["result_dir"]),
        log_dir=str(workspace["log_dir"]),
        provider_priority=settings.provider_priority,
        status=JOB_STATUS_QUEUED if auto_queue else JOB_STATUS_CREATED,
        current_stage="queued" if auto_queue else "ocr_pending",
        progress_summary=(
            "PDF 已上传并入队，等待执行 OCR。"
            if auto_queue
            else "PDF 已上传。当前未自动执行 OCR；配置云 OCR 后可重新入队。"
        ),
        created_at=now,
        updated_at=now,
        started_at="",
        finished_at="",
        error_message="",
        raw_error_message="",
        user_friendly_error="",
        recommended_action="",
        run_id="",
        command_executed="",
        exit_code=None,
        timeout_seconds=settings.job_timeout_seconds,
    )
    return create_job(settings, job)


def discover_output_files(job: JobRecord) -> list[OutputArtifact]:
    output_dir = Path(job.output_dir)
    result_dir = Path(job.result_dir)
    review_dir = output_dir.resolve().parent / "review"
    artifacts: list[OutputArtifact] = []
    for slug, label, filename in COMMON_OUTPUT_DEFS:
        path = output_dir / filename
        artifacts.append(
            OutputArtifact(
                slug=slug,
                label=label,
                path=str(path),
                relative_path=_repo_relative_or_absolute(path),
                exists=path.exists(),
                size_bytes=path.stat().st_size if path.exists() else 0,
                download_name=filename,
            )
        )
    for slug, label, filename in (
        ("job_summary", "任务摘要", "job_summary.json"),
        ("quality_summary", "质量摘要", "job_quality_summary.json"),
        ("logs", "任务日志", "job_log_bundle.json"),
    ):
        path = result_dir / filename
        artifacts.append(
            OutputArtifact(
                slug=slug,
                label=label,
                path=str(path),
                relative_path=_repo_relative_or_absolute(path),
                exists=path.exists(),
                size_bytes=path.stat().st_size if path.exists() else 0,
                download_name=filename,
            )
        )
    for slug, label, filename in (
        ("review_actions_csv", "复核动作导出 CSV", "review_actions_filled.csv"),
        ("review_actions_xlsx", "复核动作导出 XLSX", "review_actions_filled.xlsx"),
        ("review_action_export_summary", "复核动作导出摘要", "review_action_export_summary.json"),
    ):
        path = review_dir / filename
        artifacts.append(
            OutputArtifact(
                slug=slug,
                label=label,
                path=str(path),
                relative_path=_repo_relative_or_absolute(path),
                exists=path.exists(),
                size_bytes=path.stat().st_size if path.exists() else 0,
                download_name=filename,
            )
        )
    return artifacts


def write_output_manifest(job: JobRecord) -> Path:
    result_dir = Path(job.result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = result_dir / "discovered_outputs.json"
    payload = {
        "job_id": job.job_id,
        "status": job.status,
        "status_label": describe_job_status(job.status),
        "output_files": [item.as_dict() for item in discover_output_files(job)],
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path

def load_job_bundle(job: JobRecord) -> dict[str, object]:
    return load_json(Path(job.result_dir) / "job_summary.json")


def load_quality_bundle(job: JobRecord) -> dict[str, object]:
    return load_json(Path(job.result_dir) / "job_quality_summary.json")


def load_log_bundle(job: JobRecord) -> dict[str, object]:
    return load_json(Path(job.result_dir) / "job_log_bundle.json")


def load_pipeline_summary(job: JobRecord) -> dict[str, object]:
    return load_json(Path(job.output_dir) / "pipeline_completion_summary.json")


def resolve_download_artifact(job: JobRecord, slug: str) -> OutputArtifact | None:
    for artifact in discover_output_files(job):
        if artifact.slug == slug:
            return artifact
    return None


def list_existing_ocr_choices(settings: WebAppSettings) -> list[str]:
    if not settings.corpus_root.exists():
        return []
    results: list[str] = []
    for child in sorted(settings.corpus_root.iterdir(), key=lambda path: path.name):
        ocr_dir = child / "ocr_outputs"
        if ocr_dir.exists() and ocr_dir.is_dir():
            results.append(_repo_relative_or_absolute(ocr_dir))
    return results


def build_job_detail_payload(job: JobRecord) -> dict[str, object]:
    write_output_manifest(job)
    quality_summary = load_quality_bundle(job)
    return {
        "job": job.as_dict(),
        "status_label": describe_job_status(job.status),
        "pipeline_summary": load_pipeline_summary(job),
        "job_summary": load_job_bundle(job),
        "quality_summary": quality_summary,
        "operator_summary": summarize_for_operator(job, quality_summary) if quality_summary else {},
        "log_bundle": load_log_bundle(job),
        "output_files": [artifact.as_dict() for artifact in discover_output_files(job)],
    }


def require_job(settings: WebAppSettings, job_id: str) -> JobRecord:
    job = get_job(settings, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在。")
    return job


def safe_remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def get_system_status(settings: WebAppSettings) -> SystemStatusRecord:
    runtime_directories = {
        _repo_relative_or_absolute(path): path.exists()
        for path in (
            settings.runtime_root,
            settings.uploads_root,
            settings.jobs_root,
            settings.results_root,
            settings.logs_root,
        )
    }
    return SystemStatusRecord(
        app_name=settings.app_name,
        app_version=settings.app_version,
        environment=settings.env_mode,
        python_version=platform.python_version(),
        template_path=_repo_relative_or_absolute(settings.template_path),
        template_exists=settings.template_path.exists(),
        runtime_directories=runtime_directories,
        available_provider_modes=list(settings.available_provider_modes),
        redis_configured=bool(settings.redis_url.strip()),
        ocr_credentials=settings.detect_ocr_credentials(),
        local_worker_enabled=settings.enable_local_worker,
        auth_enabled=settings.auth_enabled,
        auth_required=settings.auth_required,
        worker_mode=settings.worker_mode,
    )
