from __future__ import annotations

import json
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from fastapi import HTTPException, UploadFile

from project_paths import REPO_ROOT

from .config import WebAppSettings
from .deployment import build_system_status
from .db import create_job, get_job, utc_now_iso
from .labels import provider_mode_label_zh
from .models import (
    JOB_MODE_EXISTING,
    JOB_MODE_UPLOAD,
    JOB_STATUS_CREATED,
    JOB_STATUS_QUEUED,
    SUCCESS_LIKE_JOB_STATUSES,
    JobRecord,
    OutputArtifact,
)
from .ocr_runtime import upload_provider_runtime_ready
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

REVIEW_OUTPUT_DEFS = (
    ("review_actions_csv", "复核动作导出 CSV", "review_actions_filled.csv"),
    ("review_actions_xlsx", "复核动作导出 XLSX", "review_actions_filled.xlsx"),
    ("review_action_export_summary", "复核动作导出摘要", "review_action_export_summary.json"),
    ("review_action_compatibility_summary", "复核动作兼容性摘要", "review_action_compatibility_summary.json"),
    ("review_dashboard_counts_summary", "复核看板计数摘要", "review_dashboard_counts_summary.json"),
    ("review_workbench_summary", "复核工作台摘要", "review_workbench_summary.json"),
    ("review_evidence_preview_summary", "证据预览摘要", "review_evidence_preview_summary.json"),
    ("review_apply_preview_summary", "应用预览摘要", "review_apply_preview_summary.json"),
    ("bulk_review_action_summary", "批量动作摘要", "bulk_review_action_summary.json"),
    ("review_operation_summary", "最近一次复核操作摘要", "review_operation_summary.json"),
    ("operation_stage_timeline", "最近一次复核操作阶段时间线", "operation_stage_timeline.json"),
    ("operation_lock_summary", "复核操作锁摘要", "operation_lock_summary.json"),
    ("operation_retry_summary", "复核操作重试摘要", "operation_retry_summary.json"),
)

JOB_STAGE_LABELS_ZH = {
    "uploaded": "已上传",
    "ocr_pending": "已上传",
    "ocr": "正在 OCR",
    "worker_claimed": "处理中",
    "standardize": "正在标准化",
    "generated": "已生成结果",
    "needs_review": "需要复核",
    "failed": "失败",
    "cancelled": "已取消",
    "queued": "排队中",
}


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


def _artifact_from_path(slug: str, label: str, path: Path, *, download_name: str | None = None) -> OutputArtifact:
    return OutputArtifact(
        slug=slug,
        label=label,
        path=str(path),
        relative_path=_repo_relative_or_absolute(path),
        exists=path.exists(),
        size_bytes=path.stat().st_size if path.exists() else 0,
        download_name=download_name or path.name,
    )


def _job_root(job: JobRecord) -> Path:
    return Path(job.output_dir).resolve().parent


def _review_dir(job: JobRecord) -> Path:
    return _job_root(job) / "review"


def _reruns_root(job: JobRecord) -> Path:
    return _job_root(job) / "reruns"


def _rerun_result_root(job: JobRecord, rerun_id: str) -> Path:
    return Path(job.result_dir).resolve() / "reruns" / rerun_id


def _latest_apply_dir(job: JobRecord) -> Path | None:
    review_dir = _review_dir(job)
    apply_dirs = sorted([path for path in review_dir.glob("apply_*") if path.is_dir()], key=lambda path: path.name)
    return apply_dirs[-1] if apply_dirs else None


def list_result_versions(job: JobRecord) -> list[dict[str, object]]:
    versions: list[dict[str, object]] = []
    original_output_dir = Path(job.output_dir).resolve()
    versions.append(
        {
            "version_id": "original",
            "label": "original",
            "status": job.status,
            "status_label": describe_job_status(job.status),
            "recommended": False,
            "workbook_slug": "filled_workbook",
            "review_summary_slug": "quality_summary",
            "delta_slug": "",
            "delta_explained_slug": "",
            "workbook_exists": (original_output_dir / "会计报表_填充结果.xlsx").exists(),
            "delta_exists": False,
            "delta_explained_exists": False,
        }
    )

    rerun_dirs = sorted([path for path in _reruns_root(job).glob("rerun_*") if path.is_dir()], key=lambda path: path.name)
    for rerun_dir in rerun_dirs:
        rerun_id = rerun_dir.name
        rerun_result_dir = _rerun_result_root(job, rerun_id)
        rerun_summary = load_json(rerun_result_dir / "review_rerun_summary.json")
        combined_summary = load_json(rerun_result_dir / "review_apply_and_rerun_summary.json")
        quality_summary = load_json(rerun_result_dir / "job_quality_summary.json")
        status = str(
            combined_summary.get("rerun_status")
            or rerun_summary.get("final_job_status")
            or quality_summary.get("final_job_status")
            or "failed"
        )
        versions.append(
            {
                "version_id": rerun_id,
                "label": rerun_id,
                "status": status,
                "status_label": describe_job_status(status),
                "recommended": False,
                "workbook_slug": f"{rerun_id}_filled_workbook",
                "review_summary_slug": f"{rerun_id}_review_rerun_summary",
                "delta_slug": f"{rerun_id}_review_rerun_delta",
                "delta_explained_slug": f"{rerun_id}_review_rerun_delta_explained",
                "workbook_exists": (rerun_dir / "standardize" / "会计报表_填充结果.xlsx").exists(),
                "delta_exists": (rerun_result_dir / "review_rerun_delta.json").exists(),
                "delta_explained_exists": (rerun_result_dir / "review_rerun_delta_explained.json").exists(),
            }
        )

    recommended_version_id = "original"
    for version in reversed(versions):
        if version["version_id"] == "original":
            recommended_version_id = "original"
            break
        if str(version["status"]) in SUCCESS_LIKE_JOB_STATUSES:
            recommended_version_id = str(version["version_id"])
            break
    for version in versions:
        version["recommended"] = str(version["version_id"]) == recommended_version_id
    return versions


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


async def create_upload_job(
    settings: WebAppSettings,
    *,
    display_name: str,
    provider_mode: str,
    files: Sequence[UploadFile],
) -> JobRecord:
    job_id = generate_job_id()
    workspace = ensure_job_workspace(settings, job_id)
    saved_files = await save_uploaded_files(settings, workspace["upload_dir"], files)
    provider_runtime = upload_provider_runtime_ready(settings, provider_mode)
    if not provider_runtime["runtime_ready"]:
        safe_remove_tree(workspace["upload_dir"])
        safe_remove_tree(workspace["job_root"])
        safe_remove_tree(workspace["result_dir"])
        safe_remove_tree(workspace["log_dir"])
        raise ValueError(str(provider_runtime["runtime_message_zh"]))
    now = utc_now_iso()
    auto_queue = settings.auto_run_upload_ocr
    job = JobRecord(
        job_id=job_id,
        display_name=_default_display_name(JOB_MODE_UPLOAD, display_name or saved_files[0].stem),
        mode=JOB_MODE_UPLOAD,
        provider_mode=str(provider_runtime["requested_provider_mode"]),
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
        current_stage="queued" if auto_queue else "uploaded",
        progress_summary=(
            "PDF 已上传并入队，等待执行 OCR。"
            if auto_queue
            else "PDF 已上传，可手动重新入队后执行 OCR。"
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
    review_dir = _review_dir(job)
    log_dir = Path(job.log_dir)
    artifacts: list[OutputArtifact] = []
    for slug, label, filename in COMMON_OUTPUT_DEFS:
        path = output_dir / filename
        artifacts.append(_artifact_from_path(slug, label, path, download_name=filename))
    for slug, label, filename in (
        ("job_summary", "任务摘要", "job_summary.json"),
        ("quality_summary", "质量摘要", "job_quality_summary.json"),
        ("logs", "任务日志", "job_log_bundle.json"),
        ("ocr_stage_summary", "OCR 阶段摘要", "ocr_stage_summary.json"),
    ):
        path = result_dir / filename
        artifacts.append(_artifact_from_path(slug, label, path, download_name=filename))
    for slug, label, filename in (
        ("ocr_stdout", "OCR stdout", "ocr_stdout.txt"),
        ("ocr_stderr", "OCR stderr", "ocr_stderr.txt"),
        ("standardize_stdout", "标准化 stdout", "standardize_stdout.txt"),
        ("standardize_stderr", "标准化 stderr", "standardize_stderr.txt"),
    ):
        path = log_dir / filename
        artifacts.append(_artifact_from_path(slug, label, path, download_name=filename))
    for slug, label, filename in REVIEW_OUTPUT_DEFS:
        path = review_dir / filename
        artifacts.append(_artifact_from_path(slug, label, path, download_name=filename))

    latest_apply_dir = _latest_apply_dir(job)
    if latest_apply_dir is not None:
        for slug, label, filename in (
            ("latest_review_apply_summary", "最近一次应用摘要", "review_apply_summary.json"),
            ("latest_review_decision_summary", "最近一次应用决策摘要", "review_decision_summary.json"),
            ("latest_applied_review_actions", "最近一次已应用动作", "applied_review_actions.csv"),
            ("latest_rejected_review_actions", "最近一次拒绝动作", "rejected_review_actions.csv"),
            ("latest_override_audit", "最近一次覆盖审计", "override_audit.csv"),
        ):
            path = latest_apply_dir / filename
            artifacts.append(_artifact_from_path(slug, label, path, download_name=filename))

    for version in list_result_versions(job):
        version_id = str(version["version_id"])
        if version_id == "original":
            continue
        rerun_root = _reruns_root(job) / version_id
        rerun_standardize_dir = rerun_root / "standardize"
        rerun_result_dir = _rerun_result_root(job, version_id)
        rerun_artifacts = (
            (f"{version_id}_filled_workbook", f"{version_id} 工作簿", rerun_standardize_dir / "会计报表_填充结果.xlsx"),
            (f"{version_id}_review_rerun_summary", f"{version_id} 结果摘要", rerun_result_dir / "review_rerun_summary.json"),
            (f"{version_id}_review_rerun_delta", f"{version_id} 前后对比", rerun_result_dir / "review_rerun_delta.json"),
            (f"{version_id}_review_rerun_delta_explained", f"{version_id} 对比说明", rerun_result_dir / "review_rerun_delta_explained.json"),
            (
                f"{version_id}_review_apply_and_rerun_summary",
                f"{version_id} 应用并重跑摘要",
                rerun_result_dir / "review_apply_and_rerun_summary.json",
            ),
            (f"{version_id}_quality_summary", f"{version_id} 质量摘要", rerun_result_dir / "job_quality_summary.json"),
            (f"{version_id}_stdout", f"{version_id} stdout", rerun_root / "standardize_stdout.txt"),
            (f"{version_id}_stderr", f"{version_id} stderr", rerun_root / "standardize_stderr.txt"),
        )
        for slug, label, path in rerun_artifacts:
            artifacts.append(_artifact_from_path(slug, label, path))
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


def job_stage_label_zh(job: JobRecord) -> str:
    if job.status == "failed":
        return JOB_STAGE_LABELS_ZH["failed"]
    if job.status == "needs_review":
        return JOB_STAGE_LABELS_ZH["needs_review"]
    if job.status in SUCCESS_LIKE_JOB_STATUSES:
        return JOB_STAGE_LABELS_ZH["generated"]
    return JOB_STAGE_LABELS_ZH.get(job.current_stage, job.current_stage or describe_job_status(job.status))


def build_job_stage_flow(job: JobRecord) -> list[dict[str, object]]:
    result_versions = list_result_versions(job)
    has_rerun = any(str(item.get("version_id")) != "original" for item in result_versions)
    stages = [
        {
            "slug": "upload",
            "label_zh": "上传" if job.mode == JOB_MODE_UPLOAD else "已有 OCR",
            "state": "done" if job.mode == JOB_MODE_UPLOAD or job.mode == JOB_MODE_EXISTING else "pending",
            "status_label_zh": "已准备",
        },
        {
            "slug": "ocr",
            "label_zh": "OCR",
            "state": "done" if job.mode == JOB_MODE_EXISTING else "pending",
            "status_label_zh": "复用已有 OCR" if job.mode == JOB_MODE_EXISTING else "待执行",
        },
        {
            "slug": "standardize",
            "label_zh": "标准化",
            "state": "pending",
            "status_label_zh": "待执行",
        },
        {
            "slug": "review",
            "label_zh": "复核",
            "state": "pending",
            "status_label_zh": "按需进入",
        },
        {
            "slug": "rerun",
            "label_zh": "重新生成",
            "state": "done" if has_rerun else "pending",
            "status_label_zh": "已生成新版本" if has_rerun else "尚未执行",
        },
    ]
    if job.mode == JOB_MODE_UPLOAD:
        if job.current_stage in {"uploaded", "queued", "worker_claimed"} and job.status in {"created", "queued", "running"}:
            stages[0]["state"] = "current"
            stages[0]["status_label_zh"] = "已上传"
        if job.current_stage == "ocr":
            stages[1]["state"] = "current"
            stages[1]["status_label_zh"] = "正在 OCR"
        elif any(Path(job.ocr_output_dir).iterdir()) if Path(job.ocr_output_dir).exists() else False:
            stages[1]["state"] = "done"
            stages[1]["status_label_zh"] = "已完成"
    else:
        stages[0]["status_label_zh"] = "使用已有 OCR 输出"

    if job.current_stage == "standardize":
        stages[2]["state"] = "current"
        stages[2]["status_label_zh"] = "正在标准化"
        if job.mode == JOB_MODE_UPLOAD:
            stages[1]["state"] = "done"
            stages[1]["status_label_zh"] = "已完成"
    elif job.status in SUCCESS_LIKE_JOB_STATUSES or job.status == "failed":
        stages[2]["state"] = "done" if job.status in SUCCESS_LIKE_JOB_STATUSES else "current"
        stages[2]["status_label_zh"] = "已完成" if job.status in SUCCESS_LIKE_JOB_STATUSES else "处理中断"

    if job.status == "needs_review":
        stages[3]["state"] = "current"
        stages[3]["status_label_zh"] = "需要复核"
    elif job.status in SUCCESS_LIKE_JOB_STATUSES:
        stages[3]["state"] = "done"
        stages[3]["status_label_zh"] = "可进入复核"
    elif job.status == "failed":
        for item in stages:
            if item["state"] == "current":
                item["status_label_zh"] = "失败"
        if not any(item["state"] == "current" for item in stages):
            stages[2]["state"] = "current"
            stages[2]["status_label_zh"] = "失败"
    return stages


def build_job_detail_payload(job: JobRecord) -> dict[str, object]:
    write_output_manifest(job)
    quality_summary = load_quality_bundle(job)
    result_versions = list_result_versions(job)
    latest_review_apply_summary = load_json((_latest_apply_dir(job) or Path("_missing")) / "review_apply_summary.json")
    review_dir = _review_dir(job)
    latest_rerun_delta_explained = {}
    if result_versions and str(result_versions[-1]["version_id"]) != "original":
        latest_rerun_delta_explained = load_json(
            _rerun_result_root(job, str(result_versions[-1]["version_id"])) / "review_rerun_delta_explained.json"
        )
    return {
        "job": job.as_dict(),
        "status_label": describe_job_status(job.status),
        "stage_label_zh": job_stage_label_zh(job),
        "stage_flow": build_job_stage_flow(job),
        "provider_mode_label_zh": provider_mode_label_zh(job.provider_mode),
        "pipeline_summary": load_pipeline_summary(job),
        "job_summary": load_job_bundle(job),
        "quality_summary": quality_summary,
        "operator_summary": summarize_for_operator(job, quality_summary) if quality_summary else {},
        "log_bundle": load_log_bundle(job),
        "output_files": [artifact.as_dict() for artifact in discover_output_files(job)],
        "result_versions": result_versions,
        "latest_recommended_result": next((item for item in result_versions if item.get("recommended")), {}),
        "latest_review_apply_summary": latest_review_apply_summary,
        "latest_review_apply_preview_summary": load_json(review_dir / "review_apply_preview_summary.json"),
        "latest_review_operation_summary": load_json(review_dir / "review_operation_summary.json"),
        "latest_bulk_review_action_summary": load_json(review_dir / "bulk_review_action_summary.json"),
        "latest_rerun_delta_explained": latest_rerun_delta_explained,
    }


def require_job(settings: WebAppSettings, job_id: str) -> JobRecord:
    job = get_job(settings, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在。")
    return job


def safe_remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def get_system_status(settings: WebAppSettings):
    return build_system_status(settings)
