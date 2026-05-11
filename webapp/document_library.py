from __future__ import annotations

import json
import re
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from fastapi import UploadFile

from project_paths import (
    RAW_METRICS_GENERATED_ROOT,
    REPO_ROOT,
    STANDARD_METRICS_GENERATED_ROOT,
)

from .config import WebAppSettings
from .document_models import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_NOT_STARTED,
    STATUS_RUNNING,
    DocumentRecord,
)
from .models import JOB_MODE_UPLOAD, JobRecord
from .ocr_runtime import build_upload_ocr_command, execute_mock_ocr, upload_provider_runtime_ready


DOC_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
MISSING_OCR_CREDENTIALS_MESSAGE = "未配置 OCR 密钥，无法识别 PDF。请联系管理员。"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_doc_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"doc_{timestamp}_{uuid.uuid4().hex[:8]}"


def sanitize_filename(filename: str) -> str:
    candidate = Path(filename or "").name.strip()
    if not candidate:
        candidate = "upload.pdf"
    candidate = re.sub(r"[^\w.\-()\u4e00-\u9fff]+", "_", candidate)
    candidate = candidate.strip("._") or "upload.pdf"
    if Path(candidate).suffix.lower() != ".pdf":
        candidate = f"{Path(candidate).stem or 'upload'}.pdf"
    return candidate


def ensure_document_library(settings: WebAppSettings) -> None:
    settings.library_root.mkdir(parents=True, exist_ok=True)


def validate_doc_id(doc_id: str) -> str:
    normalized = str(doc_id or "").strip()
    if not normalized or not DOC_ID_PATTERN.match(normalized):
        raise KeyError(doc_id)
    return normalized


def document_root(settings: WebAppSettings, doc_id: str) -> Path:
    return settings.library_root / validate_doc_id(doc_id)


def metadata_path_for(settings: WebAppSettings, doc_id: str) -> Path:
    return document_root(settings, doc_id) / "metadata.json"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_document(settings: WebAppSettings, document: DocumentRecord) -> DocumentRecord:
    payload = document.as_dict()
    payload["metadata_path"] = str(metadata_path_for(settings, document.doc_id))
    write_json(metadata_path_for(settings, document.doc_id), payload)
    return load_document(settings, document.doc_id, refresh=False)


def load_document(settings: WebAppSettings, doc_id: str, *, refresh: bool = True) -> DocumentRecord:
    path = metadata_path_for(settings, doc_id)
    if not path.exists():
        raise KeyError(doc_id)
    document = DocumentRecord.from_metadata(load_json(path), metadata_path=str(path))
    if not document.doc_id:
        document.doc_id = validate_doc_id(doc_id)
    return refresh_document_status(settings, document, persist=True) if refresh else document


def list_documents(settings: WebAppSettings) -> list[DocumentRecord]:
    ensure_document_library(settings)
    documents: list[DocumentRecord] = []
    for child in sorted(settings.library_root.iterdir(), key=lambda path: path.name):
        if not child.is_dir():
            continue
        metadata_path = child / "metadata.json"
        if not metadata_path.exists():
            continue
        try:
            document = DocumentRecord.from_metadata(load_json(metadata_path), metadata_path=str(metadata_path))
            if document.deleted_at:
                continue
            documents.append(refresh_document_status(settings, document, persist=True))
        except Exception:
            continue
    return sorted(documents, key=lambda item: item.updated_at or item.created_at, reverse=True)


async def save_uploaded_documents(settings: WebAppSettings, files: Sequence[UploadFile]) -> list[DocumentRecord]:
    ensure_document_library(settings)
    if not files:
        raise ValueError("请至少上传一个 PDF 文件。")
    documents: list[DocumentRecord] = []
    for index, upload in enumerate(files, start=1):
        original_filename = upload.filename or f"upload_{index}.pdf"
        if Path(original_filename).suffix.lower() not in settings.allowed_upload_extensions:
            raise ValueError(f"只支持 PDF 文件: {original_filename}")
        sanitized = sanitize_filename(original_filename)

        doc_id = generate_doc_id()
        root = document_root(settings, doc_id)
        input_dir = root / "input"
        ocr_output_dir = root / "ocr_outputs"
        input_dir.mkdir(parents=True, exist_ok=True)
        ocr_output_dir.mkdir(parents=True, exist_ok=True)
        target_path = input_dir / sanitized

        total_bytes = 0
        with target_path.open("wb") as handle:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > settings.max_upload_bytes:
                    handle.close()
                    shutil.rmtree(root, ignore_errors=True)
                    raise ValueError(f"{original_filename} 超过上传大小限制 {settings.max_upload_bytes} 字节。")
                handle.write(chunk)
        await upload.close()

        now = utc_now_iso()
        document = DocumentRecord(
            doc_id=doc_id,
            display_name=Path(original_filename).stem or sanitized,
            original_filename=original_filename,
            pdf_path=str(target_path),
            input_dir=str(input_dir),
            ocr_output_dir=str(ocr_output_dir),
            latest_job_id="",
            ocr_status=STATUS_NOT_STARTED,
            raw_metrics_status=STATUS_NOT_STARTED,
            standard_metrics_status=STATUS_NOT_STARTED,
            created_at=now,
            updated_at=now,
            deleted_at="",
            metadata_path=str(root / "metadata.json"),
            error_message="",
        )
        write_document(settings, document)
        documents.append(load_document(settings, doc_id))
    return documents


def refresh_document_status(settings: WebAppSettings, document: DocumentRecord, *, persist: bool) -> DocumentRecord:
    changed = False
    if document.ocr_status != STATUS_RUNNING and _ocr_outputs_exist(Path(document.ocr_output_dir)):
        if document.ocr_status != STATUS_COMPLETED:
            document.ocr_status = STATUS_COMPLETED
            changed = True

    raw_summary = load_json(_raw_step_summary_path(settings, document.doc_id))
    raw_path = Path(str(raw_summary.get("raw_metrics_csv", "") or ""))
    if not raw_path.exists():
        raw_path = _latest_file(RAW_METRICS_GENERATED_ROOT / document.doc_id, "raw_metrics.csv")
    if raw_path.exists() and document.raw_metrics_status != STATUS_COMPLETED:
        document.raw_metrics_status = STATUS_COMPLETED
        changed = True

    standard_summary = load_json(_standard_step_summary_path(settings, document.doc_id))
    standard_path = Path(str(standard_summary.get("standardized_metrics_csv", "") or ""))
    if not standard_path.exists():
        standard_path = _latest_file(STANDARD_METRICS_GENERATED_ROOT / document.doc_id, "standardized_metrics.csv")
    if standard_path.exists() and document.standard_metrics_status != STATUS_COMPLETED:
        document.standard_metrics_status = STATUS_COMPLETED
        changed = True

    if changed and persist:
        document.updated_at = utc_now_iso()
        write_json(metadata_path_for(settings, document.doc_id), document.as_dict())
    return document


def _ocr_outputs_exist(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    return any(child.is_file() for child in path.rglob("*"))


def _latest_file(root: Path, filename: str) -> Path:
    if not root.exists():
        return Path("")
    matches = sorted(root.rglob(filename), key=lambda path: path.stat().st_mtime if path.exists() else 0)
    return matches[-1] if matches else Path("")


def _raw_step_summary_path(settings: WebAppSettings, doc_id: str) -> Path:
    return settings.jobs_root / doc_id / "simple_flow" / "raw_metrics_step_summary.json"


def _standard_step_summary_path(settings: WebAppSettings, doc_id: str) -> Path:
    return settings.jobs_root / doc_id / "simple_flow" / "standard_metrics_step_summary.json"


def document_to_job(settings: WebAppSettings, document: DocumentRecord) -> JobRecord:
    root = settings.jobs_root / document.doc_id
    output_dir = root / "standardize"
    result_dir = settings.results_root / document.doc_id
    log_dir = settings.logs_root / document.doc_id
    for path in (root, output_dir, result_dir, log_dir):
        path.mkdir(parents=True, exist_ok=True)
    status = "succeeded" if document.standard_metrics_status == STATUS_COMPLETED else "created"
    stage = "generated" if document.standard_metrics_status == STATUS_COMPLETED else "uploaded"
    return JobRecord(
        job_id=document.doc_id,
        display_name=document.display_name,
        mode=JOB_MODE_UPLOAD,
        provider_mode="cloud_first",
        input_path=document.ocr_output_dir,
        source_image_dir=document.input_dir,
        upload_dir=document.input_dir,
        ocr_output_dir=document.ocr_output_dir,
        template_path=str(settings.template_path),
        output_dir=str(output_dir),
        result_dir=str(result_dir),
        log_dir=str(log_dir),
        provider_priority=settings.provider_priority,
        status=status,
        current_stage=stage,
        progress_summary=document.document_status_label_zh,
        created_at=document.created_at,
        updated_at=document.updated_at,
        started_at="",
        finished_at="",
        error_message=document.error_message,
        raw_error_message="",
        user_friendly_error=document.error_message,
        recommended_action="",
        run_id=document.latest_job_id,
        command_executed="",
        exit_code=None,
        timeout_seconds=settings.job_timeout_seconds,
    )


def update_document_status(
    settings: WebAppSettings,
    doc_id: str,
    *,
    ocr_status: str | None = None,
    raw_metrics_status: str | None = None,
    standard_metrics_status: str | None = None,
    latest_job_id: str | None = None,
    error_message: str | None = None,
) -> DocumentRecord:
    document = load_document(settings, doc_id, refresh=False)
    if ocr_status is not None:
        document.ocr_status = ocr_status
    if raw_metrics_status is not None:
        document.raw_metrics_status = raw_metrics_status
    if standard_metrics_status is not None:
        document.standard_metrics_status = standard_metrics_status
    if latest_job_id is not None:
        document.latest_job_id = latest_job_id
    if error_message is not None:
        document.error_message = error_message
    document.updated_at = utc_now_iso()
    write_json(metadata_path_for(settings, document.doc_id), document.as_dict())
    return load_document(settings, document.doc_id, refresh=True)


def archive_existing_ocr_outputs(document: DocumentRecord) -> Path | None:
    ocr_dir = Path(document.ocr_output_dir)
    if not _ocr_outputs_exist(ocr_dir):
        return None
    archive_root = ocr_dir.parent / "ocr_outputs_archive"
    archive_path = archive_root / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    counter = 1
    original_archive_path = archive_path
    while archive_path.exists():
        archive_path = original_archive_path.parent / f"{original_archive_path.name}_{counter}"
        counter += 1
    shutil.move(str(ocr_dir), str(archive_path))
    ocr_dir.mkdir(parents=True, exist_ok=True)
    return archive_path


def run_document_ocr(settings: WebAppSettings, doc_id: str, *, rerun: bool) -> dict[str, Any]:
    document = load_document(settings, doc_id, refresh=False)
    provider_mode = "cloud_first"
    runtime = upload_provider_runtime_ready(settings, provider_mode)
    if not runtime["runtime_ready"]:
        update_document_status(
            settings,
            doc_id,
            ocr_status=STATUS_FAILED,
            error_message=MISSING_OCR_CREDENTIALS_MESSAGE,
        )
        raise ValueError(MISSING_OCR_CREDENTIALS_MESSAGE)

    run_id = f"ocr_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
    archived_path = str(archive_existing_ocr_outputs(document) or "") if rerun else ""
    update_document_status(settings, doc_id, ocr_status=STATUS_RUNNING, latest_job_id=run_id, error_message="")

    log_dir = settings.logs_root / doc_id / "ocr"
    stdout_path = log_dir / f"{run_id}_stdout.txt"
    stderr_path = log_dir / f"{run_id}_stderr.txt"
    summary_path = log_dir / f"{run_id}_summary.json"
    command, resolution = build_upload_ocr_command(
        settings,
        upload_dir=Path(document.input_dir),
        output_dir=Path(document.ocr_output_dir),
        provider_mode=provider_mode,
    )
    command_text = subprocess.list2cmdline([str(part) for part in command])
    try:
        if runtime["mock_enabled"]:
            result = execute_mock_ocr(
                output_dir=Path(document.ocr_output_dir),
                provider_mode=str(resolution["resolved_provider_mode"]),
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            )
            returncode = int(result["returncode"])
            used_mock = True
            cloud_ocr_executed = False
        else:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
                completed = subprocess.run(
                    command,
                    cwd=str(REPO_ROOT),
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    timeout=settings.job_timeout_seconds,
                    check=False,
                )
            returncode = int(completed.returncode)
            used_mock = False
            cloud_ocr_executed = True
    except Exception as exc:
        payload = _ocr_run_payload(
            document=document,
            run_id=run_id,
            command_text=command_text,
            returncode=-1,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            summary_path=summary_path,
            resolved_provider_mode=str(resolution["resolved_provider_mode"]),
            archived_path=archived_path,
            used_mock=False,
            cloud_ocr_executed=False,
            error_message=str(exc),
        )
        write_json(summary_path, payload)
        update_document_status(settings, doc_id, ocr_status=STATUS_FAILED, error_message=str(exc))
        return payload

    error_message = ""
    final_status = STATUS_COMPLETED
    if returncode != 0:
        final_status = STATUS_FAILED
        error_message = _tail_text(stderr_path) or "OCR 执行失败。"
    payload = _ocr_run_payload(
        document=document,
        run_id=run_id,
        command_text=command_text,
        returncode=returncode,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        summary_path=summary_path,
        resolved_provider_mode=str(resolution["resolved_provider_mode"]),
        archived_path=archived_path,
        used_mock=used_mock,
        cloud_ocr_executed=cloud_ocr_executed,
        error_message=error_message,
    )
    write_json(summary_path, payload)
    update_document_status(settings, doc_id, ocr_status=final_status, error_message=error_message)
    return payload


def _ocr_run_payload(
    *,
    document: DocumentRecord,
    run_id: str,
    command_text: str,
    returncode: int,
    stdout_path: Path,
    stderr_path: Path,
    summary_path: Path,
    resolved_provider_mode: str,
    archived_path: str,
    used_mock: bool,
    cloud_ocr_executed: bool,
    error_message: str,
) -> dict[str, Any]:
    return {
        "doc_id": document.doc_id,
        "run_id": run_id,
        "requested_provider_mode": "cloud_first",
        "resolved_provider_mode": resolved_provider_mode,
        "command_executed": command_text,
        "returncode": returncode,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "summary_path": str(summary_path),
        "archived_previous_ocr_outputs": archived_path,
        "used_mock": used_mock,
        "cloud_ocr_executed": cloud_ocr_executed,
        "error_message": error_message,
        "pass": returncode == 0,
    }


def _tail_text(path: Path, limit_chars: int = 4000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-limit_chars:] if len(text) > limit_chars else text


def build_delete_plan(settings: WebAppSettings, document: DocumentRecord) -> dict[str, Any]:
    doc_root = document_root(settings, document.doc_id).resolve()
    raw_root = (RAW_METRICS_GENERATED_ROOT / document.doc_id).resolve()
    standard_root = (STANDARD_METRICS_GENERATED_ROOT / document.doc_id).resolve()
    job_root = (settings.jobs_root / document.doc_id).resolve()
    result_root = (settings.results_root / document.doc_id).resolve()
    log_root = (settings.logs_root / document.doc_id).resolve()

    listed_items = [
        {"label": "原始 PDF", "path": document.pdf_path},
        {"label": "上传输入目录", "path": document.input_dir},
        {"label": "OCR 输出", "path": document.ocr_output_dir},
        {"label": "原始数据结果", "path": str(raw_root)},
        {"label": "标准化数据结果", "path": str(standard_root)},
        {"label": "相关网页任务文件", "path": str(job_root)},
        {"label": "相关网页结果", "path": str(result_root)},
        {"label": "相关网页日志", "path": str(log_root)},
        {"label": "文档元数据", "path": document.metadata_path},
    ]
    delete_targets = [doc_root, raw_root, standard_root, job_root, result_root, log_root]
    allowed_roots = [
        settings.library_root.resolve(),
        RAW_METRICS_GENERATED_ROOT.resolve(),
        STANDARD_METRICS_GENERATED_ROOT.resolve(),
        settings.jobs_root.resolve(),
        settings.results_root.resolve(),
        settings.logs_root.resolve(),
    ]
    unsafe_paths = []
    for raw_path in [item["path"] for item in listed_items if item.get("path")]:
        path = resolve_maybe_relative(str(raw_path))
        if not any(is_within(path, root) for root in allowed_roots):
            unsafe_paths.append(str(path))
    for path in delete_targets:
        if not any(is_within(path, root) and path != root for root in allowed_roots):
            unsafe_paths.append(str(path))

    return {
        "doc_id": document.doc_id,
        "display_name": document.display_name,
        "items": [
            {
                **item,
                "exists": resolve_maybe_relative(str(item["path"])).exists() if item.get("path") else False,
            }
            for item in listed_items
        ],
        "delete_targets": [str(path) for path in delete_targets],
        "unsafe_paths": sorted(set(unsafe_paths)),
        "allowed_roots": [str(path) for path in allowed_roots],
    }


def execute_delete(settings: WebAppSettings, doc_id: str) -> dict[str, Any]:
    document = load_document(settings, doc_id, refresh=False)
    plan = build_delete_plan(settings, document)
    if plan["unsafe_paths"]:
        summary = _delete_summary(settings, document, plan=plan, deleted_paths=[], status="rejected")
        write_json(Path(summary["summary_path"]), summary)
        raise ValueError("删除路径安全检查失败。")

    deleted_paths: list[str] = []
    for raw_path in plan["delete_targets"]:
        path = Path(raw_path)
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        deleted_paths.append(str(path))
    summary = _delete_summary(settings, document, plan=plan, deleted_paths=deleted_paths, status="deleted")
    write_json(Path(summary["summary_path"]), summary)
    return summary


def _delete_summary(
    settings: WebAppSettings,
    document: DocumentRecord,
    *,
    plan: dict[str, Any],
    deleted_paths: Sequence[str],
    status: str,
) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = settings.deletions_root / f"{document.doc_id}_{timestamp}_delete_summary.json"
    return {
        "status": status,
        "doc_id": document.doc_id,
        "display_name": document.display_name,
        "deleted_at": utc_now_iso(),
        "deleted_paths": list(deleted_paths),
        "delete_plan": plan,
        "summary_path": str(summary_path),
    }


def resolve_maybe_relative(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def path_hygiene_ok(paths: Iterable[str], allowed_roots: Sequence[Path]) -> bool:
    roots = [root.resolve() for root in allowed_roots]
    for raw_path in paths:
        path = resolve_maybe_relative(raw_path)
        if not any(is_within(path, root) for root in roots):
            return False
    return True
