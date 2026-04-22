from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from project_paths import REPO_ROOT

from .config import WebAppSettings
from .db import list_jobs
from .labels import provider_mode_label_zh
from .models import SystemStatusRecord
from .ocr_runtime import resolve_upload_provider, upload_provider_runtime_ready
from .quality import describe_job_status


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def repo_relative_or_absolute(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def worker_heartbeat_path(settings: WebAppSettings) -> Path:
    return settings.runtime_root / "worker_heartbeat.json"


def write_worker_heartbeat(
    settings: WebAppSettings,
    *,
    source: str,
    note: str = "",
    job_id: str = "",
    operation_id: str = "",
) -> dict[str, Any]:
    payload = {
        "timestamp": utc_now_iso(),
        "source": source,
        "queue_backend": settings.queue_backend,
        "job_id": job_id,
        "operation_id": operation_id,
        "note": note,
        "pid": os.getpid(),
    }
    path = worker_heartbeat_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_worker_heartbeat(settings: WebAppSettings) -> dict[str, Any]:
    path = worker_heartbeat_path(settings)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def worker_available(settings: WebAppSettings) -> tuple[bool, dict[str, Any], str]:
    heartbeat = load_worker_heartbeat(settings)
    if not heartbeat:
        return False, {}, "尚未发现 worker 心跳。"
    raw_timestamp = str(heartbeat.get("timestamp", "") or "")
    try:
        timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False, heartbeat, "worker 心跳时间格式异常。"
    age_seconds = max((datetime.now(timezone.utc) - timestamp).total_seconds(), 0.0)
    available = age_seconds <= max(settings.worker_heartbeat_stale_seconds, settings.worker_poll_seconds * 3)
    message = f"最近心跳 {round(age_seconds, 1)} 秒前。"
    return available, {**heartbeat, "age_seconds": round(age_seconds, 3)}, message


def queue_available(settings: WebAppSettings) -> tuple[bool, str]:
    if settings.queue_backend != "rq":
        return True, "当前使用本地轮询队列，不依赖 Redis。"
    if not settings.redis_url.strip():
        return False, "当前配置为 RQ，但 REDIS_URL 为空。"
    try:
        from redis import Redis

        connection = Redis.from_url(settings.redis_url)
        pong = connection.ping()
    except Exception as exc:
        return False, f"Redis 连接失败: {exc}"
    return bool(pong), "Redis 可连接，RQ 队列可用。"


def storage_writability(settings: WebAppSettings) -> tuple[bool, dict[str, dict[str, Any]]]:
    details: dict[str, dict[str, Any]] = {}
    all_writable = True
    for path in (
        settings.runtime_root,
        settings.uploads_root,
        settings.jobs_root,
        settings.results_root,
        settings.logs_root,
    ):
        path.mkdir(parents=True, exist_ok=True)
        probe_path = path / ".write_probe.tmp"
        writable = True
        error = ""
        try:
            probe_path.write_text("ok", encoding="utf-8")
            probe_path.unlink(missing_ok=True)
        except Exception as exc:
            writable = False
            error = str(exc)
            all_writable = False
        details[repo_relative_or_absolute(path)] = {"writable": writable, "error": error}
    return all_writable, details


def disk_free_status(settings: WebAppSettings, *, min_free_bytes: int) -> tuple[bool, dict[str, Any]]:
    target = settings.runtime_root if settings.runtime_root.exists() else settings.runtime_root.parent
    target.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(target)
    payload = {
        "target": repo_relative_or_absolute(target),
        "free_bytes": int(usage.free),
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "min_free_bytes": int(min_free_bytes),
    }
    return usage.free >= min_free_bytes, payload


def recent_job_rows(settings: WebAppSettings, *, limit: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job in list_jobs(settings, limit=limit):
        rows.append(
            {
                "job_id": job.job_id,
                "display_name": job.display_name,
                "status": job.status,
                "status_label_zh": describe_job_status(job.status),
                "current_stage": job.current_stage,
                "progress_summary": job.progress_summary,
                "provider_mode": job.provider_mode,
                "provider_mode_label_zh": provider_mode_label_zh(job.provider_mode),
                "updated_at": job.updated_at,
            }
        )
    return rows


def common_fault_hints(
    *,
    template_exists: bool,
    provider_ready: bool,
    queue_ok: bool,
    worker_ok: bool,
    storage_ok: bool,
) -> list[str]:
    hints: list[str] = []
    if not template_exists:
        hints.append("模板缺失时，标准化任务会直接失败。请确认 data/templates/会计报表.xlsx 已随部署一起提供。")
    if not provider_ready:
        hints.append("上传 PDF 失败最常见的原因是云 OCR 密钥未配置或配置格式错误。")
    if not queue_ok:
        hints.append("如果启用了 RQ 模式，请先确认 Redis 地址可达并允许容器访问。")
    if not worker_ok:
        hints.append("Worker 无心跳时，任务会一直停留在排队中。请检查 worker 容器日志。")
    if not storage_ok:
        hints.append("当 data/generated/web 不可写时，上传、日志、SQLite 和结果文件都会失败。")
    if not hints:
        hints.append("若任务失败，请优先下载 OCR/标准化日志，再核对模板、密钥和上传文件。")
    return hints


def build_system_status(settings: WebAppSettings) -> SystemStatusRecord:
    runtime_directories = {
        repo_relative_or_absolute(path): path.exists()
        for path in (
            settings.runtime_root,
            settings.uploads_root,
            settings.jobs_root,
            settings.results_root,
            settings.logs_root,
        )
    }
    provider_resolution = resolve_upload_provider(settings, settings.upload_ocr_method)
    runtime_provider = upload_provider_runtime_ready(settings, settings.upload_ocr_method)
    queue_ok, queue_message = queue_available(settings)
    worker_ok, heartbeat, worker_message = worker_available(settings)
    storage_ok, storage_details = storage_writability(settings)
    system_available = bool(
        settings.template_path.exists()
        and runtime_provider["provider_ready"]
        and queue_ok
        and worker_ok
        and storage_ok
    )
    return SystemStatusRecord(
        app_name=settings.app_name,
        app_version=settings.app_version,
        environment=settings.env_mode,
        python_version=platform.python_version(),
        template_path=repo_relative_or_absolute(settings.template_path),
        template_exists=settings.template_path.exists(),
        runtime_directories=runtime_directories,
        available_provider_modes=list(settings.available_provider_modes),
        redis_configured=bool(settings.redis_url.strip()),
        ocr_credentials={
            **settings.detect_ocr_credentials(),
            "active_upload_method_requested": settings.upload_ocr_method,
            "resolved_upload_method": provider_resolution["resolved_provider_mode"],
            "active_upload_method_ready": provider_resolution["provider_ready"],
            "mock_enabled": runtime_provider["mock_enabled"],
            "mock_mode": runtime_provider["mock_mode"],
        },
        local_worker_enabled=settings.enable_local_worker,
        auth_enabled=settings.auth_enabled,
        auth_required=settings.auth_required,
        worker_mode=settings.worker_mode,
        queue_backend=settings.queue_backend,
        operation_timeout_seconds=settings.operation_timeout_seconds,
        system_available=system_available,
        system_available_label_zh="可用" if system_available else "需处理部署问题",
        default_upload_provider_mode=settings.upload_ocr_method,
        default_upload_provider_label_zh=provider_mode_label_zh(settings.upload_ocr_method),
        resolved_upload_provider_mode=str(provider_resolution["resolved_provider_mode"]),
        resolved_upload_provider_label_zh=provider_mode_label_zh(str(provider_resolution["resolved_provider_mode"])),
        queue_available=queue_ok,
        queue_status_message_zh=queue_message,
        worker_available=worker_ok,
        worker_status_message_zh=worker_message,
        storage_writable=storage_ok,
        storage_details=storage_details,
        recent_jobs=recent_job_rows(settings),
        common_fault_hints=common_fault_hints(
            template_exists=settings.template_path.exists(),
            provider_ready=bool(provider_resolution["provider_ready"]),
            queue_ok=queue_ok,
            worker_ok=worker_ok,
            storage_ok=storage_ok,
        ),
        worker_heartbeat=heartbeat,
        max_upload_bytes=settings.max_upload_bytes,
    )


def _check_row(
    *,
    name: str,
    passed: bool,
    message_zh: str,
    severity: str = "error",
    detail: dict[str, Any] | None = None,
    recommended_action_zh: str = "",
) -> dict[str, Any]:
    return {
        "name": name,
        "pass": passed,
        "severity": severity,
        "message_zh": message_zh,
        "detail": detail or {},
        "recommended_action_zh": recommended_action_zh,
    }


def run_deployment_preflight(
    settings: WebAppSettings,
    *,
    deployment_profile: str = "aliyun",
    nginx_config_path: Path | None = None,
    min_free_bytes: int = 1024 * 1024 * 1024,
) -> dict[str, Any]:
    settings.ensure_directories()
    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[str] = []

    python_ok = sys.version_info >= (3, 10)
    checks.append(
        _check_row(
            name="python_version",
            passed=python_ok,
            message_zh=f"当前 Python 版本: {platform.python_version()}",
            recommended_action_zh="请使用 Python 3.10 或更高版本部署。" if not python_ok else "",
        )
    )

    required_dirs = [
        settings.runtime_root,
        settings.uploads_root,
        settings.jobs_root,
        settings.results_root,
        settings.logs_root,
    ]
    dirs_ok = all(path.exists() for path in required_dirs)
    checks.append(
        _check_row(
            name="required_directories",
            passed=dirs_ok,
            message_zh="运行目录已准备。" if dirs_ok else "运行目录缺失。",
            detail={"paths": [repo_relative_or_absolute(path) for path in required_dirs]},
            recommended_action_zh="请创建 data/generated/web 及其 uploads/jobs/results/logs 子目录。" if not dirs_ok else "",
        )
    )

    template_ok = settings.template_path.exists()
    checks.append(
        _check_row(
            name="template_exists",
            passed=template_ok,
            message_zh="模板文件已找到。" if template_ok else "模板文件不存在。",
            detail={"template_path": repo_relative_or_absolute(settings.template_path)},
            recommended_action_zh="请确认 data/templates/会计报表.xlsx 已部署到服务器。" if not template_ok else "",
        )
    )

    writable_ok, writable_details = storage_writability(settings)
    checks.append(
        _check_row(
            name="storage_writable",
            passed=writable_ok,
            message_zh="运行目录可写。" if writable_ok else "运行目录不可写。",
            detail=writable_details,
            recommended_action_zh="请修正 data/generated/web 的目录权限和挂载权限。" if not writable_ok else "",
        )
    )

    admin_password_ok = not (settings.env_mode == "prod" and not settings.admin_password)
    checks.append(
        _check_row(
            name="admin_password_set",
            passed=admin_password_ok,
            message_zh="管理员密码已配置。" if admin_password_ok else "生产模式缺少 WEBAPP_ADMIN_PASSWORD。",
            recommended_action_zh="请在 .env 中设置强密码并重启服务。" if not admin_password_ok else "",
        )
    )

    queue_ok, queue_message = queue_available(settings)
    checks.append(
        _check_row(
            name="queue_backend_ready",
            passed=queue_ok,
            message_zh=queue_message,
            recommended_action_zh="请确认 REDIS_URL 正确且 Redis 已启动。" if not queue_ok and settings.queue_backend == "rq" else "",
        )
    )

    credentials = settings.detect_ocr_credentials()
    secret_exists = bool(credentials.get("secret_path_exists", False))
    secret_parse_ok = bool(credentials.get("secret_file_parse_ok", False)) if secret_exists else False
    checks.append(
        _check_row(
            name="ocr_secret_exists",
            passed=secret_exists,
            message_zh="OCR 密钥文件已找到。" if secret_exists else "OCR 密钥文件不存在。",
            detail={"secret_path": repo_relative_or_absolute(settings.secret_path)},
            recommended_action_zh="请将密钥文件放到 data/secrets/secret 或改用环境变量注入。" if not secret_exists else "",
        )
    )
    checks.append(
        _check_row(
            name="ocr_secret_parse",
            passed=secret_parse_ok,
            message_zh="OCR 密钥文件解析成功。" if secret_parse_ok else "OCR 密钥文件解析失败。",
            recommended_action_zh="请核对 data/secrets/secret 的 aliyun/tencent 段落格式。" if secret_exists and not secret_parse_ok else "",
        )
    )

    provider_runtime = upload_provider_runtime_ready(settings, settings.upload_ocr_method)
    provider_ready = bool(provider_runtime["provider_ready"])
    checks.append(
        _check_row(
            name="active_ocr_provider_ready",
            passed=provider_ready,
            message_zh=(
                f"默认 OCR 方式可用: {provider_mode_label_zh(str(provider_runtime['resolved_provider_mode']))}"
                if provider_ready
                else str(provider_runtime["runtime_message_zh"] or "默认 OCR 方式不可用。")
            ),
            detail={
                "requested_provider_mode": provider_runtime["requested_provider_mode"],
                "resolved_provider_mode": provider_runtime["resolved_provider_mode"],
                "mock_enabled": provider_runtime["mock_enabled"],
            },
            recommended_action_zh=str(provider_runtime["runtime_action_zh"] or ""),
        )
    )

    disk_ok, disk_payload = disk_free_status(settings, min_free_bytes=min_free_bytes)
    checks.append(
        _check_row(
            name="disk_free",
            passed=disk_ok,
            message_zh="磁盘空间充足。" if disk_ok else "磁盘剩余空间偏低。",
            severity="warning" if not disk_ok else "error",
            detail=disk_payload,
            recommended_action_zh="请清理旧任务、扩容磁盘或迁移数据目录后再部署。" if not disk_ok else "",
        )
    )

    nginx_path = nginx_config_path or REPO_ROOT / "deploy" / deployment_profile / "nginx.conf"
    nginx_ok = nginx_path.exists()
    checks.append(
        _check_row(
            name="nginx_config_exists",
            passed=nginx_ok,
            message_zh="Nginx 配置文件已找到。" if nginx_ok else "Nginx 配置文件不存在。",
            detail={"nginx_config_path": repo_relative_or_absolute(nginx_path)},
            recommended_action_zh="请补充 deploy/aliyun/nginx.conf 后再执行部署。" if not nginx_ok else "",
        )
    )

    for check in checks:
        if check["pass"]:
            continue
        if check["severity"] == "warning":
            warnings.append(check["message_zh"])
        else:
            errors.append(check["message_zh"])

    recommendations = [str(item.get("recommended_action_zh", "")).strip() for item in checks if not item["pass"]]
    recommendations = [item for item in recommendations if item]
    pass_flag = not errors
    return {
        "generated_at": utc_now_iso(),
        "deployment_profile": deployment_profile,
        "environment": settings.env_mode,
        "queue_backend": settings.queue_backend,
        "pass": pass_flag,
        "checks": checks,
        "warnings": warnings,
        "errors": errors,
        "recommended_action_zh": "；".join(recommendations) if recommendations else "当前检查通过，可继续部署。",
    }
