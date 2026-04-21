from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from project_paths import (
    CORPUS_ROOT,
    DEFAULT_SECRET_PATH,
    DEFAULT_TEMPLATE_PATH,
    REPO_ROOT,
    WEB_DB_PATH,
    WEB_GENERATED_ROOT,
    WEB_JOBS_ROOT,
    WEB_LOGS_ROOT,
    WEB_RESULTS_ROOT,
    WEB_UPLOADS_ROOT,
)

from . import APP_NAME, APP_VERSION


PACKAGE_ROOT = Path(__file__).resolve().parent


def _env_optional_bool(name: str) -> bool | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _resolve_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        return default
    path = Path(value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


@dataclass(slots=True)
class WebAppSettings:
    app_name: str = APP_NAME
    app_version: str = APP_VERSION
    env_mode: str = "dev"
    runtime_root: Path = WEB_GENERATED_ROOT
    uploads_root: Path = WEB_UPLOADS_ROOT
    jobs_root: Path = WEB_JOBS_ROOT
    results_root: Path = WEB_RESULTS_ROOT
    logs_root: Path = WEB_LOGS_ROOT
    db_path: Path = WEB_DB_PATH
    corpus_root: Path = CORPUS_ROOT
    template_path: Path = DEFAULT_TEMPLATE_PATH
    secret_path: Path = DEFAULT_SECRET_PATH
    enable_local_worker: bool = True
    worker_poll_seconds: int = 2
    max_upload_bytes: int = 25 * 1024 * 1024
    job_timeout_seconds: int = 3600
    operation_timeout_seconds: int = 3600
    auto_run_upload_ocr: bool = False
    upload_ocr_method: str = "aliyun_table"
    provider_priority: str = "aliyun,tencent"
    python_executable: str = sys.executable
    auth_required: bool = False
    admin_password: str = ""
    queue_backend: str = "local"
    redis_url: str = ""
    operation_log_tail_chars: int = 4000
    standardize_flags: tuple[str, ...] = field(
        default_factory=lambda: (
            "--enable-conflict-merge",
            "--enable-period-normalization",
            "--enable-dedupe",
            "--enable-validation",
            "--enable-integrity-check",
            "--enable-review-pack",
            "--enable-validation-aware-conflicts",
            "--enable-mapping-suggestions",
            "--enable-label-canonicalization",
            "--enable-derived-facts",
            "--enable-main-statement-specialization",
            "--enable-single-period-role-inference",
            "--enable-benchmark-alignment-repair",
            "--enable-export-target-scoping",
        )
    )

    @property
    def templates_dir(self) -> Path:
        return PACKAGE_ROOT / "templates"

    @property
    def static_dir(self) -> Path:
        return PACKAGE_ROOT / "static"

    @property
    def allowed_upload_extensions(self) -> tuple[str, ...]:
        return (".pdf",)

    @property
    def available_provider_modes(self) -> tuple[str, ...]:
        return (
            "cloud_first",
            "aliyun_table",
            "tencent_table_v3",
            "paddle_table_local (pilot_only)",
        )

    @property
    def auth_enabled(self) -> bool:
        return self.auth_required and bool(self.admin_password)

    @property
    def worker_mode(self) -> str:
        return "in_process" if self.enable_local_worker else "external_worker"

    def ensure_directories(self) -> None:
        for path in (self.runtime_root, self.uploads_root, self.jobs_root, self.results_root, self.logs_root):
            path.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def allowed_existing_input_roots(self) -> tuple[Path, ...]:
        return (
            self.corpus_root.resolve(),
            self.uploads_root.resolve(),
            self.jobs_root.resolve(),
        )

    def detect_ocr_credentials(self) -> dict[str, object]:
        sections: dict[str, dict[str, str]] = {}
        parse_error = ""
        if self.secret_path.exists():
            try:
                from OCR import parse_secret_file

                sections = parse_secret_file(self.secret_path)
            except Exception as exc:  # pragma: no cover
                parse_error = str(exc)

        tencent_configured = bool(
            (os.environ.get("TENCENTCLOUD_SECRET_ID") and os.environ.get("TENCENTCLOUD_SECRET_KEY"))
            or (sections.get("tencent", {}).get("secretid") and sections.get("tencent", {}).get("secretkey"))
        )
        aliyun_configured = bool(
            (os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID") and os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET"))
            or (sections.get("aliyun", {}).get("accesskeyid") and sections.get("aliyun", {}).get("accesskeysecret"))
        )

        active_ready = False
        if self.upload_ocr_method.startswith("aliyun"):
            active_ready = aliyun_configured
        elif self.upload_ocr_method.startswith("tencent"):
            active_ready = tencent_configured

        return {
            "secret_path_exists": self.secret_path.exists(),
            "secret_file_parse_ok": (not parse_error) if self.secret_path.exists() else False,
            "secret_file_parse_error_present": bool(parse_error),
            "aliyun_configured": aliyun_configured,
            "tencent_configured": tencent_configured,
            "active_upload_method": self.upload_ocr_method,
            "active_upload_method_ready": active_ready,
        }

    def validate_runtime_configuration(self) -> None:
        normalized_env = (self.env_mode or "dev").strip().lower()
        if normalized_env not in {"dev", "prod"}:
            raise RuntimeError(f"WEBAPP_ENV must be dev or prod, got: {self.env_mode}")
        normalized_queue_backend = (self.queue_backend or "local").strip().lower()
        if normalized_queue_backend not in {"local", "rq"}:
            raise RuntimeError(f"WEBAPP_QUEUE_BACKEND must be local or rq, got: {self.queue_backend}")
        if self.auth_required and not self.admin_password:
            raise RuntimeError(
                "WEBAPP_ADMIN_PASSWORD is required when authentication is enabled. "
                "Set WEBAPP_ADMIN_PASSWORD before starting the web app."
            )
        if normalized_queue_backend == "rq" and not self.redis_url.strip():
            raise RuntimeError("REDIS_URL is required when WEBAPP_QUEUE_BACKEND=rq.")
        if self.operation_timeout_seconds <= 0:
            raise RuntimeError("WEBAPP_OPERATION_TIMEOUT_SECONDS must be greater than 0.")


def load_settings() -> WebAppSettings:
    env_mode = os.environ.get("WEBAPP_ENV", "dev").strip().lower() or "dev"
    auth_required_override = _env_optional_bool("WEBAPP_AUTH_REQUIRED")
    legacy_password = os.environ.get("WEBAPP_PASSWORD", "")
    admin_password = os.environ.get("WEBAPP_ADMIN_PASSWORD", legacy_password)
    auth_required = (
        auth_required_override
        if auth_required_override is not None
        else (env_mode == "prod" or bool(admin_password))
    )
    runtime_root = _resolve_env_path("WEBAPP_RUNTIME_ROOT", WEB_GENERATED_ROOT)
    uploads_root = _resolve_env_path("WEBAPP_UPLOADS_ROOT", runtime_root / "uploads")
    jobs_root = _resolve_env_path("WEBAPP_JOBS_ROOT", runtime_root / "jobs")
    results_root = _resolve_env_path("WEBAPP_RESULTS_ROOT", runtime_root / "results")
    logs_root = _resolve_env_path("WEBAPP_LOGS_ROOT", runtime_root / "logs")
    db_path = _resolve_env_path("WEBAPP_DB_PATH", runtime_root / "webapp.sqlite3")

    return WebAppSettings(
        env_mode=env_mode,
        runtime_root=runtime_root,
        uploads_root=uploads_root,
        jobs_root=jobs_root,
        results_root=results_root,
        logs_root=logs_root,
        db_path=db_path,
        corpus_root=_resolve_env_path("WEBAPP_CORPUS_ROOT", CORPUS_ROOT),
        template_path=_resolve_env_path("WEBAPP_TEMPLATE_PATH", DEFAULT_TEMPLATE_PATH),
        secret_path=_resolve_env_path("WEBAPP_SECRET_PATH", DEFAULT_SECRET_PATH),
        enable_local_worker=_env_bool("WEBAPP_ENABLE_LOCAL_WORKER", True),
        worker_poll_seconds=_env_int("WEBAPP_WORKER_POLL_SECONDS", 2),
        max_upload_bytes=_env_int("WEBAPP_MAX_UPLOAD_BYTES", 25 * 1024 * 1024),
        job_timeout_seconds=_env_int("WEBAPP_JOB_TIMEOUT_SECONDS", 3600),
        operation_timeout_seconds=_env_int("WEBAPP_OPERATION_TIMEOUT_SECONDS", 3600),
        auto_run_upload_ocr=_env_bool("WEBAPP_AUTO_RUN_UPLOAD_OCR", False),
        upload_ocr_method=os.environ.get("WEBAPP_UPLOAD_OCR_METHOD", "aliyun_table").strip() or "aliyun_table",
        provider_priority=os.environ.get("WEBAPP_PROVIDER_PRIORITY", "aliyun,tencent").strip() or "aliyun,tencent",
        python_executable=os.environ.get("WEBAPP_PYTHON_EXECUTABLE", sys.executable).strip() or sys.executable,
        auth_required=auth_required,
        admin_password=admin_password,
        queue_backend=os.environ.get("WEBAPP_QUEUE_BACKEND", "local").strip().lower() or "local",
        redis_url=os.environ.get("REDIS_URL", ""),
        operation_log_tail_chars=_env_int("WEBAPP_OPERATION_LOG_TAIL_CHARS", 4000),
    )


def ensure_parent_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
