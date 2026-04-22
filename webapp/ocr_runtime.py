from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from project_paths import REPO_ROOT

from .config import WebAppSettings


UPLOAD_PROVIDER_MODES = ("cloud_first", "aliyun_table", "tencent_table_v3")

_PROVIDER_CREDENTIAL_FLAGS = {
    "aliyun_table": "aliyun_configured",
    "tencent_table_v3": "tencent_configured",
}
_PROVIDER_PRIORITY_ALIASES = {
    "aliyun": "aliyun_table",
    "aliyun_table": "aliyun_table",
    "tencent": "tencent_table_v3",
    "tencent_table_v3": "tencent_table_v3",
}


def normalize_upload_provider_mode(provider_mode: str) -> str:
    normalized = (provider_mode or "").strip().lower() or "cloud_first"
    if normalized not in UPLOAD_PROVIDER_MODES:
        raise ValueError(f"不支持的 OCR 方式: {provider_mode}")
    return normalized


def ordered_cloud_provider_modes(settings: WebAppSettings) -> list[str]:
    ordered: list[str] = []
    for token in [item.strip().lower() for item in settings.provider_priority.split(",") if item.strip()]:
        resolved = _PROVIDER_PRIORITY_ALIASES.get(token)
        if resolved and resolved not in ordered:
            ordered.append(resolved)
    default_mode = (settings.upload_ocr_method or "").strip().lower()
    if default_mode in _PROVIDER_CREDENTIAL_FLAGS and default_mode not in ordered:
        ordered.append(default_mode)
    for fallback in ("aliyun_table", "tencent_table_v3"):
        if fallback not in ordered:
            ordered.append(fallback)
    return ordered


def resolve_upload_provider(settings: WebAppSettings, provider_mode: str) -> dict[str, Any]:
    requested_mode = normalize_upload_provider_mode(provider_mode)
    credentials = settings.detect_ocr_credentials()
    ordered_modes = ordered_cloud_provider_modes(settings)

    if requested_mode == "cloud_first":
        resolved_mode = next(
            (
                candidate
                for candidate in ordered_modes
                if bool(credentials.get(_PROVIDER_CREDENTIAL_FLAGS[candidate], False))
            ),
            ordered_modes[0],
        )
    else:
        resolved_mode = requested_mode

    ready = bool(credentials.get(_PROVIDER_CREDENTIAL_FLAGS[resolved_mode], False))
    recommended_action = ""
    failure_message = ""
    if not ready:
        if resolved_mode == "aliyun_table":
            failure_message = "当前未配置阿里云 OCR 密钥，无法处理上传 PDF。"
            recommended_action = "请配置阿里云密钥，或在页面中改选已配置的腾讯 OCR。"
        else:
            failure_message = "当前未配置腾讯 OCR 密钥，无法处理上传 PDF。"
            recommended_action = "请配置腾讯 OCR 密钥，或在页面中改选已配置的阿里云 OCR。"
        if requested_mode == "cloud_first":
            failure_message = "当前未检测到可用的云 OCR 密钥，cloud_first 无法处理上传 PDF。"
            recommended_action = "请至少配置阿里云或腾讯 OCR 密钥后重试。"

    return {
        "requested_provider_mode": requested_mode,
        "resolved_provider_mode": resolved_mode,
        "provider_ready": ready,
        "configured_provider_modes": [
            candidate
            for candidate in ("aliyun_table", "tencent_table_v3")
            if bool(credentials.get(_PROVIDER_CREDENTIAL_FLAGS[candidate], False))
        ],
        "failure_message_zh": failure_message,
        "recommended_action_zh": recommended_action,
        "credential_summary": credentials,
    }


def mock_ocr_enabled() -> bool:
    return bool((os.environ.get("WEBAPP_UPLOAD_OCR_MOCK_MODE", "") or "").strip())


def mock_ocr_mode() -> str:
    return (os.environ.get("WEBAPP_UPLOAD_OCR_MOCK_MODE", "") or "").strip().lower()


def mock_ocr_source_dir() -> Path | None:
    raw_value = (os.environ.get("WEBAPP_UPLOAD_OCR_MOCK_SOURCE_DIR", "") or "").strip()
    if not raw_value:
        return None
    candidate = Path(raw_value)
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def upload_provider_runtime_ready(settings: WebAppSettings, provider_mode: str) -> dict[str, Any]:
    resolution = resolve_upload_provider(settings, provider_mode)
    runtime_ready = bool(resolution["provider_ready"]) or mock_ocr_enabled()
    runtime_message = str(resolution["failure_message_zh"] or "")
    runtime_action = str(resolution["recommended_action_zh"] or "")
    if not resolution["provider_ready"] and mock_ocr_enabled():
        runtime_message = "当前未配置真实云 OCR 密钥，但已启用 smoke/mock OCR 路径。"
        runtime_action = "仅用于测试或演示验证；正式部署请配置真实云 OCR 密钥。"
    return {
        **resolution,
        "runtime_ready": runtime_ready,
        "mock_enabled": mock_ocr_enabled(),
        "mock_mode": mock_ocr_mode(),
        "runtime_message_zh": runtime_message,
        "runtime_action_zh": runtime_action,
    }


def build_upload_ocr_command(
    settings: WebAppSettings,
    *,
    upload_dir: Path,
    output_dir: Path,
    provider_mode: str,
) -> tuple[list[str], dict[str, Any]]:
    resolution = resolve_upload_provider(settings, provider_mode)
    command = [
        settings.python_executable,
        "OCR.py",
        "--method",
        str(resolution["resolved_provider_mode"]),
        "--input",
        str(upload_dir),
        "--output",
        str(output_dir),
    ]
    return command, resolution


def _copy_mock_provider_tree(source_dir: Path, target_dir: Path, provider_mode: str) -> None:
    direct_provider_dir = source_dir / provider_mode
    if direct_provider_dir.exists() and direct_provider_dir.is_dir():
        shutil.copytree(direct_provider_dir, target_dir / provider_mode, dirs_exist_ok=True)
        return

    if (source_dir / "result.json").exists():
        shutil.copytree(source_dir, target_dir / provider_mode, dirs_exist_ok=True)
        return

    raise FileNotFoundError(f"未找到 mock OCR 目录: {source_dir}")


def execute_mock_ocr(
    *,
    output_dir: Path,
    provider_mode: str,
    stdout_path: Path,
    stderr_path: Path,
) -> dict[str, Any]:
    mode = mock_ocr_mode()
    source_dir = mock_ocr_source_dir()
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.write_text("", encoding="utf-8")

    if mode in {"copy_fixture", "copy_existing"}:
        if source_dir is None:
            raise FileNotFoundError("WEBAPP_UPLOAD_OCR_MOCK_SOURCE_DIR 未配置，无法执行 mock OCR。")
        _copy_mock_provider_tree(source_dir, output_dir, provider_mode)
        stdout_path.write_text(
            json.dumps(
                {
                    "mode": mode,
                    "provider_mode": provider_mode,
                    "source_dir": str(source_dir),
                    "message_zh": "已复制 mock OCR 结果，未调用真实云 OCR。",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return {
            "returncode": 0,
            "used_mock": True,
            "cloud_ocr_executed": False,
            "mock_mode": mode,
            "mock_source_dir": str(source_dir),
        }

    if mode == "minimal_success":
        provider_root = output_dir / provider_mode / "mock_doc"
        raw_dir = provider_root / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "page_0001.json").write_text("{}", encoding="utf-8")
        (provider_root / "result.json").write_text(
            json.dumps(
                {
                    "provider": provider_mode,
                    "pages": [{"page_number": 1, "text": "mock", "raw_file": "raw/page_0001.json"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        stdout_path.write_text("minimal mock OCR completed\n", encoding="utf-8")
        return {
            "returncode": 0,
            "used_mock": True,
            "cloud_ocr_executed": False,
            "mock_mode": mode,
            "mock_source_dir": "",
        }

    raise ValueError(f"不支持的 WEBAPP_UPLOAD_OCR_MOCK_MODE: {mode}")
