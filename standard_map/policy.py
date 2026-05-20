from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from project_paths import MAPPING_POLICY_PATH


DEFAULT_AUTO_ACCEPT_ONCE_CONFIDENCE_THRESHOLD = 0.90


def load_mapping_policy(path: str | Path | None = None) -> dict[str, Any]:
    policy_path = Path(path or MAPPING_POLICY_PATH).resolve()
    if not policy_path.exists():
        return {"auto_accept_once_confidence_threshold": DEFAULT_AUTO_ACCEPT_ONCE_CONFIDENCE_THRESHOLD}
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("auto_accept_once_confidence_threshold", DEFAULT_AUTO_ACCEPT_ONCE_CONFIDENCE_THRESHOLD)
    payload.setdefault("future_bulk_accept_default_decision", "accept_once")
    return payload


def default_confidence_threshold(path: str | Path | None = None) -> float:
    policy = load_mapping_policy(path)
    try:
        return float(policy.get("auto_accept_once_confidence_threshold", DEFAULT_AUTO_ACCEPT_ONCE_CONFIDENCE_THRESHOLD))
    except (TypeError, ValueError):
        return DEFAULT_AUTO_ACCEPT_ONCE_CONFIDENCE_THRESHOLD
