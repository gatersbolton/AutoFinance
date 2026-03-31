from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml


DEFAULT_OVERRIDE_FILES = {
    "mapping": "mapping_overrides.yml",
    "conflict": "conflict_overrides.yml",
    "period": "period_overrides.yml",
    "suppression": "suppression_overrides.yml",
    "placement": "placement_overrides.yml",
}


def ensure_override_store(config_dir: Path) -> Path:
    store_dir = config_dir / "manual_overrides"
    store_dir.mkdir(parents=True, exist_ok=True)
    for filename in DEFAULT_OVERRIDE_FILES.values():
        path = store_dir / filename
        if not path.exists():
            path.write_text("overrides: []\n", encoding="utf-8")
    return store_dir


def load_override_entries(config_dir: Path, override_key: str) -> List[Dict[str, Any]]:
    store_dir = ensure_override_store(config_dir)
    filename = DEFAULT_OVERRIDE_FILES[override_key]
    path = store_dir / filename
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    overrides = payload.get("overrides", [])
    return [item for item in overrides if isinstance(item, dict)]


def append_override_entry(config_dir: Path, override_key: str, entry: Dict[str, Any]) -> Path:
    store_dir = ensure_override_store(config_dir)
    filename = DEFAULT_OVERRIDE_FILES[override_key]
    path = store_dir / filename
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    overrides = payload.get("overrides", [])
    if not isinstance(overrides, list):
        overrides = []
    entry_id = str(entry.get("override_id", "")).strip()
    if entry_id:
        overrides = [item for item in overrides if str(item.get("override_id", "")).strip() != entry_id]
    overrides.append(entry)
    payload["overrides"] = overrides
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path
