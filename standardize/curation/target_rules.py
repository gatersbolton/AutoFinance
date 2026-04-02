from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_target_scope_rules(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return payload or {}
