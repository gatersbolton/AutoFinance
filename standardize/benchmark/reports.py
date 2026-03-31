from __future__ import annotations

from typing import Any, Dict


def with_run_id(payload: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    result = dict(payload)
    result["run_id"] = run_id
    return result
