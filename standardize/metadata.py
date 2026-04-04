from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


def prepare_summary_payload(payload: Dict[str, Any] | None, run_id: str) -> Dict[str, Any]:
    data = dict(payload or {})
    data["run_id"] = run_id
    return data


def prepare_nested_summary_payload(payload: Dict[str, Any] | None, run_id: str) -> Dict[str, Any]:
    data = prepare_summary_payload(payload, run_id)
    for key, value in list(data.items()):
        if isinstance(value, dict):
            nested = dict(value)
            nested["run_id"] = run_id
            data[key] = nested
    return data


def evaluate_summary_payloads(
    payloads: Iterable[Tuple[str, Dict[str, Any] | None]],
    expected_run_id: str,
) -> Dict[str, Any]:
    checked: List[str] = []
    missing_run_id_files: List[str] = []
    mismatched_run_id_files: List[Dict[str, Any]] = []
    for filename, payload in payloads:
        checked.append(filename)
        run_id = ""
        if isinstance(payload, dict):
            run_id = str(payload.get("run_id", "")).strip()
        if not run_id:
            missing_run_id_files.append(filename)
            continue
        if run_id != expected_run_id:
            mismatched_run_id_files.append({"file": filename, "found_run_id": run_id})
    checked = sorted(set(checked))
    missing_run_id_files = sorted(set(missing_run_id_files))
    mismatched_run_id_files.sort(key=lambda item: str(item.get("file", "")))
    return {
        "run_id_expected": expected_run_id,
        "summary_files_checked": checked,
        "missing_run_id_files": missing_run_id_files,
        "mismatched_run_id_files": mismatched_run_id_files,
        "pass": not missing_run_id_files and not mismatched_run_id_files,
    }


def scan_summary_run_ids(
    output_dir: Path,
    expected_run_id: str,
    *,
    required_summary_files: Sequence[str] | None = None,
) -> Dict[str, Any]:
    required = sorted(set(str(value).strip() for value in (required_summary_files or []) if str(value).strip()))
    payloads: List[Tuple[str, Dict[str, Any] | None]] = []
    present = {path.name for path in output_dir.glob("*summary.json")}
    missing_run_id_files: List[str] = []
    mismatched_run_id_files: List[Dict[str, Any]] = []
    checked = sorted(present | set(required))

    for filename in required:
        if filename not in present:
            missing_run_id_files.append(filename)

    for filename in sorted(present):
        path = output_dir / filename
        payload = load_json(path)
        payloads.append((filename, payload))

    evaluated = evaluate_summary_payloads(payloads, expected_run_id)
    missing_run_id_files.extend(evaluated["missing_run_id_files"])
    mismatched_run_id_files.extend(evaluated["mismatched_run_id_files"])
    deduped_missing = sorted(set(missing_run_id_files))
    deduped_mismatched = _dedupe_mismatched(mismatched_run_id_files)
    return {
        "run_id_expected": expected_run_id,
        "required_summary_files": required,
        "checked_summary_files": checked,
        "summary_files_checked": checked,
        "missing_run_id_files": deduped_missing,
        "mismatched_run_id_files": deduped_mismatched,
        "pass": not deduped_missing and not deduped_mismatched,
    }


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dedupe_mismatched(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        filename = str(row.get("file", "")).strip()
        if not filename:
            continue
        deduped[filename] = {"file": filename, "found_run_id": str(row.get("found_run_id", "")).strip()}
    return [deduped[key] for key in sorted(deduped)]
