from __future__ import annotations

import csv
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .stable_ids import stable_hash


DEFAULT_CORE_EXCLUDED_DIRS = {"review_pack", "reocr_inputs"}


def generate_run_id(cli_args: Sequence[str] | None = None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = list(cli_args or [])
    return f"RUN_{timestamp}_{stable_hash(payload or [timestamp], length=8)}"


def collect_source_files(sources: Iterable[Any]) -> List[str]:
    paths = set()
    for source in sources:
        for field in ("raw_file", "artifact_file", "result_json_file"):
            value = getattr(source, field, None)
            if value:
                paths.add(str(value))
    return sorted(paths)


def write_run_manifest(
    run_id: str,
    output_dir: Path,
    cli_args: Sequence[str],
    input_dir: Path,
    template_path: Path,
    source_files: Sequence[str],
    run_summary: Dict[str, Any],
    feature_flags: Dict[str, Any] | None = None,
    manifest_rules: Dict[str, Any] | None = None,
    artifact_manifest_mode: str = "core",
) -> Dict[str, Any]:
    manifest_rules = manifest_rules or {}
    mode = normalize_artifact_manifest_mode(artifact_manifest_mode)
    manifest_path = output_dir / "run_manifest.json"
    core_manifest_path = output_dir / "artifact_manifest_core.csv"
    legacy_manifest_path = output_dir / "artifact_manifest.csv"
    full_manifest_path = output_dir / "artifact_manifest_full.csv"

    manifest_payload = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cli_args": list(cli_args),
        "git_commit_hash": git_commit_hash(output_dir.parent),
        "source_input_directories": [str(input_dir), str(template_path.parent)],
        "source_file_list": list(source_files),
        "summary_metrics_snapshot": dict(run_summary),
        "feature_flags": dict(feature_flags or {}),
        "artifact_manifest_mode": mode,
        "artifact_manifest_files": {
            "core": core_manifest_path.name,
            "legacy_core_alias": legacy_manifest_path.name,
            "full": full_manifest_path.name if mode == "full" else "",
        },
        "snapshot_index_file": "run_snapshot_index.json",
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    core_rows = collect_artifact_rows(output_dir, run_id, mode="core", manifest_rules=manifest_rules)
    write_artifact_manifest(core_manifest_path, core_rows)
    if legacy_manifest_path != core_manifest_path:
        shutil.copyfile(core_manifest_path, legacy_manifest_path)

    full_rows: List[Dict[str, Any]] = []
    if mode == "full":
        full_rows = collect_artifact_rows(output_dir, run_id, mode="full", manifest_rules=manifest_rules)
        write_artifact_manifest(full_manifest_path, full_rows)
    elif full_manifest_path.exists():
        full_manifest_path.unlink()

    snapshot_rows = full_rows if mode == "full" else core_rows
    snapshot_index = snapshot_run_artifacts(
        run_id=run_id,
        output_dir=output_dir,
        artifact_rows=snapshot_rows,
        manifest_rules=manifest_rules,
    )
    snapshot_index_path = output_dir / "run_snapshot_index.json"
    snapshot_index_path.write_text(json.dumps(snapshot_index, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "manifest": manifest_payload,
        "artifact_rows": core_rows,
        "artifact_rows_core": core_rows,
        "artifact_rows_full": full_rows,
        "snapshot_index": snapshot_index,
    }


def normalize_artifact_manifest_mode(value: str | None) -> str:
    mode = str(value or "core").strip().lower()
    return "full" if mode == "full" else "core"


def collect_artifact_rows(
    output_dir: Path,
    run_id: str,
    *,
    mode: str = "full",
    manifest_rules: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    manifest_rules = manifest_rules or {}
    normalized_mode = normalize_artifact_manifest_mode(mode)
    rows: List[Dict[str, Any]] = []
    for path in iter_manifest_paths(output_dir, normalized_mode, manifest_rules):
        rows.append(
            {
                "run_id": run_id,
                "relative_path": str(path.relative_to(output_dir)),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return rows


def iter_manifest_paths(output_dir: Path, mode: str, manifest_rules: Dict[str, Any]) -> List[Path]:
    if mode == "core":
        candidates = [
            path
            for path in sorted(output_dir.iterdir())
            if path.is_file() and path.name != "artifact_manifest_full.csv"
        ]
        return candidates

    excluded_dirs = {
        str(name).strip()
        for name in (manifest_rules.get("full_excluded_dirs", []) or [])
        if str(name).strip()
    }
    paths: List[Path] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(output_dir)
        if relative.parts and relative.parts[0] in excluded_dirs:
            continue
        paths.append(path)
    return paths


def write_artifact_manifest(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "relative_path", "size_bytes", "sha256"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def snapshot_run_artifacts(run_id: str, output_dir: Path, artifact_rows: Sequence[Dict[str, Any]], manifest_rules: Dict[str, Any]) -> Dict[str, Any]:
    snapshot_root_name = str(manifest_rules.get("snapshot_root", "normalized_runs"))
    snapshot_root = output_dir.parent / snapshot_root_name
    snapshot_dir = snapshot_root / run_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    core_artifacts = set(manifest_rules.get("core_artifacts", []))
    copied: List[str] = []
    for row in artifact_rows:
        relative_path = row["relative_path"]
        if core_artifacts and relative_path not in core_artifacts and not relative_path.startswith("benchmark_") and not relative_path.startswith("derived_"):
            continue
        src = output_dir / relative_path
        dst = snapshot_dir / relative_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied.append(str(dst))
    return {
        "run_id": run_id,
        "snapshot_dir": str(snapshot_dir),
        "copied_artifacts": copied,
    }


def git_commit_hash(repo_dir: Path) -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "-C", str(repo_dir), "rev-parse", "--short", "HEAD"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            .strip()
        )
    except Exception:
        return ""


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
