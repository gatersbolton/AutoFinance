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
) -> Dict[str, Any]:
    manifest_rules = manifest_rules or {}
    manifest_path = output_dir / "run_manifest.json"
    artifact_manifest_path = output_dir / "artifact_manifest.csv"

    base_manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cli_args": list(cli_args),
        "git_commit_hash": git_commit_hash(output_dir.parent),
        "source_input_directories": [str(input_dir), str(template_path.parent)],
        "source_file_list": list(source_files),
        "summary_metrics_snapshot": dict(run_summary),
        "feature_flags": dict(feature_flags or {}),
        "generated_artifacts": [],
    }
    manifest_path.write_text(json.dumps(base_manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    artifact_rows = collect_artifact_rows(output_dir, run_id)
    write_artifact_manifest(artifact_manifest_path, artifact_rows)
    artifact_rows = collect_artifact_rows(output_dir, run_id)
    write_artifact_manifest(artifact_manifest_path, artifact_rows)
    base_manifest["generated_artifacts"] = artifact_rows
    manifest_path.write_text(json.dumps(base_manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    snapshot_index = snapshot_run_artifacts(
        run_id=run_id,
        output_dir=output_dir,
        artifact_rows=artifact_rows,
        manifest_rules=manifest_rules,
    )
    snapshot_index_path = output_dir / "run_snapshot_index.json"
    snapshot_index_path.write_text(json.dumps(snapshot_index, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "manifest": base_manifest,
        "artifact_rows": artifact_rows,
        "snapshot_index": snapshot_index,
    }


def collect_artifact_rows(output_dir: Path, run_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue
        rows.append(
            {
                "run_id": run_id,
                "relative_path": str(path.relative_to(output_dir)),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return rows


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
