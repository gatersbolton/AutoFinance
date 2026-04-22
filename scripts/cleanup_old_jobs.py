from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project_paths import REPO_ROOT
from webapp.config import load_settings
from webapp.db import iter_jobs


TERMINAL_STATUSES = {"succeeded", "succeeded_with_warnings", "needs_review", "failed", "cancelled"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean up old AutoFinance demo jobs.")
    parser.add_argument("--age-days", type=int, default=14, help="Delete jobs older than this many days. Defaults to 14.")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Preview only. Enabled by default.")
    parser.add_argument("--apply", action="store_true", help="Actually delete matching job data and DB rows.")
    return parser


def _repo_relative_or_absolute(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _safe_under_runtime(runtime_root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(runtime_root.resolve())
        return True
    except ValueError:
        return False


def _job_timestamp(raw_timestamp: str) -> datetime:
    return datetime.fromisoformat((raw_timestamp or "").replace("Z", "+00:00"))


def _delete_job_rows(db_path: Path, job_ids: list[str]) -> None:
    if not job_ids:
        return
    placeholders = ", ".join("?" for _ in job_ids)
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(f"DELETE FROM review_actions WHERE job_id IN ({placeholders})", job_ids)
        connection.execute(f"DELETE FROM review_operations WHERE job_id IN ({placeholders})", job_ids)
        connection.execute(f"DELETE FROM jobs WHERE job_id IN ({placeholders})", job_ids)
        connection.commit()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings()
    settings.ensure_directories()
    dry_run = not args.apply
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(args.age_days, 1))

    matched_jobs: list[dict[str, object]] = []
    delete_job_ids: list[str] = []
    deleted_paths: list[str] = []
    errors: list[str] = []

    for job in iter_jobs(settings):
        if job.status not in TERMINAL_STATUSES:
            continue
        job_time = _job_timestamp(job.finished_at or job.updated_at or job.created_at)
        if job_time > cutoff:
            continue
        target_paths = [
            Path(job.upload_dir),
            Path(job.output_dir).resolve().parent,
            Path(job.result_dir),
            Path(job.log_dir),
        ]
        safe_paths = [path for path in target_paths if str(path).strip() and _safe_under_runtime(settings.runtime_root, path)]
        matched_jobs.append(
            {
                "job_id": job.job_id,
                "status": job.status,
                "updated_at": job.updated_at,
                "paths": [_repo_relative_or_absolute(path) for path in safe_paths],
            }
        )
        delete_job_ids.append(job.job_id)
        if dry_run:
            continue
        for path in safe_paths:
            if not path.exists():
                continue
            try:
                shutil.rmtree(path)
                deleted_paths.append(_repo_relative_or_absolute(path))
            except Exception as exc:
                errors.append(f"{path}: {exc}")

    if not dry_run:
        _delete_job_rows(settings.db_path, delete_job_ids)

    summary = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "pass": not errors,
        "dry_run": dry_run,
        "age_days": max(args.age_days, 1),
        "matched_jobs": matched_jobs,
        "deleted_job_ids": [] if dry_run else delete_job_ids,
        "deleted_paths": deleted_paths,
        "errors": errors,
    }
    summary_path = settings.runtime_root / "cleanup_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["pass"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
