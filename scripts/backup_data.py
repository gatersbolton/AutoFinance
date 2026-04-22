from __future__ import annotations

import argparse
import json
import sys
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project_paths import AUDITS_ROOT, CORPUS_ROOT, REPO_ROOT, WEB_GENERATED_ROOT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backup AutoFinance demo data.")
    parser.add_argument(
        "--output-dir",
        default=str(AUDITS_ROOT / "backups"),
        help="Directory for backup archives. Defaults to data/generated/audits/backups.",
    )
    parser.add_argument("--include-corpus", action="store_true", help="Include data/corpus in the archive.")
    parser.add_argument("--label", default="", help="Optional archive label.")
    return parser


def _repo_relative_or_absolute(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    label = f"_{args.label.strip()}" if args.label.strip() else ""
    archive_path = output_dir / f"autofinance_demo_backup_{timestamp}{label}.tar.gz"

    included_paths = [WEB_GENERATED_ROOT]
    if args.include_corpus and CORPUS_ROOT.exists():
        included_paths.append(CORPUS_ROOT)

    with tarfile.open(archive_path, "w:gz") as handle:
        for path in included_paths:
            if not path.exists():
                continue
            handle.add(path, arcname=_repo_relative_or_absolute(path))

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "pass": archive_path.exists(),
        "archive_path": _repo_relative_or_absolute(archive_path),
        "archive_size_bytes": archive_path.stat().st_size if archive_path.exists() else 0,
        "included_paths": [_repo_relative_or_absolute(path) for path in included_paths if path.exists()],
        "include_corpus": bool(args.include_corpus),
    }
    summary_path = WEB_GENERATED_ROOT / "backup_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["pass"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
