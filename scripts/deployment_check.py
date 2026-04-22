from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from webapp.config import load_settings
from webapp.deployment import run_deployment_preflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AutoFinance deployment preflight checks.")
    parser.add_argument("--profile", default="aliyun", help="Deployment profile name. Defaults to aliyun.")
    parser.add_argument(
        "--output",
        default="",
        help="Optional output path for the summary JSON. Defaults to data/generated/web/deployment_check_summary.json.",
    )
    parser.add_argument("--min-free-mb", type=int, default=1024, help="Minimum free disk space in MB. Defaults to 1024.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings()
    settings.ensure_directories()
    output_path = Path(args.output).resolve() if args.output else settings.runtime_root / "deployment_check_summary.json"
    summary = run_deployment_preflight(
        settings,
        deployment_profile=args.profile,
        min_free_bytes=max(int(args.min_free_mb), 0) * 1024 * 1024,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"pass": summary["pass"], "output_path": str(output_path)}, ensure_ascii=False))
    return 0 if summary["pass"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
