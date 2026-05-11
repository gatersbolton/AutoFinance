from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from project_paths import STANDARD_METRICS_GENERATED_ROOT, STANDARD_TERMS_PATH, repo_relative

from .mapper import run_standard_mapping


LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 13 standard metric mapping from raw metrics tables.")
    parser.add_argument("--input", required=True, help="Stage 12 raw_metrics.csv or raw_metrics.xlsx.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help=f"Base output directory under ./{repo_relative(STANDARD_METRICS_GENERATED_ROOT)}.",
    )
    parser.add_argument(
        "--mapping-registry",
        default=str(STANDARD_TERMS_PATH),
        help="YAML standard term registry. Defaults to config/standard_terms.yml.",
    )
    parser.add_argument("--doc-id", default="", help="Optional document id override.")
    parser.add_argument("--company-name", default="", help="Optional company name override for all rows.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(bool(args.debug))
    try:
        result = run_standard_mapping(args=args, cli_args=list(argv or []))
    except ValueError as exc:
        parser.error(str(exc))
        return 2
    LOGGER.info("Standard mapping wrote %s files to %s", len(result.output_files), result.output_dir)
    LOGGER.info("Mapped=%s review_required=%s unmapped=%s", result.summary.get("mapped_total"), result.summary.get("review_required_total"), result.summary.get("unmapped_total"))
    return 0 if result.summary.get("pass") else 1


def configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
