from __future__ import annotations

from pathlib import Path
from typing import Any

from project_paths import STANDARD_METRICS_GENERATED_ROOT

from .models import MappingResult


def validate_output_base(
    output_base: Path,
    *,
    standard_metrics_root: Path = STANDARD_METRICS_GENERATED_ROOT,
) -> None:
    standard_root = standard_metrics_root.resolve()
    try:
        output_base.resolve().relative_to(standard_root)
    except ValueError as exc:
        raise ValueError(f"Stage 13 outputs must be under {standard_root}; got {output_base}") from exc


def build_summary(*, run_id: str, rows: list[MappingResult], output_files: list[str]) -> dict[str, Any]:
    mapped_total = sum(1 for row in rows if row.mapping_status == "mapped")
    review_required_total = sum(1 for row in rows if row.mapping_status == "review_required")
    unmapped_total = sum(1 for row in rows if row.mapping_status == "unmapped")
    skipped_total = sum(1 for row in rows if row.mapping_status == "skipped")
    return {
        "pass": True,
        "run_id": run_id,
        "standardized_rows_total": len(rows),
        "mapped_total": mapped_total,
        "review_required_total": review_required_total,
        "unmapped_total": unmapped_total,
        "skipped_total": skipped_total,
        "review_items_total": sum(1 for row in rows if row.review_required or row.mapping_status in {"unmapped", "skipped"}),
        "output_files": list(output_files),
        "no_ocr_api_called": True,
        "no_paddle_ocr_called": True,
        "no_accounting_template_filled": True,
    }
