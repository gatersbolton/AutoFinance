from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

from project_paths import RAW_METRICS_GENERATED_ROOT, repo_relative
from standardize.manifest import generate_run_id

from .audit import build_summary
from .company_resolver import resolve_company_name
from .export import export_raw_metrics_run, write_json
from .loader import collect_source_files, expand_provider_priority, infer_doc_id, load_existing_provider_pages, load_registry_metadata
from .metric_extractor import extract_raw_metric_candidates, select_accepted_candidates
from .models import RawExtractionResult, RawMetricIssue
from .table_rebuild import rebuild_logical_subtables


LOGGER = logging.getLogger(__name__)
OUTPUT_FILENAMES = [
    "raw_metrics.csv",
    "raw_metrics.xlsx",
    "raw_metrics.jsonl",
    "raw_metrics_detailed.csv",
    "raw_metric_candidates.csv",
    "raw_metrics_issues.csv",
    "raw_metrics_summary.json",
    "date_resolution_audit.csv",
    "company_resolution_audit.csv",
    "extraction_run_manifest.json",
    "raw_metrics_smoke_summary.json",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 12 raw metrics extraction from existing OCR outputs.")
    parser.add_argument("--input-dir", required=True, help="Directory containing existing OCR provider outputs.")
    parser.add_argument("--output-dir", required=True, help=f"Base output directory under ./{repo_relative(RAW_METRICS_GENERATED_ROOT)}.")
    parser.add_argument("--source-image-dir", default="", help="Optional PDF/image directory for future evidence links.")
    parser.add_argument("--provider-priority", required=True, help="Comma-separated provider priority, e.g. aliyun_table,tencent_table_v3,paddle_table_local.")
    parser.add_argument("--doc-id", default="", help="Optional output doc id override.")
    parser.add_argument("--company-name", default="", help="Optional company name override.")
    parser.add_argument("--default-fill-date", default="", help="Optional fallback fill date, e.g. 2022-12-31.")
    parser.add_argument("--include-ratios", type=parse_bool, default=True, help="Include ratio/percent cells in accepted metrics. Default true.")
    parser.add_argument("--include-blank", type=parse_bool, default=False, help="Include blank value cells. Default false.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging and re-raise loader errors.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.debug)

    try:
        result = run_raw_extraction(args=args, cli_args=list(argv or []))
    except ValueError as exc:
        parser.error(str(exc))
        return 2
    LOGGER.info("Raw metrics extraction wrote %s files to %s", len(result.output_files), result.output_dir)
    LOGGER.info("Accepted raw metrics: %s", result.summary.get("accepted_metrics_total", 0))
    return 0 if result.summary.get("pass") else 1


def run_raw_extraction(*, args: argparse.Namespace, cli_args: Sequence[str] | None = None) -> RawExtractionResult:
    input_dir = Path(args.input_dir).resolve()
    output_base = Path(args.output_dir).resolve()
    raw_metrics_root = Path(str(getattr(args, "raw_metrics_root", "") or RAW_METRICS_GENERATED_ROOT)).resolve()
    source_image_dir = Path(args.source_image_dir).resolve() if args.source_image_dir else None
    provider_priority = expand_provider_priority(args.provider_priority)
    doc_id = infer_doc_id(input_dir, args.doc_id)
    run_id = generate_run_id(cli_args or build_cli_args_for_manifest(args))
    output_dir = resolve_run_output_dir(output_base, run_id)

    validate_inputs(
        input_dir=input_dir,
        output_base=output_base,
        source_image_dir=source_image_dir,
        raw_metrics_root=raw_metrics_root,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    pages, sources, load_errors = load_existing_provider_pages(
        input_dir=input_dir,
        provider_priority=provider_priority,
        doc_id_override=doc_id,
        debug=bool(args.debug),
    )
    all_cells, subtables, table_issues = rebuild_logical_subtables(pages)
    registry_metadata = load_registry_metadata(doc_id)
    company = resolve_company_name(
        doc_id=doc_id,
        pages=pages,
        input_dir=input_dir,
        source_image_dir=source_image_dir,
        override=args.company_name,
        registry_metadata=registry_metadata,
    )
    candidates, date_audits = extract_raw_metric_candidates(
        subtables=subtables,
        pages=pages,
        company=company,
        input_dir=input_dir,
        source_image_dir=source_image_dir,
        provider_priority=provider_priority,
        default_fill_date=args.default_fill_date,
        include_blank=bool(args.include_blank),
        include_ratios=bool(args.include_ratios),
    )
    accepted, issues = select_accepted_candidates(
        candidates,
        include_blank=bool(args.include_blank),
        include_ratios=bool(args.include_ratios),
    )
    issues.extend(load_error_issues(load_errors))
    issues.extend(table_issue_rows(table_issues))

    output_files = [str(output_dir / name) for name in OUTPUT_FILENAMES]
    summary = build_summary(
        run_id=run_id,
        docs_processed=len({page.doc_id for page in pages}),
        providers_seen=[page.provider for page in pages],
        candidates=candidates,
        accepted=accepted,
        issues=issues,
        output_files=output_files,
        load_error_total=len(load_errors),
    )
    manifest = build_manifest(
        run_id=run_id,
        args=args,
        input_dir=input_dir,
        output_dir=output_dir,
        provider_priority=provider_priority,
        doc_id=doc_id,
        source_files=collect_source_files(sources),
        summary=summary,
    )
    actual_output_files = export_raw_metrics_run(
        output_dir=output_dir,
        accepted=accepted,
        candidates=candidates,
        issues=issues,
        date_audits=date_audits,
        company_audits=[company],
        summary=summary,
        manifest=manifest,
    )
    summary["output_files"] = actual_output_files
    manifest["output_files"] = actual_output_files
    write_json(output_dir / "raw_metrics_summary.json", summary)
    write_json(output_dir / "raw_metrics_smoke_summary.json", summary)
    write_json(output_dir / "extraction_run_manifest.json", manifest)

    return RawExtractionResult(
        run_id=run_id,
        output_dir=str(output_dir),
        candidates=list(candidates),
        accepted=list(accepted),
        issues=list(issues),
        date_audits=list(date_audits),
        company_audits=[company],
        summary=summary,
        output_files=actual_output_files,
    )


def validate_inputs(
    *,
    input_dir: Path,
    output_base: Path,
    source_image_dir: Path | None,
    raw_metrics_root: Path = RAW_METRICS_GENERATED_ROOT,
) -> None:
    if not input_dir.exists():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    if source_image_dir and not source_image_dir.exists():
        raise ValueError(f"Source image directory does not exist: {source_image_dir}")
    validate_output_base(output_base, raw_metrics_root=raw_metrics_root)


def validate_output_base(
    output_base: Path,
    *,
    raw_metrics_root: Path = RAW_METRICS_GENERATED_ROOT,
) -> None:
    raw_root = raw_metrics_root.resolve()
    try:
        output_base.resolve().relative_to(raw_root)
    except ValueError as exc:
        raise ValueError(f"Stage 12 outputs must be under {raw_root}; got {output_base}") from exc


def resolve_run_output_dir(output_base: Path, run_id: str) -> Path:
    return output_base / run_id


def load_error_issues(load_errors: List[Dict[str, Any]]) -> List[RawMetricIssue]:
    rows: List[RawMetricIssue] = []
    for index, error in enumerate(load_errors, start=1):
        rows.append(
            RawMetricIssue(
                issue_id=f"loader_error_{index}",
                issue_type="provider_load_failed",
                severity="warning",
                message=str(error.get("error", "")),
                doc_id=str(error.get("doc_id", "")),
                provider=str(error.get("provider", "")),
                page_no=int(error.get("page_no", 0) or 0),
                meta_json=json.dumps(error, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            )
        )
    return rows


def table_issue_rows(table_issues: Sequence[Any]) -> List[RawMetricIssue]:
    rows: List[RawMetricIssue] = []
    for index, issue in enumerate(table_issues, start=1):
        issue_type = getattr(issue, "issue_type", "table_rebuild_issue")
        rows.append(
            RawMetricIssue(
                issue_id=f"table_issue_{index}",
                issue_type=issue_type,
                severity=getattr(issue, "severity", "warning"),
                message=getattr(issue, "message", issue_type),
                doc_id=getattr(issue, "doc_id", ""),
                provider=getattr(issue, "provider", ""),
                source_file=getattr(issue, "source_file", ""),
                page_no=int(getattr(issue, "page_no", 0) or 0),
                table_id=getattr(issue, "table_id", ""),
                logical_subtable_id=getattr(issue, "logical_subtable_id", ""),
                source_cell_ref=getattr(issue, "source_cell_ref", ""),
                value_raw=getattr(issue, "text_raw", ""),
                meta_json=getattr(issue, "meta_json", ""),
            )
        )
    return rows


def build_manifest(
    *,
    run_id: str,
    args: argparse.Namespace,
    input_dir: Path,
    output_dir: Path,
    provider_priority: Sequence[str],
    doc_id: str,
    source_files: Sequence[str],
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": "stage_12_raw_metrics_extraction",
        "cli_args": build_cli_args_for_manifest(args),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "source_image_dir": str(Path(args.source_image_dir).resolve()) if args.source_image_dir else "",
        "provider_priority": list(provider_priority),
        "doc_id": doc_id,
        "manual_company_override_used": bool(str(args.company_name or "").strip()),
        "manual_default_fill_date_used": bool(str(args.default_fill_date or "").strip()),
        "include_ratios": bool(args.include_ratios),
        "include_blank": bool(args.include_blank),
        "source_file_list": list(source_files),
        "summary_metrics_snapshot": dict(summary),
        "output_files": list(summary.get("output_files", [])),
        "no_ocr_api_called": True,
        "no_paddle_ocr_called": True,
        "no_zt_mapping_confirmed": True,
        "no_accounting_template_filled": True,
    }


def build_cli_args_for_manifest(args: argparse.Namespace) -> List[str]:
    values: List[str] = []
    for key, value in vars(args).items():
        if isinstance(value, bool):
            if value:
                values.append(f"--{key.replace('_', '-')}")
            continue
        if value not in ("", None):
            values.extend([f"--{key.replace('_', '-')}", str(value)])
    return values


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected true/false, got {value!r}")


def configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
