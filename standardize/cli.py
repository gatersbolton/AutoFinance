from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import yaml

from .dedupe import assign_fact_ids, dedupe_facts
from .discover import SUPPORTED_TABLE_PROVIDERS, TEXT_ONLY_PROVIDERS, discover_provider_sources, list_provider_dirs
from .integrity import run_artifact_integrity
from .models import ArtifactIntegrityRecord, CellRecord, ConflictDecisionAuditRecord, ConflictRecord, DiscoveredSource, DuplicateRecord, FactRecord, IssueRecord
from .models import MappingCandidateRecord, MappingReviewRecord, PageSelectionRecord, ProviderComparisonRecord, ReOCRTaskRecord, ReviewQueueRecord, SecondaryOCRCandidateRecord
from .models import UnmappedLabelSummaryRecord, ValidationImpactRecord, ValidationResultRecord, dataclass_row
from .normalize.conflicts import enrich_conflicts, resolve_conflicts
from .normalize.export import export_template, rewrite_meta_summary
from .normalize.mapping import apply_subject_mapping, load_alias_mapping, load_relation_mapping, load_template_subjects
from .normalize.periods import apply_period_normalization
from .normalize.statements import classify_statement
from .normalize.tables import extract_facts, standardize_page
from .providers import load_aliyun_page, load_tencent_page, load_xlsx_fallback_page
from .quality_report import build_run_summary, build_top_suspicious_values, build_top_unknown_labels
from .review import build_review_queue, export_review_workbook
from .routing import build_page_selection, build_reocr_tasks, build_secondary_ocr_candidates
from .validation import run_validation


LOGGER = logging.getLogger(__name__)
PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR / "config"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standardize financial statement OCR outputs.")
    parser.add_argument("--input-dir", default="outputs", help="Directory containing OCR provider outputs.")
    parser.add_argument("--template", required=True, help="Path to the standard accounting template workbook.")
    parser.add_argument("--output-dir", default="normalized", help="Directory for normalized outputs.")
    parser.add_argument("--source-image-dir", default="", help="Optional PDF/image directory for routing/evidence generation.")
    parser.add_argument(
        "--provider-priority",
        default="aliyun,tencent",
        help="Comma-separated provider families or provider names, e.g. aliyun,tencent or aliyun_table,tencent_table_v3.",
    )
    parser.add_argument("--enable-conflict-merge", action="store_true", help="Enable provider conflict merge into the final facts.")
    parser.add_argument("--enable-period-normalization", action="store_true", help="Enable stage2 period normalization.")
    parser.add_argument("--enable-dedupe", action="store_true", help="Enable stage2 fact deduplication.")
    parser.add_argument("--enable-validation", action="store_true", help="Enable stage2 validation checks.")
    parser.add_argument("--emit-routing-plan", action="store_true", help="Emit pre/post OCR routing plans.")
    parser.add_argument("--enable-integrity-check", action="store_true", help="Run artifact integrity checks after export.")
    parser.add_argument("--enable-mapping-suggestions", action="store_true", help="Emit mapping candidate and unmapped summary files.")
    parser.add_argument("--enable-review-pack", action="store_true", help="Emit review workbook and evidence pack.")
    parser.add_argument("--enable-validation-aware-conflicts", action="store_true", help="Use validation-aware conflict decisions.")
    parser.add_argument("--emit-reocr-tasks", action="store_true", help="Emit targeted re-OCR task definitions.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, e.g. INFO or DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    input_dir = Path(args.input_dir).resolve()
    template_path = Path(args.template).resolve()
    output_dir = Path(args.output_dir).resolve()
    source_image_dir = Path(args.source_image_dir).resolve() if args.source_image_dir else None
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        parser.error(f"Input directory does not exist: {input_dir}")
    if not template_path.exists():
        parser.error(f"Template workbook does not exist: {template_path}")
    if source_image_dir and not source_image_dir.exists():
        parser.error(f"Source image directory does not exist: {source_image_dir}")

    provider_config = load_yaml(CONFIG_DIR / "provider_priority.yml")
    statement_config = load_yaml(CONFIG_DIR / "statement_keywords.yml")
    period_config = load_yaml(CONFIG_DIR / "period_rules.yml")
    validation_config = load_yaml(CONFIG_DIR / "validation_rules.yml")
    routing_config = load_yaml(CONFIG_DIR / "routing_rules.yml")
    export_rules = load_yaml(CONFIG_DIR / "export_rules.yml")
    mapping_rules = load_yaml(CONFIG_DIR / "mapping_rules.yml")
    conflict_config = load_yaml(CONFIG_DIR / "conflict_rules.yml")
    review_config = load_yaml(CONFIG_DIR / "review_rules.yml")
    reocr_config = load_yaml(CONFIG_DIR / "reocr_rules.yml")

    provider_priority = expand_provider_priority(args.provider_priority, provider_config)
    LOGGER.info("Using provider priority: %s", provider_priority)

    found_provider_dirs = list_provider_dirs(input_dir)
    skipped_text = [provider for provider in found_provider_dirs if provider in TEXT_ONLY_PROVIDERS]
    unsupported = [provider for provider in found_provider_dirs if provider not in SUPPORTED_TABLE_PROVIDERS and provider not in TEXT_ONLY_PROVIDERS]
    for provider in skipped_text:
        LOGGER.info("Text-only provider %s will only be used as context hints.", provider)
    for provider in unsupported:
        LOGGER.warning("Skipping unsupported provider directory %s.", provider)

    selected_providers = [provider for provider in provider_priority if provider in SUPPORTED_TABLE_PROVIDERS]
    sources: List[DiscoveredSource] = []
    for provider in selected_providers:
        provider_sources = discover_provider_sources(input_dir, provider)
        LOGGER.info("Discovered %s page sources for provider %s.", len(provider_sources), provider)
        sources.extend(provider_sources)

    all_cells: List[CellRecord] = []
    all_facts: List[FactRecord] = []
    all_issues: List[IssueRecord] = []
    all_pages = []
    page_errors: List[Dict[str, Any]] = []
    provider_hits: Dict[str, int] = {}
    pages_with_tables = set()
    total_tables = 0

    for source in sorted(sources, key=lambda item: (item.doc_id, item.page_no, provider_priority.index(item.provider))):
        try:
            page = load_provider_page(source)
            all_pages.append(page)
            provider_hits[page.provider] = provider_hits.get(page.provider, 0) + 1
            statement_meta = classify_statement(page, statement_config)
            page_cells, logical_subtables, page_issues = standardize_page(page, statement_meta, statement_config)
            page_facts, fact_issues = extract_facts(logical_subtables, statement_config)

            if logical_subtables:
                pages_with_tables.add((source.doc_id, source.page_no))
                total_tables += len(logical_subtables)

            all_cells.extend(page_cells)
            all_facts.extend(page_facts)
            all_issues.extend(page_issues)
            all_issues.extend(fact_issues)

            LOGGER.info(
                "Processed %s page %s provider %s: %s cells, %s facts, %s issues.",
                source.doc_id,
                source.page_no,
                source.provider,
                len(page_cells),
                len(page_facts),
                len(page_issues) + len(fact_issues),
            )
        except Exception as exc:  # pragma: no cover
            LOGGER.exception("Failed to process %s page %s provider %s", source.doc_id, source.page_no, source.provider)
            page_errors.append(
                {
                    "doc_id": source.doc_id,
                    "page_no": source.page_no,
                    "provider": source.provider,
                    "error": str(exc),
                }
            )

    assign_fact_ids(all_facts)
    all_facts = apply_period_normalization(
        facts=all_facts,
        provider_pages=all_pages,
        input_dir=input_dir,
        keyword_config=statement_config,
        period_config=period_config,
        enabled=args.enable_period_normalization,
    )

    subjects, subject_sheet, header_row = load_template_subjects(template_path)
    alias_mapping = load_alias_mapping(CONFIG_DIR / "subject_aliases.yml", subjects)
    relation_mapping = load_relation_mapping(CONFIG_DIR / "subject_relations.yml", subjects)
    all_facts, mapping_review, mapping_candidates, unmapped_labels_summary, mapping_stats = apply_subject_mapping(
        all_facts,
        subjects,
        alias_mapping,
        relation_mapping,
        mapping_rules,
    )

    initial_conflict_merge = args.enable_conflict_merge and not args.enable_validation_aware_conflicts
    all_facts, conflicts, provider_comparisons = resolve_conflicts(all_facts, provider_priority, initial_conflict_merge)

    facts_raw = copy.deepcopy(all_facts)
    duplicates: List[DuplicateRecord] = []
    facts_deduped = all_facts
    if args.enable_dedupe:
        facts_deduped, duplicates = dedupe_facts(all_facts, provider_priority)

    effective_validation = args.enable_validation or args.enable_validation_aware_conflicts
    validation_results: List[ValidationResultRecord] = []
    validation_summary = {
        "validation_total": 0,
        "validation_pass_total": 0,
        "validation_fail_total": 0,
        "validation_review_total": 0,
        "validation_skipped_total": 0,
        "validation_reason_breakdown": {},
    }
    if effective_validation:
        validation_results, validation_summary = run_validation(facts_deduped, validation_config)

    conflicts_enriched: List[ConflictRecord] = conflicts
    conflict_decision_audit: List[ConflictDecisionAuditRecord] = []
    validation_impact_of_conflicts: List[ValidationImpactRecord] = []
    if conflicts:
        facts_deduped, conflicts_enriched, conflict_decision_audit, validation_impact_of_conflicts = enrich_conflicts(
            facts=facts_deduped,
            conflicts=copy.deepcopy(conflicts),
            provider_priority=provider_priority,
            validation_config=validation_config,
            conflict_config=conflict_config,
            merge_enabled=args.enable_conflict_merge,
            validation_aware_enabled=args.enable_validation_aware_conflicts,
        )
        if effective_validation:
            validation_results, validation_summary = run_validation(facts_deduped, validation_config)

    page_selection_records: List[PageSelectionRecord] = []
    pre_ocr_plan: Dict[str, Any] = {}
    secondary_ocr_candidates: List[SecondaryOCRCandidateRecord] = []
    post_ocr_plan: Dict[str, Any] = {}
    if args.emit_routing_plan:
        if source_image_dir:
            page_selection_records, pre_ocr_plan = build_page_selection(source_image_dir, input_dir, routing_config)
        else:
            LOGGER.warning("Skipping pre-OCR routing because --source-image-dir was not provided.")
        secondary_ocr_candidates, post_ocr_plan = build_secondary_ocr_candidates(
            facts=facts_deduped,
            issues=all_issues,
            validations=validation_results,
            provider_comparisons=provider_comparisons,
            routing_config=routing_config,
        )

    review_items, review_summary = build_review_queue(
        facts=facts_deduped,
        cells=all_cells,
        issues=all_issues,
        conflicts=conflicts_enriched,
        validations=validation_results,
        mapping_candidates=mapping_candidates,
        source_image_dir=source_image_dir,
        output_dir=output_dir,
        review_config=review_config,
        generate_evidence=args.enable_review_pack or args.emit_reocr_tasks,
    )

    reocr_tasks: List[ReOCRTaskRecord] = []
    reocr_summary: Dict[str, Any] = {}
    if args.emit_reocr_tasks:
        reocr_tasks, reocr_summary = build_reocr_tasks(review_items, conflicts_enriched, reocr_config)

    unique_pages = {(page.doc_id, page.page_no) for page in all_pages}
    pages_total = len(unique_pages)
    docs_total = len({page.doc_id for page in all_pages})
    pages_skipped_as_non_table = (
        sum(1 for record in page_selection_records if not record.is_candidate_table_page)
        if page_selection_records
        else max(pages_total - len(pages_with_tables), 0)
    )
    run_summary = build_run_summary(
        docs_total=docs_total,
        pages_total=pages_total,
        pages_with_tables=len(pages_with_tables),
        pages_skipped_as_non_table=pages_skipped_as_non_table,
        tables_total=total_tables,
        cells=all_cells,
        facts_raw=facts_raw,
        facts_deduped=facts_deduped,
        duplicates=duplicates,
        provider_comparisons=provider_comparisons,
        validations=validation_results,
        conflicts=conflicts_enriched,
        mapping_stats=mapping_stats,
        review_summary=review_summary,
        integrity_summary={},
    )

    export_stats = export_template(
        template_path=template_path,
        output_path=output_dir / "会计报表_填充结果.xlsx",
        facts=facts_deduped,
        run_summary=run_summary,
        issues=all_issues,
        validations=validation_results,
        duplicates=duplicates,
        conflicts=conflicts_enriched,
        review_queue=review_items,
        export_rules=export_rules,
    )
    LOGGER.info("Exported helper sheets: %s", ",".join(export_stats.get("helper_sheets", [])))

    write_dataclass_csv(output_dir / "cells.csv", all_cells, CellRecord)
    write_dataclass_csv(output_dir / "facts_raw.csv", facts_raw, FactRecord)
    write_dataclass_csv(output_dir / "facts_deduped.csv", facts_deduped, FactRecord)
    write_dataclass_csv(output_dir / "facts.csv", facts_deduped, FactRecord)
    write_dataclass_csv(output_dir / "issues.csv", all_issues, IssueRecord)
    write_dataclass_csv(output_dir / "conflicts.csv", conflicts, ConflictRecord)
    write_dataclass_csv(output_dir / "conflicts_enriched.csv", conflicts_enriched, ConflictRecord)
    write_dataclass_csv(output_dir / "conflict_decision_audit.csv", conflict_decision_audit, ConflictDecisionAuditRecord)
    write_dataclass_csv(output_dir / "validation_impact_of_conflicts.csv", validation_impact_of_conflicts, ValidationImpactRecord)
    write_dataclass_csv(output_dir / "mapping_review.csv", mapping_review, MappingReviewRecord)
    if args.enable_mapping_suggestions:
        write_dataclass_csv(output_dir / "mapping_candidates.csv", mapping_candidates, MappingCandidateRecord)
        write_dataclass_csv(output_dir / "unmapped_labels_summary.csv", unmapped_labels_summary, UnmappedLabelSummaryRecord)
    write_dataclass_csv(output_dir / "duplicates.csv", duplicates, DuplicateRecord)
    write_dataclass_csv(output_dir / "provider_comparison_summary.csv", provider_comparisons, ProviderComparisonRecord)
    write_dataclass_csv(output_dir / "validation_results.csv", validation_results, ValidationResultRecord)
    write_dataclass_csv(output_dir / "review_queue.csv", review_items, ReviewQueueRecord)
    if page_selection_records:
        write_dataclass_csv(output_dir / "page_selection.csv", page_selection_records, PageSelectionRecord)
    if secondary_ocr_candidates:
        write_dataclass_csv(output_dir / "secondary_ocr_candidates.csv", secondary_ocr_candidates, SecondaryOCRCandidateRecord)
    if args.emit_reocr_tasks:
        write_dataclass_csv(output_dir / "reocr_tasks.csv", reocr_tasks, ReOCRTaskRecord)
        write_json(output_dir / "reocr_task_summary.json", reocr_summary)

    top_unknown_labels = build_top_unknown_labels(facts_deduped)
    top_suspicious_values = build_top_suspicious_values(all_cells)
    write_dict_csv(output_dir / "top_unknown_labels.csv", top_unknown_labels, ["label", "count"])
    write_dict_csv(output_dir / "top_suspicious_values.csv", top_suspicious_values, ["text_raw", "reason", "count"])
    write_json(output_dir / "validation_summary.json", validation_summary)
    write_json(output_dir / "review_summary.json", review_summary)
    if args.enable_review_pack:
        export_review_workbook(output_dir / "review_workbook.xlsx", review_items)
    if pre_ocr_plan:
        write_json(output_dir / "pre_ocr_routing_plan.json", pre_ocr_plan)
    if post_ocr_plan:
        write_json(output_dir / "post_ocr_routing_plan.json", post_ocr_plan)

    integrity_summary = {"integrity_fail_total": 0, "checks_total": 0, "integrity_pass_total": 0, "integrity_review_total": 0}
    integrity_records: List[ArtifactIntegrityRecord] = []
    if args.enable_integrity_check:
        integrity_result = run_artifact_integrity(
            output_dir=output_dir,
            workbook_path=output_dir / "会计报表_填充结果.xlsx",
            run_summary=run_summary,
            export_stats=export_stats,
            export_rules=export_rules,
        )
        integrity_records = integrity_result["records"]
        integrity_summary = integrity_result["summary"]
        write_dataclass_csv(output_dir / "artifact_integrity.csv", integrity_records, ArtifactIntegrityRecord)
        write_json(output_dir / "artifact_integrity.json", integrity_summary)

    run_summary = build_run_summary(
        docs_total=docs_total,
        pages_total=pages_total,
        pages_with_tables=len(pages_with_tables),
        pages_skipped_as_non_table=pages_skipped_as_non_table,
        tables_total=total_tables,
        cells=all_cells,
        facts_raw=facts_raw,
        facts_deduped=facts_deduped,
        duplicates=duplicates,
        provider_comparisons=provider_comparisons,
        validations=validation_results,
        conflicts=conflicts_enriched,
        mapping_stats=mapping_stats,
        review_summary=review_summary,
        integrity_summary=integrity_summary,
    )
    write_json(output_dir / "run_summary.json", run_summary)
    write_dict_csv(output_dir / "run_summary.csv", [run_summary], list(run_summary.keys()))
    rewrite_meta_summary(output_dir / "会计报表_填充结果.xlsx", run_summary)

    summary = {
        "docs_processed": docs_total,
        "pages_discovered": len(sources),
        "providers_seen": provider_hits,
        "skipped_text_providers": skipped_text,
        "unsupported_providers": unsupported,
        "cells_count": len(all_cells),
        "facts_count": len(facts_deduped),
        "issues_count": len(all_issues),
        "conflicts_count": len(conflicts_enriched),
        "mapping_review_count": len(mapping_review),
        "template_sheet": subject_sheet,
        "template_header_row": header_row,
        "export_stats": export_stats,
        "page_errors": page_errors,
        "run_summary": run_summary,
        "validation_summary": validation_summary,
        "review_summary": review_summary,
        "integrity_summary": integrity_summary,
    }
    write_json(output_dir / "summary.json", summary)
    LOGGER.info("Review items queued: %s", review_summary.get("review_total", 0))
    LOGGER.info("Unplaced facts routed away from main sheet: %s", export_stats.get("unplaced_count", 0))
    LOGGER.info("Wrote normalized outputs to %s", output_dir)
    return 0


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}


def expand_provider_priority(spec: str, provider_config: Dict[str, Any]) -> List[str]:
    tokens = [token.strip() for token in spec.split(",") if token.strip()]
    families = provider_config.get("families", {}) or {}
    default_priority = provider_config.get("default_priority", []) or []
    expanded: List[str] = []

    if not tokens:
        tokens = list(default_priority)

    for token in tokens:
        if token in families:
            expanded.extend(families[token])
        else:
            expanded.append(token)

    deduped: List[str] = []
    for provider in expanded:
        if provider not in deduped:
            deduped.append(provider)
    return deduped


def load_provider_page(source: DiscoveredSource):
    if source.provider == "aliyun_table" and source.raw_file:
        return load_aliyun_page(source)
    if source.provider == "tencent_table_v3" and source.raw_file:
        return load_tencent_page(source)
    if source.artifact_file:
        return load_xlsx_fallback_page(source)
    raise ValueError(f"No supported source found for {source.doc_id} page {source.page_no} provider {source.provider}")


def write_dataclass_csv(path: Path, rows: Iterable[Any], model_cls) -> None:
    rows = list(rows)
    fieldnames = [item.name for item in fields(model_cls)]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dataclass_row(row))


def write_dict_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: serialize_csv_value(row.get(key)) for key in fieldnames})


def serialize_csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return value if value is not None else ""


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
