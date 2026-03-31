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
from .feedback import apply_review_actions, build_delta_reports, build_priority_backlog, export_review_actions_template, parse_review_actions_file
from .feedback.audit import build_review_decision_summary
from .feedback.delta import load_artifact_snapshot
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
from .overrides import apply_conflict_overrides, apply_local_mapping_overrides, apply_period_overrides, apply_placement_overrides, apply_suppression_overrides
from .overrides import build_manual_alias_records, ensure_override_store, filter_review_items_by_placement, load_override_entries
from .providers import load_aliyun_page, load_tencent_page, load_xlsx_fallback_page
from .quality_report import build_run_summary, build_top_suspicious_values, build_top_unknown_labels
from .review import build_review_queue, export_review_workbook
from .routing import build_page_selection, build_reocr_tasks, build_secondary_ocr_candidates, ingest_reocr_results, materialize_reocr_inputs
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
    parser.add_argument("--emit-review-actions-template", action="store_true", help="Emit editable review actions template workbook/csv.")
    parser.add_argument("--review-actions-file", default="", help="Optional filled reviewer action workbook/csv to apply.")
    parser.add_argument("--apply-review-actions", action="store_true", help="Apply reviewer actions from --review-actions-file into manual overrides and rerun.")
    parser.add_argument("--materialize-reocr-inputs", action="store_true", help="Materialize crop inputs for targeted re-OCR tasks.")
    parser.add_argument("--reocr-results-dir", default="", help="Optional directory containing returned re-OCR result json files keyed by task_id.")
    parser.add_argument("--emit-delta-report", action="store_true", help="Emit before/after delta reports when baseline artifacts exist.")
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
    review_actions_path = Path(args.review_actions_file).resolve() if args.review_actions_file else None
    reocr_results_dir = Path(args.reocr_results_dir).resolve() if args.reocr_results_dir else None
    baseline_snapshot = load_artifact_snapshot(output_dir) if output_dir.exists() else {}
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        parser.error(f"Input directory does not exist: {input_dir}")
    if not template_path.exists():
        parser.error(f"Template workbook does not exist: {template_path}")
    if source_image_dir and not source_image_dir.exists():
        parser.error(f"Source image directory does not exist: {source_image_dir}")
    if args.apply_review_actions and not review_actions_path:
        parser.error("--apply-review-actions requires --review-actions-file")
    if review_actions_path and not review_actions_path.exists():
        parser.error(f"Review actions file does not exist: {review_actions_path}")

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
    manual_action_rules = load_yaml(CONFIG_DIR / "manual_action_rules.yml")
    priority_rules = load_yaml(CONFIG_DIR / "priority_rules.yml")
    override_rules = load_yaml(CONFIG_DIR / "override_rules.yml")
    reocr_merge_rules = load_yaml(CONFIG_DIR / "reocr_merge_rules.yml")
    ensure_override_store(CONFIG_DIR)

    applied_actions_rows: List[Dict[str, Any]] = []
    rejected_actions_rows: List[Dict[str, Any]] = []
    override_audit_rows: List[Dict[str, Any]] = []
    review_decision_summary: Dict[str, Any] = {}
    if args.apply_review_actions and review_actions_path:
        review_action_rows = parse_review_actions_file(review_actions_path)
        applied_actions_rows, rejected_actions_rows, override_audit_rows, review_decision_summary = apply_review_actions(
            action_rows=review_action_rows,
            valid_review_ids=[row.get("review_id", "") for row in baseline_snapshot.get("review_rows", [])],
            config_dir=CONFIG_DIR,
        )
        review_decision_summary = build_review_decision_summary(
            applied_rows=applied_actions_rows,
            rejected_rows=rejected_actions_rows,
            touched_files=review_decision_summary.get("touched_files", []),
        )
        LOGGER.info(
            "Applied reviewer actions: %s applied, %s rejected. Touched override files: %s",
            review_decision_summary.get("applied_total", 0),
            review_decision_summary.get("rejected_total", 0),
            ",".join(review_decision_summary.get("touched_files", [])),
        )

    provider_priority = expand_provider_priority(args.provider_priority, provider_config)
    LOGGER.info("Using provider priority: %s", provider_priority)
    mapping_override_entries = load_override_entries(CONFIG_DIR, "mapping")
    conflict_override_entries = load_override_entries(CONFIG_DIR, "conflict")
    period_override_entries = load_override_entries(CONFIG_DIR, "period")
    suppression_override_entries = load_override_entries(CONFIG_DIR, "suppression")
    placement_override_entries = load_override_entries(CONFIG_DIR, "placement")

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
    all_facts = apply_period_overrides(all_facts, period_override_entries)

    subjects, subject_sheet, header_row = load_template_subjects(template_path)
    alias_mapping = load_alias_mapping(CONFIG_DIR / "subject_aliases.yml", subjects)
    alias_mapping.extend(build_manual_alias_records(mapping_override_entries, subjects))
    relation_mapping = load_relation_mapping(CONFIG_DIR / "subject_relations.yml", subjects)
    all_facts = apply_local_mapping_overrides(all_facts, mapping_override_entries, subjects)
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
    facts_deduped = apply_suppression_overrides(facts_deduped, suppression_override_entries)
    facts_deduped = apply_placement_overrides(facts_deduped, placement_override_entries)

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
    if conflict_override_entries:
        facts_deduped, conflicts_enriched = apply_conflict_overrides(
            facts=facts_deduped,
            conflicts=conflicts_enriched,
            entries=conflict_override_entries,
            merge_enabled=args.enable_conflict_merge,
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
    review_items = filter_review_items_by_placement(review_items, placement_override_entries)
    review_summary["review_total"] = len(review_items)
    review_summary["reason_breakdown"] = build_reason_breakdown(review_items)
    review_summary["with_evidence_total"] = sum(
        1 for item in review_items if item.evidence_cell_path or item.evidence_row_path or item.evidence_table_path
    )

    reocr_tasks: List[ReOCRTaskRecord] = []
    reocr_summary: Dict[str, Any] = {}
    if args.emit_reocr_tasks:
        reocr_tasks, reocr_summary = build_reocr_tasks(review_items, conflicts_enriched, reocr_config)
    reocr_manifest_rows: List[Dict[str, Any]] = []
    reocr_manifest_summary: Dict[str, Any] = {}
    reocr_merge_audit_rows: List[Dict[str, Any]] = []
    reocr_merge_summary: Dict[str, Any] = {}
    if reocr_results_dir:
        facts_deduped, reocr_merge_audit_rows, reocr_merge_summary = ingest_reocr_results(
            tasks=reocr_tasks,
            results_dir=reocr_results_dir,
            review_items=review_items,
            facts=facts_deduped,
            merge_config=reocr_merge_rules,
        )
        if reocr_merge_summary.get("merged_total", 0):
            if effective_validation:
                validation_results, validation_summary = run_validation(facts_deduped, validation_config)
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
            review_items = filter_review_items_by_placement(review_items, placement_override_entries)
            review_summary["review_total"] = len(review_items)
            review_summary["reason_breakdown"] = build_reason_breakdown(review_items)
            review_summary["with_evidence_total"] = sum(
                1 for item in review_items if item.evidence_cell_path or item.evidence_row_path or item.evidence_table_path
            )
            if args.emit_reocr_tasks:
                reocr_tasks, reocr_summary = build_reocr_tasks(review_items, conflicts_enriched, reocr_config)
        LOGGER.info("Merged re-OCR results: %s", reocr_merge_summary.get("merged_total", 0))
    if args.materialize_reocr_inputs and args.emit_reocr_tasks:
        reocr_manifest_rows, reocr_manifest_summary = materialize_reocr_inputs(
            tasks=reocr_tasks,
            source_image_dir=source_image_dir,
            output_dir=output_dir,
        )
        LOGGER.info("Materialized re-OCR crops: %s", reocr_manifest_summary.get("materialized_total", 0))

    unique_pages = {(page.doc_id, page.page_no) for page in all_pages}
    pages_total = len(unique_pages)
    docs_total = len({page.doc_id for page in all_pages})
    pages_skipped_as_non_table = (
        sum(1 for record in page_selection_records if not record.is_candidate_table_page)
        if page_selection_records
        else max(pages_total - len(pages_with_tables), 0)
    )
    mapping_stats = summarize_mapping_stats(facts_deduped)
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
        applied_actions=applied_actions_rows,
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
    if export_stats.get("unplaced_rows"):
        write_dict_csv(output_dir / "unplaced_facts.csv", export_stats["unplaced_rows"], export_stats["unplaced_rows"][0].keys())
    if args.apply_review_actions or review_actions_path:
        write_dict_csv(
            output_dir / "applied_review_actions.csv",
            applied_actions_rows,
            ["action_id", "review_id", "action_type", "action_value", "target_id", "target_scope", "config_file_touched", "apply_timestamp", "apply_message"],
        )
        write_dict_csv(
            output_dir / "rejected_review_actions.csv",
            rejected_actions_rows,
            ["action_id", "review_id", "action_type", "action_value", "reject_reason"],
        )
        write_dict_csv(
            output_dir / "override_audit.csv",
            override_audit_rows,
            ["action_id", "review_id", "action_type", "target_id", "target_scope", "old_state", "new_state", "config_file_touched", "apply_timestamp", "apply_message"],
        )
        write_json(output_dir / "review_decision_summary.json", review_decision_summary)
    if reocr_manifest_rows:
        write_dict_csv(output_dir / "reocr_input_manifest.csv", reocr_manifest_rows, list(reocr_manifest_rows[0].keys()))
        write_json(output_dir / "reocr_input_manifest.json", {"summary": reocr_manifest_summary, "tasks": reocr_manifest_rows})
    if reocr_results_dir:
        write_dict_csv(
            output_dir / "reocr_merge_audit.csv",
            reocr_merge_audit_rows,
            list(reocr_merge_audit_rows[0].keys()) if reocr_merge_audit_rows else ["task_id", "status", "reason", "result_file"],
        )
        write_json(output_dir / "reocr_merge_summary.json", reocr_merge_summary)

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

    if args.emit_review_actions_template:
        export_review_actions_template(
            output_dir=output_dir,
            review_items=review_items,
            mapping_candidates=mapping_candidates,
            conflicts=conflicts_enriched,
            validations=validation_results,
            unmapped_summary=unmapped_labels_summary,
            reocr_tasks=reocr_tasks,
        )

    backlog_rows, opportunity_summary, mapping_opportunities = build_priority_backlog(
        review_rows=[dataclass_row(item) for item in review_items],
        unmapped_rows=[dataclass_row(item) for item in unmapped_labels_summary],
        reocr_rows=[dataclass_row(item) for item in reocr_tasks],
        priority_rules=priority_rules,
    )
    write_dict_csv(
        output_dir / "review_priority_backlog.csv",
        backlog_rows,
        list(backlog_rows[0].keys()) if backlog_rows else ["review_id", "row_label_std", "statement_type", "period_key", "value_num", "reason_codes", "priority_score", "has_mapping_candidate", "has_reocr_task", "occurrences_for_label"],
    )
    write_json(output_dir / "coverage_opportunity_summary.json", opportunity_summary)
    write_dict_csv(
        output_dir / "mapping_opportunities.csv",
        mapping_opportunities,
        list(mapping_opportunities[0].keys()) if mapping_opportunities else ["row_label_std", "occurrences", "amount_abs_total", "top_candidate_code", "top_candidate_name", "top_candidate_score"],
    )

    if args.emit_delta_report or args.apply_review_actions:
        after_snapshot = load_artifact_snapshot(output_dir)
        after_snapshot["run_summary"] = run_summary
        delta_payload = build_delta_reports(before=baseline_snapshot, after=after_snapshot)
        write_json(output_dir / "coverage_delta.json", delta_payload["coverage_delta"])
        write_dict_csv(output_dir / "coverage_delta.csv", delta_payload["coverage_rows"], ["metric", "before", "after", "delta"])
        write_dict_csv(
            output_dir / "review_delta.csv",
            delta_payload["review_delta_rows"],
            ["review_id", "status", "before_priority_score", "after_priority_score", "row_label_std", "period_key"],
        )
        write_json(output_dir / "export_delta_summary.json", delta_payload["export_delta_summary"])
        write_dict_csv(
            output_dir / "top_resolved_items.csv",
            delta_payload["top_resolved_items"],
            ["review_id", "status", "before_priority_score", "after_priority_score", "row_label_std", "period_key"],
        )
        write_dict_csv(
            output_dir / "top_remaining_unmapped.csv",
            delta_payload["top_remaining_unmapped"],
            list(delta_payload["top_remaining_unmapped"][0].keys()) if delta_payload["top_remaining_unmapped"] else ["row_label_std", "normalized_label", "occurrences", "numeric_occurrences", "amount_abs_total", "example_source_cell_ref", "top_candidate_code", "top_candidate_name", "top_candidate_score", "top_candidate_method", "meta_json"],
        )
        write_dict_csv(
            output_dir / "top_remaining_review_items.csv",
            delta_payload["top_remaining_review_items"],
            list(delta_payload["top_remaining_review_items"][0].keys()) if delta_payload["top_remaining_review_items"] else ["review_id", "priority_score", "reason_codes", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "period_key", "value_raw", "value_num", "provider"],
        )

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
        "review_decision_summary": review_decision_summary,
        "reocr_merge_summary": reocr_merge_summary,
        "reocr_manifest_summary": reocr_manifest_summary,
    }
    write_json(output_dir / "summary.json", summary)
    LOGGER.info("Review items queued: %s", review_summary.get("review_total", 0))
    LOGGER.info("Unplaced facts routed away from main sheet: %s", export_stats.get("unplaced_count", 0))
    LOGGER.info("Conflict decision breakdown: %s", json.dumps(run_summary.get("conflict_decision_breakdown", {}), ensure_ascii=False))
    LOGGER.info("Integrity fail total: %s", integrity_summary.get("integrity_fail_total", 0))
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


def summarize_mapping_stats(facts: Sequence[FactRecord]) -> Dict[str, int]:
    stats = {
        "mapped_by_exact": 0,
        "mapped_by_alias": 0,
        "mapped_by_relation": 0,
        "unmapped_total": 0,
    }
    for fact in facts:
        if fact.status == "suppressed":
            continue
        if not fact.mapping_code:
            stats["unmapped_total"] += 1
            continue
        if fact.mapping_method == "exact":
            stats["mapped_by_exact"] += 1
        elif "alias" in (fact.mapping_method or "") or fact.mapping_method == "manual_alias":
            stats["mapped_by_alias"] += 1
        elif fact.mapping_relation_type:
            stats["mapped_by_relation"] += 1
        else:
            stats["mapped_by_alias"] += 1
    return stats


def build_reason_breakdown(review_items: Sequence[ReviewQueueRecord]) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for item in review_items:
        for reason in item.reason_codes:
            counter[reason] = counter.get(reason, 0) + 1
    return counter


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
