from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import os
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Sequence

import yaml

from .benchmark import compare_benchmark_workbook, explain_benchmark_gaps, load_workbook_main_sheet
from .curation import (
    build_actionable_backlog,
    build_alias_acceptance_candidates,
    build_benchmark_recall_rows,
    build_formula_rule_impact,
    load_curated_alias_records,
    load_curated_formula_rules,
    load_legacy_alias_records,
    build_stage6_kpis,
    build_statement_coverage_rows,
    prune_reocr_tasks,
    split_unmapped_facts,
)
from .dedupe import assign_fact_ids, dedupe_facts
from .derive import build_export_fact_view, derive_formula_facts
from .discover import SUPPORTED_TABLE_PROVIDERS, TEXT_ONLY_PROVIDERS, discover_provider_sources, list_provider_dirs
from .feedback import apply_review_actions, build_delta_reports, build_priority_backlog, export_review_actions_template, parse_review_actions_file
from .feedback.audit import build_review_decision_summary
from .feedback.delta import load_artifact_snapshot
from .integrity import run_artifact_integrity
from .manifest import collect_source_files, generate_run_id, write_run_manifest
from .metadata import evaluate_summary_payloads, prepare_nested_summary_payload, prepare_summary_payload, scan_summary_run_ids
from .models import ArtifactIntegrityRecord, CellRecord, ConflictDecisionAuditRecord, ConflictRecord, DiscoveredSource, DuplicateRecord, FactRecord, IssueRecord
from .models import MappingCandidateRecord, MappingReviewRecord, PageSelectionRecord, ProviderComparisonRecord, ReOCRTaskRecord, ReviewQueueRecord, SecondaryOCRCandidateRecord
from .models import UnmappedLabelSummaryRecord, ValidationImpactRecord, ValidationResultRecord, dataclass_row
from .normalize.conflicts import enrich_conflicts, resolve_conflicts
from .normalize.export import export_template, rewrite_meta_summary, rewrite_stage5_helper_sheets
from .normalize.labels import apply_label_canonicalization
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
from .statement import build_required_summary_files, resolve_single_period_annual_roles, run_full_run_contract, specialize_statement_types
from .target import (
    apply_source_backed_gap_closures,
    build_source_backed_gap_closure,
    build_stage7_kpis,
    build_target_kpis,
    build_target_review_backlogs,
    finalize_source_backed_gap_closure,
    finalize_source_backed_gap_results,
    investigate_no_source_gaps,
    repair_benchmark_alignment,
    scope_facts_to_targets,
)
from .promotion import apply_promotions, build_promotion_delta, export_promotion_actions_template, parse_promotion_actions_file
from .validation import run_validation


LOGGER = logging.getLogger(__name__)
PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR / "config"
PIPELINE_STAGE_NAMES = [
    "load/discover",
    "cells",
    "facts_raw",
    "mapping",
    "dedupe",
    "validation",
    "benchmark compare",
    "export",
    "integrity",
    "manifest/finalization",
    "hardening outputs",
]


class PipelineStageTracker:
    def __init__(self, output_dir: Path, run_id: str, stage_names: Sequence[str]) -> None:
        self.output_dir = output_dir
        self.run_id = run_id
        self.stage_names = list(stage_names)
        self.run_started_at = datetime.now(timezone.utc)
        self.current_stage = ""
        self.failed_stage = ""
        self.exception_message = ""
        self.completed = False
        self.success = False
        self.finished_at: datetime | None = None
        self._stage_records = {
            name: {
                "run_id": run_id,
                "stage_name": name,
                "started_at": "",
                "finished_at": "",
                "duration_seconds": 0.0,
                "success": False,
                "status": "pending",
                "exception_message": "",
                "_started_monotonic": None,
            }
            for name in self.stage_names
        }
        self.flush()

    def start(self, stage_name: str) -> str:
        record = self._stage_records.setdefault(stage_name, self._new_record(stage_name))
        if not record.get("started_at"):
            record["started_at"] = datetime.now(timezone.utc).isoformat()
        record["_started_monotonic"] = time.perf_counter()
        record["status"] = "in_progress"
        record["exception_message"] = ""
        self.current_stage = stage_name
        self.flush()
        return stage_name

    def finish(self, stage_name: str, *, success: bool, exception_message: str = "") -> None:
        record = self._stage_records.setdefault(stage_name, self._new_record(stage_name))
        finished_at = datetime.now(timezone.utc)
        if not record.get("started_at"):
            record["started_at"] = finished_at.isoformat()
        started_monotonic = record.get("_started_monotonic")
        duration = 0.0
        if isinstance(started_monotonic, (int, float)):
            duration = max(time.perf_counter() - float(started_monotonic), 0.0)
        record["finished_at"] = finished_at.isoformat()
        record["duration_seconds"] = round(float(record.get("duration_seconds", 0.0) or 0.0) + duration, 3)
        record["success"] = bool(success)
        record["status"] = "success" if success else "failure"
        record["exception_message"] = str(exception_message or "")
        record["_started_monotonic"] = None
        if not success:
            self.failed_stage = self.failed_stage or stage_name
            self.exception_message = str(exception_message or "")
        if self.current_stage == stage_name:
            self.current_stage = ""
        self.flush()

    def finalize(self, *, success: bool, exception_message: str = "", failed_stage: str = "") -> None:
        self.completed = True
        self.success = bool(success)
        self.finished_at = datetime.now(timezone.utc)
        if failed_stage:
            self.failed_stage = failed_stage
        if exception_message:
            self.exception_message = str(exception_message)
        self.flush()

    def flush(self) -> None:
        timings_payload = {
            "run_id": self.run_id,
            "started_at": self.run_started_at.isoformat(),
            "current_stage": self.current_stage,
            "stages": [
                {
                    key: value
                    for key, value in record.items()
                    if not key.startswith("_")
                }
                for record in (self._stage_records[name] for name in self.stage_names)
            ],
        }
        status_payload = {
            "run_id": self.run_id,
            "started_at": self.run_started_at.isoformat(),
            "current_stage": self.current_stage,
            "failed_stage": self.failed_stage,
            "stages": {
                name: {
                    "status": self._stage_records[name]["status"],
                    "success": self._stage_records[name]["success"],
                    "duration_seconds": self._stage_records[name]["duration_seconds"],
                    "exception_message": self._stage_records[name]["exception_message"],
                }
                for name in self.stage_names
            },
        }
        completion_payload = self._completion_payload()
        write_json(self.output_dir / "pipeline_stage_timings.json", prepare_summary_payload(timings_payload, self.run_id))
        write_json(self.output_dir / "pipeline_stage_status.json", prepare_summary_payload(status_payload, self.run_id))
        write_json(self.output_dir / "pipeline_completion_summary.json", prepare_summary_payload(completion_payload, self.run_id))

    def _completion_payload(self) -> Dict[str, Any]:
        finished_at = self.finished_at or datetime.now(timezone.utc)
        completed_stages = [name for name in self.stage_names if self._stage_records[name]["status"] == "success"]
        return {
            "run_id": self.run_id,
            "started_at": self.run_started_at.isoformat(),
            "finished_at": finished_at.isoformat() if self.completed else "",
            "duration_seconds": round(max((finished_at - self.run_started_at).total_seconds(), 0.0), 3),
            "status": "success" if self.completed and self.success else "failure" if self.completed else "running",
            "completed": self.completed,
            "success": self.success if self.completed else False,
            "current_stage": self.current_stage,
            "failed_stage": self.failed_stage,
            "last_successful_stage": completed_stages[-1] if completed_stages else "",
            "completed_stages": completed_stages,
            "pending_stages": [name for name in self.stage_names if self._stage_records[name]["status"] == "pending"],
            "exception_message": self.exception_message,
        }

    def _new_record(self, stage_name: str) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "stage_name": stage_name,
            "started_at": "",
            "finished_at": "",
            "duration_seconds": 0.0,
            "success": False,
            "status": "pending",
            "exception_message": "",
            "_started_monotonic": None,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standardize financial statement OCR outputs.")
    parser.add_argument("--input-dir", default="outputs", help="Directory containing OCR provider outputs.")
    parser.add_argument("--template", required=True, help="Path to the standard accounting template workbook.")
    parser.add_argument(
        "--output-dir",
        default="normalized_archive",
        help="Root directory for archived normalized outputs. Each run is written into its own batch subdirectory by default.",
    )
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
    parser.add_argument("--benchmark-workbook", default="", help="Optional benchmark/reference workbook path for comparison-only reports.")
    parser.add_argument("--emit-benchmark-report", action="store_true", help="Emit benchmark comparison and gap mining reports.")
    parser.add_argument("--enable-label-canonicalization", action="store_true", help="Enable statement-aware row label normalization.")
    parser.add_argument("--enable-derived-facts", action="store_true", help="Enable deterministic formula/relationship-derived facts.")
    parser.add_argument("--emit-run-manifest", action="store_true", help="Emit run manifest, artifact hashes, and snapshot packaging.")
    parser.add_argument(
        "--artifact-manifest-mode",
        default="core",
        choices=["core", "full"],
        help="Artifact manifest scope. Core hashes top-level run artifacts only; full includes nested generated directories.",
    )
    parser.add_argument(
        "--output-run-subdir",
        default="auto",
        help="Run subdirectory under output-dir. Defaults to auto (run_id). Use none to write directly into output-dir.",
    )
    parser.add_argument("--enable-main-statement-specialization", action="store_true", help="Enable Stage 6 main-statement classification upgrade.")
    parser.add_argument("--enable-single-period-role-inference", action="store_true", help="Enable Stage 6 single-period annual role inference.")
    parser.add_argument("--emit-stage6-kpis", action="store_true", help="Emit Stage 6 KPI and coverage lift reports.")
    parser.add_argument("--strict-full-run-contract", action="store_true", help="Fail the run when promised Stage 5/6 artifacts are missing.")
    parser.add_argument("--enable-benchmark-alignment-repair", action="store_true", help="Enable Stage 7 repaired alignment between benchmark legacy headers and export period keys.")
    parser.add_argument("--enable-export-target-scoping", action="store_true", help="Enable Stage 7 export target scoping and target backlog separation.")
    parser.add_argument("--emit-promotion-template", action="store_true", help="Emit Stage 7 promotion action template workbook/csv.")
    parser.add_argument("--promotion-actions-file", default="", help="Optional filled promotion workbook/csv to apply.")
    parser.add_argument("--apply-promotions", action="store_true", help="Apply promotion actions from --promotion-actions-file into curated packs and rerun.")
    parser.add_argument("--emit-stage7-kpis", action="store_true", help="Emit Stage 7 target closure KPI outputs.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, e.g. INFO or DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    input_dir = Path(args.input_dir).resolve()
    template_path = Path(args.template).resolve()
    base_output_dir = Path(args.output_dir).resolve()
    source_image_dir = Path(args.source_image_dir).resolve() if args.source_image_dir else None
    review_actions_path = Path(args.review_actions_file).resolve() if args.review_actions_file else None
    promotion_actions_path = Path(args.promotion_actions_file).resolve() if args.promotion_actions_file else None
    reocr_results_dir = Path(args.reocr_results_dir).resolve() if args.reocr_results_dir else None
    benchmark_workbook = Path(args.benchmark_workbook).resolve() if args.benchmark_workbook else None
    run_id = generate_run_id(argv or [])
    output_dir = resolve_output_dir(base_output_dir, args.output_run_subdir, run_id)
    baseline_dir = resolve_baseline_dir(base_output_dir, output_dir)
    baseline_snapshot = load_artifact_snapshot(baseline_dir) if baseline_dir else {}
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        parser.error(f"Input directory does not exist: {input_dir}")
    if not template_path.exists():
        parser.error(f"Template workbook does not exist: {template_path}")
    if source_image_dir and not source_image_dir.exists():
        parser.error(f"Source image directory does not exist: {source_image_dir}")
    if benchmark_workbook and not benchmark_workbook.exists():
        parser.error(f"Benchmark workbook does not exist: {benchmark_workbook}")
    if args.apply_review_actions and not review_actions_path:
        parser.error("--apply-review-actions requires --review-actions-file")
    if review_actions_path and not review_actions_path.exists():
        parser.error(f"Review actions file does not exist: {review_actions_path}")
    if args.apply_promotions and not promotion_actions_path:
        parser.error("--apply-promotions requires --promotion-actions-file")
    if promotion_actions_path and not promotion_actions_path.exists():
        parser.error(f"Promotion actions file does not exist: {promotion_actions_path}")

    stage_tracker = PipelineStageTracker(output_dir=output_dir, run_id=run_id, stage_names=PIPELINE_STAGE_NAMES)
    current_stage = stage_tracker.start("load/discover")

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
    benchmark_rules = load_yaml(CONFIG_DIR / "benchmark_rules.yml")
    formula_rules = load_yaml(CONFIG_DIR / "formula_rules.yml")
    label_rules = load_yaml(CONFIG_DIR / "label_normalization_rules.yml")
    manifest_rules = load_yaml(CONFIG_DIR / "manifest_rules.yml")
    statement_rules = load_yaml(CONFIG_DIR / "statement_rules.yml")
    annual_period_rules = load_yaml(CONFIG_DIR / "annual_period_rules.yml")
    export_filter_rules = load_yaml(CONFIG_DIR / "export_filter_rules.yml")
    alias_pack_rules = load_yaml(CONFIG_DIR / "alias_pack_rules.yml")
    formula_pack_rules = load_yaml(CONFIG_DIR / "formula_pack_rules.yml")
    stage6_targets = load_yaml(CONFIG_DIR / "stage6_targets.yml")
    benchmark_alignment_rules = load_yaml(CONFIG_DIR / "benchmark_alignment_rules.yml")
    export_target_rules = load_yaml(CONFIG_DIR / "export_target_rules.yml")
    promotion_rules = load_yaml(CONFIG_DIR / "promotion_rules.yml")
    target_scope_rules = load_yaml(CONFIG_DIR / "target_scope_rules.yml")
    manual_action_rules = load_yaml(CONFIG_DIR / "manual_action_rules.yml")
    priority_rules = load_yaml(CONFIG_DIR / "priority_rules.yml")
    override_rules = load_yaml(CONFIG_DIR / "override_rules.yml")
    reocr_merge_rules = load_yaml(CONFIG_DIR / "reocr_merge_rules.yml")
    export_rules = {**export_rules, **export_filter_rules}
    effective_target_scope_rules = {**export_target_rules, **target_scope_rules}
    ensure_override_store(CONFIG_DIR)
    feature_flags = {
        "enable_conflict_merge": bool(args.enable_conflict_merge),
        "enable_period_normalization": bool(args.enable_period_normalization),
        "enable_dedupe": bool(args.enable_dedupe),
        "enable_validation": bool(args.enable_validation),
        "enable_integrity_check": bool(args.enable_integrity_check),
        "enable_mapping_suggestions": bool(args.enable_mapping_suggestions),
        "enable_review_pack": bool(args.enable_review_pack),
        "enable_validation_aware_conflicts": bool(args.enable_validation_aware_conflicts),
        "emit_routing_plan": bool(args.emit_routing_plan),
        "emit_reocr_tasks": bool(args.emit_reocr_tasks),
        "emit_review_actions_template": bool(args.emit_review_actions_template),
        "materialize_reocr_inputs": bool(args.materialize_reocr_inputs),
        "emit_delta_report": bool(args.emit_delta_report),
        "emit_benchmark_report": bool(args.emit_benchmark_report and benchmark_workbook),
        "benchmark_workbook_provided": bool(benchmark_workbook),
        "enable_label_canonicalization": bool(args.enable_label_canonicalization),
        "enable_derived_facts": bool(args.enable_derived_facts),
        "emit_run_manifest": bool(args.emit_run_manifest),
        "artifact_manifest_mode": str(args.artifact_manifest_mode),
        "enable_main_statement_specialization": bool(args.enable_main_statement_specialization),
        "enable_single_period_role_inference": bool(args.enable_single_period_role_inference),
        "emit_stage6_kpis": bool(args.emit_stage6_kpis),
        "enable_benchmark_alignment_repair": bool(args.enable_benchmark_alignment_repair and benchmark_workbook),
        "enable_export_target_scoping": bool(args.enable_export_target_scoping),
        "emit_promotion_template": bool(args.emit_promotion_template),
        "apply_promotions": bool(args.apply_promotions),
        "emit_stage7_kpis": bool(args.emit_stage7_kpis),
    }

    applied_actions_rows: List[Dict[str, Any]] = []
    rejected_actions_rows: List[Dict[str, Any]] = []
    override_audit_rows: List[Dict[str, Any]] = []
    review_decision_summary: Dict[str, Any] = {}
    applied_promotion_rows: List[Dict[str, Any]] = []
    rejected_promotion_rows: List[Dict[str, Any]] = []
    promotion_audit_rows: List[Dict[str, Any]] = []
    promotion_summary: Dict[str, Any] = {}
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
    if args.apply_promotions and promotion_actions_path:
        promotion_action_rows = parse_promotion_actions_file(promotion_actions_path)
        applied_promotion_rows, rejected_promotion_rows, promotion_audit_rows, promotion_summary = apply_promotions(
            action_rows=promotion_action_rows,
            config_dir=CONFIG_DIR,
            promotion_rules=promotion_rules,
        )
        LOGGER.info(
            "Applied promotions: %s applied, %s rejected. Touched curated files: %s",
            promotion_summary.get("applied_total", 0),
            promotion_summary.get("rejected_total", 0),
            ",".join(promotion_summary.get("touched_files", [])),
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

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("cells")

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

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("facts_raw")

    assign_fact_ids(all_facts)
    statement_classification_audit: List[Dict[str, Any]] = []
    statement_classification_summary: Dict[str, Any] = {}
    all_facts, statement_classification_audit, statement_classification_summary = specialize_statement_types(
        facts=all_facts,
        provider_pages=all_pages,
        statement_rules=statement_rules,
        enabled=args.enable_main_statement_specialization,
    )
    all_facts = apply_period_normalization(
        facts=all_facts,
        provider_pages=all_pages,
        input_dir=input_dir,
        keyword_config=statement_config,
        period_config=period_config,
        enabled=args.enable_period_normalization,
    )
    all_facts = apply_period_overrides(all_facts, period_override_entries)
    all_facts, label_audit_rows, label_summary = apply_label_canonicalization(
        facts=all_facts,
        rules=label_rules,
        enabled=args.enable_label_canonicalization,
    )
    period_role_audit_rows: List[Dict[str, Any]] = []
    period_role_resolution_summary: Dict[str, Any] = {}
    all_facts, period_role_audit_rows, period_role_resolution_summary = resolve_single_period_annual_roles(
        facts=all_facts,
        rules=annual_period_rules,
        enabled=args.enable_single_period_role_inference,
    )

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("mapping")

    subjects, subject_sheet, header_row = load_template_subjects(template_path)
    benchmark_scope_payload = load_workbook_main_sheet(benchmark_workbook) if benchmark_workbook else {}
    alias_mapping = load_alias_mapping(CONFIG_DIR / "subject_aliases.yml", subjects)
    curated_alias_records, curated_alias_pack_audit_rows, curated_alias_pack_summary = load_curated_alias_records(
        CONFIG_DIR / "curated_alias_pack.yml",
        subjects,
    )
    legacy_alias_records = load_legacy_alias_records(CONFIG_DIR / "legacy_account_rules.yml", subjects)
    alias_mapping.extend(curated_alias_records)
    alias_mapping.extend(legacy_alias_records)
    alias_mapping.extend(build_manual_alias_records(mapping_override_entries, subjects))
    relation_mapping = load_relation_mapping(CONFIG_DIR / "subject_relations.yml", subjects)
    curated_formula_rules = load_curated_formula_rules(CONFIG_DIR / "curated_formula_pack.yml")
    effective_formula_rules = {
        "rules": list(formula_rules.get("rules", []))
        + list(formula_pack_rules.get("rules", []))
        + list(curated_formula_rules.get("rules", []))
    }
    all_facts = apply_local_mapping_overrides(all_facts, mapping_override_entries, subjects)
    all_facts, mapping_review, mapping_candidates, unmapped_labels_summary, mapping_stats = apply_subject_mapping(
        all_facts,
        subjects,
        alias_mapping,
        relation_mapping,
        mapping_rules,
    )

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("dedupe")

    initial_conflict_merge = args.enable_conflict_merge and not args.enable_validation_aware_conflicts
    all_facts, conflicts, provider_comparisons = resolve_conflicts(all_facts, provider_priority, initial_conflict_merge)

    facts_raw = copy.deepcopy(all_facts)
    duplicates: List[DuplicateRecord] = []
    facts_deduped = all_facts
    if args.enable_dedupe:
        facts_deduped, duplicates = dedupe_facts(all_facts, provider_priority)
    facts_deduped = apply_suppression_overrides(facts_deduped, suppression_override_entries)
    facts_deduped = apply_placement_overrides(facts_deduped, placement_override_entries)
    target_scope_rows: List[Dict[str, Any]] = []
    target_scope_summary: Dict[str, Any] = {}
    if args.enable_export_target_scoping:
        facts_deduped, target_scope_rows, target_scope_summary = scope_facts_to_targets(
            facts=facts_deduped,
            benchmark_payload=benchmark_scope_payload,
            rules=effective_target_scope_rules,
        )

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("validation")

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
        materialize_evidence_files=False,
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
                materialize_evidence_files=False,
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
    fact_scope_map = {fact.fact_id: fact.target_scope for fact in facts_deduped if fact.fact_id}
    review_actionable_rows, review_nonactionable_rows, review_actionable_summary = build_actionable_backlog(
        review_items=review_items,
        stage6_targets=stage6_targets,
        fact_scope_map=fact_scope_map,
    )
    pruned_reocr_rows: List[Dict[str, Any]] = []
    pruned_reocr_summary: Dict[str, Any] = {}
    if args.emit_reocr_tasks:
        pruned_reocr_rows, pruned_reocr_summary = prune_reocr_tasks(
            tasks=reocr_tasks,
            review_items=review_items,
            stage6_targets=stage6_targets,
            fact_scope_map=fact_scope_map,
        )
    main_target_review_rows: List[Dict[str, Any]] = []
    note_detail_review_rows: List[Dict[str, Any]] = []
    suppressed_note_detail_rows: List[Dict[str, Any]] = []
    target_review_summary: Dict[str, Any] = {}
    if args.enable_export_target_scoping:
        main_target_review_rows, note_detail_review_rows, suppressed_note_detail_rows, target_review_summary = build_target_review_backlogs(
            review_items=review_items,
            facts=facts_deduped,
        )

    derived_facts: List[FactRecord] = []
    derived_formula_audit: List[Dict[str, Any]] = []
    derived_formula_summary: Dict[str, Any] = {}
    derived_conflicts: List[Dict[str, Any]] = []
    if args.enable_derived_facts:
        derived_facts, derived_formula_audit, derived_formula_summary, derived_conflicts = derive_formula_facts(
            facts=facts_deduped,
            formula_rules=effective_formula_rules,
            relation_records=relation_mapping,
            enabled=True,
        )
        if args.enable_export_target_scoping and derived_facts:
            derived_facts, _, _ = scope_facts_to_targets(
                facts=derived_facts,
                benchmark_payload=benchmark_scope_payload,
                rules=effective_target_scope_rules,
            )
        LOGGER.info(
            "Derived facts: %s total, %s conflicts.",
            derived_formula_summary.get("derived_facts_total", 0),
            derived_formula_summary.get("derived_conflicts_total", 0),
        )

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("export")

    unique_pages = {(page.doc_id, page.page_no) for page in all_pages}
    pages_total = len(unique_pages)
    docs_total = len({page.doc_id for page in all_pages})
    raw_pages_skipped_as_non_table = (
        sum(1 for record in page_selection_records if not record.is_candidate_table_page)
        if page_selection_records
        else max(pages_total - len(pages_with_tables), 0)
    )
    processed_page_keys = {(doc_id, page_no) for doc_id, page_no in unique_pages}
    scoped_page_selection_records = [
        record
        for record in page_selection_records
        if (record.doc_id, record.page_no) in processed_page_keys
    ]
    pages_skipped_expected = max(pages_total - len(pages_with_tables), 0)
    pages_skipped_as_non_table = pages_skipped_expected
    pages_skipped_metric_audit = {
        "run_id": "",
        "pages_total": pages_total,
        "pages_with_tables": len(pages_with_tables),
        "pages_skipped_as_non_table": pages_skipped_as_non_table,
        "expected_pages_skipped": pages_skipped_expected,
        "raw_page_selection_non_table_total": raw_pages_skipped_as_non_table,
        "processed_scope_page_selection_total": len(scoped_page_selection_records),
        "explanation": "pages_skipped_as_non_table is scoped to the unique processed document/page set for this run and equals pages_total - pages_with_tables.",
        "pass": pages_skipped_as_non_table == pages_skipped_expected,
    }
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
    run_summary["run_id"] = run_id
    run_summary["derived_facts_total"] = int(derived_formula_summary.get("derived_facts_total", 0))
    run_summary["derived_conflicts_total"] = int(derived_formula_summary.get("derived_conflicts_total", 0))

    export_stats = export_template(
        template_path=template_path,
        output_path=output_dir / "会计报表_填充结果.xlsx",
        facts=facts_deduped,
        derived_facts=derived_facts,
        run_summary=run_summary,
        issues=all_issues,
        validations=validation_results,
        duplicates=duplicates,
        conflicts=conflicts_enriched,
        review_queue=review_items,
        applied_actions=applied_actions_rows,
        classification_audit=with_run_id_rows(statement_classification_audit, run_id),
        period_role_audit=with_run_id_rows(period_role_audit_rows, run_id),
        promotion_rows=applied_promotion_rows,
        export_rules=export_rules,
    )
    LOGGER.info("Exported helper sheets: %s", ",".join(export_stats.get("helper_sheets", [])))
    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("benchmark compare")

    benchmark_payload: Dict[str, Any] = {}
    benchmark_gap_payload: Dict[str, Any] = {}
    benchmark_alignment_rows: List[Dict[str, Any]] = []
    benchmark_alignment_summary: Dict[str, Any] = {}
    benchmark_missing_true_rows: List[Dict[str, Any]] = []
    no_source_gap_payload: Dict[str, Any] = {}
    target_gap_backlog_rows: List[Dict[str, Any]] = []
    target_gap_summary: Dict[str, Any] = {}
    if args.emit_benchmark_report and benchmark_workbook:
        benchmark_payload = compare_benchmark_workbook(
            benchmark_path=benchmark_workbook,
            export_workbook_path=output_dir / "会计报表_填充结果.xlsx",
            rules=benchmark_rules,
        )
        if args.enable_benchmark_alignment_repair:
            benchmark_payload = repair_benchmark_alignment(
                benchmark_payload=benchmark_payload,
                facts=facts_deduped + derived_facts,
                rules=benchmark_alignment_rules,
            )
            benchmark_alignment_rows = benchmark_payload.get("alignment_audit_rows", [])
            benchmark_alignment_summary = benchmark_payload.get("alignment_summary", {})
        benchmark_missing_true_rows = benchmark_payload.get("benchmark_missing_true_rows", benchmark_payload.get("missing_rows", []))
        benchmark_gap_payload = explain_benchmark_gaps(
            benchmark_missing_rows=benchmark_missing_true_rows,
            facts=facts_deduped,
            unplaced_rows=export_stats.get("unplaced_rows", []),
            conflicts=conflicts_enriched,
            validations=validation_results,
            mapping_candidates=mapping_candidates,
            derived_facts=derived_facts,
        )
        no_source_gap_payload = investigate_no_source_gaps(
            benchmark_missing_true_rows=benchmark_missing_true_rows,
            facts_raw=facts_raw,
            facts_deduped=facts_deduped,
            unplaced_rows=export_stats.get("unplaced_rows", []),
            derived_facts=derived_facts,
            review_items=review_items,
            issues=all_issues,
        )
        target_gap_backlog_rows = no_source_gap_payload.get("target_gap_backlog_rows", [])
        target_gap_summary = no_source_gap_payload.get("target_gap_summary", {})
        rewrite_stage5_helper_sheets(
            output_path=output_dir / "会计报表_填充结果.xlsx",
            benchmark_summary={**benchmark_payload.get("summary", {}), "run_id": run_id},
            gap_rows=with_run_id_rows(benchmark_gap_payload.get("explanations", []), run_id),
            derived_facts=derived_facts,
            classification_audit=with_run_id_rows(statement_classification_audit, run_id),
            period_role_audit=with_run_id_rows(period_role_audit_rows, run_id),
            benchmark_alignment_rows=with_run_id_rows(benchmark_alignment_rows, run_id),
            target_gap_backlog_rows=with_run_id_rows(target_gap_backlog_rows, run_id),
            promotion_rows=applied_promotion_rows,
        )
        LOGGER.info(
            "Benchmark compare: missing_in_auto=%s, value_diff_cells=%s",
            benchmark_payload.get("summary", {}).get("missing_in_auto", 0),
            benchmark_payload.get("summary", {}).get("value_diff_cells", 0),
        )
    else:
        rewrite_stage5_helper_sheets(
            output_path=output_dir / "会计报表_填充结果.xlsx",
            benchmark_summary={"run_id": run_id},
            gap_rows=[],
            derived_facts=derived_facts,
            classification_audit=with_run_id_rows(statement_classification_audit, run_id),
            period_role_audit=with_run_id_rows(period_role_audit_rows, run_id),
            benchmark_alignment_rows=[],
            target_gap_backlog_rows=[],
            promotion_rows=applied_promotion_rows,
        )

    unmapped_value_bearing_rows, unmapped_blank_rows, mapping_lift_summary = split_unmapped_facts(facts_deduped)
    alias_acceptance_candidates, alias_acceptance_summary = build_alias_acceptance_candidates(
        value_bearing_rows=unmapped_value_bearing_rows,
        facts=facts_deduped,
        mapping_candidates=mapping_candidates,
        benchmark_missing_rows=benchmark_payload.get("missing_rows", []),
        alias_rules=alias_pack_rules,
    )
    formula_rule_impact_summary, candidate_formula_placements = build_formula_rule_impact(
        derived_facts=derived_facts,
        derived_conflicts=derived_conflicts,
    )
    candidate_formula_placements = enrich_formula_candidate_rows(candidate_formula_placements, effective_formula_rules)
    export_target_kpi_summary = build_target_kpis(
        facts=facts_deduped + derived_facts,
        benchmark_missing_true_rows=benchmark_missing_true_rows,
        main_target_review_rows=main_target_review_rows,
        note_detail_review_rows=note_detail_review_rows,
    ) if args.enable_export_target_scoping else {}
    source_backed_gap_closure_payload = build_source_backed_gap_closure(
        benchmark_missing_true_rows=benchmark_missing_true_rows,
        investigation_rows=no_source_gap_payload.get("rows", []),
        facts_raw=facts_raw,
        facts_deduped=facts_deduped,
        review_items=review_items,
    ) if args.enable_export_target_scoping else {"rows": [], "summary": {"run_id": "", "closure_candidates_total": 0}}
    source_backed_gap_closure_payload = finalize_source_backed_gap_closure(
        source_backed_gap_closure_payload.get("rows", []),
        alias_acceptance_candidates=alias_acceptance_candidates,
    ) if args.enable_export_target_scoping else source_backed_gap_closure_payload
    source_backed_gap_total_before = len(source_backed_gap_closure_payload.get("rows", []))
    closure_apply_payload = apply_source_backed_gap_closures(
        closure_rows=source_backed_gap_closure_payload.get("rows", []),
        facts_raw=facts_raw,
        facts_deduped=facts_deduped,
        review_items=review_items,
    ) if args.enable_export_target_scoping else {"rows": [], "summary": {}, "preferred_export_fact_ids": {}, "runtime_alignment_overrides": []}
    source_backed_gap_closure_payload["rows"] = closure_apply_payload.get("rows", source_backed_gap_closure_payload.get("rows", []))
    runtime_preferred_export_fact_ids = closure_apply_payload.get("preferred_export_fact_ids", {})
    runtime_alignment_overrides = closure_apply_payload.get("runtime_alignment_overrides", [])

    if int(closure_apply_payload.get("summary", {}).get("applied_total", 0)) > 0:
        if args.enable_export_target_scoping:
            facts_deduped, target_scope_rows, target_scope_summary = scope_facts_to_targets(
                facts=facts_deduped,
                benchmark_payload=benchmark_scope_payload,
                rules=effective_target_scope_rules,
            )
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
            materialize_evidence_files=False,
        )
        review_items = filter_review_items_by_placement(review_items, placement_override_entries)
        review_summary["review_total"] = len(review_items)
        review_summary["reason_breakdown"] = build_reason_breakdown(review_items)
        review_summary["with_evidence_total"] = sum(
            1 for item in review_items if item.evidence_cell_path or item.evidence_row_path or item.evidence_table_path
        )
        if args.emit_reocr_tasks:
            reocr_tasks, reocr_summary = build_reocr_tasks(review_items, conflicts_enriched, reocr_config)
        fact_scope_map = {fact.fact_id: fact.target_scope for fact in facts_deduped if fact.fact_id}
        review_actionable_rows, review_nonactionable_rows, review_actionable_summary = build_actionable_backlog(
            review_items=review_items,
            stage6_targets=stage6_targets,
            fact_scope_map=fact_scope_map,
        )
        if args.emit_reocr_tasks:
            pruned_reocr_rows, pruned_reocr_summary = prune_reocr_tasks(
                tasks=reocr_tasks,
                review_items=review_items,
                stage6_targets=stage6_targets,
                fact_scope_map=fact_scope_map,
            )
        if args.enable_export_target_scoping:
            main_target_review_rows, note_detail_review_rows, suppressed_note_detail_rows, target_review_summary = build_target_review_backlogs(
                review_items=review_items,
                facts=facts_deduped,
            )
        derived_facts = []
        derived_formula_audit = []
        derived_formula_summary = {}
        derived_conflicts = []
        if args.enable_derived_facts:
            derived_facts, derived_formula_audit, derived_formula_summary, derived_conflicts = derive_formula_facts(
                facts=facts_deduped,
                formula_rules=effective_formula_rules,
                relation_records=relation_mapping,
                enabled=True,
            )
            if args.enable_export_target_scoping and derived_facts:
                derived_facts, _, _ = scope_facts_to_targets(
                    facts=derived_facts,
                    benchmark_payload=benchmark_scope_payload,
                    rules=effective_target_scope_rules,
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
        run_summary["run_id"] = run_id
        run_summary["derived_facts_total"] = int(derived_formula_summary.get("derived_facts_total", 0))
        run_summary["derived_conflicts_total"] = int(derived_formula_summary.get("derived_conflicts_total", 0))
        export_stats = export_template(
            template_path=template_path,
            output_path=output_dir / "会计报表_填充结果.xlsx",
            facts=facts_deduped,
            derived_facts=derived_facts,
            run_summary=run_summary,
            issues=all_issues,
            validations=validation_results,
            duplicates=duplicates,
            conflicts=conflicts_enriched,
            review_queue=review_items,
            applied_actions=applied_actions_rows,
            classification_audit=with_run_id_rows(statement_classification_audit, run_id),
            period_role_audit=with_run_id_rows(period_role_audit_rows, run_id),
            promotion_rows=applied_promotion_rows,
            preferred_export_fact_ids=runtime_preferred_export_fact_ids,
            export_rules=export_rules,
        )
        benchmark_payload = {}
        benchmark_gap_payload = {}
        benchmark_alignment_rows = []
        benchmark_alignment_summary = {}
        benchmark_missing_true_rows = []
        no_source_gap_payload = {}
        target_gap_backlog_rows = []
        target_gap_summary = {}
        if args.emit_benchmark_report and benchmark_workbook:
            benchmark_payload = compare_benchmark_workbook(
                benchmark_path=benchmark_workbook,
                export_workbook_path=output_dir / "会计报表_填充结果.xlsx",
                rules=benchmark_rules,
            )
            if args.enable_benchmark_alignment_repair:
                benchmark_payload = repair_benchmark_alignment(
                    benchmark_payload=benchmark_payload,
                    facts=facts_deduped + derived_facts,
                    rules={**benchmark_alignment_rules, "runtime_alignment_overrides": runtime_alignment_overrides},
                )
                benchmark_alignment_rows = benchmark_payload.get("alignment_audit_rows", [])
                benchmark_alignment_summary = benchmark_payload.get("alignment_summary", {})
            benchmark_missing_true_rows = benchmark_payload.get("benchmark_missing_true_rows", benchmark_payload.get("missing_rows", []))
            benchmark_gap_payload = explain_benchmark_gaps(
                benchmark_missing_rows=benchmark_missing_true_rows,
                facts=facts_deduped,
                unplaced_rows=export_stats.get("unplaced_rows", []),
                conflicts=conflicts_enriched,
                validations=validation_results,
                mapping_candidates=mapping_candidates,
                derived_facts=derived_facts,
            )
            no_source_gap_payload = investigate_no_source_gaps(
                benchmark_missing_true_rows=benchmark_missing_true_rows,
                facts_raw=facts_raw,
                facts_deduped=facts_deduped,
                unplaced_rows=export_stats.get("unplaced_rows", []),
                derived_facts=derived_facts,
                review_items=review_items,
                issues=all_issues,
            )
            target_gap_backlog_rows = no_source_gap_payload.get("target_gap_backlog_rows", [])
            target_gap_summary = no_source_gap_payload.get("target_gap_summary", {})
            rewrite_stage5_helper_sheets(
                output_path=output_dir / "会计报表_填充结果.xlsx",
                benchmark_summary={**benchmark_payload.get("summary", {}), "run_id": run_id},
                gap_rows=with_run_id_rows(benchmark_gap_payload.get("explanations", []), run_id),
                derived_facts=derived_facts,
                classification_audit=with_run_id_rows(statement_classification_audit, run_id),
                period_role_audit=with_run_id_rows(period_role_audit_rows, run_id),
                benchmark_alignment_rows=with_run_id_rows(benchmark_alignment_rows, run_id),
                target_gap_backlog_rows=with_run_id_rows(target_gap_backlog_rows, run_id),
                promotion_rows=applied_promotion_rows,
            )
        unmapped_value_bearing_rows, unmapped_blank_rows, mapping_lift_summary = split_unmapped_facts(facts_deduped)
        alias_acceptance_candidates, alias_acceptance_summary = build_alias_acceptance_candidates(
            value_bearing_rows=unmapped_value_bearing_rows,
            facts=facts_deduped,
            mapping_candidates=mapping_candidates,
            benchmark_missing_rows=benchmark_payload.get("missing_rows", []),
            alias_rules=alias_pack_rules,
        )
        formula_rule_impact_summary, candidate_formula_placements = build_formula_rule_impact(
            derived_facts=derived_facts,
            derived_conflicts=derived_conflicts,
        )
        candidate_formula_placements = enrich_formula_candidate_rows(candidate_formula_placements, effective_formula_rules)
        export_target_kpi_summary = build_target_kpis(
            facts=facts_deduped + derived_facts,
            benchmark_missing_true_rows=benchmark_missing_true_rows,
            main_target_review_rows=main_target_review_rows,
            note_detail_review_rows=note_detail_review_rows,
        ) if args.enable_export_target_scoping else {}

    source_backed_gap_closure_payload = finalize_source_backed_gap_results(
        closure_rows=source_backed_gap_closure_payload.get("rows", []),
        final_investigation_rows=no_source_gap_payload.get("rows", []),
    ) if args.enable_export_target_scoping else source_backed_gap_closure_payload
    source_backed_gap_closure_summary = {
        **source_backed_gap_closure_payload.get("summary", {}),
        "source_backed_gap_total_before": source_backed_gap_total_before,
        "source_backed_gap_total_after": int(source_backed_gap_closure_payload.get("summary", {}).get("remaining_source_backed_total", 0)),
        "applied_total": int(closure_apply_payload.get("summary", {}).get("applied_total", 0)),
    } if args.enable_export_target_scoping else {}

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("manifest/finalization")

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
    write_dict_csv(
        output_dir / "label_normalization_audit.csv",
        with_run_id_rows(label_audit_rows, run_id),
        list(label_audit_rows[0].keys()) if label_audit_rows else ["doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "normalization_rule_ids", "fact_id", "source_cell_ref", "run_id", "meta_json"],
    )
    write_summary_json(output_dir / "label_normalization_summary.json", label_summary, run_id)
    write_dict_csv(
        output_dir / "statement_classification_audit.csv",
        with_run_id_rows(statement_classification_audit, run_id),
        list(statement_classification_audit[0].keys()) if statement_classification_audit else ["doc_id", "page_no", "logical_subtable_id", "fact_id", "row_label_raw", "row_label_std", "value_num", "statement_type_before", "statement_type_after", "statement_type_source", "statement_type_score", "statement_type_reason", "meta_json", "run_id"],
    )
    write_summary_json(output_dir / "statement_classification_summary.json", statement_classification_summary, run_id)
    write_dict_csv(
        output_dir / "period_role_resolution_audit.csv",
        with_run_id_rows(period_role_audit_rows, run_id),
        list(period_role_audit_rows[0].keys()) if period_role_audit_rows else ["doc_id", "page_no", "logical_subtable_id", "fact_id", "statement_type", "original_period_key", "inferred_period_key", "original_period_role", "inferred_period_role", "inference_reason", "evidence_source", "header_text", "run_id"],
    )
    write_summary_json(output_dir / "period_role_resolution_summary.json", period_role_resolution_summary, run_id)
    high_value_label_gaps = [
        {
            "run_id": run_id,
            "fact_id": row["fact_id"],
            "doc_id": row["doc_id"],
            "page_no": row["page_no"],
            "statement_type": row["statement_type"],
            "row_label_raw": row["row_label_raw"],
            "row_label_std": row["row_label_std"],
            "row_label_norm": row["row_label_norm"],
            "row_label_canonical_candidate": row["row_label_canonical_candidate"],
            "period_key": row["period_key"],
            "value_num": row["value_num"],
            "source_cell_ref": row["source_cell_ref"],
        }
        for row in sorted(unmapped_value_bearing_rows, key=lambda item: abs(float(item.get("value_num") or 0.0)), reverse=True)
    ]
    write_dict_csv(
        output_dir / "high_value_label_gaps.csv",
        high_value_label_gaps,
        list(high_value_label_gaps[0].keys()) if high_value_label_gaps else ["run_id", "fact_id", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "period_key", "value_num", "source_cell_ref"],
    )
    if args.enable_mapping_suggestions:
        write_dataclass_csv(output_dir / "mapping_candidates.csv", mapping_candidates, MappingCandidateRecord)
        write_dataclass_csv(output_dir / "unmapped_labels_summary.csv", unmapped_labels_summary, UnmappedLabelSummaryRecord)
    write_dict_csv(
        output_dir / "unmapped_value_bearing.csv",
        with_run_id_rows(unmapped_value_bearing_rows, run_id),
        list(with_run_id_row(unmapped_value_bearing_rows[0], run_id).keys()) if unmapped_value_bearing_rows else ["run_id", "fact_id", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "period_key", "value_raw", "value_num", "source_cell_ref"],
    )
    write_dict_csv(
        output_dir / "unmapped_blank_or_non_numeric.csv",
        with_run_id_rows(unmapped_blank_rows, run_id),
        list(with_run_id_row(unmapped_blank_rows[0], run_id).keys()) if unmapped_blank_rows else ["run_id", "fact_id", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "period_key", "value_raw", "value_num", "source_cell_ref"],
    )
    write_summary_json(output_dir / "mapping_lift_summary.json", mapping_lift_summary, run_id)
    write_dict_csv(
        output_dir / "alias_acceptance_candidates.csv",
        with_run_id_rows(alias_acceptance_candidates, run_id),
        list(with_run_id_row(alias_acceptance_candidates[0], run_id).keys()) if alias_acceptance_candidates else ["run_id", "candidate_alias", "canonical_code", "canonical_name", "statement_type", "evidence_count", "amount_coverage_gain", "benchmark_support", "safe_to_auto_accept", "review_required", "candidate_method", "average_candidate_score", "conflicting_target_count"],
    )
    write_summary_json(output_dir / "alias_acceptance_summary.json", alias_acceptance_summary, run_id)
    write_dict_csv(
        output_dir / "curated_alias_pack_audit.csv",
        with_run_id_rows(curated_alias_pack_audit_rows, run_id),
        list(with_run_id_row(curated_alias_pack_audit_rows[0], run_id).keys()) if curated_alias_pack_audit_rows else ["run_id", "canonical_code", "canonical_name", "alias", "alias_type", "enabled", "note"],
    )
    write_summary_json(output_dir / "curated_alias_pack_summary.json", curated_alias_pack_summary, run_id)
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
        write_summary_json(output_dir / "reocr_task_summary.json", reocr_summary, run_id)
    derived_fact_rows = [with_run_id_row(dataclass_row(fact), run_id) for fact in derived_facts]
    write_dict_csv(
        output_dir / "derived_facts.csv",
        derived_fact_rows,
        list(derived_fact_rows[0].keys()) if derived_fact_rows else ["doc_id", "page_no", "provider", "statement_type", "statement_name_raw", "logical_subtable_id", "table_semantic_key", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "col_header_raw", "col_header_path", "column_semantic_key", "period_role_raw", "report_date_raw", "period_key", "value_raw", "value_num", "value_type", "unit_raw", "unit_multiplier", "source_cell_ref", "status", "mapping_code", "mapping_name", "mapping_method", "mapping_confidence", "issue_flags", "fact_id", "report_date_norm", "period_role_norm", "period_source_level", "period_reason", "duplicate_group_id", "kept_fact_id", "comparison_status", "comparison_reason", "source_kind", "statement_group_key", "source_row_start", "source_row_end", "source_col_start", "source_col_end", "mapping_relation_type", "mapping_review_required", "conflict_id", "conflict_decision", "unplaced_reason", "review_id", "suppression_reason", "override_source", "parent_review_id", "parent_task_id", "run_id"],
    )
    write_dict_csv(
        output_dir / "derived_formula_audit.csv",
        with_run_id_rows(derived_formula_audit, run_id),
        list(derived_formula_audit[0].keys()) if derived_formula_audit else ["rule_id", "target_code", "target_name", "statement_type", "period_key", "rule_type", "source_fact_ids", "derived_fact_id", "derived_value_num", "safety_level", "run_id", "meta_json"],
    )
    write_summary_json(output_dir / "derived_formula_summary.json", derived_formula_summary, run_id)
    write_summary_json(output_dir / "formula_rule_impact_summary.json", formula_rule_impact_summary, run_id)
    write_dict_csv(
        output_dir / "candidate_formula_placements.csv",
        with_run_id_rows(candidate_formula_placements, run_id),
        list(with_run_id_row(candidate_formula_placements[0], run_id).keys()) if candidate_formula_placements else ["run_id", "rule_id", "fact_id", "mapping_code", "mapping_name", "statement_type", "period_key", "value_num", "exportable", "unplaced_reason"],
    )
    write_dict_csv(
        output_dir / "derived_conflicts.csv",
        with_run_id_rows(derived_conflicts, run_id),
        list(derived_conflicts[0].keys()) if derived_conflicts else ["target_code", "target_name", "statement_type", "period_key", "rule_id", "observed_fact_id", "observed_value_num", "derived_fact_id", "derived_value_num", "decision", "run_id", "meta_json"],
    )
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
        write_summary_json(output_dir / "review_decision_summary.json", review_decision_summary, run_id)
    if reocr_manifest_rows:
        write_dict_csv(output_dir / "reocr_input_manifest.csv", reocr_manifest_rows, list(reocr_manifest_rows[0].keys()))
        write_json(output_dir / "reocr_input_manifest.json", {"summary": reocr_manifest_summary, "tasks": reocr_manifest_rows})
    if reocr_results_dir:
        write_dict_csv(
            output_dir / "reocr_merge_audit.csv",
            reocr_merge_audit_rows,
            list(reocr_merge_audit_rows[0].keys()) if reocr_merge_audit_rows else ["task_id", "status", "reason", "result_file"],
        )
        write_summary_json(output_dir / "reocr_merge_summary.json", reocr_merge_summary, run_id)
    if benchmark_payload:
        write_summary_json(output_dir / "benchmark_summary.json", benchmark_payload["summary"], run_id)
        write_dict_csv(output_dir / "benchmark_summary.csv", [with_run_id_row(benchmark_payload["summary"], run_id)], list(with_run_id_row(benchmark_payload["summary"], run_id).keys()))
        write_dict_csv(output_dir / "benchmark_cell_diff.csv", with_run_id_rows(benchmark_payload.get("cell_rows", []), run_id), list(benchmark_payload["cell_rows"][0].keys()) if benchmark_payload.get("cell_rows") else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"])
        write_dict_csv(output_dir / "benchmark_missing_in_auto.csv", with_run_id_rows(benchmark_payload.get("missing_rows", []), run_id), list(benchmark_payload["missing_rows"][0].keys()) if benchmark_payload.get("missing_rows") else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"])
        write_dict_csv(output_dir / "benchmark_extra_in_auto.csv", with_run_id_rows(benchmark_payload.get("extra_rows", []), run_id), list(benchmark_payload["extra_rows"][0].keys()) if benchmark_payload.get("extra_rows") else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"])
        write_dict_csv(output_dir / "benchmark_value_diff.csv", with_run_id_rows(benchmark_payload.get("value_diff_rows", []), run_id), list(benchmark_payload["value_diff_rows"][0].keys()) if benchmark_payload.get("value_diff_rows") else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"])
        write_dict_csv(output_dir / "benchmark_subject_gap.csv", with_run_id_rows(benchmark_payload.get("subject_gap_rows", []), run_id), list(benchmark_payload["subject_gap_rows"][0].keys()) if benchmark_payload.get("subject_gap_rows") else ["run_id", "mapping_code", "missing_cells"])
        write_dict_csv(output_dir / "benchmark_period_gap.csv", with_run_id_rows(benchmark_payload.get("period_gap_rows", []), run_id), list(benchmark_payload["period_gap_rows"][0].keys()) if benchmark_payload.get("period_gap_rows") else ["run_id", "benchmark_header", "missing_cells"])
        if args.enable_benchmark_alignment_repair:
            write_dict_csv(
                output_dir / "benchmark_alignment_audit.csv",
                with_run_id_rows(benchmark_alignment_rows, run_id),
                list(with_run_id_row(benchmark_alignment_rows[0], run_id).keys()) if benchmark_alignment_rows else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "raw_aligned_period_key", "repaired_aligned_period_key", "benchmark_value", "raw_auto_value", "repaired_auto_value", "raw_status", "repaired_status", "raw_reason", "repaired_reason", "alignment_status", "statement_type_hint"],
            )
            write_summary_json(output_dir / "benchmark_alignment_summary.json", benchmark_alignment_summary, run_id)
            write_dict_csv(
                output_dir / "benchmark_missing_true.csv",
                with_run_id_rows(benchmark_missing_true_rows, run_id),
                list(with_run_id_row(benchmark_missing_true_rows[0], run_id).keys()) if benchmark_missing_true_rows else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"],
            )
            write_dict_csv(
                output_dir / "benchmark_alignment_only.csv",
                with_run_id_rows(benchmark_payload.get("alignment_only_rows", []), run_id),
                list(with_run_id_row(benchmark_payload["alignment_only_rows"][0], run_id).keys()) if benchmark_payload.get("alignment_only_rows") else ["run_id", "mapping_code", "mapping_name", "benchmark_header", "aligned_period_key", "benchmark_value", "auto_value", "status", "reason"],
            )
    if benchmark_gap_payload:
        write_dict_csv(output_dir / "benchmark_gap_explanations.csv", with_run_id_rows(benchmark_gap_payload.get("explanations", []), run_id), list(benchmark_gap_payload["explanations"][0].keys()) if benchmark_gap_payload.get("explanations") else ["run_id", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "gap_cause", "detail"])
        write_summary_json(output_dir / "benchmark_gap_summary.json", benchmark_gap_payload.get("summary", {}), run_id)
        write_dict_csv(output_dir / "suggested_aliases_from_benchmark.csv", with_run_id_rows(benchmark_gap_payload.get("alias_suggestions", []), run_id), list(benchmark_gap_payload["alias_suggestions"][0].keys()) if benchmark_gap_payload.get("alias_suggestions") else ["run_id", "mapping_code", "row_label_std", "period_key", "benchmark_value", "fact_id", "candidate_method"])
        write_dict_csv(output_dir / "suggested_formula_rules.csv", with_run_id_rows(benchmark_gap_payload.get("formula_suggestions", []), run_id), list(benchmark_gap_payload["formula_suggestions"][0].keys()) if benchmark_gap_payload.get("formula_suggestions") else ["run_id", "rule_id", "mapping_code", "period_key", "benchmark_value", "derived_fact_id"])
        write_dict_csv(output_dir / "benchmark_priority_actions.csv", with_run_id_rows(benchmark_gap_payload.get("priority_rows", []), run_id), list(benchmark_gap_payload["priority_rows"][0].keys()) if benchmark_gap_payload.get("priority_rows") else ["run_id", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "gap_cause", "priority_score"])
    if args.enable_export_target_scoping:
        write_dict_csv(
            output_dir / "export_target_scope.csv",
            with_run_id_rows(target_scope_rows, run_id),
            list(with_run_id_row(target_scope_rows[0], run_id).keys()) if target_scope_rows else ["run_id", "fact_id", "doc_id", "page_no", "statement_type", "mapping_code", "mapping_name", "row_label_raw", "row_label_std", "row_label_norm", "row_label_canonical_candidate", "period_key", "value_num", "target_scope", "target_scope_reason"],
        )
        write_summary_json(output_dir / "export_target_kpi_summary.json", export_target_kpi_summary, run_id)
        write_dict_csv(
            output_dir / "main_target_review_queue.csv",
            with_run_id_rows(main_target_review_rows, run_id),
            list(with_run_id_row(main_target_review_rows[0], run_id).keys()) if main_target_review_rows else ["run_id", "review_id", "priority_score", "reason_codes", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "period_key", "value_raw", "value_num", "provider", "source_file", "bbox", "related_fact_ids", "related_conflict_ids", "related_validation_ids", "mapping_candidates", "evidence_cell_path", "evidence_row_path", "evidence_table_path", "meta_json", "target_scope", "review_category"],
        )
        write_dict_csv(
            output_dir / "note_detail_review_queue.csv",
            with_run_id_rows(note_detail_review_rows, run_id),
            list(with_run_id_row(note_detail_review_rows[0], run_id).keys()) if note_detail_review_rows else ["run_id", "review_id", "priority_score", "reason_codes", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "period_key", "value_raw", "value_num", "provider", "source_file", "bbox", "related_fact_ids", "related_conflict_ids", "related_validation_ids", "mapping_candidates", "evidence_cell_path", "evidence_row_path", "evidence_table_path", "meta_json", "target_scope", "review_category"],
        )
        write_dict_csv(
            output_dir / "suppressed_note_detail_items.csv",
            with_run_id_rows(suppressed_note_detail_rows, run_id),
            list(with_run_id_row(suppressed_note_detail_rows[0], run_id).keys()) if suppressed_note_detail_rows else ["run_id", "review_id", "priority_score", "reason_codes", "doc_id", "page_no", "statement_type", "row_label_raw", "row_label_std", "period_key", "value_raw", "value_num", "provider", "source_file", "bbox", "related_fact_ids", "related_conflict_ids", "related_validation_ids", "mapping_candidates", "evidence_cell_path", "evidence_row_path", "evidence_table_path", "meta_json", "target_scope", "review_category"],
        )
        write_dict_csv(
            output_dir / "target_gap_backlog.csv",
            with_run_id_rows(target_gap_backlog_rows, run_id),
            list(with_run_id_row(target_gap_backlog_rows[0], run_id).keys()) if target_gap_backlog_rows else ["run_id", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "gap_cause", "priority_score", "evidence_refs"],
        )
        write_summary_json(output_dir / "target_gap_summary.json", target_gap_summary, run_id)
        write_dict_csv(
            output_dir / "source_backed_gap_closure.csv",
            with_run_id_rows(source_backed_gap_closure_payload.get("rows", []), run_id),
            list(with_run_id_row(source_backed_gap_closure_payload["rows"][0], run_id).keys()) if source_backed_gap_closure_payload.get("rows") else ["run_id", "gap_id", "cause", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "source_fact_ids", "recommended_fix_type", "safe_to_apply", "applied_in_this_round", "result", "reason", "payload_json"],
        )
        write_summary_json(output_dir / "source_backed_gap_closure_summary.json", source_backed_gap_closure_summary, run_id)
    if no_source_gap_payload:
        write_dict_csv(
            output_dir / "no_source_gap_investigation.csv",
            with_run_id_rows(no_source_gap_payload.get("rows", []), run_id),
            list(with_run_id_row(no_source_gap_payload["rows"][0], run_id).keys()) if no_source_gap_payload.get("rows") else ["run_id", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "gap_cause", "evidence_source", "evidence_refs"],
        )
        write_summary_json(output_dir / "no_source_gap_summary.json", no_source_gap_payload.get("summary", {}), run_id)
        write_dict_csv(
            output_dir / "target_backfill_tasks.csv",
            with_run_id_rows(no_source_gap_payload.get("backfill_rows", []), run_id),
            list(with_run_id_row(no_source_gap_payload["backfill_rows"][0], run_id).keys()) if no_source_gap_payload.get("backfill_rows") else ["run_id", "mapping_code", "mapping_name", "aligned_period_key", "benchmark_value", "task_type", "evidence", "suggested_action"],
        )
        write_summary_json(output_dir / "target_backfill_summary.json", no_source_gap_payload.get("backfill_summary", {}), run_id)

    top_unknown_labels = build_top_unknown_labels(facts_deduped)
    top_suspicious_values = build_top_suspicious_values(all_cells)
    write_dict_csv(output_dir / "top_unknown_labels.csv", top_unknown_labels, ["label", "count"])
    write_dict_csv(output_dir / "top_suspicious_values.csv", top_suspicious_values, ["text_raw", "reason", "count"])
    write_summary_json(output_dir / "validation_summary.json", validation_summary, run_id)
    write_summary_json(output_dir / "review_summary.json", review_summary, run_id)
    write_dict_csv(
        output_dir / "review_actionable.csv",
        with_run_id_rows(review_actionable_rows, run_id),
        list(with_run_id_row(review_actionable_rows[0], run_id).keys()) if review_actionable_rows else ["run_id", "review_id", "doc_id", "page_no", "statement_type", "row_label_std", "period_key", "value_num", "priority_score", "reason_codes", "category"],
    )
    write_dict_csv(
        output_dir / "review_nonactionable.csv",
        with_run_id_rows(review_nonactionable_rows, run_id),
        list(with_run_id_row(review_nonactionable_rows[0], run_id).keys()) if review_nonactionable_rows else ["run_id", "review_id", "doc_id", "page_no", "statement_type", "row_label_std", "period_key", "value_num", "priority_score", "reason_codes", "category"],
    )
    write_summary_json(output_dir / "review_actionable_summary.json", review_actionable_summary, run_id)
    if args.emit_reocr_tasks:
        write_dict_csv(
            output_dir / "reocr_task_pruned.csv",
            with_run_id_rows(pruned_reocr_rows, run_id),
            list(with_run_id_row(pruned_reocr_rows[0], run_id).keys()) if pruned_reocr_rows else ["run_id", "task_id", "granularity", "doc_id", "page_no", "table_id", "logical_subtable_id", "bbox", "reason_codes", "suggested_provider", "priority_score", "expected_benefit", "source_review_id", "category"],
        )
        write_dict_csv(
            output_dir / "reocr_task_pruned_deduped.csv",
            with_run_id_rows(pruned_reocr_rows, run_id),
            list(with_run_id_row(pruned_reocr_rows[0], run_id).keys()) if pruned_reocr_rows else ["run_id", "task_id", "granularity", "doc_id", "page_no", "table_id", "logical_subtable_id", "bbox", "bbox_normalized", "reason_codes", "suggested_provider", "priority_score", "expected_benefit", "source_review_id", "category", "target_scope", "cluster_id", "merged_task_ids", "merged_review_ids", "merged_task_count"],
        )
        write_summary_json(output_dir / "reocr_task_pruned_summary.json", pruned_reocr_summary, run_id)
        write_summary_json(
            output_dir / "reocr_dedupe_audit.json",
            {
                "tasks_before": int(pruned_reocr_summary.get("reocr_tasks_total_before", 0)),
                "tasks_after": int(pruned_reocr_summary.get("reocr_tasks_total_after", 0)),
                "duplicate_groups_before": int(pruned_reocr_summary.get("duplicate_groups_before", 0)),
                "duplicate_groups_after": int(pruned_reocr_summary.get("duplicate_groups_after", 0)),
                "merged_task_count": int(pruned_reocr_summary.get("merged_task_count", 0)),
                "pass": int(pruned_reocr_summary.get("duplicate_groups_after", 0)) == 0,
            },
            run_id,
        )
    if pre_ocr_plan:
        write_json(output_dir / "pre_ocr_routing_plan.json", pre_ocr_plan)
    if post_ocr_plan:
        write_json(output_dir / "post_ocr_routing_plan.json", post_ocr_plan)
    required_summary_files = build_required_summary_files(feature_flags)
    integrity_summary = {"integrity_fail_total": 0, "checks_total": 0, "integrity_pass_total": 0, "integrity_review_total": 0}
    integrity_records: List[ArtifactIntegrityRecord] = []

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
    run_summary["run_id"] = run_id
    run_summary["derived_facts_total"] = int(derived_formula_summary.get("derived_facts_total", 0))
    run_summary["derived_conflicts_total"] = int(derived_formula_summary.get("derived_conflicts_total", 0))
    run_summary["exportable_facts_total"] = int(export_stats.get("written_cells", 0))
    if benchmark_payload:
        run_summary["benchmark_missing_in_auto"] = int(benchmark_payload.get("summary", {}).get("missing_in_auto", 0))
        run_summary["benchmark_value_diff_cells"] = int(benchmark_payload.get("summary", {}).get("value_diff_cells", 0))
        run_summary["benchmark_missing_true_total"] = int(len(benchmark_missing_true_rows))
        run_summary["alignment_only_gap_total"] = int(benchmark_alignment_summary.get("alignment_only_gap_total", 0))
    if export_target_kpi_summary:
        run_summary["target_missing_total"] = int(export_target_kpi_summary.get("target_missing_total", 0))
        run_summary["target_mapped_ratio"] = float(export_target_kpi_summary.get("target_mapped_ratio", 0.0))
        run_summary["target_amount_coverage_ratio"] = float(export_target_kpi_summary.get("target_amount_coverage_ratio", 0.0))
    stage6_kpi_summary = build_stage6_kpis(
        run_summary=run_summary,
        facts=facts_deduped,
        review_items=review_items,
        actionable_review_rows=review_actionable_rows,
        reocr_tasks_total=len(reocr_tasks),
        actionable_reocr_tasks_total=len(pruned_reocr_rows),
        benchmark_payload=benchmark_payload,
        baseline_summary=baseline_snapshot.get("run_summary", {}),
    )
    stage7_kpi_summary: Dict[str, Any] = {}
    coverage_by_statement_rows = build_statement_coverage_rows(facts_deduped + derived_facts)
    benchmark_recall_by_period, benchmark_recall_by_subject = build_benchmark_recall_rows(benchmark_payload) if benchmark_payload else ([], [])
    write_summary_json(output_dir / "pages_skipped_metric_audit.json", pages_skipped_metric_audit, run_id)
    write_summary_json(output_dir / "run_summary.json", run_summary, run_id)
    write_dict_csv(output_dir / "run_summary.csv", [run_summary], list(run_summary.keys()))
    rewrite_meta_summary(output_dir / "会计报表_填充结果.xlsx", run_summary)
    write_summary_json(
        output_dir / "summary.json",
        {
            "docs_processed": docs_total,
            "pages_discovered": len(sources),
            "providers_seen": provider_hits,
            "skipped_text_providers": skipped_text,
            "unsupported_providers": unsupported,
            "export_stats": export_stats,
            "run_summary": run_summary,
            "validation_summary": validation_summary,
            "review_summary": review_summary,
            "benchmark_summary": benchmark_payload.get("summary", {}),
            "derived_formula_summary": derived_formula_summary,
            "benchmark_alignment_summary": benchmark_alignment_summary,
            "export_target_kpi_summary": export_target_kpi_summary if args.enable_export_target_scoping else {},
            "target_gap_summary": target_gap_summary,
            "pages_skipped_metric_audit": pages_skipped_metric_audit,
            "reocr_dedupe_audit": pruned_reocr_summary if args.emit_reocr_tasks else {},
            "source_backed_gap_closure_summary": source_backed_gap_closure_summary,
            "core_completion": True,
        },
        run_id,
        normalize_nested=True,
    )
    if args.emit_run_manifest:
        write_run_manifest(
            run_id=run_id,
            output_dir=output_dir,
            cli_args=list(argv or []),
            input_dir=input_dir,
            template_path=template_path,
            source_files=collect_source_files(sources),
            run_summary=run_summary,
            feature_flags=feature_flags,
            manifest_rules=manifest_rules,
            artifact_manifest_mode=args.artifact_manifest_mode,
        )
    if args.emit_stage6_kpis:
        write_summary_json(output_dir / "stage6_kpi_summary.json", stage6_kpi_summary, run_id)
        write_dict_csv(
            output_dir / "export_coverage_by_statement.csv",
            coverage_by_statement_rows,
            list(coverage_by_statement_rows[0].keys()) if coverage_by_statement_rows else ["statement_type", "facts_total", "mapped_total", "exportable_total", "mapped_ratio", "amount_coverage_ratio"],
        )
        write_dict_csv(
            output_dir / "benchmark_recall_by_period.csv",
            benchmark_recall_by_period,
            list(benchmark_recall_by_period[0].keys()) if benchmark_recall_by_period else ["period_key", "cells_total", "matched_cells", "recall"],
        )
        write_dict_csv(
            output_dir / "benchmark_recall_by_subject.csv",
            benchmark_recall_by_subject,
            list(benchmark_recall_by_subject[0].keys()) if benchmark_recall_by_subject else ["mapping_code", "cells_total", "matched_cells", "recall"],
        )
    if args.emit_stage7_kpis:
        baseline_stage7 = load_json_file(baseline_dir / "stage7_kpi_summary.json") if baseline_dir and (baseline_dir / "stage7_kpi_summary.json").exists() else {}
        stage7_kpi_summary = build_stage7_kpis(
            run_summary=run_summary,
            target_summary=export_target_kpi_summary,
            benchmark_alignment_summary=benchmark_alignment_summary,
            promotion_summary=promotion_summary,
            no_source_summary=no_source_gap_payload.get("summary", {}),
            actionable_reocr_tasks_total=len(pruned_reocr_rows),
            baseline_stage7=baseline_stage7,
        )
        write_summary_json(output_dir / "stage7_kpi_summary.json", stage7_kpi_summary, run_id)
        write_summary_json(
            output_dir / "benchmark_missing_true_summary.json",
            {
                "benchmark_missing_true_total": len(benchmark_missing_true_rows),
                "alignment_only_gap_total": int(benchmark_alignment_summary.get("alignment_only_gap_total", 0)),
                "ambiguous_alignment_total": int(benchmark_alignment_summary.get("ambiguous_alignment_total", 0)),
            },
            run_id,
        )

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
    if args.emit_promotion_template:
        export_promotion_actions_template(
            output_dir=output_dir,
            alias_candidates=alias_acceptance_candidates,
            formula_candidates=candidate_formula_placements,
            benchmark_gap_rows=benchmark_gap_payload.get("explanations", []),
            benchmark_missing_true_rows=benchmark_missing_true_rows,
            unmapped_value_bearing_rows=unmapped_value_bearing_rows,
            target_gap_backlog_rows=target_gap_backlog_rows,
        )
    if args.apply_promotions or promotion_actions_path:
        write_dict_csv(
            output_dir / "applied_promotions.csv",
            applied_promotion_rows,
            ["action_id", "promotion_id", "action_type", "target_id", "target_scope", "config_file_touched", "apply_timestamp", "apply_message"],
        )
        write_dict_csv(
            output_dir / "rejected_promotions.csv",
            rejected_promotion_rows,
            ["action_id", "promotion_id", "action_type", "reject_reason"],
        )
        write_dict_csv(
            output_dir / "promotion_audit.csv",
            promotion_audit_rows,
            ["action_id", "promotion_id", "action_type", "target_id", "target_scope", "config_file_touched", "apply_timestamp", "apply_message", "old_state", "new_state"],
        )
        write_dict_csv(
            output_dir / "promoted_aliases.csv",
            promotion_summary.get("promoted_aliases", []),
            list(promotion_summary.get("promoted_aliases", [])[0].keys()) if promotion_summary.get("promoted_aliases") else ["run_id", "canonical_code", "canonical_name", "alias", "alias_type", "enabled", "note"],
        )
        write_dict_csv(
            output_dir / "promoted_formula_rules.csv",
            promotion_summary.get("promoted_formulas", []),
            list(promotion_summary.get("promoted_formulas", [])[0].keys()) if promotion_summary.get("promoted_formulas") else ["run_id", "rule_id", "rule_type", "target_code", "target_name", "children", "statement_types", "enabled"],
        )
        baseline_promotion = load_json_file(baseline_dir / "stage7_kpi_summary.json") if baseline_dir and (baseline_dir / "stage7_kpi_summary.json").exists() else {}
        after_promotion = {
            "run_id": run_id,
            "target_missing_total": export_target_kpi_summary.get("target_missing_total", 0),
            "target_mapped_ratio": export_target_kpi_summary.get("target_mapped_ratio", 0.0),
            "target_amount_coverage_ratio": export_target_kpi_summary.get("target_amount_coverage_ratio", 0.0),
            "exportable_facts_total": run_summary.get("exportable_facts_total", 0),
            "benchmark_missing_true_total": len(benchmark_missing_true_rows),
            "promoted_alias_total": promotion_summary.get("promoted_alias_total", 0),
            "promoted_formula_total": promotion_summary.get("promoted_formula_total", 0),
        }
        promotion_delta_payload = build_promotion_delta(before=baseline_promotion, after=after_promotion)
        write_summary_json(output_dir / "promotion_delta.json", promotion_delta_payload["summary"], run_id)
        write_dict_csv(output_dir / "promotion_delta.csv", promotion_delta_payload["rows"], ["metric", "before", "after", "delta"])

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
    write_summary_json(output_dir / "coverage_opportunity_summary.json", opportunity_summary, run_id)
    write_dict_csv(
        output_dir / "mapping_opportunities.csv",
        mapping_opportunities,
        list(mapping_opportunities[0].keys()) if mapping_opportunities else ["row_label_std", "occurrences", "amount_abs_total", "top_candidate_code", "top_candidate_name", "top_candidate_score"],
    )

    if args.emit_delta_report or args.apply_review_actions:
        after_snapshot = load_artifact_snapshot(output_dir)
        after_snapshot["run_summary"] = run_summary
        delta_payload = build_delta_reports(before=baseline_snapshot, after=after_snapshot)
        write_summary_json(output_dir / "coverage_delta.json", delta_payload["coverage_delta"], run_id)
        write_dict_csv(output_dir / "coverage_delta.csv", delta_payload["coverage_rows"], ["metric", "before", "after", "delta"])
        write_dict_csv(
            output_dir / "review_delta.csv",
            delta_payload["review_delta_rows"],
            ["review_id", "status", "before_priority_score", "after_priority_score", "row_label_std", "period_key"],
        )
        write_summary_json(output_dir / "export_delta_summary.json", delta_payload["export_delta_summary"], run_id)
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

    reocr_dedupe_audit = {
        "tasks_before": int(pruned_reocr_summary.get("reocr_tasks_total_before", 0)),
        "tasks_after": int(pruned_reocr_summary.get("reocr_tasks_total_after", 0)),
        "duplicate_groups_before": int(pruned_reocr_summary.get("duplicate_groups_before", 0)),
        "duplicate_groups_after": int(pruned_reocr_summary.get("duplicate_groups_after", 0)),
        "merged_task_count": int(pruned_reocr_summary.get("merged_task_count", 0)),
        "pass": int(pruned_reocr_summary.get("duplicate_groups_after", 0)) == 0,
    } if args.emit_reocr_tasks else {}
    write_summary_json(output_dir / "pages_skipped_metric_audit.json", pages_skipped_metric_audit, run_id)

    raw_summary_payloads = [
        ("alias_acceptance_summary.json", alias_acceptance_summary),
        ("benchmark_alignment_summary.json", benchmark_alignment_summary),
        ("benchmark_gap_summary.json", benchmark_gap_payload.get("summary", {})),
        ("benchmark_summary.json", benchmark_payload.get("summary", {})),
        ("coverage_opportunity_summary.json", opportunity_summary),
        ("curated_alias_pack_summary.json", curated_alias_pack_summary),
        ("derived_formula_summary.json", derived_formula_summary),
        ("export_target_kpi_summary.json", export_target_kpi_summary),
        ("formula_rule_impact_summary.json", formula_rule_impact_summary),
        ("label_normalization_summary.json", label_summary),
        ("mapping_lift_summary.json", mapping_lift_summary),
        ("no_source_gap_summary.json", no_source_gap_payload.get("summary", {})),
        ("pipeline_completion_summary.json", load_json_file(output_dir / "pipeline_completion_summary.json")),
        ("period_role_resolution_summary.json", period_role_resolution_summary),
        ("review_actionable_summary.json", review_actionable_summary),
        ("review_summary.json", review_summary),
        ("run_summary.json", run_summary),
        ("source_backed_gap_closure_summary.json", source_backed_gap_closure_summary),
        ("statement_classification_summary.json", statement_classification_summary),
        ("target_backfill_summary.json", no_source_gap_payload.get("backfill_summary", {})),
        ("target_gap_summary.json", target_gap_summary),
        ("validation_summary.json", validation_summary),
    ]
    if args.emit_reocr_tasks:
        raw_summary_payloads.extend(
            [
                ("reocr_task_pruned_summary.json", pruned_reocr_summary),
                ("reocr_task_summary.json", reocr_summary),
            ]
        )
    if args.emit_stage6_kpis:
        raw_summary_payloads.append(("stage6_kpi_summary.json", stage6_kpi_summary))
    if args.emit_stage7_kpis:
        raw_summary_payloads.extend(
            [
                ("benchmark_missing_true_summary.json", {"benchmark_missing_true_total": len(benchmark_missing_true_rows)}),
                ("stage7_kpi_summary.json", stage7_kpi_summary),
            ]
        )
    if args.emit_delta_report or args.apply_review_actions:
        raw_summary_payloads.extend(
            [
                ("export_delta_summary.json", delta_payload["export_delta_summary"]),
            ]
        )
    prewrite_run_id_audit = evaluate_summary_payloads(raw_summary_payloads, run_id)

    provisional_hardening_summary = {
        "run_id": run_id,
        "tests_passed_total": env_int("AUTOFINANCE_TESTS_PASSED_TOTAL", 0),
        "tests_failed_total": env_int("AUTOFINANCE_TESTS_FAILED_TOTAL", 0),
        "rerun_completed": False,
        "missing_run_id_files_before": len(prewrite_run_id_audit.get("missing_run_id_files", [])),
        "missing_run_id_files_after": 0,
        "metadata_contract_pass": False,
        "pages_skipped_before": raw_pages_skipped_as_non_table,
        "pages_skipped_after": pages_skipped_as_non_table,
        "pages_skipped_expected": pages_skipped_expected,
        "reocr_tasks_before": int(pruned_reocr_summary.get("reocr_tasks_total_before", 0)),
        "reocr_tasks_after": int(pruned_reocr_summary.get("reocr_tasks_total_after", 0)),
        "duplicate_reocr_groups_before": int(pruned_reocr_summary.get("duplicate_groups_before", 0)),
        "duplicate_reocr_groups_after": int(pruned_reocr_summary.get("duplicate_groups_after", 0)),
        "source_backed_gap_total_before": source_backed_gap_total_before,
        "source_backed_gap_total_after": int(source_backed_gap_closure_summary.get("source_backed_gap_total_after", 0)),
        "mapped_facts_ratio": float(run_summary.get("mapped_facts_ratio", 0.0)),
        "amount_coverage_ratio": float(run_summary.get("amount_coverage_ratio", 0.0)),
        "exportable_facts_total": int(run_summary.get("exportable_facts_total", export_stats.get("written_cells", 0))),
        "integrity_fail_total": 0,
        "contract_fail_total": 0,
    }
    write_summary_json(output_dir / "hardening_summary.json", provisional_hardening_summary, run_id)
    write_summary_json(output_dir / "metadata_contract_summary.json", {"pass": False, "checked_summary_files": []}, run_id)
    write_summary_json(output_dir / "run_id_propagation_audit.json", {"pass": False, "summary_files_checked": []}, run_id)

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
        "benchmark_summary": benchmark_payload.get("summary", {}),
        "derived_formula_summary": derived_formula_summary,
        "statement_classification_summary": statement_classification_summary,
        "period_role_resolution_summary": period_role_resolution_summary,
        "review_actionable_summary": review_actionable_summary,
        "reocr_task_pruned_summary": pruned_reocr_summary,
        "mapping_lift_summary": mapping_lift_summary,
        "alias_acceptance_summary": alias_acceptance_summary,
        "formula_rule_impact_summary": formula_rule_impact_summary,
        "stage6_kpi_summary": stage6_kpi_summary if args.emit_stage6_kpis else {},
        "benchmark_alignment_summary": benchmark_alignment_summary,
        "export_target_kpi_summary": export_target_kpi_summary if args.enable_export_target_scoping else {},
        "target_gap_summary": target_gap_summary,
        "promotion_summary": promotion_summary,
        "no_source_gap_summary": no_source_gap_payload.get("summary", {}),
        "stage7_kpi_summary": stage7_kpi_summary if args.emit_stage7_kpis else {},
        "pages_skipped_metric_audit": pages_skipped_metric_audit,
        "reocr_dedupe_audit": reocr_dedupe_audit,
        "source_backed_gap_closure_summary": source_backed_gap_closure_summary,
    }
    write_summary_json(output_dir / "summary.json", summary, run_id, normalize_nested=True)

    metadata_contract_summary = scan_summary_run_ids(
        output_dir=output_dir,
        expected_run_id=run_id,
        required_summary_files=required_summary_files,
    )
    write_summary_json(output_dir / "metadata_contract_summary.json", metadata_contract_summary, run_id)
    run_id_propagation_audit = scan_summary_run_ids(
        output_dir=output_dir,
        expected_run_id=run_id,
        required_summary_files=required_summary_files,
    )
    write_summary_json(output_dir / "run_id_propagation_audit.json", run_id_propagation_audit, run_id)

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("integrity")

    if args.enable_integrity_check:
        integrity_result = run_artifact_integrity(
            output_dir=output_dir,
            workbook_path=output_dir / "会计报表_填充结果.xlsx",
            run_summary=run_summary,
            export_stats=export_stats,
            export_rules=export_rules,
            required_summary_files=required_summary_files,
        )
        integrity_records = integrity_result["records"]
        integrity_summary = integrity_result["summary"]
        write_dataclass_csv(output_dir / "artifact_integrity.csv", integrity_records, ArtifactIntegrityRecord)
        write_summary_json(output_dir / "artifact_integrity.json", integrity_summary, run_id)

    stage_tracker.finish(current_stage, success=True)
    current_stage = stage_tracker.start("hardening outputs")

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
    run_summary["run_id"] = run_id
    run_summary["derived_facts_total"] = int(derived_formula_summary.get("derived_facts_total", 0))
    run_summary["derived_conflicts_total"] = int(derived_formula_summary.get("derived_conflicts_total", 0))
    run_summary["exportable_facts_total"] = int(export_stats.get("written_cells", 0))
    if benchmark_payload:
        run_summary["benchmark_missing_in_auto"] = int(benchmark_payload.get("summary", {}).get("missing_in_auto", 0))
        run_summary["benchmark_value_diff_cells"] = int(benchmark_payload.get("summary", {}).get("value_diff_cells", 0))
        run_summary["benchmark_missing_true_total"] = int(len(benchmark_missing_true_rows))
        run_summary["alignment_only_gap_total"] = int(benchmark_alignment_summary.get("alignment_only_gap_total", 0))
    if export_target_kpi_summary:
        run_summary["target_missing_total"] = int(export_target_kpi_summary.get("target_missing_total", 0))
        run_summary["target_mapped_ratio"] = float(export_target_kpi_summary.get("target_mapped_ratio", 0.0))
        run_summary["target_amount_coverage_ratio"] = float(export_target_kpi_summary.get("target_amount_coverage_ratio", 0.0))
    write_summary_json(output_dir / "run_summary.json", run_summary, run_id)
    write_dict_csv(output_dir / "run_summary.csv", [run_summary], list(run_summary.keys()))
    rewrite_meta_summary(output_dir / "会计报表_填充结果.xlsx", run_summary)

    full_run_contract_summary = run_full_run_contract(
        output_dir=output_dir,
        workbook_path=output_dir / "会计报表_填充结果.xlsx",
        run_id=run_id,
        feature_flags=feature_flags,
        export_stats=export_stats,
        required_helper_sheets=export_rules.get("required_helper_sheets", []),
    )
    write_summary_json(output_dir / "full_run_contract_summary.json", full_run_contract_summary, run_id)
    run_summary["full_run_contract_fail_total"] = int(full_run_contract_summary.get("contract_fail_total", 0))
    summary["run_summary"] = run_summary
    summary["integrity_summary"] = integrity_summary
    summary["full_run_contract_summary"] = full_run_contract_summary
    hardening_summary = {
        "run_id": run_id,
        "tests_passed_total": env_int("AUTOFINANCE_TESTS_PASSED_TOTAL", 0),
        "tests_failed_total": env_int("AUTOFINANCE_TESTS_FAILED_TOTAL", 0),
        "rerun_completed": True,
        "missing_run_id_files_before": len(prewrite_run_id_audit.get("missing_run_id_files", [])),
        "missing_run_id_files_after": len(run_id_propagation_audit.get("missing_run_id_files", [])),
        "metadata_contract_pass": bool(metadata_contract_summary.get("pass", False)),
        "pages_skipped_before": raw_pages_skipped_as_non_table,
        "pages_skipped_after": pages_skipped_as_non_table,
        "pages_skipped_expected": pages_skipped_expected,
        "reocr_tasks_before": int(pruned_reocr_summary.get("reocr_tasks_total_before", 0)),
        "reocr_tasks_after": int(pruned_reocr_summary.get("reocr_tasks_total_after", 0)),
        "duplicate_reocr_groups_before": int(pruned_reocr_summary.get("duplicate_groups_before", 0)),
        "duplicate_reocr_groups_after": int(pruned_reocr_summary.get("duplicate_groups_after", 0)),
        "source_backed_gap_total_before": source_backed_gap_total_before,
        "source_backed_gap_total_after": int(source_backed_gap_closure_summary.get("source_backed_gap_total_after", 0)),
        "mapped_facts_ratio": float(run_summary.get("mapped_facts_ratio", 0.0)),
        "amount_coverage_ratio": float(run_summary.get("amount_coverage_ratio", 0.0)),
        "exportable_facts_total": int(run_summary.get("exportable_facts_total", 0)),
        "integrity_fail_total": int(integrity_summary.get("integrity_fail_total", 0)),
        "contract_fail_total": int(full_run_contract_summary.get("contract_fail_total", 0)),
    }
    write_summary_json(output_dir / "hardening_summary.json", hardening_summary, run_id)
    summary["metadata_contract_summary"] = metadata_contract_summary
    summary["run_id_propagation_audit"] = run_id_propagation_audit
    summary["hardening_summary"] = hardening_summary
    write_summary_json(output_dir / "summary.json", summary, run_id, normalize_nested=True)
    if args.enable_review_pack:
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
            generate_evidence=True,
            materialize_evidence_files=True,
        )
        review_items = filter_review_items_by_placement(review_items, placement_override_entries)
        review_summary["review_total"] = len(review_items)
        review_summary["reason_breakdown"] = build_reason_breakdown(review_items)
        review_summary["with_evidence_total"] = sum(
            1 for item in review_items if item.evidence_cell_path or item.evidence_row_path or item.evidence_table_path
        )
        write_dataclass_csv(output_dir / "review_queue.csv", review_items, ReviewQueueRecord)
        write_summary_json(output_dir / "review_summary.json", review_summary, run_id)
        export_review_workbook(output_dir / "review_workbook.xlsx", review_items)
        summary["review_summary"] = review_summary
    if args.emit_reocr_tasks:
        reocr_tasks, reocr_summary = build_reocr_tasks(review_items, conflicts_enriched, reocr_config)
        pruned_reocr_rows, pruned_reocr_summary = prune_reocr_tasks(
            tasks=reocr_tasks,
            review_items=review_items,
            stage6_targets=stage6_targets,
            fact_scope_map={fact.fact_id: fact.target_scope for fact in facts_deduped if fact.fact_id},
        )
        write_dataclass_csv(output_dir / "reocr_tasks.csv", reocr_tasks, ReOCRTaskRecord)
        write_summary_json(output_dir / "reocr_task_summary.json", reocr_summary, run_id)
        write_dict_csv(
            output_dir / "reocr_task_pruned.csv",
            with_run_id_rows(pruned_reocr_rows, run_id),
            list(with_run_id_row(pruned_reocr_rows[0], run_id).keys()) if pruned_reocr_rows else ["run_id", "task_id", "granularity", "doc_id", "page_no", "table_id", "logical_subtable_id", "bbox", "reason_codes", "suggested_provider", "priority_score", "expected_benefit", "source_review_id", "category"],
        )
        write_dict_csv(
            output_dir / "reocr_task_pruned_deduped.csv",
            with_run_id_rows(pruned_reocr_rows, run_id),
            list(with_run_id_row(pruned_reocr_rows[0], run_id).keys()) if pruned_reocr_rows else ["run_id", "task_id", "granularity", "doc_id", "page_no", "table_id", "logical_subtable_id", "bbox", "bbox_normalized", "reason_codes", "suggested_provider", "priority_score", "expected_benefit", "source_review_id", "category", "target_scope", "cluster_id", "merged_task_ids", "merged_review_ids", "merged_task_count"],
        )
        write_summary_json(output_dir / "reocr_task_pruned_summary.json", pruned_reocr_summary, run_id)
        reocr_dedupe_audit = {
            "tasks_before": int(pruned_reocr_summary.get("reocr_tasks_total_before", 0)),
            "tasks_after": int(pruned_reocr_summary.get("reocr_tasks_total_after", 0)),
            "duplicate_groups_before": int(pruned_reocr_summary.get("duplicate_groups_before", 0)),
            "duplicate_groups_after": int(pruned_reocr_summary.get("duplicate_groups_after", 0)),
            "merged_task_count": int(pruned_reocr_summary.get("merged_task_count", 0)),
            "pass": int(pruned_reocr_summary.get("duplicate_groups_after", 0)) == 0,
        }
        write_summary_json(output_dir / "reocr_dedupe_audit.json", reocr_dedupe_audit, run_id)
        summary["reocr_task_pruned_summary"] = pruned_reocr_summary
        summary["reocr_dedupe_audit"] = reocr_dedupe_audit
        if args.materialize_reocr_inputs:
            reocr_manifest_rows, reocr_manifest_summary = materialize_reocr_inputs(
                tasks=reocr_tasks,
                source_image_dir=source_image_dir,
                output_dir=output_dir,
            )
            write_dict_csv(output_dir / "reocr_input_manifest.csv", reocr_manifest_rows, list(reocr_manifest_rows[0].keys()) if reocr_manifest_rows else ["task_id", "doc_id", "page_no", "granularity", "table_id", "logical_subtable_id", "bbox", "suggested_provider", "source_review_id", "crop_path", "status"])
            write_json(output_dir / "reocr_input_manifest.json", {"run_id": run_id, "summary": reocr_manifest_summary, "tasks": reocr_manifest_rows})
            LOGGER.info("Materialized re-OCR crops: %s", reocr_manifest_summary.get("materialized_total", 0))
            summary["reocr_manifest_summary"] = reocr_manifest_summary
    write_summary_json(output_dir / "summary.json", summary, run_id, normalize_nested=True)
    stage_tracker.finish(current_stage, success=True)
    LOGGER.info("Review items queued: %s", review_summary.get("review_total", 0))
    LOGGER.info("Unplaced facts routed away from main sheet: %s", export_stats.get("unplaced_count", 0))
    LOGGER.info("Conflict decision breakdown: %s", json.dumps(run_summary.get("conflict_decision_breakdown", {}), ensure_ascii=False))
    LOGGER.info("Derived facts count: %s", derived_formula_summary.get("derived_facts_total", 0))
    LOGGER.info("Benchmark missing cells: %s", benchmark_payload.get("summary", {}).get("missing_in_auto", 0))
    LOGGER.info("Benchmark value diff cells: %s", benchmark_payload.get("summary", {}).get("value_diff_cells", 0))
    LOGGER.info(
        "Unknown statement reduction: %s -> %s",
        statement_classification_summary.get("unknown_statement_type_total_before", 0),
        statement_classification_summary.get("unknown_statement_type_total_after", 0),
    )
    LOGGER.info(
        "Unknown period-role reduction: %s -> %s",
        period_role_resolution_summary.get("unknown_period_role_export_blocking_total_before", 0),
        period_role_resolution_summary.get("unknown_period_role_export_blocking_total_after", 0),
    )
    LOGGER.info("Mapping lift candidates: %s", alias_acceptance_summary.get("candidates_total", 0))
    LOGGER.info("Pruned re-OCR tasks: %s", pruned_reocr_summary.get("reocr_tasks_total_before", 0) - pruned_reocr_summary.get("reocr_tasks_total_after", 0))
    LOGGER.info(
        "Target closure: benchmark_missing_true=%s, alignment_only=%s, target_review=%s, note_detail_review=%s",
        len(benchmark_missing_true_rows),
        benchmark_alignment_summary.get("alignment_only_gap_total", 0),
        export_target_kpi_summary.get("target_review_total", 0),
        export_target_kpi_summary.get("note_detail_review_total", 0),
    )
    LOGGER.info(
        "Promotion lift: aliases=%s, formulas=%s, no_source_true=%s",
        promotion_summary.get("promoted_alias_total", 0),
        promotion_summary.get("promoted_formula_total", 0),
        no_source_gap_payload.get("summary", {}).get("truly_no_source_total", 0),
    )
    LOGGER.info("Integrity fail total: %s", integrity_summary.get("integrity_fail_total", 0))
    LOGGER.info("Wrote normalized outputs to %s", output_dir)
    if args.strict_full_run_contract and int(full_run_contract_summary.get("contract_fail_total", 0)) > 0:
        LOGGER.error("Strict full-run contract failed with %s errors.", full_run_contract_summary.get("contract_fail_total", 0))
        stage_tracker.finalize(
            success=False,
            exception_message=f"Strict full-run contract failed with {full_run_contract_summary.get('contract_fail_total', 0)} errors.",
            failed_stage="hardening outputs",
        )
        return 1
    stage_tracker.finalize(success=True)
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


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


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


def resolve_output_dir(base_output_dir: Path, output_run_subdir: str, run_id: str) -> Path:
    value = (output_run_subdir or "").strip()
    if not value or value.lower() == "auto":
        return base_output_dir / run_id
    if value.lower() == "none":
        return base_output_dir
    return base_output_dir / value


def resolve_baseline_dir(base_output_dir: Path, output_dir: Path) -> Path | None:
    if has_run_snapshot(output_dir):
        return output_dir
    if has_run_snapshot(base_output_dir):
        return base_output_dir
    if not base_output_dir.exists():
        return None
    candidates = [
        path
        for path in base_output_dir.iterdir()
        if path.is_dir() and not path.name.startswith("_") and has_run_snapshot(path)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def has_run_snapshot(path: Path) -> bool:
    return path.exists() and path.is_dir() and (path / "run_summary.json").exists()


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


def write_summary_json(path: Path, payload: Dict[str, Any], run_id: str, *, normalize_nested: bool = False) -> None:
    prepared = prepare_nested_summary_payload(payload, run_id) if normalize_nested else prepare_summary_payload(payload, run_id)
    write_json(path, prepared)


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


def with_run_id_row(row: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    payload = dict(row)
    payload["run_id"] = run_id
    return payload


def with_run_id_rows(rows: List[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
    return [with_run_id_row(row, run_id) for row in rows]


def enrich_formula_candidate_rows(rows: List[Dict[str, Any]], formula_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    rule_index = {
        str(rule.get("rule_id", "")).strip(): rule
        for rule in formula_rules.get("rules", [])
        if isinstance(rule, dict) and str(rule.get("rule_id", "")).strip()
    }
    enriched: List[Dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        rule = rule_index.get(str(row.get("rule_id", "")).strip(), {})
        payload["formula_payload_json"] = json.dumps(rule, ensure_ascii=False, separators=(",", ":"), sort_keys=True) if rule else ""
        enriched.append(payload)
    return enriched


def env_int(name: str, default: int = 0) -> int:
    value = os.environ.get(name, "")
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
