from __future__ import annotations

import argparse
import difflib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from project_paths import STANDARD_METRICS_GENERATED_ROOT
from standardize.manifest import generate_run_id

from .audit import build_summary, validate_output_base
from .export import OUTPUT_FILENAMES, export_standard_mapping_run, write_json
from .loader import infer_doc_id, load_raw_metrics
from .models import MappingCandidate, MappingResult, RawMetricRow, StandardMappingRun, StandardRegistry, StandardTerm
from .normalizer import normalize_metric_name
from .registry import load_standard_registry
from .relations import SAFE_RELATION_TYPES, UNSAFE_RELATION_TYPES, find_relation_matches


def run_standard_mapping(*, args: argparse.Namespace, cli_args: Sequence[str] | None = None) -> StandardMappingRun:
    input_path = Path(args.input).resolve()
    output_base = Path(args.output_dir).resolve()
    validate_output_base(output_base)
    doc_id = infer_doc_id(input_path, str(getattr(args, "doc_id", "") or ""))
    run_id = generate_run_id(cli_args or build_cli_args_for_manifest(args))
    output_dir = resolve_run_output_dir(output_base, run_id)

    registry_path = Path(args.mapping_registry).resolve() if getattr(args, "mapping_registry", "") else None
    registry = load_standard_registry(registry_path)
    raw_rows = load_raw_metrics(input_path, company_name_override=str(getattr(args, "company_name", "") or ""))
    mapped_rows = [map_raw_metric(row, registry) for row in raw_rows]

    expected_output_files = [str(output_dir / name) for name in OUTPUT_FILENAMES]
    summary = build_summary(run_id=run_id, rows=mapped_rows, output_files=expected_output_files)
    manifest = build_manifest(
        run_id=run_id,
        args=args,
        input_path=input_path,
        output_dir=output_dir,
        doc_id=doc_id,
        registry=registry,
        summary=summary,
    )
    actual_output_files = export_standard_mapping_run(
        output_dir=output_dir,
        rows=mapped_rows,
        summary=summary,
        manifest=manifest,
    )
    summary["output_files"] = actual_output_files
    manifest["output_files"] = actual_output_files
    write_json(output_dir / "standard_mapping_summary.json", summary)
    write_json(output_dir / "standard_mapping_run_manifest.json", manifest)

    return StandardMappingRun(
        run_id=run_id,
        input_path=str(input_path),
        output_dir=str(output_dir),
        doc_id=doc_id,
        rows=mapped_rows,
        summary=summary,
        manifest=manifest,
        output_files=actual_output_files,
    )


def resolve_run_output_dir(output_base: Path, run_id: str) -> Path:
    return output_base / run_id


def map_raw_metric(raw: RawMetricRow, registry: StandardRegistry) -> MappingResult:
    normalized = normalize_metric_name(raw.metric_name)
    if not normalized:
        return MappingResult(
            raw=raw,
            standard_code="",
            standard_name="",
            mapping_method="none",
            mapping_status="skipped",
            notes="原始指标名为空，未执行映射。",
            review_required=False,
            issue_reason="empty_metric_name",
            candidates=[],
        )

    unsafe_relation_result = _map_unsafe_relation(raw, registry)
    if unsafe_relation_result is not None:
        return unsafe_relation_result

    exact_terms = registry.normalized_term_lookup.get(normalized, [])
    if len(exact_terms) == 1:
        term = exact_terms[0]
        candidate = _candidate(raw, term, 1, 1.0, "exact", "", False, "")
        return MappingResult(
            raw=raw,
            standard_code=term.code,
            standard_name=term.name,
            mapping_method="exact",
            mapping_status="mapped",
            notes="按标准指标名称精确匹配。",
            review_required=False,
            candidates=[candidate],
        )
    if len(exact_terms) > 1:
        candidates = [_candidate(raw, term, index, 1.0, "exact", "", True, "duplicate_standard_name") for index, term in enumerate(exact_terms, start=1)]
        return _review_required(raw, candidates, "candidate", "标准术语存在重复名称，需人工确认。", "duplicate_standard_name")

    alias_entries = registry.normalized_alias_lookup.get(normalized, [])
    if alias_entries:
        return _map_alias_entries(raw, alias_entries, method="alias", note="按标准指标别名匹配。")

    legacy_entries = registry.normalized_legacy_alias_lookup.get(normalized, [])
    if legacy_entries:
        return _map_alias_entries(raw, legacy_entries, method="legacy_alias", note="按安全旧名称别名匹配。")

    safe_relation_result = _map_safe_relation(raw, registry)
    if safe_relation_result is not None:
        return safe_relation_result

    fuzzy_candidates = _fuzzy_candidates(raw, registry)
    if not fuzzy_candidates:
        return MappingResult(
            raw=raw,
            standard_code="",
            standard_name="",
            mapping_method="none",
            mapping_status="unmapped",
            notes="未找到可用标准指标候选。",
            review_required=True,
            issue_reason="unmapped_metric",
            candidates=[],
        )

    top = fuzzy_candidates[0]
    second_score = fuzzy_candidates[1].candidate_score if len(fuzzy_candidates) > 1 else 0.0
    very_safe = top.candidate_score >= 0.97 and top.candidate_score - second_score >= 0.05
    if very_safe:
        return MappingResult(
            raw=raw,
            standard_code=top.candidate_code,
            standard_name=top.candidate_name,
            mapping_method="candidate",
            mapping_status="mapped",
            notes="候选名称高度相似，按安全候选自动映射。",
            review_required=False,
            candidates=fuzzy_candidates,
        )
    return _review_required(raw, fuzzy_candidates, "candidate", "存在相似标准指标候选，需人工确认。", "candidate_requires_review")


def _map_unsafe_relation(raw: RawMetricRow, registry: StandardRegistry) -> MappingResult | None:
    relations = [relation for relation in find_relation_matches(raw.metric_name, registry.relations) if relation.relation_type in UNSAFE_RELATION_TYPES]
    if not relations:
        return None
    relation = relations[0]
    candidates: list[MappingCandidate] = []
    candidate_codes = list(relation.candidate_codes)
    if relation.canonical_code:
        candidate_codes.insert(0, relation.canonical_code)
    for code in candidate_codes:
        term = registry.term_by_code.get(code)
        if term is None:
            continue
        candidates.append(
            _candidate(
                raw,
                term,
                len(candidates) + 1,
                0.92,
                "relation_review",
                relation.relation_type,
                True,
                f"{relation.relation_type}_relation_requires_review",
            )
        )
    if not candidates and relation.canonical_name:
        candidates.append(
            MappingCandidate(
                raw_metric_id=raw.raw_metric_id,
                raw_metric_name=raw.metric_name,
                candidate_rank=1,
                candidate_code=relation.canonical_code,
                candidate_name=relation.canonical_name,
                candidate_score=0.9,
                candidate_method="relation_review",
                relation_type=relation.relation_type,
                review_required=True,
                issue_reason=f"{relation.relation_type}_relation_requires_review",
            )
        )
    top = candidates[0] if candidates else None
    return MappingResult(
        raw=raw,
        standard_code=top.candidate_code if top else "",
        standard_name=top.candidate_name if top else "",
        mapping_method="relation_review",
        mapping_status="review_required",
        notes=relation.note or "该指标涉及口径关系，需人工确认。",
        review_required=True,
        issue_reason=f"{relation.relation_type}_relation_requires_review",
        candidates=candidates,
    )


def _map_safe_relation(raw: RawMetricRow, registry: StandardRegistry) -> MappingResult | None:
    relations = [relation for relation in find_relation_matches(raw.metric_name, registry.relations) if relation.relation_type in SAFE_RELATION_TYPES]
    if not relations:
        return None
    relation = relations[0]
    term = registry.term_by_code.get(relation.canonical_code)
    if term is None:
        return None
    method = "legacy_alias" if relation.relation_type == "legacy_alias" else "alias"
    candidate = _candidate(raw, term, 1, 0.98, method, relation.relation_type, False, "")
    return MappingResult(
        raw=raw,
        standard_code=term.code,
        standard_name=term.name,
        mapping_method=method,
        mapping_status="mapped",
        notes=relation.note or "按安全关系别名自动映射。",
        review_required=False,
        candidates=[candidate],
    )


def _map_alias_entries(raw: RawMetricRow, entries, *, method: str, note: str) -> MappingResult:
    deduped = {}
    for entry in entries:
        current = deduped.get(entry.term.code)
        if current is None or (entry.safe_auto_map and not current.safe_auto_map):
            deduped[entry.term.code] = entry
    entries = list(deduped.values())
    if len(entries) == 1 and entries[0].safe_auto_map:
        entry = entries[0]
        candidate = _candidate(raw, entry.term, 1, 0.98, method, "", False, "")
        return MappingResult(
            raw=raw,
            standard_code=entry.term.code,
            standard_name=entry.term.name,
            mapping_method=method,
            mapping_status="mapped",
            notes=entry.note or note,
            review_required=False,
            candidates=[candidate],
        )
    candidates = [
        _candidate(raw, entry.term, index, 0.98, method, "", True, "ambiguous_alias")
        for index, entry in enumerate(entries, start=1)
    ]
    return _review_required(raw, candidates, method, "别名匹配不唯一或未标记为安全自动映射，需人工确认。", "ambiguous_alias")


def _fuzzy_candidates(raw: RawMetricRow, registry: StandardRegistry, limit: int = 3) -> list[MappingCandidate]:
    normalized = normalize_metric_name(raw.metric_name)
    scored: dict[str, tuple[StandardTerm, float]] = {}
    for term in registry.terms:
        score = _term_score(normalized, term)
        if score < 0.72:
            continue
        current = scored.get(term.code)
        if current is None or score > current[1]:
            scored[term.code] = (term, score)
    ordered = sorted(scored.values(), key=lambda item: (-item[1], item[0].code))[:limit]
    return [
        _candidate(raw, term, index, round(score, 6), "candidate", "", True, "candidate_requires_review")
        for index, (term, score) in enumerate(ordered, start=1)
    ]


def _term_score(normalized: str, term: StandardTerm) -> float:
    names = [term.name, *term.aliases, *term.legacy_aliases]
    best = 0.0
    for name in names:
        target = normalize_metric_name(name)
        if not target:
            continue
        if normalized == target:
            return 1.0
        sequence_score = difflib.SequenceMatcher(None, normalized, target).ratio()
        union = set(normalized) | set(target)
        overlap = len(set(normalized) & set(target)) / len(union) if union else 0.0
        prefix_bonus = 0.08 if target.startswith(normalized) or normalized.startswith(target) else 0.0
        score = min(1.0, sequence_score * 0.62 + overlap * 0.3 + prefix_bonus)
        best = max(best, score)
    return best


def _candidate(
    raw: RawMetricRow,
    term: StandardTerm,
    rank: int,
    score: float,
    method: str,
    relation_type: str,
    review_required: bool,
    issue_reason: str,
) -> MappingCandidate:
    return MappingCandidate(
        raw_metric_id=raw.raw_metric_id,
        raw_metric_name=raw.metric_name,
        candidate_rank=rank,
        candidate_code=term.code,
        candidate_name=term.name,
        candidate_score=score,
        candidate_method=method,
        relation_type=relation_type,
        review_required=review_required,
        issue_reason=issue_reason,
    )


def _review_required(raw: RawMetricRow, candidates: list[MappingCandidate], method: str, note: str, issue_reason: str) -> MappingResult:
    top = candidates[0] if candidates else None
    return MappingResult(
        raw=raw,
        standard_code=top.candidate_code if top else "",
        standard_name=top.candidate_name if top else "",
        mapping_method=method,
        mapping_status="review_required",
        notes=note,
        review_required=True,
        issue_reason=issue_reason,
        candidates=candidates,
    )


def build_manifest(
    *,
    run_id: str,
    args: argparse.Namespace,
    input_path: Path,
    output_dir: Path,
    doc_id: str,
    registry: StandardRegistry,
    summary: dict[str, object],
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": "stage_13_standard_metrics_mapping",
        "cli_args": build_cli_args_for_manifest(args),
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "doc_id": doc_id,
        "mapping_registry": registry.registry_path,
        "mapping_aliases": registry.aliases_path,
        "mapping_relations": registry.relations_path,
        "standard_terms_total": len(registry.terms),
        "summary_metrics_snapshot": dict(summary),
        "output_files": list(summary.get("output_files", [])),
        "no_ocr_api_called": True,
        "no_paddle_ocr_called": True,
        "no_accounting_template_filled": True,
        "base_configs_mutated": False,
    }


def build_cli_args_for_manifest(args: argparse.Namespace) -> list[str]:
    values: list[str] = []
    for key, value in vars(args).items():
        if isinstance(value, bool):
            if value:
                values.append(f"--{key.replace('_', '-')}")
            continue
        if value not in ("", None):
            values.extend([f"--{key.replace('_', '-')}", str(value)])
    return values


def mapping_run_to_web_summary(run: StandardMappingRun) -> dict[str, object]:
    return {
        "pass": bool(run.summary.get("pass")),
        "run_id": run.run_id,
        "input_path": run.input_path,
        "output_dir": run.output_dir,
        "standardized_metrics_csv": str(Path(run.output_dir) / "standardized_metrics.csv"),
        "standardized_metrics_xlsx": str(Path(run.output_dir) / "standardized_metrics.xlsx"),
        "standardized_rows_total": run.summary.get("standardized_rows_total", 0),
        "mapped_total": run.summary.get("mapped_total", 0),
        "review_required_total": run.summary.get("review_required_total", 0),
        "unmapped_total": run.summary.get("unmapped_total", 0),
        "output_files": list(run.output_files),
    }
