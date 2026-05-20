from __future__ import annotations

import argparse
import difflib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from project_paths import STANDARD_METRICS_GENERATED_ROOT, WEB_MAPPING_STORE_PATH
from standardize.manifest import generate_run_id

from .audit import build_summary, validate_output_base
from .confidence import build_confidence_bulk_accept_preview
from .export import OUTPUT_FILENAMES, export_standard_mapping_run, write_json
from .loader import infer_doc_id, load_raw_metrics
from .llm import LLMSuggestionService, load_deepseek_config
from .models import MappingCandidate, MappingResult, RawMetricRow, StandardMappingRun, StandardRegistry, StandardTerm
from .normalizer import normalize_metric_name
from .registry import extend_registry_aliases, load_standard_registry
from .relations import SAFE_RELATION_TYPES, UNSAFE_RELATION_TYPES, find_raw_relation_matches, find_relation_matches
from .search import normalize_standard_code
from .store import LocalMappingStore


def run_standard_mapping(*, args: argparse.Namespace, cli_args: Sequence[str] | None = None) -> StandardMappingRun:
    input_path = Path(args.input).resolve()
    output_base = Path(args.output_dir).resolve()
    validate_output_base(output_base)
    doc_id = infer_doc_id(input_path, str(getattr(args, "doc_id", "") or ""))
    run_id = generate_run_id(cli_args or build_cli_args_for_manifest(args))
    output_dir = resolve_run_output_dir(output_base, run_id)

    registry_path = Path(args.mapping_registry).resolve() if getattr(args, "mapping_registry", "") else None
    registry = load_standard_registry(registry_path)
    mapping_store_path = Path(str(getattr(args, "mapping_store_path", "") or WEB_MAPPING_STORE_PATH)).resolve()
    store = LocalMappingStore(mapping_store_path)
    store.sync_registry(registry)
    extend_registry_aliases(registry, store.load_local_alias_entries(registry))
    raw_rows = load_raw_metrics(input_path, company_name_override=str(getattr(args, "company_name", "") or ""))
    llm_mock_arg = getattr(args, "llm_mock", None)
    llm_mock_override = None if llm_mock_arg is None else bool(llm_mock_arg)
    llm_config = load_deepseek_config(
        env_file=str(getattr(args, "llm_env_file", "") or "") or None,
        enabled_override=_llm_enabled_override(args),
        model_override=str(getattr(args, "llm_model", "") or "") or None,
        mock_mode=llm_mock_override,
        cache_enabled_override=False if bool(getattr(args, "disable_llm_cache", False)) else None,
    )
    llm_service = LLMSuggestionService(config=llm_config, store=store, registry=registry) if llm_config.enabled else None
    mapped_rows = [map_raw_metric(row, registry, llm_service=llm_service) for row in raw_rows]
    llm_summary = (
        llm_service.summary()
        if llm_service is not None
        else {
            "pass": True,
            "llm_enabled": False,
            "mock_llm_used": bool(getattr(args, "llm_mock", False)),
            "live_llm_executed": False,
            "suggestions_total": 0,
            "cached_suggestions_total": 0,
            "invalid_responses_total": 0,
            "disabled_reason": llm_config.disabled_reason,
        }
    )

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
        llm_suggestion_rows=llm_service.records if llm_service is not None else [],
        llm_mapping_summary=llm_summary,
    )
    before_alias_count = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    store.write_snapshot(output_dir / "mapping_store_snapshot.yml")
    build_confidence_bulk_accept_preview(
        output_dir,
        before_alias_count=before_alias_count,
        after_alias_count=before_alias_count,
    )
    store.export_aliases()
    store.export_decision_audit()
    store.export_llm_suggestions()
    store.export_llm_suggestion_audit()
    actual_output_files = sorted(str(path) for path in output_dir.iterdir() if path.is_file())
    summary["output_files"] = actual_output_files
    summary["mapping_store_path"] = str(mapping_store_path)
    summary["local_aliases_total"] = before_alias_count
    summary["llm_mapping"] = llm_summary
    manifest["output_files"] = actual_output_files
    manifest["mapping_store_path"] = str(mapping_store_path)
    manifest["llm_mapping"] = llm_summary
    write_json(output_dir / "standard_mapping_summary.json", summary)
    write_json(output_dir / "standard_mapping_run_manifest.json", manifest)
    write_json(output_dir / "llm_mapping_summary.json", llm_summary)

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


def map_raw_metric(raw: RawMetricRow, registry: StandardRegistry, *, llm_service: LLMSuggestionService | None = None) -> MappingResult:
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

    alias_entries = registry.normalized_alias_lookup.get(normalized, [])
    local_alias_entries = [entry for entry in alias_entries if entry.source != "base"]
    if local_alias_entries:
        return _map_alias_entries(raw, local_alias_entries, method="local_alias", note="按人工记住的本地别名匹配。")

    legacy_entries = registry.normalized_legacy_alias_lookup.get(normalized, [])
    local_legacy_entries = [entry for entry in legacy_entries if entry.source != "base"]
    if local_legacy_entries:
        return _map_alias_entries(raw, local_legacy_entries, method="local_alias", note="按人工记住的本地旧名称匹配。")

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
            confidence=1.0,
            notes="按标准指标名称精确匹配。",
            review_required=False,
            candidates=[candidate],
        )
    if len(exact_terms) > 1:
        candidates = [_candidate(raw, term, index, 1.0, "exact", "", True, "duplicate_standard_name") for index, term in enumerate(exact_terms, start=1)]
        return _review_required(raw, candidates, "candidate", "标准术语存在重复名称，需人工确认。", "duplicate_standard_name")

    base_alias_entries = [entry for entry in alias_entries if entry.source == "base"]
    if base_alias_entries:
        return _map_alias_entries(raw, base_alias_entries, method="alias", note="按标准指标别名匹配。")

    base_legacy_entries = [entry for entry in legacy_entries if entry.source == "base"]
    if base_legacy_entries:
        return _map_alias_entries(raw, base_legacy_entries, method="legacy_alias", note="按安全旧名称别名匹配。")

    safe_relation_result = _map_safe_relation(raw, registry)
    if safe_relation_result is not None:
        return safe_relation_result

    code_candidates = _code_candidates(raw, registry)
    if code_candidates:
        top = code_candidates[0]
        return MappingResult(
            raw=raw,
            standard_code=top.candidate_code,
            standard_name=top.candidate_name,
            mapping_method="exact",
            mapping_status="mapped",
            confidence=top.candidate_score,
            notes="按标准指标编码精确匹配。",
            review_required=False,
            candidates=code_candidates,
        )

    fuzzy_candidates = _fuzzy_candidates(raw, registry)
    llm_candidates = _llm_candidate_pool(raw, registry, limit=llm_service.config.max_candidates if llm_service is not None else 20)
    if not fuzzy_candidates and not llm_candidates:
        return MappingResult(
            raw=raw,
            standard_code="",
            standard_name="",
            mapping_method="none",
            mapping_status="unmapped",
            confidence=None,
            notes="未找到可用标准指标候选。",
            review_required=True,
            issue_reason="unmapped_metric",
            candidates=[],
        )

    top = fuzzy_candidates[0] if fuzzy_candidates else None
    second_score = fuzzy_candidates[1].candidate_score if len(fuzzy_candidates) > 1 else 0.0
    very_safe = bool(top) and top.candidate_score >= 0.97 and top.candidate_score - second_score >= 0.05
    if very_safe and top is not None:
        return MappingResult(
            raw=raw,
            standard_code=top.candidate_code,
            standard_name=top.candidate_name,
            mapping_method="candidate",
            mapping_status="mapped",
            confidence=top.candidate_score,
            notes="候选名称高度相似，按安全候选自动映射。",
            review_required=False,
            candidates=fuzzy_candidates,
        )
    if llm_service is not None and _llm_eligible(raw):
        llm_result = _try_llm_suggestion(raw, registry, llm_service, llm_candidates or fuzzy_candidates)
        if llm_result is not None:
            return llm_result
    if fuzzy_candidates:
        return _review_required(raw, fuzzy_candidates, "candidate", "存在相似标准指标候选，需人工确认。", "candidate_requires_review")
    return MappingResult(
        raw=raw,
        standard_code="",
        standard_name="",
        mapping_method="none",
        mapping_status="unmapped",
        confidence=None,
        notes="本地候选不够明确，需人工确认。",
        review_required=True,
        issue_reason="unmapped_metric",
        candidates=llm_candidates,
    )


def _map_unsafe_relation(raw: RawMetricRow, registry: StandardRegistry) -> MappingResult | None:
    relations = [relation for relation in find_raw_relation_matches(raw.metric_name, registry.relations) if relation.relation_type in UNSAFE_RELATION_TYPES]
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
        confidence=top.candidate_score if top else 0.9,
        relation_type=relation.relation_type,
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
        confidence=0.98,
        relation_type=relation.relation_type,
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
        entry_method = "local_alias" if entry.source != "base" else method
        candidate = _candidate(raw, entry.term, 1, 0.98, entry_method, entry.alias_type, False, "")
        return MappingResult(
            raw=raw,
            standard_code=entry.term.code,
            standard_name=entry.term.name,
            mapping_method=entry_method,
            mapping_status="mapped",
            confidence=0.98,
            relation_type=entry.alias_type,
            notes=entry.note or note,
            review_required=False,
            candidates=[candidate],
        )
    candidates = [
        _candidate(raw, entry.term, index, 0.98, "local_alias" if entry.source != "base" else method, entry.alias_type, True, "ambiguous_alias")
        for index, entry in enumerate(entries, start=1)
    ]
    return _review_required(raw, candidates, method, "别名匹配不唯一或未标记为安全自动映射，需人工确认。", "ambiguous_alias")


def _code_candidates(raw: RawMetricRow, registry: StandardRegistry) -> list[MappingCandidate]:
    normalized = normalize_standard_code(str(raw.metric_name or ""))
    term = registry.term_by_code.get(normalized)
    if term is None:
        return []
    return [_candidate(raw, term, 1, 1.0, "exact", "", False, "")]


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


def _llm_candidate_pool(raw: RawMetricRow, registry: StandardRegistry, limit: int = 20) -> list[MappingCandidate]:
    normalized = normalize_metric_name(raw.metric_name)
    scored: list[tuple[StandardTerm, float]] = []
    for term in registry.terms:
        if not term.enabled:
            continue
        score = _term_score(normalized, term)
        scored.append((term, score))
    ordered = sorted(scored, key=lambda item: (-item[1], item[0].code))[: max(1, min(limit, 50))]
    return [
        _candidate(raw, term, index, round(score, 6), "candidate", "", True, "llm_candidate_context")
        for index, (term, score) in enumerate(ordered, start=1)
    ]


def _try_llm_suggestion(
    raw: RawMetricRow,
    registry: StandardRegistry,
    llm_service: LLMSuggestionService,
    candidates: list[MappingCandidate],
) -> MappingResult | None:
    suggestion = llm_service.suggest(raw, candidates)
    if suggestion is None:
        return None
    validation_status = str(suggestion.get("validation_status", "") or "")
    if validation_status == "valid" and suggestion.get("candidate_code"):
        code = str(suggestion.get("candidate_code", "") or "")
        term = registry.term_by_code.get(code)
        standard_name = term.name if term is not None else str(suggestion.get("candidate_name", "") or "")
        relation_type = str(suggestion.get("relation_type", "") or "")
        confidence = _safe_float(suggestion.get("confidence"), default=0.0)
        issue_reason = "llm_suggested_requires_review"
        if relation_type in {"aggregate", "split", "formula", "ambiguous", "broader_than", "narrower_than"}:
            issue_reason = f"{relation_type}_llm_relation_requires_review"
        llm_candidate = MappingCandidate(
            raw_metric_id=raw.raw_metric_id,
            raw_metric_name=raw.metric_name,
            candidate_rank=1,
            candidate_code=code,
            candidate_name=standard_name,
            candidate_score=confidence,
            candidate_method="llm_suggested",
            relation_type=relation_type,
            review_required=True,
            issue_reason=issue_reason,
        )
        remaining = [candidate for candidate in candidates if candidate.candidate_code != code]
        return MappingResult(
            raw=raw,
            standard_code=code,
            standard_name=standard_name,
            mapping_method="llm_suggested",
            mapping_status="review_required",
            confidence=confidence,
            relation_type=relation_type,
            notes=str(suggestion.get("reason", "") or "DeepSeek 给出候选建议，需人工确认。"),
            review_required=True,
            issue_reason=issue_reason,
            candidates=[llm_candidate, *remaining],
            llm_suggestion=suggestion,
        )
    if validation_status == "valid_unknown":
        return MappingResult(
            raw=raw,
            standard_code="",
            standard_name="",
            mapping_method="none",
            mapping_status="unmapped",
            confidence=None,
            relation_type="unknown",
            notes=str(suggestion.get("reason", "") or "DeepSeek 未在候选列表中找到足够合适的标准术语。"),
            review_required=True,
            issue_reason="llm_unknown",
            candidates=candidates,
            llm_suggestion=suggestion,
        )
    return MappingResult(
        raw=raw,
        standard_code="",
        standard_name="",
        mapping_method="none",
        mapping_status="unmapped" if not candidates else "review_required",
        confidence=None,
        relation_type="unknown",
        notes="DeepSeek 返回结果未通过候选约束校验，需人工确认。",
        review_required=True,
        issue_reason=validation_status or "llm_invalid_response",
        candidates=candidates,
        llm_suggestion=suggestion,
    )


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


def _llm_eligible(raw: RawMetricRow) -> bool:
    return bool(str(raw.metric_name or "").strip()) and bool(str(raw.metric_value or "").strip())


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
        confidence=top.candidate_score if top else None,
        relation_type=top.relation_type if top else "",
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
        "stage": "stage_15_2_llm_mapping_suggestions",
        "cli_args": build_cli_args_for_manifest(args),
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "doc_id": doc_id,
        "mapping_registry": registry.registry_path,
        "mapping_aliases": registry.aliases_path,
        "mapping_relations": registry.relations_path,
        "mapping_policy": "config/mapping_policy.yml",
        "standard_terms_total": len(registry.terms),
        "summary_metrics_snapshot": dict(summary),
        "output_files": list(summary.get("output_files", [])),
        "no_ocr_api_called": True,
        "no_paddle_ocr_called": True,
        "no_accounting_template_filled": True,
        "base_configs_mutated": False,
        "llm_direct_base_config_mutation": False,
        "llm_candidate_constrained": True,
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


def _llm_enabled_override(args: argparse.Namespace) -> bool | None:
    if bool(getattr(args, "disable_llm_mapping", False)):
        return False
    if bool(getattr(args, "enable_llm_mapping", False)):
        return True
    if bool(getattr(args, "llm_mock", False)):
        return True
    return None


def _safe_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
        "mapping_store_path": run.summary.get("mapping_store_path", ""),
        "local_aliases_total": run.summary.get("local_aliases_total", 0),
        "output_files": list(run.output_files),
    }
