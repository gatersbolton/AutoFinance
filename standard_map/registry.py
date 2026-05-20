from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from project_paths import (
    STANDARD_TERM_ALIASES_PATH,
    STANDARD_TERM_RELATIONS_PATH,
    STANDARD_TERMS_PATH,
)

from .models import AliasEntry, StandardRegistry, StandardTerm, TermRelation
from .normalizer import normalize_metric_name
from .relations import normalize_relation_type


def load_standard_registry(
    registry_path: Path | None = None,
    *,
    aliases_path: Path | None = None,
    relations_path: Path | None = None,
) -> StandardRegistry:
    registry_path = (registry_path or STANDARD_TERMS_PATH).resolve()
    aliases_path = (aliases_path or STANDARD_TERM_ALIASES_PATH).resolve()
    relations_path = (relations_path or STANDARD_TERM_RELATIONS_PATH).resolve()

    terms = _load_terms(registry_path)
    term_by_code = {term.code: term for term in terms}
    aliases = _aliases_from_terms(terms)
    aliases.extend(_load_alias_entries(aliases_path, term_by_code))
    relations = _load_relations(relations_path, term_by_code)

    normalized_term_lookup: dict[str, list[StandardTerm]] = defaultdict(list)
    for term in terms:
        normalized_term_lookup[normalize_metric_name(term.name)].append(term)

    normalized_alias_lookup: dict[str, list[AliasEntry]] = defaultdict(list)
    normalized_legacy_alias_lookup: dict[str, list[AliasEntry]] = defaultdict(list)
    for alias in aliases:
        if not alias.alias:
            continue
        target = normalized_legacy_alias_lookup if alias.alias_type == "legacy_alias" else normalized_alias_lookup
        target[normalize_metric_name(alias.alias)].append(alias)

    return StandardRegistry(
        terms=terms,
        aliases=aliases,
        relations=relations,
        registry_path=str(registry_path),
        aliases_path=str(aliases_path),
        relations_path=str(relations_path),
        term_by_code=term_by_code,
        normalized_term_lookup=dict(normalized_term_lookup),
        normalized_alias_lookup=dict(normalized_alias_lookup),
        normalized_legacy_alias_lookup=dict(normalized_legacy_alias_lookup),
    )


def extend_registry_aliases(registry: StandardRegistry, aliases: list[AliasEntry]) -> StandardRegistry:
    if not aliases:
        return registry
    registry.aliases.extend(aliases)
    for alias in aliases:
        if not alias.alias:
            continue
        target = registry.normalized_legacy_alias_lookup if alias.alias_type == "legacy_alias" else registry.normalized_alias_lookup
        normalized = normalize_metric_name(alias.alias)
        target.setdefault(normalized, []).append(alias)
    return registry


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _string_list(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return (str(value).strip(),)


def _load_terms(path: Path) -> list[StandardTerm]:
    payload = _load_yaml(path)
    items = payload.get("terms", payload.get("standard_terms", []))
    if not isinstance(items, list):
        return []
    terms: list[StandardTerm] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not code or not name:
            continue
        terms.append(
            StandardTerm(
                code=code,
                name=name,
                category=str(item.get("category", "") or "").strip(),
                aliases=_string_list(item.get("aliases", ())),
                statement_scope=str(item.get("statement_scope", "") or "").strip(),
                metric_type=str(item.get("metric_type", "") or "").strip(),
                period_type=str(item.get("period_type", "") or "").strip(),
                legacy_aliases=_string_list(item.get("legacy_aliases", ())),
                description=str(item.get("description", "") or "").strip(),
                enabled=bool(item.get("enabled", True)),
                notes=str(item.get("notes", "") or "").strip(),
            )
        )
    return terms


def _aliases_from_terms(terms: list[StandardTerm]) -> list[AliasEntry]:
    entries: list[AliasEntry] = []
    for term in terms:
        for alias in term.aliases:
            if normalize_metric_name(alias) == normalize_metric_name(term.name):
                continue
            entries.append(AliasEntry(term=term, alias=alias, alias_type="exact_alias", safe_auto_map=True, source="base", note=term.notes))
        for alias in term.legacy_aliases:
            entries.append(AliasEntry(term=term, alias=alias, alias_type="legacy_alias", safe_auto_map=True, source="base", note=term.notes))
    return entries


def _load_alias_entries(path: Path, term_by_code: dict[str, StandardTerm]) -> list[AliasEntry]:
    payload = _load_yaml(path)
    items = payload.get("aliases", [])
    if not isinstance(items, list):
        return []
    entries: list[AliasEntry] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("canonical_code", item.get("code", "")) or "").strip()
        term = term_by_code.get(code)
        if term is None:
            continue
        alias = str(item.get("alias", "") or "").strip()
        if not alias:
            continue
        alias_type = normalize_relation_type(item.get("alias_type", "exact_alias")) or "exact_alias"
        if alias_type not in {"exact_alias", "legacy_alias"}:
            alias_type = "exact_alias"
        entries.append(
            AliasEntry(
                term=term,
                alias=alias,
                alias_type=alias_type,
                safe_auto_map=bool(item.get("safe_auto_map", True)),
                source="base",
                note=str(item.get("note", "") or "").strip(),
            )
        )
    return entries


def _load_relations(path: Path, term_by_code: dict[str, StandardTerm]) -> list[TermRelation]:
    payload = _load_yaml(path)
    items = payload.get("relations", [])
    if not isinstance(items, list):
        return []
    relations: list[TermRelation] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        relation_type = normalize_relation_type(item.get("relation_type", ""))
        canonical_code = str(item.get("canonical_code", "") or "").strip()
        canonical_name = str(item.get("canonical_name", "") or "").strip()
        term = term_by_code.get(canonical_code)
        if term is not None and not canonical_name:
            canonical_name = term.name
        relations.append(
            TermRelation(
                relation_type=relation_type,
                relation_id=str(item.get("relation_id", "") or "").strip(),
                canonical_code=canonical_code,
                canonical_name=canonical_name,
                raw_names=_string_list(item.get("raw_names", ())),
                related_names=_string_list(item.get("related_names", ())),
                candidate_codes=_string_list(item.get("candidate_codes", ())),
                formula_json=json.dumps(item.get("formula", {}) or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                if item.get("formula") is not None
                else "",
                auto_apply=bool(item.get("auto_apply", False)),
                review_required=bool(item.get("review_required", True)),
                enabled=bool(item.get("enabled", True)),
                note=str(item.get("note", item.get("notes", "")) or "").strip(),
            )
        )
    return relations
