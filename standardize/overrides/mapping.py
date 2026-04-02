from __future__ import annotations

from typing import Dict, List, Sequence

from ..mapping.masterdata import normalize_label_for_matching
from ..models import AliasRecord, FactRecord, TemplateSubject


def build_manual_alias_records(entries: Sequence[Dict[str, object]], subjects: Sequence[TemplateSubject]) -> List[AliasRecord]:
    subject_by_code = {subject.code: subject for subject in subjects}
    records: List[AliasRecord] = []
    for entry in entries:
        if str(entry.get("action_type", "")).strip() != "accept_mapping_alias":
            continue
        canonical_code = str(entry.get("canonical_code", "")).strip()
        alias = str(entry.get("alias", "")).strip()
        if not canonical_code or not alias or canonical_code not in subject_by_code:
            continue
        subject = subject_by_code[canonical_code]
        records.append(
            AliasRecord(
                canonical_code=subject.code,
                canonical_name=subject.canonical_name,
                alias=alias,
                alias_type=str(entry.get("alias_type", "exact_alias")).strip() or "exact_alias",
                enabled=bool(entry.get("enabled", True)),
                statement_types=[str(value).strip() for value in entry.get("statement_types", []) if str(value).strip()],
                note=str(entry.get("note", "")).strip(),
            )
        )
    return records


def apply_local_mapping_overrides(facts: List[FactRecord], entries: Sequence[Dict[str, object]], subjects: Sequence[TemplateSubject]) -> List[FactRecord]:
    subject_by_code = {subject.code: subject for subject in subjects}
    by_fact_id: Dict[str, Dict[str, object]] = {}
    by_source_ref: Dict[str, Dict[str, object]] = {}
    by_label: Dict[str, Dict[str, object]] = {}

    for entry in entries:
        action_type = str(entry.get("action_type", "")).strip()
        if action_type not in {"set_mapping_override", "accept_mapping_alias"}:
            continue
        canonical_code = str(entry.get("canonical_code", "")).strip()
        if canonical_code and canonical_code not in subject_by_code:
            continue
        fact_id = str(entry.get("fact_id", "")).strip()
        source_cell_ref = str(entry.get("source_cell_ref", "")).strip()
        label_key = normalize_label_for_matching(str(entry.get("row_label_std", "")).strip())
        if fact_id:
            by_fact_id[fact_id] = entry
        elif source_cell_ref:
            by_source_ref[source_cell_ref] = entry
        elif action_type == "accept_mapping_alias" and label_key:
            by_label[label_key] = entry

    for fact in facts:
        entry = by_fact_id.get(fact.fact_id) or by_source_ref.get(fact.source_cell_ref)
        if entry is None:
            entry = by_label.get(normalize_label_for_matching(fact.row_label_std or fact.row_label_raw))
        if entry is None:
            continue
        canonical_code = str(entry.get("canonical_code", "")).strip()
        subject = subject_by_code.get(canonical_code)
        if subject is None:
            continue
        fact.mapping_code = subject.code
        fact.mapping_name = subject.canonical_name
        fact.mapping_method = str(entry.get("mapping_method", entry.get("action_type", "manual_override"))).strip() or "manual_override"
        fact.mapping_confidence = 1.0
        fact.mapping_relation_type = ""
        fact.mapping_review_required = False
        fact.override_source = "manual_override"
    return facts
