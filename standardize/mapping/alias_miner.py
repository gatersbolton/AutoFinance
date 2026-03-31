from __future__ import annotations

import difflib
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Tuple

from ..models import AliasRecord, MappingCandidateRecord, RelationRecord, TemplateSubject
from ..normalize.text import clean_text, normalize_label_for_matching


PREFIX_RE = re.compile(r"^(其中|减|加|注|其中:|减:|加:|注:|其中：|减：|加：|注：)")
ENUMERATION_RE = re.compile(r"^\s*(?:[一二三四五六七八九十]+[、,，.]|[(（]?\d+[)）.]?)")
PAREN_RE = re.compile(r"[（(][^()（）]{1,20}[)）]$")


def normalize_subject_label(value: object) -> str:
    text = normalize_label_for_matching(value)
    text = ENUMERATION_RE.sub("", text)
    text = PREFIX_RE.sub("", text)
    text = PAREN_RE.sub("", text)
    return text.strip()


def build_alias_lookup(alias_records: Sequence[AliasRecord], subjects: Sequence[TemplateSubject]) -> Dict[str, List[Tuple[AliasRecord, TemplateSubject]]]:
    subject_by_code = {subject.code: subject for subject in subjects}
    lookup: Dict[str, List[Tuple[AliasRecord, TemplateSubject]]] = defaultdict(list)
    for record in alias_records:
        if not record.enabled or not record.alias:
            continue
        subject = subject_by_code.get(record.canonical_code)
        if not subject:
            continue
        lookup[normalize_subject_label(record.alias)].append((record, subject))
    return dict(lookup)


def score_subject_candidate(normalized_label: str, subject: TemplateSubject, alias_text: str = "") -> Tuple[float, str]:
    subject_norm = normalize_subject_label(subject.canonical_name)
    if normalized_label == subject_norm:
        return 1.0, "exact_normalized_match"
    if alias_text and normalized_label == normalize_subject_label(alias_text):
        return 0.98, "alias_table"

    sequence_score = difflib.SequenceMatcher(None, normalized_label, subject_norm).ratio()
    char_overlap = safe_ratio(len(set(normalized_label) & set(subject_norm)), len(set(normalized_label) | set(subject_norm)))
    prefix_bonus = 0.08 if subject_norm.startswith(normalized_label) or normalized_label.startswith(subject_norm) else 0.0
    suffix_bonus = 0.05 if subject_norm.endswith(normalized_label) or normalized_label.endswith(subject_norm) else 0.0
    token_score = min(1.0, sequence_score * 0.55 + char_overlap * 0.35 + prefix_bonus + suffix_bonus)
    method = "token_overlap"
    if prefix_bonus or suffix_bonus:
        method = "prefix_suffix_heuristic"
    if sequence_score >= 0.9:
        method = "fuzzy_sequence"
    return token_score, method


def mine_candidates(
    normalized_label: str,
    subjects: Sequence[TemplateSubject],
    alias_lookup: Dict[str, List[Tuple[AliasRecord, TemplateSubject]]],
    relation_records: Sequence[RelationRecord],
    max_candidates: int = 3,
) -> List[Tuple[TemplateSubject, float, str, str, bool]]:
    candidates: List[Tuple[TemplateSubject, float, str, str, bool]] = []

    for record, subject in alias_lookup.get(normalized_label, []):
        candidates.append((subject, 0.98, "alias_table", record.alias_type, record.alias_type not in {"exact_alias", "legacy_alias"}))

    for relation in relation_records:
        if not relation.enabled:
            continue
        names = [normalize_subject_label(name) for name in relation.related_names + [relation.canonical_name]]
        if normalized_label in names:
            candidates.append(
                (
                    TemplateSubject(
                        code=relation.canonical_code,
                        canonical_name=relation.canonical_name,
                        row_index=0,
                        sheet_name="",
                        source_value=relation.canonical_name,
                    ),
                    0.92,
                    "relation",
                    relation.relation_type,
                    relation.review_required,
                )
            )

    for subject in subjects:
        score, method = score_subject_candidate(normalized_label, subject)
        if score < 0.62:
            continue
        candidates.append((subject, round(score, 6), method, "", False))

    deduped: Dict[str, Tuple[TemplateSubject, float, str, str, bool]] = {}
    for candidate in candidates:
        subject, score, method, relation_type, review_required = candidate
        current = deduped.get(subject.code)
        if current is None or score > current[1]:
            deduped[subject.code] = candidate

    ordered = sorted(
        deduped.values(),
        key=lambda item: (-item[1], item[4], item[0].code),
    )
    return ordered[:max_candidates]


def build_candidate_records(
    fact,
    candidates: Iterable[Tuple[TemplateSubject, float, str, str, bool]],
) -> List[MappingCandidateRecord]:
    normalized_label = normalize_subject_label(
        getattr(fact, "row_label_canonical_candidate", "")
        or getattr(fact, "row_label_norm", "")
        or fact.row_label_std
        or fact.row_label_raw
    )
    rows: List[MappingCandidateRecord] = []
    for rank, (subject, score, method, relation_type, review_required) in enumerate(candidates, start=1):
        rows.append(
            MappingCandidateRecord(
                doc_id=fact.doc_id,
                page_no=fact.page_no,
                provider=fact.provider,
                statement_type=fact.statement_type,
                row_label_raw=fact.row_label_raw,
                row_label_std=fact.row_label_std,
                normalized_label=normalized_label,
                candidate_code=subject.code,
                candidate_name=subject.canonical_name,
                candidate_rank=rank,
                candidate_score=round(score, 6),
                candidate_method=method,
                relation_type=relation_type,
                review_required=review_required,
                source_cell_ref=fact.source_cell_ref,
                meta_json="",
            )
        )
    return rows


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)
