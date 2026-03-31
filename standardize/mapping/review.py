from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List, Sequence, Tuple

from ..models import FactRecord, MappingCandidateRecord, MappingReviewRecord, TemplateSubject, UnmappedLabelSummaryRecord, compact_json
from ..normalize.text import normalize_label_for_matching
from .alias_miner import build_alias_lookup, build_candidate_records, mine_candidates, normalize_subject_label
from .masterdata import build_subject_index


def apply_subject_mapping(
    facts: List[FactRecord],
    subjects: Sequence[TemplateSubject],
    alias_records,
    relation_records,
    mapping_rules: Dict[str, object] | None = None,
) -> Tuple[List[FactRecord], List[MappingReviewRecord], List[MappingCandidateRecord], List[UnmappedLabelSummaryRecord], Dict[str, int]]:
    mapping_rules = mapping_rules or {}
    subject_index = build_subject_index(list(subjects))
    alias_lookup = build_alias_lookup(alias_records, subjects)
    mapping_review: List[MappingReviewRecord] = []
    mapping_candidates: List[MappingCandidateRecord] = []
    unmapped_groups: Dict[str, List[FactRecord]] = defaultdict(list)
    stats = Counter()

    for fact in facts:
        if fact.mapping_code:
            if fact.mapping_method == "exact":
                stats["mapped_by_exact"] += 1
            elif "alias" in (fact.mapping_method or "") or fact.mapping_method == "manual_alias":
                stats["mapped_by_alias"] += 1
            elif fact.mapping_relation_type:
                stats["mapped_by_relation"] += 1
            else:
                stats["mapped_by_alias"] += 1
            continue
        label = fact.row_label_std or fact.row_label_raw
        normalized_label = normalize_subject_label(label)
        if not normalized_label:
            continue

        exact = subject_index.get(normalized_label)
        if exact:
            fact.mapping_code = exact.code
            fact.mapping_name = exact.canonical_name
            fact.mapping_method = "exact"
            fact.mapping_confidence = 1.0
            fact.mapping_relation_type = ""
            fact.mapping_review_required = False
            stats["mapped_by_exact"] += 1
            continue

        alias_matches = alias_lookup.get(normalized_label, [])
        if alias_matches:
            alias_record, subject = sorted(alias_matches, key=lambda item: (0 if item[0].alias_type == "exact_alias" else 1, item[1].code))[0]
            fact.mapping_code = subject.code
            fact.mapping_name = subject.canonical_name
            fact.mapping_method = alias_record.alias_type or "alias"
            fact.mapping_confidence = 0.95 if alias_record.alias_type == "exact_alias" else 0.9
            fact.mapping_relation_type = ""
            fact.mapping_review_required = False
            stats["mapped_by_alias"] += 1
            continue

        candidates = mine_candidates(
            normalized_label=normalized_label,
            subjects=subjects,
            alias_lookup=alias_lookup,
            relation_records=relation_records,
            max_candidates=int(mapping_rules.get("max_candidates", 3)),
        )
        if candidates:
            mapping_candidates.extend(build_candidate_records(fact, candidates))
            best = candidates[0]
            fact.mapping_method = "candidate_only"
            fact.mapping_confidence = best[1]
            fact.mapping_relation_type = best[3]
            fact.mapping_review_required = best[4] or bool(best[3])
            mapping_review.append(
                MappingReviewRecord(
                    doc_id=fact.doc_id,
                    page_no=fact.page_no,
                    provider=fact.provider,
                    statement_type=fact.statement_type,
                    row_label_raw=fact.row_label_raw,
                    row_label_std=fact.row_label_std,
                    best_code=best[0].code,
                    best_name=best[0].canonical_name,
                    candidate_codes_json=";".join(
                        f"{candidate[0].code} {candidate[0].canonical_name} ({candidate[1]:.3f})"
                        for candidate in candidates
                    ),
                    reason="mapping_candidate_only",
                    source_cell_ref=fact.source_cell_ref,
                    status="review",
                )
            )
        else:
            mapping_review.append(
                MappingReviewRecord(
                    doc_id=fact.doc_id,
                    page_no=fact.page_no,
                    provider=fact.provider,
                    statement_type=fact.statement_type,
                    row_label_raw=fact.row_label_raw,
                    row_label_std=fact.row_label_std,
                    best_code="",
                    best_name="",
                    candidate_codes_json="",
                    reason="no_match",
                    source_cell_ref=fact.source_cell_ref,
                    status="review",
                )
            )

        unmapped_groups[normalized_label].append(fact)

    unmapped_summary: List[UnmappedLabelSummaryRecord] = []
    for normalized_label, items in sorted(unmapped_groups.items()):
        top_candidates = [candidate for candidate in mapping_candidates if candidate.normalized_label == normalized_label]
        top_candidate = sorted(top_candidates, key=lambda item: (item.candidate_rank, -item.candidate_score))[:1]
        top = top_candidate[0] if top_candidate else None
        unmapped_summary.append(
            UnmappedLabelSummaryRecord(
                row_label_std=items[0].row_label_std or items[0].row_label_raw,
                normalized_label=normalized_label,
                occurrences=len(items),
                numeric_occurrences=sum(1 for item in items if item.value_num is not None),
                amount_abs_total=round(sum(abs(float(item.value_num or 0.0)) for item in items if item.value_num is not None), 6),
                example_source_cell_ref=items[0].source_cell_ref,
                top_candidate_code=top.candidate_code if top else "",
                top_candidate_name=top.candidate_name if top else "",
                top_candidate_score=top.candidate_score if top else 0.0,
                top_candidate_method=top.candidate_method if top else "",
                meta_json=compact_json(
                    {
                        "providers": sorted({item.provider for item in items}),
                        "statement_types": sorted({item.statement_type for item in items}),
                    }
                ),
            )
        )

    stats["mapped_by_relation"] += 0
    stats["unmapped_total"] += len(unmapped_groups)
    return facts, mapping_review, mapping_candidates, unmapped_summary, dict(stats)
