from __future__ import annotations

from typing import Any, Dict, Iterable, List

from ..models import RelationRecord


def seed_formula_candidates_from_relations(relations: Iterable[RelationRecord]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for relation in relations:
        if not relation.enabled:
            continue
        if relation.relation_type != "aggregate_relation":
            continue
        if relation.review_required:
            continue
        if relation.related_codes:
            candidates.append(
                {
                    "rule_id": f"relation::{relation.canonical_code}",
                    "target_code": relation.canonical_code,
                    "target_name": relation.canonical_name,
                    "rule_type": "sum_if_present",
                    "children": relation.related_codes,
                    "statement_types": [],
                    "enabled": True,
                    "source": "subject_relations",
                }
            )
    return candidates
