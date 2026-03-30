from __future__ import annotations

from typing import Iterable, List

from ..models import RelationRecord
from .alias_miner import normalize_subject_label


def relation_matches_label(label: str, relations: Iterable[RelationRecord]) -> List[RelationRecord]:
    normalized_label = normalize_subject_label(label)
    matches: List[RelationRecord] = []
    for relation in relations:
        if not relation.enabled:
            continue
        names = [normalize_subject_label(relation.canonical_name)] + [normalize_subject_label(name) for name in relation.related_names]
        if normalized_label in names:
            matches.append(relation)
    return matches
