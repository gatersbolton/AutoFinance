from __future__ import annotations

from .models import TermRelation
from .normalizer import normalize_metric_name


UNSAFE_RELATION_TYPES = {"aggregate", "split", "ambiguous", "broader_narrower"}
SAFE_RELATION_TYPES = {"exact_alias", "legacy_alias"}


def normalize_relation_type(value: object) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "aggregate_relation": "aggregate",
        "split_relation": "split",
        "broader/narrower": "broader_narrower",
        "broader-narrower": "broader_narrower",
        "legacy": "legacy_alias",
        "alias": "exact_alias",
    }
    return aliases.get(text, text)


def relation_match_names(relation: TermRelation) -> list[str]:
    names = [
        relation.canonical_name,
        *relation.raw_names,
        *relation.related_names,
    ]
    return [normalize_metric_name(name) for name in names if str(name or "").strip()]


def find_relation_matches(metric_name: str, relations: list[TermRelation]) -> list[TermRelation]:
    normalized = normalize_metric_name(metric_name)
    matches: list[TermRelation] = []
    for relation in relations:
        if normalized and normalized in relation_match_names(relation):
            matches.append(relation)
    return matches
