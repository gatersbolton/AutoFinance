from __future__ import annotations

from typing import Dict, List, Sequence

from ..models import FactRecord


def apply_placement_overrides(facts: List[FactRecord], entries: Sequence[Dict[str, object]]) -> List[FactRecord]:
    by_fact_id = {str(entry.get("fact_id", "")).strip(): entry for entry in entries if str(entry.get("fact_id", "")).strip()}
    for fact in facts:
        entry = by_fact_id.get(fact.fact_id)
        if entry is None:
            continue
        fact.unplaced_reason = str(entry.get("placement_reason", fact.unplaced_reason)).strip() or fact.unplaced_reason
        fact.override_source = "manual_override"
    return facts
