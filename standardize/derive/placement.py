from __future__ import annotations

from typing import List, Sequence

from ..models import FactRecord


def build_export_fact_view(base_facts: Sequence[FactRecord], derived_facts: Sequence[FactRecord]) -> List[FactRecord]:
    return list(base_facts) + [fact for fact in derived_facts if fact.unplaced_reason == ""]
