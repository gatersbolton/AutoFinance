from __future__ import annotations

from typing import Dict, List, Sequence

from ..models import ConflictRecord, FactRecord


ACCEPTED_DECISIONS = {"accepted", "accepted_with_rule_support", "accepted_with_validation_support"}


def apply_conflict_overrides(
    facts: List[FactRecord],
    conflicts: List[ConflictRecord],
    entries: Sequence[Dict[str, object]],
    merge_enabled: bool,
) -> tuple[List[FactRecord], List[ConflictRecord]]:
    fact_by_id = {fact.fact_id: fact for fact in facts}
    conflict_by_id = {conflict.conflict_id: conflict for conflict in conflicts}

    for entry in entries:
        conflict_id = str(entry.get("conflict_id", "")).strip()
        if not conflict_id or conflict_id not in conflict_by_id:
            continue
        winner_fact_id = str(entry.get("winner_fact_id", "")).strip()
        winner_provider = str(entry.get("winner_provider", "")).strip()
        conflict = conflict_by_id[conflict_id]
        accepted_fact = fact_by_id.get(winner_fact_id) if winner_fact_id else None
        if accepted_fact is None and winner_provider:
            accepted_fact = next((fact for fact in facts if fact.conflict_id == conflict_id and fact.provider == winner_provider), None)
        if accepted_fact is None:
            continue

        conflict.decision = "accepted_with_rule_support"
        conflict.reason = str(entry.get("note", "manual_conflict_override")).strip() or "manual_conflict_override"
        conflict.accepted_provider = accepted_fact.provider
        conflict.accepted_fact_id = accepted_fact.fact_id
        conflict.needs_review = False
        for fact in facts:
            if fact.conflict_id != conflict_id:
                continue
            fact.conflict_decision = conflict.decision
            fact.override_source = "manual_override"
            if merge_enabled and fact.fact_id == accepted_fact.fact_id:
                fact.status = "observed"
                fact.comparison_status = "accepted"
                fact.comparison_reason = conflict.reason
            elif merge_enabled:
                fact.status = "conflict"
                fact.comparison_status = "conflict"
                fact.comparison_reason = conflict.reason
    return facts, conflicts
