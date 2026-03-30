from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from ..mapping.masterdata import load_alias_records, load_subject_relations, load_template_subjects
from ..mapping.review import apply_subject_mapping
from ..models import AliasRecord, FactRecord, MappingCandidateRecord, MappingReviewRecord, RelationRecord, TemplateSubject, UnmappedLabelSummaryRecord


def load_alias_mapping(config_path: Path, subjects: List[TemplateSubject]) -> List[AliasRecord]:
    return load_alias_records(config_path, subjects)


def load_relation_mapping(config_path: Path, subjects: List[TemplateSubject]) -> List[RelationRecord]:
    return load_subject_relations(config_path, subjects)


__all__ = [
    "apply_subject_mapping",
    "load_alias_mapping",
    "load_relation_mapping",
    "load_template_subjects",
    "AliasRecord",
    "FactRecord",
    "MappingCandidateRecord",
    "MappingReviewRecord",
    "RelationRecord",
    "TemplateSubject",
    "UnmappedLabelSummaryRecord",
]
