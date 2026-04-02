from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Sequence

import yaml

from ..models import AliasRecord, TemplateSubject
from ..normalize.text import normalize_label_for_matching


def load_legacy_alias_records(config_path: Path, subjects: Sequence[TemplateSubject]) -> List[AliasRecord]:
    if not config_path.exists():
        return []
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    subject_by_code = {subject.code: subject for subject in subjects}
    subject_by_name = {normalize_label_for_matching(subject.canonical_name): subject for subject in subjects}
    rows: List[AliasRecord] = []
    for item in payload.get("aliases", []):
        if not isinstance(item, dict):
            continue
        subject = None
        canonical_code = str(item.get("canonical_code", "")).strip()
        canonical_name = str(item.get("canonical_name", "")).strip()
        if canonical_code and canonical_code in subject_by_code:
            subject = subject_by_code[canonical_code]
        elif canonical_name:
            subject = subject_by_name.get(normalize_label_for_matching(canonical_name))
        if subject is None:
            continue
        rows.append(
            AliasRecord(
                canonical_code=subject.code,
                canonical_name=subject.canonical_name,
                alias=str(item.get("alias", "")).strip(),
                alias_type=str(item.get("alias_type", "legacy_alias")).strip() or "legacy_alias",
                enabled=bool(item.get("enabled", True)),
                note=str(item.get("note", "")).strip(),
            )
        )
    return rows
