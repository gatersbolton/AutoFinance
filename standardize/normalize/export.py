from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from openpyxl import load_workbook

from ..models import FactRecord, TemplateSubject
from .mapping import load_template_subjects


def export_template(
    template_path: Path,
    output_path: Path,
    facts: List[FactRecord],
) -> Dict[str, int]:
    subjects, sheet_name, header_row = load_template_subjects(template_path)
    row_by_code = {subject.code: subject.row_index for subject in subjects}

    workbook = load_workbook(template_path)
    worksheet = workbook[sheet_name]

    eligible_facts = [
        fact
        for fact in facts
        if fact.mapping_code
        and fact.value_num is not None
        and fact.status not in {"review", "conflict"}
    ]

    period_keys = sorted({fact.period_key for fact in eligible_facts})
    period_columns: Dict[str, int] = {}
    next_col = worksheet.max_column + 1
    for period_key in period_keys:
        period_columns[period_key] = next_col
        worksheet.cell(row=header_row, column=next_col, value=period_key)
        next_col += 1

    grouped: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for fact in eligible_facts:
        adjusted_value = float(fact.value_num) * float(fact.unit_multiplier or 1.0)
        grouped[(fact.mapping_code, fact.period_key)].append(adjusted_value)

    written = 0
    conflicted = 0
    for (mapping_code, period_key), values in grouped.items():
        unique_values = {round(value, 8) for value in values}
        if len(unique_values) > 1:
            conflicted += 1
            continue
        row_idx = row_by_code.get(mapping_code)
        col_idx = period_columns.get(period_key)
        if row_idx is None or col_idx is None:
            continue
        worksheet.cell(row=row_idx, column=col_idx, value=values[0])
        written += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)
    return {
        "written_cells": written,
        "conflicted_cells": conflicted,
        "period_columns": len(period_columns),
    }
