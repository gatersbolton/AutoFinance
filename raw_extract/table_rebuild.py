from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

from standardize.models import CellRecord, IssueRecord, LogicalSubtable, ProviderPage
from standardize.normalize.statements import classify_statement
from standardize.normalize.tables import standardize_page


CONFIG_DIR = Path(__file__).resolve().parent.parent / "standardize" / "config"


def rebuild_logical_subtables(
    pages: Iterable[ProviderPage],
    *,
    keyword_config: Dict[str, Any] | None = None,
) -> Tuple[List[CellRecord], List[LogicalSubtable], List[IssueRecord]]:
    keyword_config = keyword_config or load_statement_keywords()
    all_cells: List[CellRecord] = []
    all_subtables: List[LogicalSubtable] = []
    all_issues: List[IssueRecord] = []

    for page in pages:
        statement_meta = classify_statement(page, keyword_config)
        cells, subtables, issues = standardize_page(page, statement_meta, keyword_config)
        all_cells.extend(cells)
        all_subtables.extend(subtables)
        all_issues.extend(issues)
    return all_cells, all_subtables, all_issues


def load_statement_keywords() -> Dict[str, Any]:
    path = CONFIG_DIR / "statement_keywords.yml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload or {}
