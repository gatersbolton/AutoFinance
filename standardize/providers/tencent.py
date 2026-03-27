from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from ..models import DiscoveredSource, ProviderCell, ProviderPage


def load_tencent_page(source: DiscoveredSource) -> ProviderPage:
    if not source.raw_file:
        raise ValueError(f"Tencent source for page {source.page_no} is missing raw_file")

    raw_path = Path(source.raw_file)
    payload = json.loads(raw_path.read_text(encoding="utf-8"))

    tables: Dict[str, List[ProviderCell]] = {}
    context_lines: List[str] = []

    for table_index, table in enumerate(payload.get("TableDetections", []), start=1):
        table_cells: List[ProviderCell] = []
        for cell in table.get("Cells", []):
            row_tl = cell.get("RowTl", -1)
            col_tl = cell.get("ColTl", -1)
            if row_tl is None or col_tl is None:
                continue
            if row_tl < 0 or col_tl < 0:
                text = str(cell.get("Text", "") or "").strip()
                if text:
                    context_lines.append(text)
                continue

            row_start, row_end = normalize_tencent_range(cell.get("RowTl"), cell.get("RowBr"))
            col_start, col_end = normalize_tencent_range(cell.get("ColTl"), cell.get("ColBr"))
            table_cells.append(
                ProviderCell(
                    table_id=str(table_index),
                    row_start=row_start,
                    row_end=row_end,
                    col_start=col_start,
                    col_end=col_end,
                    text=str(cell.get("Text", "") or ""),
                    bbox=cell.get("Polygon") or None,
                    confidence=float(cell["Confidence"]) if cell.get("Confidence") is not None else None,
                    cell_type=str(cell.get("Type", "body") or "body"),
                    meta={
                        "table_index": table_index,
                        "table_type": table.get("Type"),
                    },
                )
            )

        if table_cells:
            tables[str(table_index)] = table_cells

    result_text = str(source.result_page_meta.get("text", "") or "")
    if not context_lines and result_text:
        context_lines = [line.strip() for line in result_text.splitlines() if line.strip()]

    return ProviderPage(
        doc_id=source.doc_id,
        page_no=source.page_no,
        provider=source.provider,
        source_file=source.raw_file,
        source_kind="json",
        page_text=result_text or "\n".join(context_lines),
        tables=tables,
        context_lines=context_lines,
        meta={"notes": list(source.notes)},
    )


def normalize_tencent_range(start_raw: Any, end_raw: Any) -> tuple[int, int]:
    """Normalize Tencent's mixed 1-based / 0-based range encoding."""

    start = int(start_raw) if start_raw is not None else 0
    end = int(end_raw) if end_raw is not None else start - 1

    normalized_start = start - 1 if start > 0 else start
    normalized_end = end if end >= 0 else normalized_start

    if normalized_end < normalized_start:
        normalized_end = normalized_start
    return normalized_start, normalized_end

