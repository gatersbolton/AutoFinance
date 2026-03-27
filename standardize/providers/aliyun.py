from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from ..models import DiscoveredSource, ProviderCell, ProviderPage


def extract_aliyun_data(raw_response: Dict[str, Any]) -> Dict[str, Any]:
    data = raw_response.get("Data")
    if data is None:
        data = raw_response.get("data")
    if isinstance(data, str):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return {"content": data}
    if isinstance(data, dict):
        return data
    return {}


def load_aliyun_page(source: DiscoveredSource) -> ProviderPage:
    if not source.raw_file:
        raise ValueError(f"Aliyun source for page {source.page_no} is missing raw_file")

    raw_path = Path(source.raw_file)
    raw_response = json.loads(raw_path.read_text(encoding="utf-8"))
    page_data = extract_aliyun_data(raw_response)
    confidence_by_cell = build_confidence_index(page_data)

    tables: Dict[str, List[ProviderCell]] = {}
    for table_index, table in enumerate(page_data.get("prism_tablesInfo", []), start=1):
        table_id = str(table.get("tableId", table_index))
        cells: List[ProviderCell] = []
        for cell in table.get("cellInfos", []):
            table_cell_id = cell.get("tableCellId")
            cells.append(
                ProviderCell(
                    table_id=table_id,
                    row_start=int(cell.get("ysc", 0)),
                    row_end=int(cell.get("yec", cell.get("ysc", 0))),
                    col_start=int(cell.get("xsc", 0)),
                    col_end=int(cell.get("xec", cell.get("xsc", 0))),
                    text=str(cell.get("word", "") or ""),
                    bbox=cell.get("pos") or None,
                    confidence=confidence_by_cell.get(table_cell_id),
                    cell_type="body",
                    meta={
                        "table_index": table_index,
                        "table_cell_id": table_cell_id,
                        "x_cell_size": table.get("xCellSize"),
                        "y_cell_size": table.get("yCellSize"),
                    },
                )
            )
        if cells:
            tables[table_id] = cells

    context_lines: List[str] = []
    for item in page_data.get("tableHeadTail", []):
        context_lines.extend(str(value) for value in item.get("head", []) if value)
        context_lines.extend(str(value) for value in item.get("tail", []) if value)

    page_text = str(page_data.get("content", "") or source.result_page_meta.get("text", "") or "")
    if not context_lines and source.result_page_meta.get("text"):
        context_lines = [line.strip() for line in str(source.result_page_meta["text"]).splitlines() if line.strip()]

    return ProviderPage(
        doc_id=source.doc_id,
        page_no=source.page_no,
        provider=source.provider,
        source_file=source.raw_file,
        source_kind="json",
        page_text=page_text,
        tables=tables,
        context_lines=context_lines,
        meta={
            "notes": list(source.notes),
            "raw_request_id": raw_response.get("RequestId"),
        },
    )


def build_confidence_index(page_data: Dict[str, Any]) -> Dict[int, float]:
    confidence: Dict[int, float] = {}
    grouped = defaultdict(list)
    for word in page_data.get("prism_wordsInfo", []):
        table_cell_id = word.get("tableCellId")
        if table_cell_id is None:
            continue
        prob = word.get("prob")
        if prob is not None:
            grouped[int(table_cell_id)].append(float(prob))
    for table_cell_id, values in grouped.items():
        confidence[table_cell_id] = sum(values) / len(values)
    return confidence

