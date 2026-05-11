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
        raw_cells = list(table.get("Cells", []) or [])
        coarse_polygon_key = detect_shared_table_polygon_key(raw_cells)
        for cell in raw_cells:
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
            polygon = cell.get("Polygon") or None
            if polygon is not None and polygon_key(polygon) == coarse_polygon_key:
                polygon = None
            table_cells.append(
                ProviderCell(
                    table_id=str(table_index),
                    row_start=row_start,
                    row_end=row_end,
                    col_start=col_start,
                    col_end=col_end,
                    text=str(cell.get("Text", "") or ""),
                    bbox=polygon,
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
    """Normalize Tencent's 0-based, end-exclusive table range encoding."""

    start = int(start_raw) if start_raw is not None else 0
    end = int(end_raw) if end_raw is not None else start + 1

    normalized_start = max(0, start)
    normalized_end = max(normalized_start, end - 1)

    if normalized_end < normalized_start:
        normalized_end = normalized_start
    return normalized_start, normalized_end


def detect_shared_table_polygon_key(cells: List[Dict[str, Any]]) -> str:
    grid_cells = [
        cell
        for cell in cells
        if cell.get("RowTl") is not None
        and cell.get("ColTl") is not None
        and int(cell.get("RowTl")) >= 0
        and int(cell.get("ColTl")) >= 0
    ]
    if len(grid_cells) <= 1:
        return ""
    polygon_keys = [polygon_key(cell.get("Polygon")) for cell in grid_cells if cell.get("Polygon")]
    if not polygon_keys:
        return ""
    unique_keys = set(polygon_keys)
    if len(unique_keys) == 1 and len(polygon_keys) >= max(2, int(len(grid_cells) * 0.8)):
        return polygon_keys[0]
    return ""


def polygon_key(polygon: Any) -> str:
    if not isinstance(polygon, list):
        return ""
    points = []
    for point in polygon:
        if not isinstance(point, dict):
            continue
        x_value = point.get("X", point.get("x"))
        y_value = point.get("Y", point.get("y"))
        if x_value is None or y_value is None:
            continue
        points.append((round(float(x_value), 3), round(float(y_value), 3)))
    return json.dumps(points, separators=(",", ":"), sort_keys=True) if points else ""
