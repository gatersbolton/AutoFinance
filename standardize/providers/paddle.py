from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from ..models import DiscoveredSource, ProviderPage
from .xlsx_fallback import load_table_cells_from_xlsx, load_xlsx_fallback_page


def load_paddle_page(source: DiscoveredSource) -> ProviderPage:
    if not source.raw_file:
        if source.artifact_file:
            return load_xlsx_fallback_page(source)
        raise ValueError(f"Paddle source for page {source.page_no} is missing raw_file")

    raw_path = Path(source.raw_file)
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    doc_dir = Path(source.provider_dir)
    tables: Dict[str, List[Any]] = {}
    context_lines: List[str] = []
    notes = list(source.notes)
    missing_fields = list(payload.get("missing_fields", []))

    for table_index, table_payload in enumerate(payload.get("tables", []), start=1):
        table_id = str(table_payload.get("table_id", table_index))
        xlsx_rel = str(table_payload.get("xlsx_file", "") or "").strip()
        if not xlsx_rel:
            missing_fields.append(f"table_{table_id}_xlsx_file")
            continue
        xlsx_path = doc_dir / xlsx_rel
        if not xlsx_path.exists():
            missing_fields.append(f"table_{table_id}_xlsx_missing")
            continue

        table_cells, table_context, worksheet_title = load_table_cells_from_xlsx(
            xlsx_path,
            table_id_override=table_id,
            extra_meta={
                "source_kind": "paddle_local_xlsx",
                "table_bbox": table_payload.get("bbox"),
                "table_region_id": table_payload.get("table_region_id"),
                "html_file": table_payload.get("html_file", ""),
                "neighbor_texts": table_payload.get("neighbor_texts", []),
            },
        )
        tables[table_id] = table_cells
        context_lines.extend(str(item).strip() for item in table_payload.get("neighbor_texts", []) if str(item).strip())
        context_lines.extend(item for item in table_context if item)

    result_text = str(payload.get("page_text", "") or source.result_page_meta.get("text", "") or "")
    if not context_lines and result_text:
        context_lines = [line.strip() for line in result_text.splitlines() if line.strip()]
    if missing_fields:
        notes.append("paddle_missing_fields_present")

    return ProviderPage(
        doc_id=source.doc_id,
        page_no=source.page_no,
        provider=source.provider,
        source_file=source.raw_file,
        source_kind="paddle_local",
        page_text=result_text,
        tables=tables,
        context_lines=context_lines,
        meta={
            "notes": notes,
            "layout_detection_enabled": payload.get("layout_detection_enabled"),
            "selected_device": payload.get("selected_device"),
            "runtime_seconds": payload.get("runtime_seconds"),
            "missing_fields": sorted(set(item for item in missing_fields if item)),
            "contract_version": payload.get("contract_version", ""),
        },
    )
