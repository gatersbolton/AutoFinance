from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from PIL import Image

from OCR import discover_pdf_files, render_pdf_pages

from ..models import CellRecord, ReviewQueueRecord


def build_cell_ref(cell: CellRecord) -> str:
    return f"{cell.doc_id}:{cell.page_no}:{cell.provider}:{cell.table_id}:{cell.row_start}-{cell.row_end}:{cell.col_start}-{cell.col_end}"


def build_cell_index(cells: Iterable[CellRecord]) -> Tuple[Dict[str, CellRecord], Dict[Tuple[str, int, str, str], List[CellRecord]], Dict[Tuple[str, int, str, str, int], List[CellRecord]]]:
    by_ref: Dict[str, CellRecord] = {}
    by_table: Dict[Tuple[str, int, str, str], List[CellRecord]] = {}
    by_row: Dict[Tuple[str, int, str, str, int], List[CellRecord]] = {}
    for cell in cells:
        ref = build_cell_ref(cell)
        by_ref[ref] = cell
        table_key = (cell.doc_id, cell.page_no, cell.provider, cell.table_id)
        row_key = (cell.doc_id, cell.page_no, cell.provider, cell.table_id, cell.row_start)
        by_table.setdefault(table_key, []).append(cell)
        by_row.setdefault(row_key, []).append(cell)
    return by_ref, by_table, by_row


def attach_review_evidence(
    review_items: List[ReviewQueueRecord],
    cells: List[CellRecord],
    source_image_dir: Path | None,
    output_dir: Path,
    review_config: Dict[str, object],
    *,
    materialize_files: bool = True,
) -> List[Dict[str, str]]:
    pack_dir = output_dir / "review_pack"
    index_rows: List[Dict[str, str]] = []
    if not review_items:
        if materialize_files:
            pack_dir.mkdir(parents=True, exist_ok=True)
            write_index(pack_dir / "index.csv", index_rows)
        return index_rows

    by_ref, by_table, by_row = build_cell_index(cells)
    rendered_cache: Dict[str, Dict[int, Image.Image]] = {}

    if materialize_files:
        pack_dir.mkdir(parents=True, exist_ok=True)

    for item in review_items:
        try:
            source_ref = item.meta_json and json.loads(item.meta_json).get("source_cell_ref", "")
        except json.JSONDecodeError:
            source_ref = ""
        cell = by_ref.get(source_ref)
        if cell is None or not cell.bbox_json:
            append_index(index_rows, item.review_id, "", "", "", "no_bbox")
            continue
        table_key = (cell.doc_id, cell.page_no, cell.provider, cell.table_id)
        row_key = (cell.doc_id, cell.page_no, cell.provider, cell.table_id, cell.row_start)
        cell_bbox = union_bbox([cell.bbox_json])
        row_bbox = union_bbox([candidate.bbox_json for candidate in by_row.get(row_key, []) if candidate.bbox_json])
        table_bbox = union_bbox([candidate.bbox_json for candidate in by_table.get(table_key, []) if candidate.bbox_json])
        item.bbox = json.dumps({"cell_bbox": cell_bbox, "row_bbox": row_bbox, "table_bbox": table_bbox}, ensure_ascii=False)
        if not materialize_files:
            append_index(index_rows, item.review_id, "", "", "", "bbox_only")
            continue
        if not source_image_dir:
            append_index(index_rows, item.review_id, "", "", "", "no_source_image")
            continue
        page_images = rendered_cache.setdefault(cell.doc_id, render_doc_images(source_image_dir, cell.doc_id))
        page_image = page_images.get(cell.page_no)
        if page_image is None:
            append_index(index_rows, item.review_id, "", "", "", "page_image_missing")
            continue
        item.evidence_cell_path = save_crop(page_image, cell_bbox, pack_dir / f"{item.review_id}_cell.png", review_config)
        item.evidence_row_path = save_crop(page_image, row_bbox, pack_dir / f"{item.review_id}_row.png", review_config)
        item.evidence_table_path = save_crop(page_image, table_bbox, pack_dir / f"{item.review_id}_table.png", review_config)
        append_index(index_rows, item.review_id, item.evidence_cell_path, item.evidence_row_path, item.evidence_table_path, "ok")

    if materialize_files:
        write_index(pack_dir / "index.csv", index_rows)
    return index_rows


def render_doc_images(source_image_dir: Path, doc_id: str) -> Dict[int, Image.Image]:
    pdf_path = next((path for path in discover_pdf_files(source_image_dir) if path.stem == doc_id), None)
    if pdf_path is None:
        return {}
    images: Dict[int, Image.Image] = {}
    for rendered in render_pdf_pages(pdf_path):
        images[rendered.page_number] = Image.open(io.BytesIO(rendered.image_bytes)).copy()
    return images


def union_bbox(bbox_payloads: List[str]) -> List[int]:
    points: List[Tuple[float, float]] = []
    for payload in bbox_payloads:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            parsed = parsed.get("points", [])
        for point in parsed or []:
            if not isinstance(point, dict):
                continue
            if "x" in point and "y" in point:
                points.append((float(point["x"]), float(point["y"])))
    if not points:
        return []
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return [int(min(xs)), int(min(ys)), int(max(xs)), int(max(ys))]


def save_crop(image: Image.Image, bbox: List[int], path: Path, review_config: Dict[str, object]) -> str:
    if not bbox:
        return ""
    padding = int(review_config.get("crop_padding", 12))
    x1, y1, x2, y2 = bbox
    crop_box = (
        max(0, x1 - padding),
        max(0, y1 - padding),
        min(image.size[0], x2 + padding),
        min(image.size[1], y2 + padding),
    )
    if crop_box[0] >= crop_box[2] or crop_box[1] >= crop_box[3]:
        return ""
    image.crop(crop_box).save(path)
    return str(path)


def append_index(rows: List[Dict[str, str]], review_id: str, cell_path: str, row_path: str, table_path: str, status: str) -> None:
    rows.append(
        {
            "review_id": review_id,
            "cell_path": cell_path,
            "row_path": row_path,
            "table_path": table_path,
            "status": status,
        }
    )


def write_index(path: Path, rows: List[Dict[str, str]]) -> None:
    headers = ["review_id", "cell_path", "row_path", "table_path", "status"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
