from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
from PIL import Image

from OCR import discover_pdf_files, render_pdf_pages

from ..discover import list_provider_dirs, parse_page_number
from ..models import PageSelectionRecord, compact_json
from ..normalize.text import clean_text


def build_page_selection(
    source_image_dir: Path,
    input_dir: Path,
    routing_config: Dict[str, Any],
) -> Tuple[List[PageSelectionRecord], Dict[str, Any]]:
    records: List[PageSelectionRecord] = []
    docs_total = 0
    pages_total = 0
    candidate_total = 0

    pdf_files = discover_pdf_files(source_image_dir) if source_image_dir.exists() else []
    if pdf_files:
        docs_total = len(pdf_files)
        for pdf_path in pdf_files:
            text_hints = load_text_hints(input_dir, pdf_path.stem)
            for rendered_page in render_pdf_pages(pdf_path):
                image = Image.open(io.BytesIO(rendered_page.image_bytes))
                record = score_page_image(
                    doc_id=pdf_path.stem,
                    page_no=rendered_page.page_number,
                    source_file=str(pdf_path),
                    image=image,
                    text_hint=text_hints.get(rendered_page.page_number, ""),
                    routing_config=routing_config,
                )
                records.append(record)
    else:
        image_files = sorted(path for path in source_image_dir.rglob("*") if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".bmp"})
        if image_files:
            docs_total = 1
        for index, image_path in enumerate(image_files, start=1):
            with Image.open(image_path) as image:
                page_no = parse_page_number(image_path.name) or index
                record = score_page_image(
                    doc_id=image_path.parent.name if image_path.parent != source_image_dir else source_image_dir.name,
                    page_no=page_no,
                    source_file=str(image_path),
                    image=image.copy(),
                    text_hint="",
                    routing_config=routing_config,
                )
                records.append(record)

    pages_total = len(records)
    candidate_total = sum(1 for record in records if record.is_candidate_table_page)
    plan = {
        "docs_total": docs_total,
        "pages_total": pages_total,
        "candidate_pages_total": candidate_total,
        "skipped_pages_total": max(pages_total - candidate_total, 0),
        "selection_threshold": float(routing_config.get("pre_ocr", {}).get("selection_threshold", 0.45)),
    }
    return records, plan


def load_text_hints(input_dir: Path, doc_id: str) -> Dict[int, str]:
    hints: Dict[int, List[str]] = {}
    for provider_name in list_provider_dirs(input_dir):
        if not provider_name.endswith("_text"):
            continue
        result_path = input_dir / provider_name / doc_id / "result.json"
        if not result_path.exists():
            continue
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        for page in payload.get("pages", []):
            page_no = page.get("page_number")
            text = clean_text(page.get("text", ""))
            if page_no and text:
                hints.setdefault(int(page_no), []).append(text)
    return {page_no: "\n".join(values) for page_no, values in hints.items()}


def score_page_image(
    doc_id: str,
    page_no: int,
    source_file: str,
    image: Image.Image,
    text_hint: str,
    routing_config: Dict[str, Any],
) -> PageSelectionRecord:
    config = routing_config.get("pre_ocr", {})
    grayscale = image.convert("L")
    array = np.asarray(grayscale, dtype=np.uint8)
    binary = array < int(config.get("binarize_threshold", 210))

    row_ratio = binary.mean(axis=1)
    col_ratio = binary.mean(axis=0)
    strong_h_count = int((row_ratio > float(config.get("strong_horizontal_ratio", 0.35))).sum())
    strong_v_count = int((col_ratio > float(config.get("strong_vertical_ratio", 0.18))).sum())
    strong_h = float((row_ratio > float(config.get("strong_horizontal_ratio", 0.35))).mean())
    strong_v = float((col_ratio > float(config.get("strong_vertical_ratio", 0.18))).mean())
    line_density_score = min(1.0, strong_h_count / 20.0 + strong_v_count / 20.0 + strong_h * 0.5 + strong_v * 0.5)
    projection_variation = min(1.0, float(row_ratio.std() * 6.0 + col_ratio.std() * 6.0))
    table_likelihood_score = min(1.0, 0.6 * line_density_score + 0.4 * projection_variation)

    if text_hint:
        digits = sum(character.isdigit() for character in text_hint)
        numeric_density_score = min(1.0, digits / max(len(text_hint), 1) * 8.0)
    else:
        numeric_density_score = transition_density_score(binary)

    keywords = config.get("keywords", [])
    keyword_hits = sum(1 for keyword in keywords if keyword in text_hint)
    keyword_score = min(1.0, keyword_hits / max(len(keywords) * 0.2, 1.0)) if text_hint else 0.0

    weights = config.get("weights", {})
    total_score = (
        table_likelihood_score * float(weights.get("table_likelihood_score", 0.4))
        + numeric_density_score * float(weights.get("numeric_density_score", 0.2))
        + line_density_score * float(weights.get("line_density_score", 0.3))
        + keyword_score * float(weights.get("keyword_score", 0.1))
    )

    reasons: List[str] = []
    if table_likelihood_score >= float(config.get("table_likelihood_trigger", 0.45)):
        reasons.append("table_layout_detected")
    if line_density_score >= float(config.get("line_density_trigger", 0.45)):
        reasons.append("grid_line_signal")
    if numeric_density_score >= float(config.get("numeric_density_trigger", 0.35)):
        reasons.append("numeric_density_high")
    if keyword_score > 0:
        reasons.append("financial_keywords_present")
    if not text_hint:
        reasons.append("no_text_hint_available")

    is_candidate = bool(
        total_score >= float(config.get("selection_threshold", 0.45))
        or keyword_hits >= int(config.get("hard_keyword_hits", 2))
        or (line_density_score >= 0.55 and table_likelihood_score >= 0.55)
    )
    if is_candidate:
        reasons.insert(0, "selected_for_table_ocr")
    else:
        reasons.insert(0, "likely_non_table_page")

    return PageSelectionRecord(
        doc_id=doc_id,
        page_no=page_no,
        source_file=source_file,
        table_likelihood_score=round(table_likelihood_score, 6),
        numeric_density_score=round(numeric_density_score, 6),
        line_density_score=round(line_density_score, 6),
        keyword_score=round(keyword_score, 6),
        is_candidate_table_page=is_candidate,
        selection_reason=",".join(reasons),
        meta_json=compact_json(
            {
                "total_score": round(total_score, 6),
                "keyword_hits": keyword_hits,
                "has_text_hint": bool(text_hint),
            }
        ),
    )


def transition_density_score(binary: np.ndarray) -> float:
    if binary.size == 0:
        return 0.0
    sample_step = max(1, binary.shape[0] // 80)
    sampled_rows = binary[::sample_step]
    transitions = []
    for row in sampled_rows:
        diff = np.abs(np.diff(row.astype(np.int8)))
        transitions.append(float(diff.mean()))
    return min(1.0, (sum(transitions) / max(len(transitions), 1)) * 8.0)
