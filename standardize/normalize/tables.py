from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from ..models import CellRecord, FactRecord, IssueRecord, LogicalSubtable, ProviderCell, ProviderPage
from ..models import StatementMeta, compact_json
from .headers import build_header_paths, joined_header_path
from .numbers import analyze_numeric_text, looks_like_integer, looks_like_numeric
from .statements import infer_period_role
from .text import clean_text, normalize_label_for_matching

SEMANTIC_HEADER_KEYWORDS = (
    "项目",
    "行次",
    "期初数",
    "期末数",
    "期末余额",
    "本期",
    "上期",
    "金额",
    "比例",
    "账龄",
    "单位名称",
    "资产类别",
    "固定资产类别",
    "预计",
)


def standardize_page(
    page: ProviderPage,
    statement_meta: StatementMeta,
    keyword_config: Dict[str, Any],
) -> Tuple[List[CellRecord], List[LogicalSubtable], List[IssueRecord]]:
    cells: List[CellRecord] = []
    subtables: List[LogicalSubtable] = []
    issues: List[IssueRecord] = []

    for table_id, provider_cells in sorted(page.tables.items(), key=lambda item: item[0]):
        slot_matrix, max_row, max_col = build_slot_matrix(provider_cells)
        segments = detect_logical_subtables(slot_matrix, max_col)
        if not segments:
            segments = [(0, max_col)]

        for segment_index, (start_col, end_col) in enumerate(segments, start=1):
            logical_subtable_id = f"{table_id}_sub{segment_index}"
            header_row_count = infer_header_row_count(slot_matrix, start_col, end_col)
            line_no_col = detect_line_no_col(slot_matrix, start_col, end_col, header_row_count)
            row_label_col = start_col
            value_cols = detect_value_columns(
                slot_matrix,
                start_col,
                end_col,
                header_row_count,
                row_label_col,
                line_no_col,
            )

            subgrid: List[List[CellRecord]] = []
            for row_idx in range(max_row + 1):
                row_records: List[CellRecord] = []
                for col_idx in range(start_col, end_col + 1):
                    expected_numeric = row_idx >= header_row_count and col_idx in value_cols
                    cell_record, issue_records = materialize_cell_record(
                        page=page,
                        table_id=table_id,
                        logical_subtable_id=logical_subtable_id,
                        slot=slot_matrix[row_idx][col_idx],
                        row_idx=row_idx,
                        col_idx=col_idx,
                        relative_col_idx=col_idx - start_col,
                        header_row_count=header_row_count,
                        expected_numeric=expected_numeric,
                    )
                    row_records.append(cell_record)
                    issues.extend(issue_records)
                subgrid.append(row_records)

            header_paths = build_header_paths(subgrid, header_row_count)
            relative_line_no_col = None if line_no_col is None else line_no_col - start_col
            relative_value_cols = [col_idx - start_col for col_idx in value_cols]
            table_semantic_key = build_table_semantic_key(
                grid=subgrid,
                statement_type=statement_meta.statement_type,
                row_label_col=0,
                line_no_col=relative_line_no_col,
                value_cols=relative_value_cols,
            )
            logical_subtable = LogicalSubtable(
                doc_id=page.doc_id,
                page_no=page.page_no,
                provider=page.provider,
                source_file=page.source_file,
                table_id=table_id,
                logical_subtable_id=logical_subtable_id,
                table_semantic_key=table_semantic_key,
                start_col=start_col,
                end_col=end_col,
                max_row=max_row,
                header_row_count=header_row_count,
                statement_meta=statement_meta,
                grid=subgrid,
                header_paths=header_paths,
                row_label_col=0,
                line_no_col=relative_line_no_col,
                value_cols=relative_value_cols,
                meta={"source_kind": page.source_kind},
            )
            subtables.append(logical_subtable)
            cells.extend(cell for row in subgrid for cell in row)

            if page.source_kind == "xlsx_fallback":
                issues.append(
                    IssueRecord(
                        doc_id=page.doc_id,
                        page_no=page.page_no,
                        provider=page.provider,
                        source_file=page.source_file,
                        table_id=table_id,
                        logical_subtable_id=logical_subtable_id,
                        source_cell_ref="",
                        issue_type="provider_fallback",
                        severity="warning",
                        message="Fallback workbook used because raw json is missing; bbox and confidence are unavailable.",
                        text_raw="",
                        text_clean="",
                        status="open",
                        meta_json=compact_json({"source_kind": "xlsx_fallback"}),
                    )
                )

    return cells, subtables, issues


def extract_facts(subtables: List[LogicalSubtable], keyword_config: Dict[str, Any]) -> Tuple[List[FactRecord], List[IssueRecord]]:
    facts: List[FactRecord] = []
    issues: List[IssueRecord] = []

    for subtable in subtables:
        for row_idx in range(subtable.header_row_count, len(subtable.grid)):
            row_cells = subtable.grid[row_idx]
            row_label_raw = extract_row_label(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col)
            has_any_value = any(not row_cells[col_idx].is_empty for col_idx in subtable.value_cols)
            if not row_label_raw and not has_any_value:
                continue

            row_label_std = normalize_label_for_matching(row_label_raw)
            row_table_semantic_key = build_row_table_semantic_key(subtable, row_idx)
            if not row_label_std and has_any_value:
                issues.append(
                    IssueRecord(
                        doc_id=subtable.doc_id,
                        page_no=subtable.page_no,
                        provider=subtable.provider,
                        source_file=subtable.source_file,
                        table_id=subtable.table_id,
                        logical_subtable_id=subtable.logical_subtable_id,
                        source_cell_ref="",
                        issue_type="row_label_missing",
                        severity="warning",
                        message="Value row is present but row label is empty.",
                        text_raw="",
                        text_clean="",
                        status="open",
                        meta_json=compact_json({"row_idx": row_idx}),
                    )
                )

            for value_col in subtable.value_cols:
                cell = row_cells[value_col]
                header_path = subtable.header_paths.get(value_col, [])
                period_role_raw = infer_period_role(header_path, keyword_config)
                period_key = f"{subtable.statement_meta.report_date_norm}__{period_role_raw}"
                issue_flags = list(parse_cell_meta(cell).get("issue_flags", []))
                if parse_cell_meta(cell).get("source_kind") == "xlsx_fallback":
                    issue_flags.extend(["missing_bbox", "missing_confidence"])

                numeric_info = analyze_numeric_text(cell.text_clean or cell.text_raw, expected_numeric=True)
                value_num = numeric_info["value_num"]
                value_type = numeric_info["value_type"]
                status = determine_fact_status(cell, value_type, value_num)
                column_semantic_key = build_column_semantic_key(header_path, period_role_raw, value_type)
                if cell.is_suspicious:
                    issue_flags.extend(flag for flag in cell.suspicious_reason.split("|") if flag)
                if cell.repair_status == "repaired":
                    issue_flags.append("repaired_numeric")

                facts.append(
                    FactRecord(
                        doc_id=subtable.doc_id,
                        page_no=subtable.page_no,
                        provider=subtable.provider,
                        statement_type=subtable.statement_meta.statement_type,
                        statement_name_raw=subtable.statement_meta.statement_name_raw,
                        logical_subtable_id=subtable.logical_subtable_id,
                        table_semantic_key=row_table_semantic_key,
                        row_label_raw=row_label_raw,
                        row_label_std=row_label_std,
                        col_header_raw=joined_header_path(header_path),
                        col_header_path=header_path,
                        column_semantic_key=column_semantic_key,
                        period_role_raw=period_role_raw,
                        report_date_raw=subtable.statement_meta.report_date_raw,
                        period_key=period_key,
                        value_raw=cell.text_raw,
                        value_num=value_num,
                        value_type=value_type,
                        unit_raw=subtable.statement_meta.unit_raw,
                        unit_multiplier=subtable.statement_meta.unit_multiplier,
                        source_cell_ref=build_source_cell_ref(
                            subtable.doc_id,
                            subtable.page_no,
                            subtable.provider,
                            subtable.table_id,
                            cell.row_start,
                            cell.row_end,
                            cell.col_start,
                            cell.col_end,
                        ),
                        status=status,
                        mapping_code="",
                        mapping_name="",
                        mapping_method="",
                        mapping_confidence=None,
                        issue_flags=list(dict.fromkeys(flag for flag in issue_flags if flag)),
                        fact_id="",
                        report_date_norm=subtable.statement_meta.report_date_norm,
                        period_role_norm=period_role_raw,
                        period_source_level=subtable.statement_meta.source_level or "page",
                        period_reason=subtable.statement_meta.reason or "",
                        duplicate_group_id="",
                        kept_fact_id="",
                        comparison_status="uncompared",
                        comparison_reason="not_compared_yet",
                        source_kind=parse_cell_meta(cell).get("source_kind", ""),
                        statement_group_key=subtable.statement_meta.statement_group_key,
                        source_row_start=cell.row_start,
                        source_row_end=cell.row_end,
                        source_col_start=cell.col_start,
                        source_col_end=cell.col_end,
                    )
                )

    return facts, issues


def build_slot_matrix(provider_cells: List[ProviderCell]) -> Tuple[List[List[Dict[str, Any]]], int, int]:
    max_row = max((cell.row_end for cell in provider_cells), default=0)
    max_col = max((cell.col_end for cell in provider_cells), default=0)
    slot_matrix: List[List[Dict[str, Any]]] = [
        [
            {
                "anchor": None,
                "text_raw": "",
                "grid_row": row_idx,
                "grid_col": col_idx,
                "is_anchor": False,
            }
            for col_idx in range(max_col + 1)
        ]
        for row_idx in range(max_row + 1)
    ]

    for cell in provider_cells:
        for row_idx in range(cell.row_start, cell.row_end + 1):
            for col_idx in range(cell.col_start, cell.col_end + 1):
                slot_matrix[row_idx][col_idx] = {
                    "anchor": cell,
                    "text_raw": cell.text,
                    "grid_row": row_idx,
                    "grid_col": col_idx,
                    "is_anchor": row_idx == cell.row_start and col_idx == cell.col_start,
                }
    return slot_matrix, max_row, max_col


def detect_logical_subtables(slot_matrix: List[List[Dict[str, Any]]], max_col: int) -> List[Tuple[int, int]]:
    if not slot_matrix:
        return []

    header_row = first_meaningful_row(slot_matrix, max_col)
    texts = [clean_text(slot_matrix[header_row][col_idx]["text_raw"]) for col_idx in range(max_col + 1)]
    line_positions = [idx for idx, text in enumerate(texts) if "行次" in text]
    if len(line_positions) < 2:
        return [(0, max_col)]

    starts = [0]
    for position in line_positions[1:]:
        candidate = max(0, position - 1)
        if candidate > starts[-1]:
            starts.append(candidate)

    segments: List[Tuple[int, int]] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] - 1 if index + 1 < len(starts) else max_col
        segments.append((start, end))
    return segments


def first_meaningful_row(slot_matrix: List[List[Dict[str, Any]]], max_col: int) -> int:
    for row_idx, row in enumerate(slot_matrix):
        nonempty = sum(1 for col_idx in range(max_col + 1) if clean_text(row[col_idx]["text_raw"]))
        joined = " ".join(clean_text(row[col_idx]["text_raw"]) for col_idx in range(max_col + 1))
        if nonempty >= 3 or "行次" in joined:
            return row_idx
    return 0


def infer_header_row_count(slot_matrix: List[List[Dict[str, Any]]], start_col: int, end_col: int) -> int:
    max_rows_to_check = min(3, len(slot_matrix))
    for row_idx in range(max_rows_to_check):
        if row_is_data_like(slot_matrix, row_idx, start_col, end_col):
            return max(1, row_idx)
    return max_rows_to_check if max_rows_to_check else 1


def row_is_data_like(slot_matrix: List[List[Dict[str, Any]]], row_idx: int, start_col: int, end_col: int) -> bool:
    texts = [clean_text(slot_matrix[row_idx][col_idx]["text_raw"]) for col_idx in range(start_col, end_col + 1)]
    if not any(texts):
        return False

    has_label = bool(texts[0])
    has_line_no = any(looks_like_integer(text) for text in texts[: min(3, len(texts))])
    has_value = any(looks_like_numeric(text) for text in texts[1:])
    header_tokens = {"行次", "期初数", "期末数", "本期", "上期", "金额", "比例", "项目"}
    if any(token in " ".join(texts) for token in header_tokens) and not has_value:
        return False
    return has_label and has_line_no and has_value


def detect_line_no_col(
    slot_matrix: List[List[Dict[str, Any]]],
    start_col: int,
    end_col: int,
    header_row_count: int,
) -> Optional[int]:
    for col_idx in range(start_col, end_col + 1):
        header_joined = " ".join(clean_text(slot_matrix[row_idx][col_idx]["text_raw"]) for row_idx in range(header_row_count))
        if "行次" in header_joined or "附注" in header_joined:
            return col_idx

    best_col = None
    best_ratio = 0.0
    for col_idx in range(start_col, end_col + 1):
        values = [clean_text(slot_matrix[row_idx][col_idx]["text_raw"]) for row_idx in range(header_row_count, len(slot_matrix))]
        nonempty = [value for value in values if value]
        if not nonempty:
            continue
        integer_count = sum(1 for value in nonempty if looks_like_integer(value))
        ratio = integer_count / len(nonempty)
        if ratio > best_ratio and ratio >= 0.6:
            best_ratio = ratio
            best_col = col_idx
    return best_col


def detect_value_columns(
    slot_matrix: List[List[Dict[str, Any]]],
    start_col: int,
    end_col: int,
    header_row_count: int,
    row_label_col: int,
    line_no_col: Optional[int],
) -> List[int]:
    value_columns: List[int] = []
    header_keywords = ("期初数", "期末数", "本期", "上期", "金额", "比例", "累计")

    for col_idx in range(start_col, end_col + 1):
        if col_idx == row_label_col or col_idx == line_no_col:
            continue

        header_joined = " ".join(clean_text(slot_matrix[row_idx][col_idx]["text_raw"]) for row_idx in range(header_row_count))
        values = [clean_text(slot_matrix[row_idx][col_idx]["text_raw"]) for row_idx in range(header_row_count, len(slot_matrix))]
        nonempty = [value for value in values if value]
        numeric_count = sum(1 for value in nonempty if looks_like_numeric(value))
        numeric_ratio = numeric_count / len(nonempty) if nonempty else 0.0

        if any(keyword in header_joined for keyword in header_keywords):
            value_columns.append(col_idx)
            continue
        if numeric_count and numeric_ratio >= 0.5:
            value_columns.append(col_idx)

    return value_columns


def materialize_cell_record(
    page: ProviderPage,
    table_id: str,
    logical_subtable_id: str,
    slot: Dict[str, Any],
    row_idx: int,
    col_idx: int,
    relative_col_idx: int,
    header_row_count: int,
    expected_numeric: bool,
) -> Tuple[CellRecord, List[IssueRecord]]:
    anchor: Optional[ProviderCell] = slot["anchor"]
    raw_text = slot["text_raw"]
    numeric_info = analyze_numeric_text(raw_text, expected_numeric=expected_numeric)
    text_clean = numeric_info["normalized_text"] if expected_numeric else clean_text(raw_text)
    repair_status = numeric_info["repair_status"]
    if not expected_numeric and text_clean != (raw_text or ""):
        repair_status = "cleaned"

    row_start = anchor.row_start if anchor else row_idx
    row_end = anchor.row_end if anchor else row_idx
    col_start = anchor.col_start if anchor else col_idx
    col_end = anchor.col_end if anchor else col_idx

    meta = {
        "grid_row": row_idx,
        "grid_col": relative_col_idx,
        "abs_col": col_idx,
        "is_span_anchor": slot["is_anchor"],
        "source_kind": page.source_kind,
        "issue_flags": numeric_info["issue_flags"],
    }
    if anchor:
        meta.update(anchor.meta)
    if numeric_info["meta"].get("repaired_from"):
        meta["repaired_from"] = numeric_info["meta"]["repaired_from"]

    cell = CellRecord(
        doc_id=page.doc_id,
        page_no=page.page_no,
        provider=page.provider,
        source_file=page.source_file,
        table_id=table_id,
        logical_subtable_id=logical_subtable_id,
        row_start=row_start,
        row_end=row_end,
        col_start=col_start,
        col_end=col_end,
        bbox_json=compact_json(anchor.bbox if anchor else None),
        text_raw=raw_text or "",
        text_clean=text_clean,
        ocr_conf=anchor.confidence if anchor else None,
        is_empty=text_clean == "",
        is_header=row_idx < header_row_count,
        is_suspicious=numeric_info["is_suspicious"],
        suspicious_reason=numeric_info["suspicious_reason"],
        repair_status=repair_status,
        meta_json=compact_json(meta),
    )

    issues: List[IssueRecord] = []
    if cell.is_suspicious or cell.repair_status in {"repaired", "unresolved"}:
        issues.append(
            IssueRecord(
                doc_id=page.doc_id,
                page_no=page.page_no,
                provider=page.provider,
                source_file=page.source_file,
                table_id=table_id,
                logical_subtable_id=logical_subtable_id,
                source_cell_ref=build_source_cell_ref(page.doc_id, page.page_no, page.provider, table_id, row_start, row_end, col_start, col_end),
                issue_type="suspicious_value" if cell.is_suspicious else "repair_trace",
                severity="warning",
                message=cell.suspicious_reason or f"Cell repair status: {cell.repair_status}",
                text_raw=cell.text_raw,
                text_clean=cell.text_clean,
                status="open",
                meta_json=cell.meta_json,
            )
        )

    return cell, issues


def extract_row_label(
    row_cells: List[CellRecord],
    row_label_col: int,
    value_cols: List[int],
    line_no_col: Optional[int],
) -> str:
    preferred = row_cells[row_label_col].text_clean if row_label_col < len(row_cells) else ""
    if preferred:
        return preferred

    for col_idx, cell in enumerate(row_cells):
        if col_idx in value_cols or col_idx == line_no_col:
            continue
        if cell.text_clean:
            return cell.text_clean
    return ""


def determine_fact_status(cell: CellRecord, value_type: str, value_num: Optional[float]) -> str:
    if cell.is_suspicious or cell.repair_status == "unresolved":
        return "review"
    if cell.repair_status == "repaired":
        return "repaired"
    if value_type == "blank":
        return "blank"
    if value_num is None and value_type == "text":
        return "review"
    return "observed"


def build_source_cell_ref(
    doc_id: str,
    page_no: int,
    provider: str,
    table_id: str,
    row_start: int,
    row_end: int,
    col_start: int,
    col_end: int,
) -> str:
    return f"{doc_id}:{page_no}:{provider}:{table_id}:{row_start}-{row_end}:{col_start}-{col_end}"


def parse_cell_meta(cell: CellRecord) -> Dict[str, Any]:
    return json.loads(cell.meta_json) if cell.meta_json else {}


def build_table_semantic_key(
    grid: List[List[CellRecord]],
    statement_type: str,
    row_label_col: int,
    line_no_col: Optional[int],
    value_cols: List[int],
) -> str:
    """Build a provider-agnostic key from table semantics rather than local table ids."""

    if not grid:
        return f"{statement_type}|empty"

    header_tokens = collect_semantic_header_tokens(grid[0])
    sample_labels: List[str] = []
    for row_idx in range(1, min(len(grid), 7)):
        label = extract_row_label(grid[row_idx], row_label_col, value_cols, line_no_col)
        normalized = normalize_semantic_token(label)
        if normalized:
            sample_labels.append(normalized)
        if len(sample_labels) >= 3:
            break

    header_signature = ",".join(header_tokens[:6]) if header_tokens else "no_header"
    row_signature = ",".join(sample_labels) if sample_labels else "no_rows"
    return f"{statement_type}|h:{header_signature}|r:{row_signature}"


def build_row_table_semantic_key(subtable: LogicalSubtable, row_idx: int) -> str:
    """Build a semantic key for the local table block that owns the given row."""

    grid = subtable.grid
    header_idx = 0
    for candidate_idx in range(0, min(row_idx, len(grid) - 1) + 1):
        if is_semantic_header_row(grid[candidate_idx]):
            header_idx = candidate_idx

    title_token = ""
    if header_idx > 0:
        for candidate_idx in range(header_idx - 1, max(-1, header_idx - 3), -1):
            row_cells = grid[candidate_idx]
            if is_semantic_header_row(row_cells):
                break
            title = normalize_semantic_token(
                extract_row_label(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col)
            )
            if title:
                title_token = title
                break

    header_tokens = collect_semantic_header_tokens(grid[header_idx])
    label_samples: List[str] = []
    for sample_row_idx in range(header_idx + 1, min(len(grid), header_idx + 6)):
        row_cells = grid[sample_row_idx]
        if is_semantic_header_row(row_cells):
            break
        label = normalize_semantic_token(
            extract_row_label(row_cells, subtable.row_label_col, subtable.value_cols, subtable.line_no_col)
        )
        if label:
            label_samples.append(label)
        if len(label_samples) >= 3:
            break

    header_signature = ",".join(header_tokens[:6]) if header_tokens else "no_header"
    row_signature = ",".join(label_samples) if label_samples else "no_rows"
    if title_token:
        return f"{subtable.statement_meta.statement_type}|t:{title_token}|h:{header_signature}|r:{row_signature}"
    return f"{subtable.statement_meta.statement_type}|h:{header_signature}|r:{row_signature}"


def collect_semantic_header_tokens(row_cells: List[CellRecord]) -> List[str]:
    tokens: List[str] = []
    for cell in row_cells:
        normalized = normalize_semantic_token(cell.text_clean or cell.text_raw)
        if normalized:
            tokens.append(normalized)
    return tokens


def normalize_semantic_token(value: str) -> str:
    cleaned = normalize_label_for_matching(value)
    if not cleaned:
        return ""
    if looks_like_numeric(cleaned):
        return ""
    return cleaned[:24]


def is_semantic_header_row(row_cells: List[CellRecord]) -> bool:
    texts = [clean_text(cell.text_clean or cell.text_raw) for cell in row_cells]
    nonempty = [text for text in texts if text]
    if len(nonempty) < 2:
        return False
    joined = " ".join(nonempty)
    return any(keyword in joined for keyword in SEMANTIC_HEADER_KEYWORDS)


def build_column_semantic_key(header_path: List[str], period_role_raw: str, value_type: str) -> str:
    tokens = [normalize_semantic_token(item) for item in header_path]
    tokens = [token for token in tokens if token]
    if tokens:
        return "|".join(tokens)
    if period_role_raw and period_role_raw != "unknown":
        return period_role_raw
    return value_type
