from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from standardize.models import ProviderPage
from standardize.normalize.text import clean_text

from .models import CompanyResolution


LABEL_RE = re.compile(r"(?P<label>编制单位|公司名称|单位名称|企业名称)\s*[:：]?\s*(?P<name>[^。\n\r]+)")
COMPANY_RE = re.compile(r"[\u4e00-\u9fffA-Za-z0-9（）()·\-]{2,60}(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司|企业)")


def resolve_company_name(
    *,
    doc_id: str,
    pages: Iterable[ProviderPage],
    input_dir: Path,
    source_image_dir: Optional[Path] = None,
    override: str = "",
    registry_metadata: Optional[Dict[str, Any]] = None,
) -> CompanyResolution:
    if override.strip():
        return CompanyResolution(
            doc_id=doc_id,
            company_name=clean_company_name(override),
            method="cli_override",
            source_text=override,
        )

    page_list = sorted(list(pages), key=lambda page: (page.page_no, page.provider))
    candidates: List[Dict[str, Any]] = []

    for page in page_list:
        for source, line in iter_page_lines(page):
            match = LABEL_RE.search(line)
            if not match:
                continue
            name = clean_company_name(match.group("name"))
            if name:
                candidates.append(candidate(name, "ocr_label", line, page.page_no, source, score_label_match(match.group("label"), line)))

    if candidates:
        best = sorted(candidates, key=lambda item: (-int(item["score"]), int(item["page_no"]), str(item["company_name"])))[0]
        return CompanyResolution(
            doc_id=doc_id,
            company_name=best["company_name"],
            method="ocr_label",
            source_text=best["source_text"],
            candidates=candidates,
        )

    for page in page_list[:3]:
        for source, line in iter_page_lines(page):
            for match in COMPANY_RE.finditer(line):
                name = clean_company_name(match.group(0))
                if name and not is_noise_company_candidate(name):
                    candidates.append(candidate(name, "document_title", line, page.page_no, source, 70))
    if candidates:
        best = sorted(candidates, key=lambda item: (-int(item["score"]), int(item["page_no"]), -len(str(item["company_name"]))))[0]
        return CompanyResolution(
            doc_id=doc_id,
            company_name=best["company_name"],
            method="document_title",
            source_text=best["source_text"],
            candidates=candidates,
        )

    metadata_name = clean_company_name(str((registry_metadata or {}).get("company_name", "") or ""))
    if metadata_name:
        return CompanyResolution(
            doc_id=doc_id,
            company_name=metadata_name,
            method="registry_metadata",
            source_text=metadata_name,
        )

    filename_candidate = filename_company_candidate(source_image_dir) or filename_company_candidate(input_dir)
    if filename_candidate:
        return CompanyResolution(
            doc_id=doc_id,
            company_name=filename_candidate,
            method="filename_heuristic",
            source_text=filename_candidate,
        )

    return CompanyResolution(
        doc_id=doc_id,
        company_name="UNKNOWN_COMPANY",
        method="unknown",
        issue_flags=["missing_company"],
    )


def iter_page_lines(page: ProviderPage) -> Iterable[tuple[str, str]]:
    for line in page.context_lines:
        cleaned = clean_text(line)
        if cleaned:
            yield "context_lines", cleaned
    text = str(page.page_text or "")
    for piece in text.splitlines() or [text]:
        cleaned = clean_text(piece)
        if cleaned:
            yield "page_text", cleaned


def candidate(name: str, method: str, source_text: str, page_no: int, source: str, score: int) -> Dict[str, Any]:
    return {
        "company_name": name,
        "method": method,
        "source_text": source_text,
        "page_no": page_no,
        "source": source,
        "score": score,
    }


def clean_company_name(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.split(r"20\d{2}年|20\d{2}[-/.]|单位[:：]|金额单位|币种[:：]|资\s*产|负债|项目|行次", text, maxsplit=1)[0]
    text = text.strip(" :：,，;；。.-")
    text = re.sub(r"^(?:编制单位|公司名称|单位名称|企业名称)\s*[:：]?", "", text)
    text = text.replace("贵公司", "")
    return text.strip(" :：,，;；。")


def is_noise_company_candidate(name: str) -> bool:
    noise_tokens = ("会计师事务所", "审计报告", "财务报表", "电话", "受托方", "委托方")
    return any(token in name for token in noise_tokens)


def score_label_match(label: str, line: str) -> int:
    if label == "单位名称":
        if any(token in line for token in ("期末余额", "账龄", "比例", "金额", "合计")):
            return 30
        return 65
    return 100


def filename_company_candidate(path: Optional[Path]) -> str:
    if not path:
        return ""
    candidates: List[str] = []
    if path.is_file():
        candidates.append(path.stem)
    elif path.exists():
        for child in sorted(path.iterdir()):
            if child.is_file():
                candidates.append(child.stem)
        candidates.append(path.name)
    else:
        candidates.append(path.name)
    for item in candidates:
        for match in COMPANY_RE.finditer(item):
            name = clean_company_name(match.group(0))
            if name and not is_noise_company_candidate(name):
                return name
    return ""
