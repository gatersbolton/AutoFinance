from __future__ import annotations

from dataclasses import replace

from standardize.models import DiscoveredSource, ProviderPage
from standardize.providers import load_aliyun_page, load_paddle_page, load_tencent_page, load_xlsx_fallback_page


def load_provider_page(source: DiscoveredSource, *, doc_id_override: str = "") -> ProviderPage:
    """Load one existing OCR provider artifact without invoking OCR."""

    if source.provider == "aliyun_table" and source.raw_file:
        page = load_aliyun_page(source)
    elif source.provider == "tencent_table_v3" and source.raw_file:
        page = load_tencent_page(source)
    elif source.provider == "paddle_table_local" and (source.raw_file or source.artifact_file):
        page = load_paddle_page(source)
    elif source.artifact_file:
        page = load_xlsx_fallback_page(source)
    else:
        raise ValueError(f"No existing OCR artifact for {source.provider} {source.doc_id} page {source.page_no}")

    if doc_id_override and page.doc_id != doc_id_override:
        page = replace(page, doc_id=doc_id_override)
    return page
