"""Provider adapters for OCR outputs."""

from .aliyun import load_aliyun_page
from .tencent import load_tencent_page
from .xlsx_fallback import load_xlsx_fallback_page

__all__ = [
    "load_aliyun_page",
    "load_tencent_page",
    "load_xlsx_fallback_page",
]
