from __future__ import annotations

import argparse
import base64
import importlib
import io
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_INPUT_DIR = "data"
DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_SECRET_FILE = "secret"
DEFAULT_TENCENT_REGION = "ap-beijing"
DEFAULT_RENDER_ZOOM = 2.0
MAX_IMAGE_DIMENSION = 8192
MAX_TENCENT_IMAGE_BINARY_BYTES = 7_300_000
INITIAL_JPEG_QUALITY = 90
MIN_JPEG_QUALITY = 35
RESIZE_RATIO = 0.85
METHOD_CHOICES = (
    "tencent_text",
    "aliyun_text",
    "tencent_table_v3",
    "aliyun_table",
)
LEGACY_PROVIDER_TO_METHODS = {
    "tencent": ["tencent_text"],
    "aliyun": ["aliyun_text"],
    "both": ["tencent_text", "aliyun_text"],
}
METHOD_TO_CREDENTIAL_PROVIDER = {
    "tencent_text": "tencent",
    "tencent_table_v3": "tencent",
    "aliyun_text": "aliyun",
    "aliyun_table": "aliyun",
}


class OCRConfigurationError(RuntimeError):
    """Raised for local configuration or dependency problems."""


class SecretFormatError(OCRConfigurationError):
    """Raised when the secret file cannot be parsed."""


@dataclass
class TencentCredentials:
    secret_id: str
    secret_key: str

    def secret_values(self) -> List[str]:
        return [self.secret_id, self.secret_key]


@dataclass
class AliyunCredentials:
    access_key_id: str
    access_key_secret: str

    def secret_values(self) -> List[str]:
        return [self.access_key_id, self.access_key_secret]


@dataclass
class CredentialBundle:
    tencent: Optional[TencentCredentials] = None
    aliyun: Optional[AliyunCredentials] = None

    def secret_values(self) -> List[str]:
        values: List[str] = []
        if self.tencent:
            values.extend(self.tencent.secret_values())
        if self.aliyun:
            values.extend(self.aliyun.secret_values())
        return [value for value in values if value]


@dataclass
class RenderedPage:
    page_number: int
    image_bytes: bytes
    width: int
    height: int
    image_format: str = "JPEG"


class OCRProvider:
    name = "base"

    def secret_values(self) -> List[str]:
        return []

    def recognize_page(self, page: RenderedPage) -> Dict[str, Any]:
        raise NotImplementedError


class TencentOCRProvider(OCRProvider):
    name = "tencent_text"

    def __init__(self, credentials: TencentCredentials, region: str = DEFAULT_TENCENT_REGION):
        self.credentials = credentials
        self.region = region
        self._client = None
        self._models_module = None

    def secret_values(self) -> List[str]:
        return self.credentials.secret_values()

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        credential_module = import_optional_module(
            "tencentcloud.common.credential",
            "tencentcloud-sdk-python",
        )
        client_profile_module = import_optional_module(
            "tencentcloud.common.profile.client_profile",
            "tencentcloud-sdk-python",
        )
        http_profile_module = import_optional_module(
            "tencentcloud.common.profile.http_profile",
            "tencentcloud-sdk-python",
        )
        ocr_client_module = import_optional_module(
            "tencentcloud.ocr.v20181119.ocr_client",
            "tencentcloud-sdk-python",
        )
        self._models_module = import_optional_module(
            "tencentcloud.ocr.v20181119.models",
            "tencentcloud-sdk-python",
        )

        credential = credential_module.Credential(
            self.credentials.secret_id,
            self.credentials.secret_key,
        )
        http_profile = http_profile_module.HttpProfile()
        http_profile.endpoint = "ocr.tencentcloudapi.com"
        client_profile = client_profile_module.ClientProfile()
        client_profile.httpProfile = http_profile
        self._client = ocr_client_module.OcrClient(credential, self.region, client_profile)
        return self._client

    def recognize_page(self, page: RenderedPage) -> Dict[str, Any]:
        client = self._get_client()
        request = self._models_module.GeneralBasicOCRRequest()
        request.ImageBase64 = base64.b64encode(page.image_bytes).decode("ascii")
        request.LanguageType = "zh"
        request.IsWords = True
        response = client.GeneralBasicOCR(request)
        raw_response = json.loads(response.to_json_string())
        response_body = raw_response.get("Response", raw_response)
        return {
            "raw": raw_response,
            "text": extract_tencent_text(response_body),
            "blocks": normalize_tencent_blocks(response_body),
        }


class AliyunOCRProvider(OCRProvider):
    name = "aliyun_text"

    def __init__(self, credentials: AliyunCredentials):
        self.credentials = credentials
        self._client = None
        self._ocr_models = None
        self._runtime_models = None

    def secret_values(self) -> List[str]:
        return self.credentials.secret_values()

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        client_module = import_optional_module(
            "alibabacloud_ocr_api20210707.client",
            "alibabacloud_ocr_api20210707",
        )
        open_api_models = import_optional_module(
            "alibabacloud_tea_openapi.models",
            "alibabacloud-tea-openapi",
        )
        self._ocr_models = import_optional_module(
            "alibabacloud_ocr_api20210707.models",
            "alibabacloud_ocr_api20210707",
        )
        self._runtime_models = import_optional_module(
            "alibabacloud_tea_util.models",
            "alibabacloud-tea-util",
        )

        config = open_api_models.Config(
            access_key_id=self.credentials.access_key_id,
            access_key_secret=self.credentials.access_key_secret,
        )
        config.endpoint = "ocr-api.cn-hangzhou.aliyuncs.com"
        self._client = client_module.Client(config)
        return self._client

    def recognize_page(self, page: RenderedPage) -> Dict[str, Any]:
        client = self._get_client()
        advanced_config = self._ocr_models.RecognizeAllTextRequestAdvancedConfig(
            output_char_info=True,
            output_paragraph=True,
            output_row=True,
            output_table=True,
            output_table_html=True,
        )
        request = self._ocr_models.RecognizeAllTextRequest(
            type="Advanced",
            advanced_config=advanced_config,
            body=page.image_bytes,
        )
        runtime = self._runtime_models.RuntimeOptions()
        response = client.recognize_all_text_with_options(request, runtime)
        raw_response = to_plain_data(getattr(response, "body", response))
        page_data = extract_aliyun_data(raw_response)
        return {
            "raw": raw_response,
            "text": extract_aliyun_text(page_data),
            "blocks": normalize_aliyun_blocks(page_data),
        }


class TencentTableV3OCRProvider(TencentOCRProvider):
    name = "tencent_table_v3"

    def recognize_page(self, page: RenderedPage) -> Dict[str, Any]:
        client = self._get_client()
        request = self._models_module.RecognizeTableAccurateOCRRequest()
        request.ImageBase64 = base64.b64encode(page.image_bytes).decode("ascii")
        request.UseNewModel = True
        response = client.RecognizeTableAccurateOCR(request)
        raw_response = json.loads(response.to_json_string())
        response_body = raw_response.get("Response", raw_response)
        artifacts = []
        excel_base64 = response_body.get("Data")
        if excel_base64:
            artifacts.append(
                {
                    "filename": f"page_{page.page_number:04d}.xlsx",
                    "bytes": base64.b64decode(excel_base64),
                }
            )
        return {
            "raw": raw_response,
            "text": extract_tencent_table_text(response_body),
            "blocks": normalize_tencent_table_blocks(response_body),
            "artifacts": artifacts,
        }


class AliyunTableOCRProvider(AliyunOCRProvider):
    name = "aliyun_table"

    def recognize_page(self, page: RenderedPage) -> Dict[str, Any]:
        client = self._get_client()
        request = self._ocr_models.RecognizeTableOcrRequest(
            body=page.image_bytes,
            need_rotate=True,
            line_less=False,
            is_hand_writing="false",
        )
        # Table OCR pages can take noticeably longer than text OCR, especially on
        # dense financial statements, so we use a longer timeout with retries.
        runtime = self._runtime_models.RuntimeOptions(
            connect_timeout=10_000,
            read_timeout=60_000,
            autoretry=True,
            max_attempts=3,
        )
        response = client.recognize_table_ocr_with_options(request, runtime)
        raw_response = to_plain_data(getattr(response, "body", response))
        page_data = extract_aliyun_data(raw_response)
        return {
            "raw": raw_response,
            "text": extract_aliyun_text(page_data),
            "blocks": normalize_aliyun_table_blocks(page_data),
        }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch OCR scanned PDF audit reports with Tencent and Aliyun.")
    method_group = parser.add_mutually_exclusive_group(required=True)
    method_group.add_argument(
        "--method",
        choices=METHOD_CHOICES,
        help="Which OCR method to use.",
    )
    method_group.add_argument(
        "--provider",
        choices=("tencent", "aliyun", "both"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--input",
        dest="input_dir",
        default=DEFAULT_INPUT_DIR,
        help="Directory containing PDF files. Defaults to ./data",
    )
    parser.add_argument(
        "--output",
        dest="output_dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for OCR outputs. Defaults to ./outputs",
    )
    parser.add_argument(
        "--secret",
        dest="secret_path",
        default=DEFAULT_SECRET_FILE,
        help="Secret file path. Defaults to ./secret",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    requested_methods = resolve_requested_methods(args.method, args.provider)
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    secret_path = Path(args.secret_path)

    try:
        pdf_files = discover_pdf_files(input_dir)
        if not pdf_files:
            print(f"No PDF files found under {input_dir}")
            return 1

        credentials = load_credentials(secret_path, required_credential_providers(requested_methods))
        providers = build_provider_clients(requested_methods, credentials)
    except OCRConfigurationError as exc:
        print(sanitize_text(str(exc), credentials.secret_values() if "credentials" in locals() else []))
        return 1

    had_failures = False
    summary: Dict[str, Dict[str, int]] = {
        provider.name: {"files": 0, "pages": 0, "failed_pages": 0} for provider in providers
    }

    for pdf_path in pdf_files:
        print(f"Rendering pages for {pdf_path.name}")
        try:
            rendered_pages = render_pdf_pages(pdf_path)
        except Exception as exc:  # pragma: no cover - exercised through CLI-level tests
            had_failures = True
            message = sanitize_text(str(exc), credentials.secret_values())
            print(f"Failed to render {pdf_path.name}: {message}")
            continue

        for provider in providers:
            failed_pages = process_pdf_with_provider(
                pdf_path=pdf_path,
                rendered_pages=rendered_pages,
                provider=provider,
                output_root=output_dir,
            )
            summary[provider.name]["files"] += 1
            summary[provider.name]["pages"] += len(rendered_pages)
            summary[provider.name]["failed_pages"] += len(failed_pages)
            if failed_pages:
                had_failures = True
                failed_page_list = ", ".join(str(page_number) for page_number, _ in failed_pages)
                print(
                    f"[{provider.name}] {pdf_path.name}: {len(rendered_pages) - len(failed_pages)}/{len(rendered_pages)} "
                    f"pages succeeded. Failed pages: {failed_page_list}"
                )
            else:
                print(f"[{provider.name}] {pdf_path.name}: {len(rendered_pages)}/{len(rendered_pages)} pages succeeded.")

    for provider_name, provider_summary in summary.items():
        print(
            f"Summary[{provider_name}] files={provider_summary['files']} "
            f"pages={provider_summary['pages']} failed_pages={provider_summary['failed_pages']}"
        )

    return 1 if had_failures else 0


def resolve_requested_methods(method: Optional[str], provider: Optional[str]) -> List[str]:
    if method:
        return [method]
    if provider:
        return list(LEGACY_PROVIDER_TO_METHODS[provider])
    raise OCRConfigurationError("Either --method or --provider is required.")


def required_credential_providers(methods: Sequence[str]) -> List[str]:
    providers: List[str] = []
    for method in methods:
        provider = METHOD_TO_CREDENTIAL_PROVIDER[method]
        if provider not in providers:
            providers.append(provider)
    return providers


def discover_pdf_files(input_dir: Path) -> List[Path]:
    if not input_dir.exists():
        raise OCRConfigurationError(f"Input directory does not exist: {input_dir}")
    if not input_dir.is_dir():
        raise OCRConfigurationError(f"Input path is not a directory: {input_dir}")
    return sorted(path for path in input_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf")


def load_credentials(secret_path: Path, credential_providers: Sequence[str]) -> CredentialBundle:
    sections: Dict[str, Dict[str, str]] = {}
    if secret_path.exists():
        sections = parse_secret_file(secret_path)

    bundle = CredentialBundle()
    if "tencent" in credential_providers:
        secret_id = os.environ.get("TENCENTCLOUD_SECRET_ID") or sections.get("tencent", {}).get("secretid")
        secret_key = os.environ.get("TENCENTCLOUD_SECRET_KEY") or sections.get("tencent", {}).get("secretkey")
        if not secret_id or not secret_key:
            raise OCRConfigurationError(
                "Tencent credentials are missing. Provide them in the secret file or via "
                "TENCENTCLOUD_SECRET_ID/TENCENTCLOUD_SECRET_KEY."
            )
        bundle.tencent = TencentCredentials(secret_id=secret_id, secret_key=secret_key)

    if "aliyun" in credential_providers:
        access_key_id = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID") or sections.get("aliyun", {}).get("accesskeyid")
        access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET") or sections.get("aliyun", {}).get("accesskeysecret")
        if not access_key_id or not access_key_secret:
            raise OCRConfigurationError(
                "Aliyun credentials are missing. Provide them in the secret file or via "
                "ALIBABA_CLOUD_ACCESS_KEY_ID/ALIBABA_CLOUD_ACCESS_KEY_SECRET."
            )
        bundle.aliyun = AliyunCredentials(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
        )

    return bundle


def parse_secret_file(secret_path: Path) -> Dict[str, Dict[str, str]]:
    current_section: Optional[str] = None
    sections: Dict[str, Dict[str, str]] = {"tencent": {}, "aliyun": {}}

    for line_number, raw_line in enumerate(secret_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        if line.endswith(":"):
            section_name = line[:-1].strip().lower()
            if section_name in sections:
                current_section = section_name
                continue

        if current_section is None:
            raise SecretFormatError(f"Expected a section header before line {line_number} in {secret_path}")
        if ":" not in line:
            raise SecretFormatError(f"Malformed secret line at {secret_path}:{line_number}")

        key, value = line.split(":", 1)
        normalized_key = normalize_secret_key(current_section, key)
        cleaned_value = value.strip()
        if not cleaned_value:
            raise SecretFormatError(f"Missing value for '{key.strip()}' at {secret_path}:{line_number}")
        sections[current_section][normalized_key] = cleaned_value

    return sections


def normalize_secret_key(section: str, key: str) -> str:
    normalized = "".join(character.lower() for character in key if character.isalnum())
    allowed_keys = {
        "tencent": {"secretid", "secretkey"},
        "aliyun": {"accesskeyid", "accesskeysecret"},
    }
    if normalized not in allowed_keys[section]:
        raise SecretFormatError(f"Unsupported key '{key.strip()}' in section '{section}'")
    return normalized


def build_provider_clients(requested_methods: Sequence[str], credentials: CredentialBundle) -> List[OCRProvider]:
    providers: List[OCRProvider] = []
    for method_name in requested_methods:
        if method_name == "tencent_text":
            if not credentials.tencent:
                raise OCRConfigurationError("Tencent credentials were not loaded.")
            providers.append(TencentOCRProvider(credentials.tencent))
        elif method_name == "tencent_table_v3":
            if not credentials.tencent:
                raise OCRConfigurationError("Tencent credentials were not loaded.")
            providers.append(TencentTableV3OCRProvider(credentials.tencent))
        elif method_name == "aliyun_text":
            if not credentials.aliyun:
                raise OCRConfigurationError("Aliyun credentials were not loaded.")
            providers.append(AliyunOCRProvider(credentials.aliyun))
        elif method_name == "aliyun_table":
            if not credentials.aliyun:
                raise OCRConfigurationError("Aliyun credentials were not loaded.")
            providers.append(AliyunTableOCRProvider(credentials.aliyun))
        else:
            raise OCRConfigurationError(f"Unsupported OCR method: {method_name}")
    return providers


def render_pdf_pages(pdf_path: Path) -> List[RenderedPage]:
    fitz = get_fitz_module()
    image_module = get_pillow_image_module()

    document = fitz.open(str(pdf_path))
    pages: List[RenderedPage] = []
    matrix = fitz.Matrix(DEFAULT_RENDER_ZOOM, DEFAULT_RENDER_ZOOM)

    try:
        for index in range(len(document)):
            page = document.load_page(index)
            pixmap = page.get_pixmap(matrix=matrix, alpha=False)
            image = image_module.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
            image_bytes, width, height = optimize_image_for_ocr(
                image=image,
                max_binary_bytes=MAX_TENCENT_IMAGE_BINARY_BYTES,
                max_dimension=MAX_IMAGE_DIMENSION,
            )
            pages.append(
                RenderedPage(
                    page_number=index + 1,
                    image_bytes=image_bytes,
                    width=width,
                    height=height,
                )
            )
    finally:
        document.close()

    return pages


def optimize_image_for_ocr(image: Any, max_binary_bytes: int, max_dimension: int) -> Tuple[bytes, int, int]:
    image = ensure_rgb_image(image)
    image = constrain_dimension(image, max_dimension)
    quality = INITIAL_JPEG_QUALITY

    while True:
        payload = save_jpeg_bytes(image, quality)
        width, height = image.size
        if len(payload) <= max_binary_bytes and max(width, height) <= max_dimension:
            return payload, width, height

        if quality > MIN_JPEG_QUALITY:
            quality = max(MIN_JPEG_QUALITY, quality - 10)
            continue

        next_size = resized_dimensions(image.size, RESIZE_RATIO)
        if next_size == image.size:
            raise OCRConfigurationError("Unable to compress a PDF page to the provider size limits.")
        image = resize_image(image, next_size)
        quality = INITIAL_JPEG_QUALITY


def ensure_rgb_image(image: Any) -> Any:
    if getattr(image, "mode", "RGB") != "RGB":
        return image.convert("RGB")
    return image


def constrain_dimension(image: Any, max_dimension: int) -> Any:
    width, height = image.size
    if max(width, height) <= max_dimension:
        return image
    ratio = float(max_dimension) / float(max(width, height))
    return resize_image(image, resized_dimensions((width, height), ratio))


def resized_dimensions(size: Tuple[int, int], ratio: float) -> Tuple[int, int]:
    width, height = size
    new_width = max(1, int(width * ratio))
    new_height = max(1, int(height * ratio))
    return new_width, new_height


def resize_image(image: Any, size: Tuple[int, int]) -> Any:
    try:
        image_module = get_pillow_image_module()
        resampling = getattr(getattr(image_module, "Resampling", image_module), "LANCZOS", None)
    except OCRConfigurationError:
        resampling = None

    if resampling is None:
        return image.resize(size)
    return image.resize(size, resampling)


def save_jpeg_bytes(image: Any, quality: int) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=quality, optimize=True)
    return buffer.getvalue()


def process_pdf_with_provider(
    pdf_path: Path,
    rendered_pages: Sequence[RenderedPage],
    provider: OCRProvider,
    output_root: Path,
) -> List[Tuple[int, str]]:
    provider_output_dir = output_root / provider.name / pdf_path.stem
    raw_output_dir = provider_output_dir / "raw"
    artifact_output_dir = provider_output_dir / "artifacts"
    raw_output_dir.mkdir(parents=True, exist_ok=True)

    page_entries: List[Dict[str, Any]] = []
    failures: List[Tuple[int, str]] = []

    for page in rendered_pages:
        try:
            result = provider.recognize_page(page)
            raw_file_path = raw_output_dir / f"page_{page.page_number:04d}.json"
            write_json(raw_file_path, result["raw"])
            artifact_files = write_artifacts(
                artifact_output_dir=artifact_output_dir,
                page_number=page.page_number,
                artifacts=result.get("artifacts", []),
                provider_output_dir=provider_output_dir,
            )
            page_entries.append(
                {
                    "page_number": page.page_number,
                    "text": result["text"],
                    "blocks": result["blocks"],
                    "raw_file": raw_file_path.relative_to(provider_output_dir).as_posix(),
                    "artifact_files": artifact_files,
                }
            )
        except Exception as exc:
            error_message = sanitize_text(str(exc), provider.secret_values())
            failures.append((page.page_number, error_message))
            page_entries.append(
                {
                    "page_number": page.page_number,
                    "text": "",
                    "blocks": [],
                    "raw_file": None,
                    "artifact_files": [],
                    "error": error_message,
                }
            )
            if should_abort_provider_after_error(error_message):
                for remaining_page in rendered_pages[len(page_entries):]:
                    skipped_message = (
                        f"Skipped after fatal provider error on page {page.page_number}: {error_message}"
                    )
                    failures.append((remaining_page.page_number, skipped_message))
                    page_entries.append(
                        {
                            "page_number": remaining_page.page_number,
                            "text": "",
                            "blocks": [],
                            "raw_file": None,
                            "artifact_files": [],
                            "error": skipped_message,
                        }
                    )
                break

    write_provider_outputs(
        provider_output_dir=provider_output_dir,
        provider_name=provider.name,
        source_pdf=pdf_path,
        page_entries=page_entries,
    )
    return failures


def write_provider_outputs(
    provider_output_dir: Path,
    provider_name: str,
    source_pdf: Path,
    page_entries: Sequence[Dict[str, Any]],
) -> None:
    provider_output_dir.mkdir(parents=True, exist_ok=True)
    result_json = {
        "provider": provider_name,
        "source_pdf": str(source_pdf),
        "page_count": len(page_entries),
        "pages": list(page_entries),
    }
    write_json(provider_output_dir / "result.json", result_json)
    (provider_output_dir / "result.txt").write_text(build_text_output(page_entries), encoding="utf-8")


def build_text_output(page_entries: Sequence[Dict[str, Any]]) -> str:
    chunks: List[str] = []
    for page in page_entries:
        chunks.append(f"===== Page {page['page_number']} =====")
        if page.get("error"):
            chunks.append(f"[ERROR] {page['error']}")
        text = (page.get("text") or "").strip()
        if text:
            chunks.append(text)
        chunks.append("")
    return "\n".join(chunks).rstrip() + "\n"


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_artifacts(
    artifact_output_dir: Path,
    page_number: int,
    artifacts: Sequence[Dict[str, Any]],
    provider_output_dir: Path,
) -> List[str]:
    written_files: List[str] = []
    if not artifacts:
        return written_files

    artifact_output_dir.mkdir(parents=True, exist_ok=True)
    for index, artifact in enumerate(artifacts, start=1):
        filename = artifact.get("filename") or f"page_{page_number:04d}_{index}.bin"
        payload = artifact.get("bytes", b"")
        artifact_path = artifact_output_dir / filename
        artifact_path.write_bytes(payload)
        written_files.append(artifact_path.relative_to(provider_output_dir).as_posix())
    return written_files


def should_abort_provider_after_error(error_message: str) -> bool:
    fatal_markers = (
        "clientnetworkerror",
        "nameresolutionerror",
        "failed to resolve",
        "failedoperation.unopenerror",
        "服务未开通",
        "ocrservicenotopen",
        "you have not activated the ocr service",
        "resourceunavailable.resourcepackagerunout",
        "账号资源包耗尽",
        "resource package run out",
        "authfailure",
        "invalidaccesskeyid",
        "signaturedoesnotmatch",
        "unauthorized",
    )
    lowered = error_message.lower()
    return any(marker in lowered for marker in fatal_markers)


def extract_tencent_text(response_body: Dict[str, Any]) -> str:
    lines = [
        detection.get("DetectedText", "").strip()
        for detection in response_body.get("TextDetections", [])
        if detection.get("DetectedText")
    ]
    return "\n".join(lines)


def extract_tencent_table_text(response_body: Dict[str, Any]) -> str:
    cell_texts: List[str] = []
    for table in response_body.get("TableDetections", []):
        for cell in table.get("Cells", []):
            text = (cell.get("Text") or "").strip()
            if text:
                cell_texts.append(text)
    return "\n".join(cell_texts)


def normalize_tencent_blocks(response_body: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for detection in response_body.get("TextDetections", []):
        advanced_info = detection.get("AdvancedInfo")
        if isinstance(advanced_info, str):
            try:
                advanced_info = json.loads(advanced_info)
            except json.JSONDecodeError:
                pass

        block = {
            "text": detection.get("DetectedText", ""),
            "confidence": detection.get("Confidence"),
            "bounding_box": detection.get("ItemPolygon"),
            "polygon": detection.get("Polygon", []),
            "advanced_info": advanced_info,
        }
        if detection.get("Words"):
            block["words"] = detection["Words"]
        if detection.get("WordCoordPoint"):
            block["word_coord_point"] = detection["WordCoordPoint"]
        blocks.append(block)
    return blocks


def normalize_tencent_table_blocks(response_body: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for table_index, table in enumerate(response_body.get("TableDetections", []), start=1):
        table_type = table.get("Type")
        table_polygon = table.get("TableCoordPoint", [])
        for cell in table.get("Cells", []):
            blocks.append(
                {
                    "text": cell.get("Text", ""),
                    "confidence": cell.get("Confidence"),
                    "polygon": cell.get("Polygon", []),
                    "cell_type": cell.get("Type"),
                    "cell_range": {
                        "col_tl": cell.get("ColTl"),
                        "row_tl": cell.get("RowTl"),
                        "col_br": cell.get("ColBr"),
                        "row_br": cell.get("RowBr"),
                    },
                    "table_type": table_type,
                    "table_polygon": table_polygon,
                    "table_index": table_index,
                }
            )
    return blocks


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


def get_mapping_value(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def extract_aliyun_text(page_data: Dict[str, Any]) -> str:
    content = get_mapping_value(page_data, "content", "Content")
    if isinstance(content, str) and content.strip():
        return content.strip()

    row_lines = []
    for row in page_data.get("prism_rowsInfo", []):
        word = row.get("word") or row.get("content")
        if word:
            row_lines.append(str(word).strip())
    if row_lines:
        return "\n".join(row_lines)

    unified_row_lines: List[str] = []
    for sub_image in get_mapping_value(page_data, "SubImages", "sub_images", "subImages") or []:
        row_info = get_mapping_value(sub_image, "RowInfo", "row_info") or {}
        for row in get_mapping_value(row_info, "RowDetails", "row_details") or []:
            row_content = get_mapping_value(row, "RowContent", "row_content")
            if row_content:
                unified_row_lines.append(str(row_content).strip())
    if unified_row_lines:
        return "\n".join(unified_row_lines)

    word_lines = []
    for block in page_data.get("prism_wordsInfo", []):
        word = block.get("word")
        if word:
            word_lines.append(str(word).strip())
    if word_lines:
        return "\n".join(word_lines)

    unified_blocks = normalize_aliyun_unified_blocks(page_data)
    block_texts = [block["text"].strip() for block in unified_blocks if block.get("text")]
    if block_texts:
        return "\n".join(block_texts)
    return "\n".join(word_lines)


def normalize_aliyun_blocks(page_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for word_info in page_data.get("prism_wordsInfo", []):
        block = {
            "text": word_info.get("word", ""),
            "confidence": word_info.get("prob"),
            "bounding_box": {
                "x": word_info.get("x"),
                "y": word_info.get("y"),
                "width": word_info.get("width"),
                "height": word_info.get("height"),
            },
            "polygon": word_info.get("pos", []),
        }
        if word_info.get("charInfo"):
            block["char_info"] = word_info["charInfo"]
        if word_info.get("tableId") is not None:
            block["table_id"] = word_info["tableId"]
        if word_info.get("tableCellId") is not None:
            block["table_cell_id"] = word_info["tableCellId"]
        blocks.append(block)
    if blocks:
        return blocks
    return normalize_aliyun_unified_blocks(page_data)


def normalize_aliyun_table_blocks(page_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for table_index, table in enumerate(page_data.get("prism_tablesInfo", []), start=1):
        table_id = table.get("tableId")
        for cell in table.get("cellInfos", []):
            blocks.append(
                {
                    "text": cell.get("word", ""),
                    "polygon": cell.get("pos", []),
                    "table_id": table_id,
                    "table_cell_id": cell.get("tableCellId"),
                    "table_index": table_index,
                    "cell_range": {
                        "xsc": cell.get("xsc"),
                        "xec": cell.get("xec"),
                        "ysc": cell.get("ysc"),
                        "yec": cell.get("yec"),
                    },
                }
            )
    return blocks


def normalize_aliyun_unified_blocks(page_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for sub_image in get_mapping_value(page_data, "SubImages", "sub_images", "subImages") or []:
        block_info = get_mapping_value(sub_image, "BlockInfo", "block_info") or {}
        sub_image_type = get_mapping_value(sub_image, "Type", "type")
        sub_image_id = get_mapping_value(sub_image, "SubImageId", "sub_image_id")
        for detail in get_mapping_value(block_info, "BlockDetails", "block_details") or []:
            block = {
                "text": get_mapping_value(detail, "BlockContent", "block_content") or "",
                "confidence": get_mapping_value(detail, "BlockConfidence", "block_confidence"),
                "bounding_box": get_mapping_value(detail, "BlockRect", "block_rect"),
                "polygon": get_mapping_value(detail, "BlockPoints", "block_points") or [],
            }
            char_infos = get_mapping_value(detail, "CharInfos", "char_infos")
            if char_infos:
                block["char_info"] = char_infos
            if sub_image_type is not None:
                block["sub_image_type"] = sub_image_type
            if sub_image_id is not None:
                block["sub_image_id"] = sub_image_id
            blocks.append(block)
    return blocks


def import_optional_module(module_name: str, package_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise OCRConfigurationError(
            f"Missing dependency '{package_name}'. Install requirements.txt before running OCR."
        ) from exc


def get_fitz_module() -> Any:
    return import_optional_module("fitz", "PyMuPDF")


def get_pillow_image_module() -> Any:
    return import_optional_module("PIL.Image", "Pillow")


def to_plain_data(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_plain_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_plain_data(item) for item in value]
    if hasattr(value, "to_map"):
        return to_plain_data(value.to_map())
    if hasattr(value, "to_dict"):
        return to_plain_data(value.to_dict())
    if hasattr(value, "__dict__"):
        public_items = {
            key: item for key, item in vars(value).items() if not key.startswith("_")
        }
        return to_plain_data(public_items)
    return str(value)


def sanitize_text(text: str, secrets: Iterable[str]) -> str:
    sanitized = text
    for secret in secrets:
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    return sanitized


if __name__ == "__main__":
    raise SystemExit(main())
