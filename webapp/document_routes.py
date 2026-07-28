from __future__ import annotations

from pathlib import Path
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from standard_map.confidence import apply_confidence_bulk_accept, build_confidence_bulk_accept_preview
from standard_map.registry import load_standard_registry
from standard_map.store import LocalMappingStore

from .base_path import app_path
from .combined_downloads import build_workbook_preview
from .document_library import (
    MISSING_OCR_CREDENTIALS_MESSAGE,
    build_delete_plan,
    document_to_job,
    execute_delete,
    list_documents,
    load_document,
    save_uploaded_documents,
)
from .document_batches import (
    add_uploaded_file_to_batch,
    build_document_batch_summary,
    create_new_document_batch,
    enqueue_document_pipeline,
    ensure_document_pipeline_ready,
    list_recent_document_batch_summaries,
    queue_document_batch,
)
from .document_models import STATUS_COMPLETED
from .jobs import resolve_download_artifact
from .models import (
    DOCUMENT_PIPELINE_STAGE_RAW_METRICS,
    DOCUMENT_PIPELINE_STAGE_STANDARD_METRICS,
)
from .routes import get_settings, password_gate
from .simple_flow import (
    build_mapping_review_sheet,
    build_raw_review_sheet,
    find_review_item,
    job_root,
    load_mapping_review_items,
    load_raw_review_items,
    load_simple_flow_state,
    mapping_review_dir,
    resolve_safe_source_file,
    refresh_combined_metrics_workbook,
    save_mapping_review_action,
    save_raw_review_action,
    source_preview_rotation_degrees,
)
from .unified_review import build_unified_review_sheet, load_unified_review_items, save_unified_review_actions


document_router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _render(
    request: Request,
    template_name: str,
    context: dict[str, object],
    *,
    status_code: int = 200,
) -> HTMLResponse:
    settings = get_settings(request)
    payload = {"request": request, "settings": settings, "url_prefix": settings.base_path}
    payload.update(context)
    return _templates(request).TemplateResponse(request, template_name, payload, status_code=status_code)


def _app_url(request: Request, path: str) -> str:
    return app_path(get_settings(request).base_path, path)


def _document_batch_urls(request: Request, batch_id: str) -> dict[str, str]:
    return {
        "upload_url": _app_url(request, f"/api/document-batches/{batch_id}/files"),
        "queue_url": _app_url(request, f"/api/document-batches/{batch_id}/queue"),
        "status_url": _app_url(request, f"/api/document-batches/{batch_id}"),
        "detail_url": _app_url(request, f"/document-batches/{batch_id}"),
    }


def _home_redirect(message: str = "", error: str = "") -> RedirectResponse:
    if error:
        return RedirectResponse(url=f"/?{urlencode({'error': error})}", status_code=303)
    if message:
        return RedirectResponse(url=f"/?{urlencode({'message': message})}", status_code=303)
    return RedirectResponse(url="/", status_code=303)


@document_router.get("/documents/upload", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def upload_page(request: Request, resume_batch_id: str = "") -> HTMLResponse:
    settings = get_settings(request)
    resume_batch: dict[str, object] | None = None
    error_message = ""
    if resume_batch_id:
        try:
            summary = build_document_batch_summary(settings, resume_batch_id)
        except KeyError:
            error_message = "要恢复的上传批次不存在。"
        else:
            if summary["status"] == "uploading":
                resume_batch = {
                    **summary,
                    **_document_batch_urls(request, resume_batch_id),
                }
            else:
                error_message = "该批次已结束上传，请返回批次状态页查看处理结果。"
    return _render(
        request,
        "document_upload.html",
        {
            "error_message": error_message,
            "max_batch_files": settings.max_upload_batch_files,
            "max_upload_mb": settings.max_upload_bytes // (1024 * 1024),
            "resume_batch": resume_batch,
        },
    )


@document_router.post("/documents/upload", dependencies=[Depends(password_gate)], response_model=None)
async def upload_documents(
    request: Request,
    uploaded_files: Annotated[list[UploadFile] | None, File()] = None,
) -> Response:
    try:
        await save_uploaded_documents(get_settings(request), uploaded_files or [])
    except ValueError as exc:
        settings = get_settings(request)
        return _render(
            request,
            "document_upload.html",
            {
                "error_message": str(exc),
                "max_batch_files": settings.max_upload_batch_files,
                "max_upload_mb": settings.max_upload_bytes // (1024 * 1024),
                "resume_batch": None,
            },
            status_code=400,
        )
    return _home_redirect("文件已保存，可以点击“开始识别”。")


@document_router.post(
    "/api/document-batches",
    response_class=JSONResponse,
    dependencies=[Depends(password_gate)],
)
def create_document_batch_api(
    request: Request,
    expected_files: Annotated[int, Form(...)],
) -> JSONResponse:
    try:
        batch = create_new_document_batch(
            get_settings(request),
            expected_files=expected_files,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        {
            **batch.as_dict(),
            **_document_batch_urls(request, batch.batch_id),
        },
        status_code=201,
    )


@document_router.post(
    "/api/document-batches/{batch_id}/files",
    response_class=JSONResponse,
    dependencies=[Depends(password_gate)],
)
async def upload_document_batch_file_api(
    request: Request,
    batch_id: str,
    upload_index: Annotated[int, Form(...)],
    uploaded_file: Annotated[UploadFile, File(...)],
) -> JSONResponse:
    try:
        item, created = await add_uploaded_file_to_batch(
            get_settings(request),
            batch_id=batch_id,
            upload_index=upload_index,
            upload=uploaded_file,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="上传批次不存在。") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(
        {
            **item.as_dict(),
            "created": created,
        },
        status_code=201 if created else 200,
    )


@document_router.post(
    "/api/document-batches/{batch_id}/queue",
    response_class=JSONResponse,
    dependencies=[Depends(password_gate)],
)
def queue_document_batch_api(request: Request, batch_id: str) -> JSONResponse:
    try:
        summary = queue_document_batch(get_settings(request), batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="上传批次不存在。") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(summary)


@document_router.get(
    "/api/document-batches/{batch_id}",
    response_class=JSONResponse,
    dependencies=[Depends(password_gate)],
)
def document_batch_status_api(request: Request, batch_id: str) -> JSONResponse:
    try:
        return JSONResponse(
            build_document_batch_summary(get_settings(request), batch_id)
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="上传批次不存在。") from exc


@document_router.get(
    "/document-batches/{batch_id}",
    response_class=HTMLResponse,
    dependencies=[Depends(password_gate)],
)
def document_batch_detail(request: Request, batch_id: str) -> HTMLResponse:
    try:
        summary = build_document_batch_summary(get_settings(request), batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="上传批次不存在。") from exc
    return _render(
        request,
        "document_batch_detail.html",
        {"batch": summary},
    )


@document_router.post("/documents/{doc_id}/start-ocr", dependencies=[Depends(password_gate)])
def start_ocr(request: Request, doc_id: str) -> RedirectResponse:
    try:
        settings = get_settings(request)
        ensure_document_pipeline_ready(settings)
        enqueue_document_pipeline(settings, doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    except ValueError as exc:
        message = MISSING_OCR_CREDENTIALS_MESSAGE if str(exc) == MISSING_OCR_CREDENTIALS_MESSAGE else str(exc)
        return _home_redirect(error=message)
    return _home_redirect("文件已进入队列，将依次完成识别和数据生成。")


@document_router.post("/documents/{doc_id}/rerun-ocr", dependencies=[Depends(password_gate)])
def rerun_ocr(request: Request, doc_id: str) -> RedirectResponse:
    try:
        settings = get_settings(request)
        ensure_document_pipeline_ready(settings)
        enqueue_document_pipeline(settings, doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    except ValueError as exc:
        message = MISSING_OCR_CREDENTIALS_MESSAGE if str(exc) == MISSING_OCR_CREDENTIALS_MESSAGE else str(exc)
        return _home_redirect(error=message)
    return _home_redirect("文件已重新进入队列。")


@document_router.get("/documents/{doc_id}/continue", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def continue_document(request: Request, doc_id: str) -> Response:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    if document.ocr_status != STATUS_COMPLETED:
        return _home_redirect(error="请先点击“开始识别”。")
    job = document_to_job(settings, document)
    return _render(
        request,
        "document_continue.html",
        {
            "document": document,
            "job": job,
            "simple_flow": load_simple_flow_state(job, settings),
        },
    )


@document_router.post("/documents/{doc_id}/raw-metrics/run", dependencies=[Depends(password_gate)])
def run_document_raw_metrics(request: Request, doc_id: str) -> RedirectResponse:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id)
        if document.ocr_status != STATUS_COMPLETED:
            return _home_redirect(error="请先点击“开始识别”。")
        enqueue_document_pipeline(
            settings,
            doc_id,
            start_stage=DOCUMENT_PIPELINE_STAGE_RAW_METRICS,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url=f"/documents/{doc_id}/continue", status_code=303)


@document_router.post("/documents/{doc_id}/standard-metrics/run", dependencies=[Depends(password_gate)])
def run_document_standard_metrics(request: Request, doc_id: str) -> RedirectResponse:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id)
        job = document_to_job(settings, document)
        if not load_simple_flow_state(job, settings).get("raw_ready"):
            raise ValueError("请先生成原始数据，再执行标准映射。")
        enqueue_document_pipeline(
            settings,
            doc_id,
            start_stage=DOCUMENT_PIPELINE_STAGE_STANDARD_METRICS,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url=f"/documents/{doc_id}/continue", status_code=303)


@document_router.get("/documents/{doc_id}/download/{slug}", dependencies=[Depends(password_gate)])
def download_document_artifact(request: Request, doc_id: str, slug: str) -> FileResponse:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    job = document_to_job(settings, document)
    if slug == "combined_metrics_xlsx":
        state = load_simple_flow_state(job, settings)
        path_text = str(state.get("combined_metrics_xlsx", "") or "")
        path = Path(path_text) if path_text else Path()
        action_path = job_root(job) / "unified_review" / "unified_review_actions.json"
        needs_refresh = not path_text or not path.exists() or (action_path.exists() and path.exists() and action_path.stat().st_mtime > path.stat().st_mtime)
        if needs_refresh and (state.get("raw_ready") or state.get("standard_ready")):
            refresh_combined_metrics_workbook(settings, job)
    artifact = resolve_download_artifact(job, slug)
    if artifact is None:
        raise HTTPException(status_code=404, detail="文件不存在。")
    path = Path(artifact.path)
    if not artifact.exists or not path.exists():
        raise HTTPException(status_code=404, detail="文件未生成。")
    return FileResponse(path=str(path), filename=artifact.download_name)


@document_router.get("/documents/{doc_id}/download-preview/combined_metrics_xlsx", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_combined_metrics_download_preview(request: Request, doc_id: str) -> HTMLResponse:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    job = document_to_job(settings, document)
    state = load_simple_flow_state(job, settings)
    path_text = str(state.get("combined_metrics_xlsx", "") or "")
    path = Path(path_text) if path_text else Path()
    action_path = job_root(job) / "unified_review" / "unified_review_actions.json"
    needs_refresh = not path_text or not path.exists() or (action_path.exists() and path.exists() and action_path.stat().st_mtime > path.stat().st_mtime)
    if needs_refresh and (state.get("raw_ready") or state.get("standard_ready")):
        refresh_combined_metrics_workbook(settings, job)
        state = load_simple_flow_state(job, settings)
        path_text = str(state.get("combined_metrics_xlsx", "") or "")
        path = Path(path_text) if path_text else Path()
    if not path_text or not path.exists():
        raise HTTPException(status_code=404, detail="数据表尚未生成。")
    return _render(
        request,
        "download_preview.html",
        {
            "document": document,
            "job": job,
            "title": "数据表预览",
            "preview": build_workbook_preview(path),
            "download_url": _app_url(request, f"/documents/{doc_id}/download/combined_metrics_xlsx"),
            "return_url": _app_url(request, f"/documents/{doc_id}/continue"),
        },
    )


@document_router.get("/documents/{doc_id}/proofread", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_unified_proofread_page(request: Request, doc_id: str) -> HTMLResponse:
    settings = get_settings(request)
    document = load_document(settings, doc_id)
    job = document_to_job(settings, document)
    sheet = build_unified_review_sheet(job)
    item = sheet.get("selected_item")
    page_image_url = (
        _app_url(request, f"/documents/{doc_id}/proofread/page-image/{item.get('review_item_id')}")
        if item and item.get("source_pdf_path")
        else ""
    )
    return _render(
        request,
        "unified_proofread.html",
        {
            "document": document,
            "job": job,
            "sheet": sheet,
            "items": sheet["items"],
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
            "save_url": _app_url(request, f"/documents/{doc_id}/proofread/save"),
        },
    )


@document_router.get("/documents/{doc_id}/proofread/items/{item_id}", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_unified_proofread_item_page(request: Request, doc_id: str, item_id: str) -> HTMLResponse:
    settings = get_settings(request)
    document = load_document(settings, doc_id)
    job = document_to_job(settings, document)
    sheet = build_unified_review_sheet(job, item_id)
    item = sheet.get("selected_item")
    if item is None or str(item.get("review_item_id", "")) != item_id:
        raise HTTPException(status_code=404, detail="统一校对项不存在。")
    page_image_url = _app_url(request, f"/documents/{doc_id}/proofread/page-image/{item_id}") if item.get("source_pdf_path") else ""
    return _render(
        request,
        "unified_proofread.html",
        {
            "document": document,
            "job": job,
            "sheet": sheet,
            "items": sheet["items"],
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
            "save_url": _app_url(request, f"/documents/{doc_id}/proofread/save"),
        },
    )


@document_router.get("/documents/{doc_id}/proofread/page-image/{item_id}", dependencies=[Depends(password_gate)])
def document_unified_proofread_page_image(request: Request, doc_id: str, item_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    item = find_review_item(load_unified_review_items(job), item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="统一校对项不存在。")
    path = resolve_safe_source_file(settings, job, str(item.get("source_pdf_path", "")))
    page_no = int(str(item.get("source_page_no", "") or "1"))
    try:
        import fitz
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="PDF 渲染组件不可用。") from exc
    try:
        document = fitz.open(path)
        page = document[page_no - 1]
        matrix = fitz.Matrix(2, 2)
        rotation = source_preview_rotation_degrees(item)
        if rotation:
            matrix = matrix.prerotate(rotation)
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        content = pixmap.tobytes("png")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"无法渲染第 {page_no} 页。") from exc
    return Response(content=content, media_type="image/png", headers={"Cache-Control": "private, max-age=86400"})


@document_router.post("/documents/{doc_id}/proofread/save", response_class=JSONResponse, dependencies=[Depends(password_gate)])
async def document_unified_proofread_save(request: Request, doc_id: str) -> JSONResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await request.json()
    edits = payload.get("edits", []) if isinstance(payload, dict) else []
    reviewer_name = str(payload.get("reviewer_name", "") or "") if isinstance(payload, dict) else ""
    try:
        summary = save_unified_review_actions(job, edits, reviewer_name=reviewer_name)
        combined_summary = refresh_combined_metrics_workbook(settings, job)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    summary["combined_workbook_refreshed"] = bool(combined_summary.get("pass"))
    summary["combined_metrics_xlsx"] = str(combined_summary.get("workbook_path", "") or combined_summary.get("output_path", "") or "")
    summary["precision_warnings_total"] = int(summary.get("precision_warnings_total", 0) or 0)
    return JSONResponse(summary)


@document_router.get("/documents/{doc_id}/raw-review", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_raw_review_page(request: Request, doc_id: str) -> HTMLResponse:
    settings = get_settings(request)
    document = load_document(settings, doc_id)
    job = document_to_job(settings, document)
    sheet = build_raw_review_sheet(job)
    item = sheet.get("selected_item")
    page_image_url = _app_url(request, f"/documents/{doc_id}/raw-review/page-image/{item.get('review_item_id')}") if item else ""
    return _render(
        request,
        "raw_review.html",
        {
            "job": job,
            "sheet": sheet,
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
        },
    )


@document_router.get("/documents/{doc_id}/raw-review/items/{item_id}", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_raw_review_item_page(request: Request, doc_id: str, item_id: str) -> HTMLResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    sheet = build_raw_review_sheet(job, item_id)
    item = sheet.get("selected_item")
    if item is None or str(item.get("review_item_id", "")) != item_id:
        raise HTTPException(status_code=404, detail="原始数据校对项不存在。")
    page_image_url = _app_url(request, f"/documents/{doc_id}/raw-review/page-image/{item_id}") if item.get("source_pdf_path") else ""
    return _render(
        request,
        "raw_review.html",
        {
            "job": job,
            "sheet": sheet,
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
        },
    )


@document_router.get("/documents/{doc_id}/raw-review/page-image/{item_id}", dependencies=[Depends(password_gate)])
def document_raw_review_page_image(request: Request, doc_id: str, item_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    item = find_review_item(load_raw_review_items(job), item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="原始数据校对项不存在。")
    path = resolve_safe_source_file(settings, job, str(item.get("source_pdf_path", "")))
    page_no = int(str(item.get("source_page_no", "") or "1"))
    try:
        import fitz
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="PDF 渲染组件不可用。") from exc
    try:
        document = fitz.open(path)
        page = document[page_no - 1]
        matrix = fitz.Matrix(2, 2)
        rotation = source_preview_rotation_degrees(item)
        if rotation:
            matrix = matrix.prerotate(rotation)
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        content = pixmap.tobytes("png")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"无法渲染第 {page_no} 页。") from exc
    return Response(content=content, media_type="image/png", headers={"Cache-Control": "private, max-age=86400"})


@document_router.post("/documents/{doc_id}/raw-review/actions", dependencies=[Depends(password_gate)])
async def document_raw_review_action_route(request: Request, doc_id: str) -> RedirectResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    form = await request.form()
    review_item_id = str(form.get("review_item_id", "")).strip()
    action = str(form.get("action", "")).strip()
    item = find_review_item(load_raw_review_items(job), review_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="原始数据校对项不存在。")
    edits = {}
    edits_json = str(form.get("edits_json", "") or "")
    if action in {"edit", "next_table"} and edits_json.strip():
        import json

        try:
            edits = {"table_edits": json.loads(edits_json)}
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="表格修改内容不是合法 JSON。") from exc
    save_raw_review_action(job, item=item, action=action, edits=edits, reviewer_note=str(form.get("reviewer_note", "") or ""))
    redirect_item_id = review_item_id
    if action == "next_table":
        sheet = build_raw_review_sheet(job, review_item_id)
        redirect_item_id = str(sheet.get("next_item_id") or form.get("next_item_id") or review_item_id)
    return RedirectResponse(url=f"/documents/{doc_id}/raw-review/items/{redirect_item_id}", status_code=303)


@document_router.get("/documents/{doc_id}/mapping-review", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_mapping_review_page(request: Request, doc_id: str) -> HTMLResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    sheet = build_mapping_review_sheet(job)
    item = sheet.get("selected_item")
    page_image_url = (
        _app_url(request, f"/documents/{doc_id}/mapping-review/page-image/{item.get('review_item_id')}")
        if item and item.get("source_pdf_path")
        else ""
    )
    return _render(
        request,
        "mapping_review.html",
        {
            "job": job,
            "sheet": sheet,
            "items": sheet["items"],
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
        },
    )


@document_router.get("/documents/{doc_id}/mapping-review/items/{item_id}", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def document_mapping_review_item_page(request: Request, doc_id: str, item_id: str) -> HTMLResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    sheet = build_mapping_review_sheet(job, item_id)
    item = sheet.get("selected_item")
    if item is None or str(item.get("review_item_id", "")) != item_id:
        raise HTTPException(status_code=404, detail="术语映射校对项不存在。")
    page_image_url = _app_url(request, f"/documents/{doc_id}/mapping-review/page-image/{item_id}") if item.get("source_pdf_path") else ""
    return _render(
        request,
        "mapping_review.html",
        {
            "job": job,
            "sheet": sheet,
            "items": sheet["items"],
            "item": item,
            "page_image_url": page_image_url,
            "review_base_url": _app_url(request, f"/documents/{doc_id}"),
            "review_return_url": _app_url(request, f"/documents/{doc_id}/continue"),
        },
    )


@document_router.get("/documents/{doc_id}/mapping-review/page-image/{item_id}", dependencies=[Depends(password_gate)])
def document_mapping_review_page_image(request: Request, doc_id: str, item_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    item = find_review_item(load_mapping_review_items(job), item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="术语映射校对项不存在。")
    path = resolve_safe_source_file(settings, job, str(item.get("source_pdf_path", "")))
    page_no = int(str(item.get("source_page_no", "") or "1"))
    try:
        import fitz
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="PDF 渲染组件不可用。") from exc
    try:
        document = fitz.open(path)
        page = document[page_no - 1]
        matrix = fitz.Matrix(2, 2)
        rotation = source_preview_rotation_degrees(item)
        if rotation:
            matrix = matrix.prerotate(rotation)
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        content = pixmap.tobytes("png")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"无法渲染第 {page_no} 页。") from exc
    return Response(content=content, media_type="image/png", headers={"Cache-Control": "private, max-age=86400"})


@document_router.get("/documents/{doc_id}/mapping-review/evidence/{item_id}", dependencies=[Depends(password_gate)])
def document_mapping_review_evidence(request: Request, doc_id: str, item_id: str) -> FileResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    item = find_review_item(load_mapping_review_items(job), item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="术语映射校对项不存在。")
    path = resolve_safe_source_file(settings, job, str(item.get("source_pdf_path", "")))
    return FileResponse(path=str(path), filename=path.name, media_type="application/pdf", content_disposition_type="inline")


@document_router.post("/documents/{doc_id}/mapping-review/actions", dependencies=[Depends(password_gate)])
async def document_mapping_review_action_route(request: Request, doc_id: str) -> RedirectResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    form = await request.form()
    review_item_id = str(form.get("review_item_id", "")).strip()
    action = str(form.get("action", "")).strip()
    item = find_review_item(load_mapping_review_items(job), review_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="术语映射校对项不存在。")
    selected_code = str(form.get("selected_code", "") or "")
    selected_name = str(form.get("selected_name", "") or "")
    if action == "change_mapping" and selected_code and not selected_name:
        registry = load_standard_registry()
        selected = registry.term_by_code.get(selected_code)
        selected_name = selected.name if selected else selected_name
    try:
        save_mapping_review_action(
            job,
            item=item,
            action=action,
            selected_code=selected_code,
            selected_name=selected_name,
            reviewer_note=str(form.get("reviewer_note", "") or ""),
            mapping_store_path=settings.mapping_store_path,
        )
        refresh_combined_metrics_workbook(settings, job)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url=f"/documents/{doc_id}/mapping-review/items/{review_item_id}", status_code=303)


@document_router.post("/documents/{doc_id}/mapping/decision", dependencies=[Depends(password_gate)])
async def document_mapping_decision_route(request: Request, doc_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await _document_mapping_decision_payload(request)
    summary = _save_document_mapping_decision(settings, job, payload)
    return _json_or_html_redirect(request, summary, f"/documents/{doc_id}/proofread")


@document_router.post("/documents/{doc_id}/mapping/accept-once", dependencies=[Depends(password_gate)])
async def document_mapping_accept_once_route(request: Request, doc_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await _document_mapping_decision_payload(request)
    payload["decision"] = "accept_once"
    summary = _save_document_mapping_decision(settings, job, payload)
    return _json_or_html_redirect(request, summary, f"/documents/{doc_id}/proofread")


@document_router.post("/documents/{doc_id}/mapping/accept-and-remember", dependencies=[Depends(password_gate)])
async def document_mapping_accept_and_remember_route(request: Request, doc_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await _document_mapping_decision_payload(request)
    payload["decision"] = "accept_and_remember"
    summary = _save_document_mapping_decision(settings, job, payload)
    return _json_or_html_redirect(request, summary, f"/documents/{doc_id}/proofread")


@document_router.post("/documents/{doc_id}/mapping/reject", dependencies=[Depends(password_gate)])
async def document_mapping_reject_route(request: Request, doc_id: str) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await _document_mapping_decision_payload(request)
    payload["decision"] = "reject"
    summary = _save_document_mapping_decision(settings, job, payload)
    return _json_or_html_redirect(request, summary, f"/documents/{doc_id}/proofread")


@document_router.get("/documents/{doc_id}/mapping/bulk-confidence-preview", response_class=JSONResponse, dependencies=[Depends(password_gate)])
def document_mapping_bulk_confidence_preview_route(request: Request, doc_id: str, threshold: float = 0.9) -> JSONResponse:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    state = load_simple_flow_state(job, settings)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    if not standard_csv.exists():
        raise HTTPException(status_code=404, detail="请先生成标准化数据。")
    store = LocalMappingStore(settings.mapping_store_path)
    before = store.count("term_aliases", where="enabled = 1 AND COALESCE(source, '') != 'base'")
    return JSONResponse(
        build_confidence_bulk_accept_preview(
            standard_csv.parent,
            threshold=threshold,
            before_alias_count=before,
            after_alias_count=before,
        )
    )


@document_router.post("/documents/{doc_id}/mapping/bulk-accept-confidence", response_class=JSONResponse, dependencies=[Depends(password_gate)])
async def document_mapping_bulk_accept_confidence_route(request: Request, doc_id: str, threshold: float = 0.9) -> Response:
    settings = get_settings(request)
    job = document_to_job(settings, load_document(settings, doc_id))
    payload = await _document_mapping_decision_payload(request)
    threshold_value = float(payload.get("threshold") or payload.get("threshold_pct") or threshold or 0.9)
    state = load_simple_flow_state(job, settings)
    standard_csv = Path(str(state.get("standardized_metrics_csv", "") or ""))
    if not standard_csv.exists():
        raise HTTPException(status_code=404, detail="请先生成标准化数据。")
    store = LocalMappingStore(settings.mapping_store_path)
    summary = apply_confidence_bulk_accept(
        standard_csv.parent,
        store=store,
        job_id=job.job_id,
        doc_id=job.job_id,
        threshold=threshold_value,
        decisions_dir=mapping_review_dir(job),
    )
    store.export_aliases(settings.mapping_store_root / "local_aliases_export.yml")
    store.export_decision_audit(settings.mapping_store_root / "mapping_decisions_audit.csv")
    refresh_combined_metrics_workbook(settings, job)
    return _json_or_html_redirect(request, summary, f"/documents/{doc_id}/proofread")


async def _document_mapping_decision_payload(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        return {str(key): str(value or "") for key, value in payload.items()} if isinstance(payload, dict) else {}
    form = await request.form()
    return {str(key): str(value or "") for key, value in form.items()}


def _json_or_html_redirect(request: Request, payload: dict[str, object], redirect_url: str) -> Response:
    if _request_prefers_html(request):
        return RedirectResponse(url=redirect_url, status_code=303)
    return JSONResponse(payload)


def _request_prefers_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    content_type = request.headers.get("content-type", "")
    if "application/json" in accept or "application/json" in content_type:
        return False
    return "text/html" in accept


def _save_document_mapping_decision(settings, job, payload: dict[str, str]) -> dict[str, object]:
    review_item_id = str(payload.get("review_item_id", "") or "")
    raw_metric_id = str(payload.get("raw_metric_id", "") or "")
    items = load_mapping_review_items(job)
    item = find_review_item(items, review_item_id) if review_item_id else None
    if item is None and raw_metric_id:
        item = next((candidate for candidate in items if str(candidate.get("raw_metric_id", "")) == raw_metric_id), None)
    if item is None:
        raise HTTPException(status_code=404, detail="术语映射校对项不存在。")
    selected_code = str(payload.get("selected_code") or payload.get("final_code") or item.get("current_code") or item.get("candidate_code") or "")
    selected_name = str(payload.get("selected_name") or payload.get("final_name") or item.get("current_name") or item.get("candidate_name") or "")
    try:
        saved = save_mapping_review_action(
            job,
            item=item,
            action=str(payload.get("decision") or payload.get("action") or ""),
            selected_code=selected_code,
            selected_name=selected_name,
            reviewer_note=str(payload.get("note") or payload.get("reviewer_note") or ""),
            mapping_store_path=settings.mapping_store_path,
            decided_by=str(payload.get("decided_by") or "web"),
        )
        refresh_combined_metrics_workbook(settings, job)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "pass": True,
        "doc_id": job.job_id,
        "review_item_id": review_item_id or saved.get("review_item_id", ""),
        "raw_metric_id": raw_metric_id or saved.get("raw_metric_id", ""),
        "decision": saved.get("decision", ""),
        "selected_code": saved.get("selected_code", ""),
        "selected_name": saved.get("selected_name", ""),
        "mapping_store_path": str(settings.mapping_store_path),
    }


@document_router.get("/documents/{doc_id}/delete-confirm", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def delete_confirm(request: Request, doc_id: str) -> HTMLResponse:
    settings = get_settings(request)
    try:
        document = load_document(settings, doc_id, refresh=False)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    return _render(request, "document_delete_confirm.html", {"document": document, "delete_plan": build_delete_plan(settings, document)})


@document_router.post("/documents/{doc_id}/delete", dependencies=[Depends(password_gate)])
def delete_document(request: Request, doc_id: str) -> RedirectResponse:
    try:
        execute_delete(get_settings(request), doc_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="文件不存在。") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _home_redirect("文件及相关结果已删除。")


def build_home_context(request: Request, *, message: str = "", error: str = "") -> dict[str, object]:
    settings = get_settings(request)
    return {
        "documents": list_documents(settings),
        "recent_batches": list_recent_document_batch_summaries(settings, limit=5),
        "message": message,
        "error": error,
    }
