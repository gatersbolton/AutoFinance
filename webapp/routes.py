from __future__ import annotations

import secrets
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .config import WebAppSettings
from .db import cancel_job, list_jobs, requeue_job
from .jobs import (
    build_job_detail_payload,
    create_existing_ocr_job,
    create_upload_job,
    get_system_status,
    list_existing_ocr_choices,
    require_job,
    resolve_download_artifact,
)
from .models import ACTIVE_JOB_STATUSES
from .quality import describe_job_status
from .review import (
    HIGH_PRIORITY_THRESHOLD,
    build_review_dashboard_summary,
    build_review_filters,
    export_review_actions,
    filter_review_items,
    load_review_items,
    resolve_evidence_file,
    save_review_action,
)


router = APIRouter()
security = HTTPBasic(auto_error=False)


def get_settings(request: Request) -> WebAppSettings:
    return request.app.state.settings


def get_templates(request: Request):
    return request.app.state.templates


def password_gate(
    request: Request,
    credentials: Annotated[HTTPBasicCredentials | None, Depends(security)],
) -> None:
    settings = get_settings(request)
    if not settings.auth_required:
        return
    if credentials is None or not settings.admin_password or not secrets.compare_digest(credentials.password, settings.admin_password):
        raise HTTPException(status_code=401, detail="需要认证。", headers={"WWW-Authenticate": "Basic"})


def _render(
    request: Request,
    template_name: str,
    context: dict[str, object],
    *,
    status_code: int = 200,
) -> HTMLResponse:
    templates = get_templates(request)
    payload = {"request": request, "settings": get_settings(request)}
    payload.update(context)
    return templates.TemplateResponse(request, template_name, payload, status_code=status_code)


def _new_job_context(request: Request, *, error_message: str = "", submitted: dict[str, object] | None = None) -> dict[str, object]:
    settings = get_settings(request)
    submitted = submitted or {}
    return {
        "error_message": error_message,
        "existing_ocr_choices": list_existing_ocr_choices(settings),
        "default_existing_path": submitted.get("existing_ocr_path") or next(iter(list_existing_ocr_choices(settings)), ""),
        "submitted_mode": submitted.get("mode", "existing_ocr_outputs"),
        "submitted_display_name": submitted.get("display_name", ""),
        "upload_auto_run_enabled": settings.auto_run_upload_ocr,
        "template_path": str(settings.template_path),
    }


def _review_redirect_target(job_id: str, next_url: str) -> str:
    candidate = (next_url or "").strip()
    if candidate.startswith(f"/jobs/{job_id}/review"):
        return candidate
    return f"/jobs/{job_id}/review/items"


def _get_review_item_or_404(request: Request, job_id: str, review_item_id: str):
    settings = get_settings(request)
    job = require_job(settings, job_id)
    items, _ = load_review_items(settings, job)
    for item in items:
        if item.review_item_id == review_item_id:
            return job, item
    raise HTTPException(status_code=404, detail="复核项不存在。")


@router.get("/", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def index(request: Request) -> HTMLResponse:
    jobs = list_jobs(get_settings(request), limit=20)
    active_jobs = [job for job in jobs if job.status in ACTIVE_JOB_STATUSES]
    return _render(
        request,
        "index.html",
        {
            "jobs": jobs[:5],
            "active_jobs": active_jobs,
            "describe_job_status": describe_job_status,
        },
    )


@router.get("/jobs/new", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def new_job(request: Request) -> HTMLResponse:
    return _render(request, "new_job.html", _new_job_context(request))


@router.post("/jobs", dependencies=[Depends(password_gate)], response_model=None)
async def create_job(
    request: Request,
    mode: Annotated[str, Form(...)],
    display_name: Annotated[str, Form()] = "",
    existing_ocr_path: Annotated[str, Form()] = "",
    uploaded_files: Annotated[list[UploadFile] | None, File()] = None,
) -> Response:
    settings = get_settings(request)
    submitted = {
        "mode": mode,
        "display_name": display_name,
        "existing_ocr_path": existing_ocr_path,
    }
    try:
        if mode == "existing_ocr_outputs":
            job = create_existing_ocr_job(settings, display_name=display_name, raw_input_path=existing_ocr_path)
        elif mode == "upload_pdf":
            job = await create_upload_job(settings, display_name=display_name, files=uploaded_files or [])
        else:
            raise ValueError(f"不支持的任务模式: {mode}")
    except ValueError as exc:
        return _render(request, "new_job.html", _new_job_context(request, error_message=str(exc), submitted=submitted), status_code=400)
    return RedirectResponse(url=f"/jobs/{job.job_id}", status_code=303)


@router.get("/jobs", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def jobs_page(request: Request) -> HTMLResponse:
    jobs = list_jobs(get_settings(request), limit=200)
    has_active_jobs = any(job.status in ACTIVE_JOB_STATUSES for job in jobs)
    return _render(
        request,
        "jobs.html",
        {
            "jobs": jobs,
            "has_active_jobs": has_active_jobs,
            "describe_job_status": describe_job_status,
        },
    )


@router.get("/jobs/{job_id}", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def job_detail(request: Request, job_id: str) -> HTMLResponse:
    job = require_job(get_settings(request), job_id)
    review_items, review_sources = load_review_items(get_settings(request), job)
    return _render(
        request,
        "job_detail.html",
        {
            "job": job,
            "payload": build_job_detail_payload(job),
            "review_summary": build_review_dashboard_summary(review_items),
            "review_sources": review_sources,
            "can_cancel": job.status in {"created", "queued"},
            "can_queue": job.status in {"created", "failed", "cancelled"},
            "auto_refresh": job.status in ACTIVE_JOB_STATUSES,
        },
    )


@router.get("/jobs/{job_id}/status", response_class=JSONResponse, dependencies=[Depends(password_gate)])
def job_status(request: Request, job_id: str) -> JSONResponse:
    job = require_job(get_settings(request), job_id)
    return JSONResponse(build_job_detail_payload(job))


@router.post("/jobs/{job_id}/cancel", dependencies=[Depends(password_gate)])
def cancel_job_route(request: Request, job_id: str) -> RedirectResponse:
    job = cancel_job(get_settings(request), job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在。")
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@router.post("/jobs/{job_id}/queue", dependencies=[Depends(password_gate)])
def queue_job_route(request: Request, job_id: str) -> RedirectResponse:
    job = requeue_job(get_settings(request), job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在。")
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@router.get("/jobs/{job_id}/download/{slug}", dependencies=[Depends(password_gate)])
def download_artifact(request: Request, job_id: str, slug: str) -> FileResponse:
    job = require_job(get_settings(request), job_id)
    artifact = resolve_download_artifact(job, slug)
    if artifact is None:
        raise HTTPException(status_code=404, detail="文件不存在。")
    path = Path(artifact.path)
    if not artifact.exists or not path.exists():
        raise HTTPException(status_code=404, detail="文件未生成。")
    return FileResponse(path=str(path), filename=artifact.download_name)


@router.get("/jobs/{job_id}/review", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def review_dashboard(request: Request, job_id: str) -> HTMLResponse:
    settings = get_settings(request)
    job = require_job(settings, job_id)
    items, source_artifacts = load_review_items(settings, job)
    return _render(
        request,
        "review_dashboard.html",
        {
            "job": job,
            "summary": build_review_dashboard_summary(items),
            "source_artifacts": source_artifacts,
            "high_priority_threshold": HIGH_PRIORITY_THRESHOLD,
        },
    )


@router.get("/jobs/{job_id}/review/items", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def review_items_page(
    request: Request,
    job_id: str,
    status: str = "",
    source_type: str = "",
    reason_code: str = "",
    page_no: str = "",
    statement_type: str = "",
    provider: str = "",
    search: str = "",
    only_high_priority: str = "",
    sort_by: str = "priority_desc",
) -> HTMLResponse:
    settings = get_settings(request)
    job = require_job(settings, job_id)
    items, source_artifacts = load_review_items(settings, job)
    filtered_items = filter_review_items(
        items,
        status=status,
        source_type=source_type,
        reason_code=reason_code,
        page_no=page_no,
        statement_type=statement_type,
        provider=provider,
        search=search,
        only_high_priority=only_high_priority in {"1", "true", "yes", "on"},
        sort_by=sort_by,
    )
    for item in filtered_items:
        item.meta["evidence_cell_available"] = resolve_evidence_file(job, item, "cell") is not None
        item.meta["evidence_row_available"] = resolve_evidence_file(job, item, "row") is not None
        item.meta["evidence_table_available"] = resolve_evidence_file(job, item, "table") is not None
    return _render(
        request,
        "review_items.html",
        {
            "job": job,
            "items": filtered_items,
            "summary": build_review_dashboard_summary(items),
            "filters": build_review_filters(items),
            "selected_filters": {
                "status": status,
                "source_type": source_type,
                "reason_code": reason_code,
                "page_no": page_no,
                "statement_type": statement_type,
                "provider": provider,
                "search": search,
                "only_high_priority": only_high_priority,
                "sort_by": sort_by,
            },
            "source_artifacts": source_artifacts,
        },
    )


@router.post("/jobs/{job_id}/review/actions", dependencies=[Depends(password_gate)])
def save_review_action_route(
    request: Request,
    job_id: str,
    review_item_id: Annotated[str, Form(...)],
    action_type: Annotated[str, Form(...)],
    action_value: Annotated[str, Form()] = "",
    reviewer_note: Annotated[str, Form()] = "",
    reviewer_name: Annotated[str, Form()] = "",
    next_url: Annotated[str, Form()] = "",
) -> RedirectResponse:
    settings = get_settings(request)
    job = require_job(settings, job_id)
    items, _ = load_review_items(settings, job)
    item = next((candidate for candidate in items if candidate.review_item_id == review_item_id), None)
    if item is None:
        raise HTTPException(status_code=404, detail="复核项不存在。")
    try:
        save_review_action(
            settings,
            job,
            item,
            action_type=action_type,
            action_value=action_value,
            reviewer_note=reviewer_note,
            reviewer_name=reviewer_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url=_review_redirect_target(job_id, next_url), status_code=303)


@router.get("/jobs/{job_id}/review/evidence/{review_item_id}/{evidence_kind}", dependencies=[Depends(password_gate)])
def review_evidence(request: Request, job_id: str, review_item_id: str, evidence_kind: str) -> FileResponse:
    if evidence_kind not in {"cell", "row", "table"}:
        raise HTTPException(status_code=404, detail="证据类型不存在。")
    job, item = _get_review_item_or_404(request, job_id, review_item_id)
    resolved = resolve_evidence_file(job, item, evidence_kind)
    if resolved is None:
        raise HTTPException(status_code=404, detail="证据图片暂不可用。")
    return FileResponse(path=str(resolved), filename=resolved.name)


@router.get("/jobs/{job_id}/review/export-actions", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def review_export_page(request: Request, job_id: str) -> HTMLResponse:
    settings = get_settings(request)
    job = require_job(settings, job_id)
    items, _ = load_review_items(settings, job)
    export_artifacts = [artifact for artifact in build_job_detail_payload(job)["output_files"] if str(artifact.get("slug", "")).startswith("review_action_") or str(artifact.get("slug", "")) in {"review_actions_csv", "review_actions_xlsx"}]
    return _render(
        request,
        "review_export_actions.html",
        {
            "job": job,
            "summary": build_review_dashboard_summary(items),
            "actions_total": sum(1 for item in items if item.action_type),
            "export_artifacts": export_artifacts,
        },
    )


@router.post("/jobs/{job_id}/review/export-actions", dependencies=[Depends(password_gate)])
def review_export_actions_route(request: Request, job_id: str) -> RedirectResponse:
    settings = get_settings(request)
    job = require_job(settings, job_id)
    export_review_actions(settings, job)
    return RedirectResponse(url=f"/jobs/{job_id}/review/export-actions", status_code=303)


@router.get("/system", response_class=HTMLResponse, dependencies=[Depends(password_gate)])
def system_page(request: Request) -> HTMLResponse:
    return _render(request, "system.html", {"status": get_system_status(get_settings(request))})


@router.get("/api/system-status", response_class=JSONResponse, dependencies=[Depends(password_gate)])
def system_status_api(request: Request) -> JSONResponse:
    return JSONResponse(get_system_status(get_settings(request)).as_dict())


@router.get("/healthz", response_class=JSONResponse)
def healthcheck(request: Request) -> JSONResponse:
    settings = get_settings(request)
    return JSONResponse(
        {
            "ok": True,
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "environment": settings.env_mode,
        }
    )
