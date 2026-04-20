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
    return _render(
        request,
        "job_detail.html",
        {
            "job": job,
            "payload": build_job_detail_payload(job),
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
