from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import WebAppSettings, load_settings
from .db import init_db
from .routes import router
from .runner import LocalWorkerThread


def create_app(settings: WebAppSettings | None = None) -> FastAPI:
    resolved_settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        resolved_settings.validate_runtime_configuration()
        resolved_settings.ensure_directories()
        init_db(resolved_settings)
        app.state.settings = resolved_settings
        app.state.templates = Jinja2Templates(directory=str(resolved_settings.templates_dir))
        worker = None
        if resolved_settings.enable_local_worker:
            worker = LocalWorkerThread(resolved_settings)
            worker.start()
        app.state.worker = worker
        try:
            yield
        finally:
            if worker is not None:
                worker.stop()

    app = FastAPI(title=resolved_settings.app_name, lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=str(resolved_settings.static_dir)), name="static")
    app.include_router(router)
    return app


app = create_app()
