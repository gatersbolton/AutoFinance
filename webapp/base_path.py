from __future__ import annotations

from collections.abc import Callable
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send


def app_path(base_path: str, path: str) -> str:
    normalized_base = normalize_base_path(base_path)
    normalized_path = str(path or "")
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"
    if not normalized_base:
        return normalized_path
    if normalized_path == "/":
        return f"{normalized_base}/"
    if normalized_path.startswith(f"{normalized_base}/") or normalized_path == normalized_base:
        return normalized_path
    return f"{normalized_base}{normalized_path}"


def normalize_base_path(value: str) -> str:
    path = (value or "").strip()
    if not path or path == "/":
        return ""
    if not path.startswith("/"):
        path = f"/{path}"
    return path.rstrip("/")


class BasePathMiddleware:
    def __init__(self, app: ASGIApp, *, base_path: str) -> None:
        self.app = app
        self.base_path = normalize_base_path(base_path)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self.base_path or scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path", "") or "")
        if path == self.base_path:
            path = "/"
        elif path.startswith(f"{self.base_path}/"):
            path = path[len(self.base_path) :] or "/"
        elif path == "/healthz":
            await self.app(scope, receive, send)
            return
        else:
            await self.app(scope, receive, self._prefix_location_send(send))
            return

        next_scope = dict(scope)
        next_scope["path"] = path
        await self.app(next_scope, receive, self._prefix_location_send(send))

    def _prefix_location_send(self, send: Send) -> Send:
        async def wrapped(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = []
                for name, value in message.get("headers", []):
                    if name.lower() == b"location":
                        value = self._prefix_location(value)
                    headers.append((name, value))
                message = dict(message)
                message["headers"] = headers
            await send(message)

        return wrapped

    def _prefix_location(self, raw_value: bytes) -> bytes:
        try:
            location = raw_value.decode("latin-1")
        except UnicodeDecodeError:
            return raw_value
        if not location.startswith("/") or location.startswith(f"{self.base_path}/") or location == self.base_path:
            return raw_value
        return f"{self.base_path}{location}".encode("latin-1")
