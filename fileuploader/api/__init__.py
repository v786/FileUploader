"""FastAPI app (stands in for the stateless Fastify API)."""
from __future__ import annotations

import asyncio
import math
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .. import adapters, db, ratelimit
from ..errors import ApiError
from ..log import log

PUBLIC_PREFIXES = ("/mock-s3", "/__faults", "/admin", "/health", "/docs", "/openapi.json")


def _error_response(err: ApiError, request: Request, headers: dict | None = None) -> JSONResponse:
    h = dict(headers or {})
    if err.retry_after_ms is not None:
        h["Retry-After"] = str(max(1, math.ceil(err.retry_after_ms / 1000)))
    return JSONResponse(err.body(getattr(request.state, "request_id", None)), status_code=err.status, headers=h)


def create_app(run_workers: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init_db()
        adapters.get()
        stop = asyncio.Event()
        task = None
        if run_workers:
            from ..worker import run_workers as _run
            task = asyncio.create_task(_run(stop))
        yield
        stop.set()
        if task:
            await task

    app = FastAPI(title="Multi-File Upload & Processing Service", lifespan=lifespan)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
        path = request.url.path
        rl_headers: dict[str, str] = {}
        if not path.startswith(PUBLIC_PREFIXES) and request.method != "OPTIONS":
            # Mock auth: identity comes from a header (or ?user_id= for EventSource, which can't set headers).
            uid = request.headers.get("X-User-Id") or request.query_params.get("user_id")
            if not uid:
                return _error_response(ApiError(401, "UNAUTHENTICATED"), request)
            request.state.user_id = uid
            d = await asyncio.to_thread(ratelimit.take, [ratelimit.inbound_spec("api", uid)])
            rl_headers = ratelimit.headers(d)
            if not d.allowed:
                return _error_response(ApiError(429, "RATE_LIMITED", retryable=True,
                                                retry_after_ms=math.ceil(d.wait_s * 1000)), request, rl_headers)
        response = await call_next(request)
        for k, v in rl_headers.items():
            response.headers.setdefault(k, v)
        response.headers["X-Request-Id"] = request.state.request_id
        return response

    # CORS: browsers PUT straight to /mock-s3 and must be able to read the ETag header.
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                       expose_headers=["ETag", "Retry-After", "RateLimit-Limit", "RateLimit-Remaining",
                                       "RateLimit-Reset", "X-Request-Id"])

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError):
        return _error_response(exc, request)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        msg = "; ".join(f"{'.'.join(map(str, e['loc'][1:]))}: {e['msg']}" for e in exc.errors())
        return _error_response(ApiError(400, "VALIDATION_ERROR", msg or None), request)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException):
        code = "NOT_FOUND" if exc.status_code == 404 else "VALIDATION_ERROR"
        return _error_response(ApiError(exc.status_code, code), request)

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        # Stack traces and vendor responses never leave the server.
        log("request.error", request_id=getattr(request.state, "request_id", None), path=request.url.path,
            error=f"{type(exc).__name__}: {exc}")
        return _error_response(ApiError(500, "INTERNAL_ERROR", retryable=True), request)

    @app.get("/health")
    def health():
        return {"ok": True}

    from . import admin, mock_s3, routes, sse
    app.include_router(routes.router)
    app.include_router(sse.router)
    app.include_router(admin.router)
    app.include_router(mock_s3.router)
    return app
