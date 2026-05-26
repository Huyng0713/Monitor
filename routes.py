import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from db import dispose_engine, init_db
from log import get_log_paths, log_activity, log_exception, log_file_issue
from stats_service import StatsService

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "frontend/index.html")
VALID_GRANULARITIES = {"minute", "hour", "day"}
stats_service = StatsService()

CACHE_HEADERS = {"Cache-Control": "public, s-maxage=30, stale-while-revalidate=60"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await init_db()
        log_activity("API startup completed")
    except Exception:
        log_exception("API lifespan startup failed (database may be offline, server starting anyway)")
    
    yield
    
    try:
        await dispose_engine()
    except Exception:
        log_exception("Failed to dispose database engine")


app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    client_ip = request.client.host if request.client else "unknown"
    try:
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        log_activity(
            "HTTP request completed: method=%s path=%s status=%s client=%s duration_ms=%s",
            request.method, request.url.path, response.status_code, client_ip, duration_ms,
        )
        return response
    except Exception:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        log_exception(
            "HTTP request failed: method=%s path=%s client=%s duration_ms=%s",
            request.method, request.url.path, client_ip, duration_ms,
        )
        raise


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    log_activity("Validation error on path=%s details=%s", request.url.path, exc.errors())
    return JSONResponse(status_code=422, content={"detail": "Invalid request parameters", "errors": exc.errors()})


@app.exception_handler(SQLAlchemyError)
async def database_exception_handler(request: Request, exc: SQLAlchemyError):
    log_exception("Database error on path=%s", request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Database operation failed"})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log_exception("Unhandled server error on path=%s", request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


def require_granularity(granularity: str) -> str:
    if granularity not in VALID_GRANULARITIES:
        log_activity("Invalid granularity received: value=%s", granularity)
        raise HTTPException(
            status_code=400,
            detail=f"Invalid granularity '{granularity}'. Expected one of: {', '.join(sorted(VALID_GRANULARITIES))}",
        )
    return granularity


def json_cached(data):
    return JSONResponse(content=data, headers=CACHE_HEADERS)


@app.get("/")
def index():
    if not os.path.exists(INDEX_PATH):
        log_file_issue(logging.ERROR, "Frontend entry file missing: path=%s", INDEX_PATH)
        raise HTTPException(status_code=500, detail="Frontend entry file is missing")
    return FileResponse(INDEX_PATH)


@app.get("/stats/summary")
async def get_summary():
    return json_cached(await stats_service.get_summary())


@app.get("/stats/top-ips")
async def get_top_ips(limit: int = Query(default=10, ge=1, le=100)):
    return json_cached(await stats_service.get_top_ips(limit))


@app.get("/stats/top-urls")
async def get_top_urls(limit: int = Query(default=10, ge=1, le=100)):
    return json_cached(await stats_service.get_top_urls(limit))


@app.get("/stats/status-codes")
async def get_status_codes():
    return json_cached(await stats_service.get_status_codes())


@app.get("/stats/traffic")
async def get_traffic(
    granularity: str = "hour",
    ip: str = None,
    limit: int = Query(default=500, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
):
    return json_cached(await stats_service.get_traffic(require_granularity(granularity), ip, limit, offset))


@app.get("/stats/anomalies")
async def get_anomalies():
    return json_cached(await stats_service.get_anomalies())


@app.get("/stats/search")
async def search_logs(
    ip: str = None,
    path: str = None,
    status: int = None,
    time_from: str = None,
    time_to: str = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    # search is not cached
    try:
        return await stats_service.search_logs(ip, path, status, time_from, time_to, limit, offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid datetime format") from exc


@app.get("/stats/status-codes-over-time")
async def get_status_codes_over_time(
    granularity: str = "hour",
    limit: int = Query(default=60, ge=1, le=1440),
    offset: int = Query(default=0, ge=0),
):
    return json_cached(await stats_service.get_status_codes_over_time(require_granularity(granularity), limit, offset))


@app.get("/stats/logs")
async def get_system_logs(
    lines: int = Query(default=50, ge=1, le=500),
    reset: bool = Query(default=False)
):
    if reset:
        try:
            await stats_service.clear_system_logs()
        except Exception:
            log_exception("Failed to clear system logs on request")
    try:
        db_logs = await stats_service.get_system_logs(lines)
        if db_logs:
            formatted_lines = []
            for log in db_logs:
                dt_str = log["created_at"]
                if "T" in dt_str:
                    dt_part, time_part = dt_str.split("T")
                    time_clean = time_part.split(".")[0].split("+")[0]
                    dt_str = f"{dt_part} {time_clean}"
                
                tb_suffix = f"\n{log['traceback']}" if log["traceback"] else ""
                formatted_lines.append(f"{dt_str} [{log['level']}] {log['logger']} {log['message']}{tb_suffix}")
            formatted_lines.reverse()
            return {"lines": formatted_lines}
    except Exception:
        log_exception("Database system logs fetch failed, falling back to local files")

    log_paths = get_log_paths()
    try:
        with open(log_paths["error"], "r", encoding="utf-8") as file_obj:
            recent_lines = list(deque(file_obj, maxlen=lines))
        log_activity("System log tail requested (file fallback): lines=%s returned=%s", lines, len(recent_lines))
        return {"lines": recent_lines}
    except FileNotFoundError:
        log_file_issue(logging.ERROR, "System log file missing: path=%s", log_paths["error"])
        return {"lines": ["No log file found yet."]}
    except OSError:
        log_exception("Unable to read system log file: path=%s", log_paths["error"])
        raise HTTPException(status_code=500, detail="Unable to read system log file")
