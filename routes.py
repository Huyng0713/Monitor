import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks, Depends, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from db import dispose_engine, init_db
from log import get_log_paths, log_activity, log_exception, log_file_issue
from stats_service import StatsService, background_tasks_var

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "frontend/index.html")
VALID_GRANULARITIES = {"second", "minute", "hour", "day"}
stats_service = StatsService()
IS_VERCEL = os.getenv("VERCEL") == "1"
LOCAL_CLIENT_HOSTS = {"127.0.0.1", "::1", "localhost"}
SERVER_STARTED_AT = datetime.now()

CACHE_HEADERS = {"Cache-Control": "no-store"}


def set_bg_tasks(background_tasks: BackgroundTasks):
    background_tasks_var.set(background_tasks)
    return background_tasks


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await init_db()
        stats_service.start_background_tasks()

        # Background loops are only reliable on long-lived local/server hosts.
        # Vercel should use /simulation/tick via Vercel Cron instead.
        if not IS_VERCEL and os.getenv("SIMULATION_ENABLED", "0") == "1":
            from simulator import simulator
            simulator.start()

        log_activity("API startup completed")
    except Exception:
        log_exception("API lifespan startup failed (database may be offline, server starting anyway)")

    yield

    try:
        if not IS_VERCEL:
            from simulator import simulator
            simulator.stop()

        await dispose_engine()
        import geoip_resolver
        geoip_resolver.close_readers()
    except Exception:
        log_exception("Failed to dispose database engine or close geoip readers")


app = FastAPI(lifespan=lifespan, dependencies=[Depends(set_bg_tasks)])
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


def is_local_request(request: Request) -> bool:
    client_host = request.client.host if request.client else ""
    host_header = request.headers.get("host", "").split(":")[0]
    return (
        client_host in LOCAL_CLIENT_HOSTS
        or host_header in LOCAL_CLIENT_HOSTS
        or host_header == "0.0.0.0"
        or host_header.startswith("192.168.")
        or host_header.startswith("10.")
        or any(host_header.startswith(f"172.{i}.") for i in range(16, 32))
    )


def require_cron_secret(request: Request) -> None:
    if not IS_VERCEL and is_local_request(request):
        return
    cron_secret = os.getenv("CRON_SECRET")
    if not cron_secret:
        raise HTTPException(status_code=401, detail="CRON_SECRET is not configured")
    if request.headers.get("authorization") != f"Bearer {cron_secret}":
        raise HTTPException(status_code=401, detail="Unauthorized")


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


@app.get("/stats/top-countries")
async def get_top_countries():
    return json_cached(await stats_service.get_top_countries())


@app.get("/stats/top-isps")
async def get_top_isps():
    return json_cached(await stats_service.get_top_isps())



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


@app.get("/stats/traffic-series")
async def get_traffic_series(range: str = Query(default="30s")):
    try:
        return json_cached(await stats_service.get_traffic_series(range))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/stats/anomalies")
async def get_anomalies():
    return json_cached(await stats_service.get_anomalies())


@app.get("/stats/dashboard")
async def get_dashboard(include_expensive: bool = Query(default=False)):
    data = await stats_service.get_dashboard_data(include_expensive=include_expensive)
    return json_cached(data)


@app.get("/stats/search")
async def search_logs(
    ip: str = None,
    country: str = None,
    path: str = None,
    status: int = None,
    time_from: str = None,
    time_to: str = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    # search is not cached
    try:
        return await stats_service.search_logs(ip, country, path, status, time_from, time_to, limit, offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid datetime format") from exc


@app.get("/stats/search/jump")
async def jump_to_page(
    page: int = Query(..., ge=0),
    page_size: int = Query(default=15, ge=1, le=500),
    ip: str = None,
    country: str = None,
    path: str = None,
    status: int = None,
    time_from: str = None,
    time_to: str = None,
    total_count: int = None,
):
    """
    Tính target_id cho page bất kỳ mà không cần OFFSET.
    Dùng index scan trên id thay vì sequential scan.
    """
    try:
        result = await stats_service.jump_to_page(
            page, page_size, ip, country, path, status, time_from, time_to, total_count
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/stats/search/count")
async def search_logs_count(
    ip: str = None,
    country: str = None,
    path: str = None,
    status: int = None,
    time_from: str = None,
    time_to: str = None,
):
    try:
        count = await stats_service.search_logs_count(ip, country, path, status, time_from, time_to)
        return {"total": count}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid datetime format") from exc


@app.get("/stats/search/keyset")
async def search_logs_keyset(
    ip: str = None,
    country: str = None,
    path: str = None,
    status: int = None,
    time_from: str = None,
    time_to: str = None,
    limit: int = Query(default=15, ge=1, le=500),
    cursor: str = None,       # ISO timestamp of the last row on the previous page
    cursor_id: int = None,    # ID of the last row on the previous page
):
    try:
        return await stats_service.search_logs_keyset(
            ip, country, path, status, time_from, time_to, limit, cursor, cursor_id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid datetime format") from exc


@app.get("/stats/status-codes-over-time")
async def get_status_codes_over_time(
    granularity: str = "hour",
    limit: int = Query(default=60, ge=1, le=1440),
    offset: int = Query(default=0, ge=0),
):
    return json_cached(await stats_service.get_status_codes_over_time(require_granularity(granularity), limit, offset))


@app.get("/stats/status-codes-series")
async def get_status_codes_series(range: str = Query(default="30s")):
    try:
        return json_cached(await stats_service.get_status_codes_series(range))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/stats/logs")
async def get_system_logs(
    lines: int = Query(default=50, ge=1, le=500),
    since_start: bool = Query(default=True),
    since_hours: int = Query(default=24, ge=0, le=24 * 30),
    reset: bool = Query(default=False)
):
    log_paths = get_log_paths()
    if reset:
        try:
            with open(log_paths["error"], "w", encoding="utf-8"):
                pass
            return {"lines": ["System logs cleared."]}
        except OSError:
            log_exception("Failed to clear system log file on request")
            raise HTTPException(status_code=500, detail="Unable to clear system log file")

    try:
        if since_start:
            cutoff = SERVER_STARTED_AT
        elif since_hours == 0:
            cutoff = None
        else:
            cutoff = datetime.now() - timedelta(hours=since_hours)
        timestamp_format = "%Y-%m-%d %H:%M:%S"
        filtered_lines = deque(maxlen=lines)
        include_current_entry = cutoff is None

        with open(log_paths["error"], "r", encoding="utf-8") as file_obj:
            for line in file_obj:
                line_time = None
                if len(line) >= 19:
                    try:
                        line_time = datetime.strptime(line[:19], timestamp_format)
                    except ValueError:
                        line_time = None

                if line_time is not None:
                    include_current_entry = cutoff is None or line_time >= cutoff

                if include_current_entry:
                    filtered_lines.append(line)

        recent_lines = list(filtered_lines)
        log_activity(
            "System log tail requested: lines=%s since_start=%s since_hours=%s returned=%s",
            lines,
            since_start,
            since_hours,
            len(recent_lines),
        )
        if not recent_lines and since_start:
            return {"lines": ["No error logs since server startup."]}
        if not recent_lines and since_hours != 0:
            return {"lines": [f"No error logs in the last {since_hours} hours."]}
        return {"lines": recent_lines}
    except FileNotFoundError:
        return {"lines": ["No log file found yet."]}
    except OSError:
        log_exception("Unable to read system log file: path=%s", log_paths["error"])
        raise HTTPException(status_code=500, detail="Unable to read system log file")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    from ws_manager import manager as ws_manager
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection open, wait for client messages/pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        log_exception(f"WebSocket connection error: {e}")
        ws_manager.disconnect(websocket)


@app.get("/simulation/tick")
async def run_simulation_tick(
    request: Request,
    batch_size: int | None = Query(default=None, ge=1, le=1000),
    spread_seconds: int = Query(default=59, ge=0, le=3600),
):
    require_cron_secret(request)
    from simulator import simulator
    result = await simulator.tick(batch_size=batch_size, broadcast=False, spread_seconds=spread_seconds)
    log_activity(
        "Simulation cron tick completed: generated=%s inserted=%s",
        result["generated"],
        result["inserted"],
    )
    return {"status": "ok", **result}


@app.post("/simulation/start")
async def start_simulation(request: Request):
    require_cron_secret(request)
    if IS_VERCEL:
        raise HTTPException(status_code=400, detail="Use /simulation/tick with Vercel Cron instead of background simulation")
    from simulator import simulator
    simulator.start()
    return {"status": "started", "info": simulator.get_status()}


@app.post("/simulation/stop")
async def stop_simulation(request: Request):
    require_cron_secret(request)
    from simulator import simulator
    simulator.stop()
    return {"status": "stopped", "info": simulator.get_status()}


@app.get("/simulation/status")
async def get_simulation_status(request: Request):
    require_cron_secret(request)
    from simulator import simulator
    return {"mode": "vercel-cron" if IS_VERCEL else "local-background", **simulator.get_status()}
