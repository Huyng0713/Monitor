from fastapi import FastAPI, Query
from db import get_connection
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))   

app = FastAPI()
@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "frontend/index.html"))

@app.get("/stats/summary")
def get_summary():
    """Total requests, unique IPs, error count."""
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    unique_ips = conn.execute("SELECT COUNT(DISTINCT ip) FROM logs").fetchone()[0]
    errors = conn.execute("SELECT COUNT(*) FROM logs WHERE status >= 400").fetchone()[0]
    conn.close()
    return {"total_requests": total, "unique_ips": unique_ips, "errors": errors}

@app.get("/stats/top-ips")
def get_top_ips(limit: int = 10):
    """Top IPs by request count."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT ip, COUNT(*) as count FROM logs
        GROUP BY ip ORDER BY count DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [{"ip": r["ip"], "count": r["count"]} for r in rows]

@app.get("/stats/top-urls")
def get_top_urls(limit: int = 10):
    """Top URLs by request count."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT path, COUNT(*) as count FROM logs
        GROUP BY path ORDER BY count DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [{"path": r["path"], "count": r["count"]} for r in rows]

@app.get("/stats/status-codes")
def get_status_codes():
    """Count per status code."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT status, COUNT(*) as count FROM logs
        GROUP BY status ORDER BY count DESC
    """).fetchall()
    conn.close()
    return [{"status": r["status"], "count": r["count"]} for r in rows]

@app.get("/stats/traffic")
def get_traffic(granularity: str = "hour", ip: str = None):
    formats = {
        "minute": "%Y-%m-%dT%H:%M",
        "hour":   "%Y-%m-%dT%H",
        "day":    "%Y-%m-%d",
    }
    fmt = formats.get(granularity, "%Y-%m-%dT%H")
    conn = get_connection()
    
    if ip:
        rows = conn.execute("""
            SELECT strftime(?, time) as period, COUNT(*) as count
            FROM logs WHERE ip = ?
            GROUP BY period ORDER BY period
        """, (fmt, f"%{ip}%")).fetchall()
    else:
        rows = conn.execute("""
            SELECT strftime(?, time) as period, COUNT(*) as count
            FROM logs GROUP BY period ORDER BY period
        """, (fmt,)).fetchall()
    
    conn.close()
    return [{"period": r["period"], "count": r["count"]} for r in rows]

@app.get("/stats/anomalies")
def get_anomalies():
    """Basic anomaly detection."""
    conn = get_connection()

    # IPs with more than 100 requests in any single minute
    high_freq = conn.execute("""
        SELECT ip, strftime('%Y-%m-%dT%H:%M', time) as minute, COUNT(*) as count
        FROM logs GROUP BY ip, minute
        HAVING count > 100
        ORDER BY count DESC
    """).fetchall()

    # IPs with many 404s
    many_404 = conn.execute("""
        SELECT ip, COUNT(*) as count FROM logs
        WHERE status = 404
        GROUP BY ip HAVING count > 20
        ORDER BY count DESC
    """).fetchall()

    # IPs with many 500s
    many_500 = conn.execute("""
        SELECT ip, COUNT(*) as count FROM logs
        WHERE status = 500
        GROUP BY ip HAVING count > 10
        ORDER BY count DESC
    """).fetchall()

    conn.close()
    return {
        "high_frequency": [{"ip": r["ip"], "minute": r["minute"], "count": r["count"]} for r in high_freq],
        "many_404s": [{"ip": r["ip"], "count": r["count"]} for r in many_404],
        "many_500s": [{"ip": r["ip"], "count": r["count"]} for r in many_500],
    }
@app.get("/stats/search")
def search_logs(ip: str = None, path: str = None, status: int = None, 
                time_from: str = None, time_to: str = None, limit: int = 100):
    conn = get_connection()
    query = "SELECT ip, time, method, path, status, size FROM logs WHERE 1=1"
    params = []
    if ip:
        query += " AND ip LIKE ?"
        params.append(f"%{ip}%")
    if path:
        query += " AND path LIKE ?"
        params.append(f"%{path}%")
    if status:
        query += " AND status = ?"
        params.append(status)
    if time_from:
        query += " AND time >= ?"
        params.append(time_from)
    if time_to:
        query += " AND time <= ?"
        params.append(time_to)
    query += " ORDER BY time DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [{"ip": r["ip"], "time": r["time"], "method": r["method"],
             "path": r["path"], "status": r["status"], "size": r["size"]} for r in rows]

@app.get("/stats/status-codes-over-time")
def get_status_codes_over_time(granularity: str = "hour"):

    formats = {
        "minute": "%Y-%m-%dT%H:%M",
        "hour": "%Y-%m-%dT%H",
        "day": "%Y-%m-%d",
    }

    fmt = formats.get(granularity, "%Y-%m-%dT%H")

    conn = get_connection()

    rows = conn.execute("""
        SELECT
            strftime(?, time) as period,
            status,
            COUNT(*) as count
        FROM logs
        GROUP BY period, status
        ORDER BY period
    """, (fmt,)).fetchall()

    conn.close()

    grouped = {}

    all_statuses = set()

    for row in rows:

        period = row["period"]
        status = str(row["status"])
        count = row["count"]

        all_statuses.add(status)

        if period not in grouped:
            grouped[period] = {}

        grouped[period][status] = count

    labels = sorted(grouped.keys())

    datasets = []

    for status in sorted(all_statuses):

        datasets.append({
            "label": status,

            "data": [
                grouped[label].get(status, 0)
                for label in labels
            ]
        })

    return {
        "labels": labels,
        "datasets": datasets
    }