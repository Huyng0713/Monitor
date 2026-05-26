import asyncio
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from db import read_connection

DATE_TRUNC_UNITS = {
    "minute": "minute",
    "hour": "hour",
    "day": "day",
}
INTERVAL_UNITS = {
    "minute": "1 minute",
    "hour": "1 hour",
    "day": "1 day",
}
TO_CHAR_FORMATS = {
    "minute": "YYYY-MM-DD\"T\"HH24:MI",
    "hour": "YYYY-MM-DD\"T\"HH24",
    "day": "YYYY-MM-DD",
}
TIMEDELTA_UNITS = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}


class StatsService:
    def __init__(self, connection_factory=read_connection):
        self.connection_factory = connection_factory
        self._cache = {}
        self._cache_ttl = 120
        self._cache_lock = asyncio.Lock()

    async def _cached(self, key, fn):
        now = time.time()
        cached = self._cache.get(key)
        if cached:
            value, ts = cached
            if now - ts < self._cache_ttl:
                return value

        async with self._cache_lock:
            now = time.time()
            cached = self._cache.get(key)
            if cached:
                value, ts = cached
                if now - ts < self._cache_ttl:
                    return value
            result = await fn()
            self._cache[key] = (result, now)
            return result

    async def fetch_scalar(self, query: str, params: dict | None = None, session = None) -> int:
        if session:
            result = await session.execute(text(query), params or {})
            value = result.scalar()
            return int(value or 0)
        async with self.connection_factory() as session_ctx:
            result = await session_ctx.execute(text(query), params or {})
            value = result.scalar()
        return int(value or 0)

    async def fetch_val(self, query: str, params: dict | None = None, session = None):
        if session:
            result = await session.execute(text(query), params or {})
            return result.scalar()
        async with self.connection_factory() as session_ctx:
            result = await session_ctx.execute(text(query), params or {})
            return result.scalar()

    async def fetch_rows(self, query: str, params: dict | None = None, session = None):
        if session:
            result = await session.execute(text(query), params or {})
            return result.mappings().all()
        async with self.connection_factory() as session_ctx:
            result = await session_ctx.execute(text(query), params or {})
            return result.mappings().all()

    async def get_max_time(self):
        return await self._cached("max_time", self._fetch_max_time)

    async def _fetch_max_time(self):
        max_time = await self.fetch_val("SELECT MAX(time) FROM logs")
        if not max_time:
            return datetime.now(timezone.utc)
        return max_time

    async def get_summary(self):
        return await self._cached("summary", self._fetch_summary)

    async def _fetch_summary(self):
        async with self.connection_factory() as session:
            total_reqs = await self.fetch_scalar("SELECT COUNT(*) FROM logs", session=session)
            errors = await self.fetch_scalar("SELECT COUNT(*) FROM logs WHERE status >= 400", session=session)
            unique_ips = await self.fetch_scalar("SELECT COUNT(DISTINCT ip) FROM logs", session=session)
        return {
            "total_requests": total_reqs,
            "unique_ips": unique_ips,
            "errors": errors,
        }

    async def get_top_ips(self, limit: int):
        return await self._cached(f"top_ips:{limit}", lambda: self._fetch_top_ips(limit))

    async def _fetch_top_ips(self, limit: int):
        rows = await self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            GROUP BY ip ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"ip": row["ip"], "count": row["count"]} for row in rows]

    async def get_top_urls(self, limit: int):
        return await self._cached(f"top_urls:{limit}", lambda: self._fetch_top_urls(limit))

    async def _fetch_top_urls(self, limit: int):
        rows = await self.fetch_rows("""
            SELECT path, COUNT(*) as count FROM logs
            GROUP BY path ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"path": row["path"], "count": row["count"]} for row in rows]

    async def get_status_codes(self):
        return await self._cached("status_codes", self._fetch_status_codes)

    async def _fetch_status_codes(self):
        rows = await self.fetch_rows("""
            SELECT status, COUNT(*) as count FROM logs
            GROUP BY status ORDER BY count DESC
        """)
        return [{"status": row["status"], "count": row["count"]} for row in rows]

    async def get_traffic(self, granularity: str, ip: str | None, limit: int, offset: int = 0):
        key = f"traffic:{granularity}:{ip or ''}:{limit}:{offset}"
        return await self._cached(key, lambda: self._fetch_traffic(granularity, ip, limit, offset))

    async def _fetch_traffic(self, granularity: str, ip: str | None, limit: int, offset: int):
        unit = DATE_TRUNC_UNITS[granularity]
        unit_delta = TIMEDELTA_UNITS[granularity]
        period_expr = self._period_expr(granularity, "l")
        
        async with self.connection_factory() as session:
            if ip:
                max_time_query = "SELECT time FROM logs WHERE ip = :ip ORDER BY time DESC LIMIT 1"
                max_time = await self.fetch_val(max_time_query, {"ip": ip}, session=session)
                if not max_time:
                    max_time = datetime.now(timezone.utc)
            else:
                max_time = await self.get_max_time()

            start_time = max_time - (limit + offset - 1) * unit_delta
            end_time = max_time - offset * unit_delta

            params: dict[str, object] = {
                "start_time": start_time,
                "end_time": end_time,
            }
            where_clause_ip_and = ""
            if ip:
                where_clause_ip_and = "AND l.ip = :ip"
                params["ip"] = ip

            query = f"""
                SELECT
                    {period_expr} AS period,
                    COUNT(*) AS count
                FROM logs l
                WHERE l.time >= :start_time
                  AND l.time <= :end_time
                  {where_clause_ip_and}
                GROUP BY period
                ORDER BY period
            """
            rows = await self.fetch_rows(query, params, session=session)
        return [{"period": row["period"], "count": row["count"]} for row in rows]

    async def get_anomalies(self):
        return await self._cached("anomalies", self._fetch_anomalies)

    async def _fetch_anomalies(self):
        max_time = await self.get_max_time()
        time_threshold = max_time - timedelta(hours=24)
        params = {"time_threshold": time_threshold}

        async with self.connection_factory() as session:
            high_freq = await self.fetch_rows("""
                SELECT ip, to_char(date_trunc('minute', l.time), 'YYYY-MM-DD"T"HH24:MI') as minute, COUNT(*) as count
                FROM logs l
                WHERE l.time >= :time_threshold
                GROUP BY ip, minute
                HAVING COUNT(*) > 100
                ORDER BY count DESC
            """, params, session=session)

            many_404 = await self.fetch_rows("""
                SELECT ip, COUNT(*) as count
                FROM logs l
                WHERE l.status = 404 AND l.time >= :time_threshold
                GROUP BY ip HAVING COUNT(*) > 20
                ORDER BY count DESC
            """, params, session=session)

            many_500 = await self.fetch_rows("""
                SELECT ip, COUNT(*) as count
                FROM logs l
                WHERE l.status = 500 AND l.time >= :time_threshold
                GROUP BY ip HAVING COUNT(*) > 10
                ORDER BY count DESC
            """, params, session=session)

        return {
            "high_frequency": [{"ip": row["ip"], "minute": row["minute"], "count": row["count"]} for row in high_freq],
            "many_404s": [{"ip": row["ip"], "count": row["count"]} for row in many_404],
            "many_500s": [{"ip": row["ip"], "count": row["count"]} for row in many_500],
        }

    async def get_status_codes_over_time(self, granularity: str, limit: int, offset: int = 0):
        return await self._cached(
            f"status_over_time:{granularity}:{limit}:{offset}",
            lambda: self._fetch_status_codes_over_time(granularity, limit, offset),
        )

    async def _fetch_status_codes_over_time(self, granularity: str, limit: int, offset: int):
        unit = DATE_TRUNC_UNITS[granularity]
        unit_delta = TIMEDELTA_UNITS[granularity]
        interval = INTERVAL_UNITS[granularity]
        
        max_time = await self.get_max_time()
        if granularity == "day":
            end_period = max_time.replace(hour=0, minute=0, second=0, microsecond=0)
        elif granularity == "hour":
            end_period = max_time.replace(minute=0, second=0, microsecond=0)
        else: # minute
            end_period = max_time.replace(second=0, microsecond=0)

        start_time = end_period - (limit + offset - 1) * unit_delta
        end_time_inclusive = end_period - offset * unit_delta
        end_time_exclusive = end_time_inclusive + unit_delta

        query = f"""
            WITH periods AS (
                SELECT generate_series(
                    CAST(:start_time AS timestamptz),
                    CAST(:end_time_inclusive AS timestamptz),
                    INTERVAL '{interval}'
                ) AS period
            ),
            counts AS (
                SELECT
                    date_trunc('{unit}', l.time) AS period,
                    l.status,
                    COUNT(*) AS count
                FROM logs l
                    WHERE l.time >= :start_time
                      AND l.time < :end_time_exclusive
                GROUP BY period, l.status
            )
            SELECT
                to_char(periods.period, 'YYYY-MM-DD"T"HH24') AS period,
                counts.status,
                COALESCE(counts.count, 0) AS count
            FROM periods
            LEFT JOIN counts ON counts.period = periods.period
            ORDER BY periods.period, counts.status
        """
        params = {
            "start_time": start_time,
            "end_time_inclusive": end_time_inclusive,
            "end_time_exclusive": end_time_exclusive
        }
        async with self.connection_factory() as session:
            rows = await self.fetch_rows(query, params, session=session)
        grouped = {}
        all_statuses = set()
        for row in rows:
            period = row["period"]
            grouped.setdefault(period, {})
            if row["status"] is not None:
                status = str(row["status"])
                all_statuses.add(status)
                grouped[period][status] = row["count"]

        labels = sorted(grouped.keys())
        datasets = [
            {"label": status, "data": [grouped[label].get(status, 0) for label in labels]}
            for status in sorted(all_statuses)
        ]
        return {"labels": labels, "datasets": datasets}

    async def search_logs(self, ip, path, status, time_from, time_to, limit, offset=0):
        if not time_from and not time_to:
            from datetime import datetime, timedelta, timezone
            time_from = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
            
        where_clauses = []
        params: dict[str, object] = {"limit": limit, "offset": offset}
        if ip:
            where_clauses.append("ip LIKE :ip")
            params["ip"] = f"%{ip}%"
        if path:
            where_clauses.append("path LIKE :path")
            params["path"] = f"%{path}%"
        if status is not None:
            where_clauses.append("status = :status")
            params["status"] = status
        if time_from:
            where_clauses.append("time >= :time_from")
            params["time_from"] = self._parse_datetime(time_from)
        if time_to:
            where_clauses.append("time <= :time_to")
            params["time_to"] = self._parse_datetime(time_to)

        where_sql = " AND ".join(where_clauses) or "1=1"
        count_params = {key: value for key, value in params.items() if key not in {"limit", "offset"}}
        async with self.connection_factory() as session:
            total = await self.fetch_scalar(f"SELECT COUNT(*) FROM logs WHERE {where_sql}", count_params, session=session)
            rows = await self.fetch_rows(f"""
                SELECT ip, time, method, path, status, size
                FROM logs
                WHERE {where_sql}
                LIMIT :limit OFFSET :offset
            """, params, session=session)
        return {
            "rows": [
                {
                    "ip": row["ip"],
                    "time": row["time"].isoformat() if hasattr(row["time"], "isoformat") else row["time"],
                    "method": row["method"],
                    "path": row["path"],
                    "status": row["status"],
                    "size": row["size"],
                }
                for row in rows
            ],
            "total": total,
        }

    def _period_expr(self, granularity: str, table_alias: str | None = None) -> str:
        unit = DATE_TRUNC_UNITS[granularity]
        fmt = TO_CHAR_FORMATS[granularity]
        time_column = f"{table_alias}.time" if table_alias else "time"
        return f"to_char(date_trunc('{unit}', {time_column}), '{fmt}')"

    def _parse_datetime(self, value: str) -> datetime:
        return datetime.fromisoformat(value)

    async def get_system_logs(self, limit: int):
        rows = await self.fetch_rows("""
            SELECT created_at, logger, level, message, traceback
            FROM system_logs
            ORDER BY created_at DESC
            LIMIT :limit
        """, {"limit": limit})
        return [
            {
                "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"],
                "logger": row["logger"],
                "level": row["level"],
                "message": row["message"],
                "traceback": row["traceback"]
            }
            for row in rows
        ]
