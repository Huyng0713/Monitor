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
        self._locks = {}

    async def _cached(self, key, fn):
        now = time.time()
        cached = self._cache.get(key)
        if cached:
            value, ts = cached
            if now - ts < self._cache_ttl:
                return value

        # Use a per-key lock to prevent deadlocks and allow concurrent execution of different stats
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.time()
            cached = self._cache.get(key)
            if cached:
                value, ts = cached
                if now - ts < self._cache_ttl:
                    return value
            result = await fn()
            self._cache[key] = (result, now)
            return result

    async def db_cached(self, key, fetch_fn, ttl=120, force_refresh=False):
        now = time.time()
        cached_in_mem = self._cache.get(key)
        if cached_in_mem and not force_refresh:
            value, ts = cached_in_mem
            if now - ts < ttl:
                return value

        import json
        from datetime import datetime, timezone
        from db import write_connection

        db_val = None
        db_updated_at = None

        try:
            async with self.connection_factory() as session:
                row = await self.fetch_rows("SELECT value, updated_at FROM cached_stats WHERE key = :key", {"key": key}, session=session)
                if row:
                    db_val = row[0]["value"]
                    db_updated_at = row[0]["updated_at"]
        except Exception:
            from log import log_exception
            log_exception("Failed to read from cached_stats table")

        if db_updated_at:
            if db_updated_at.tzinfo is None:
                db_updated_at = db_updated_at.replace(tzinfo=timezone.utc)
            db_age = (datetime.now(timezone.utc) - db_updated_at).total_seconds()
        else:
            db_age = 9999999

        if db_val is not None and db_age < ttl and not force_refresh:
            parsed_val = json.loads(db_val)
            self._cache[key] = (parsed_val, now)
            return parsed_val

        # Stale-While-Revalidate: return stale value if age is under 10 minutes and refresh in background
        if db_val is not None and db_age < 600 and not force_refresh:
            parsed_val = json.loads(db_val)
            self._cache[key] = (parsed_val, now)
            asyncio.create_task(self._refresh_stat_in_db(key, fetch_fn))
            return parsed_val

        # Cold start/hard refresh: compute synchronously
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            cached_in_mem = self._cache.get(key)
            if cached_in_mem and not force_refresh:
                value, ts = cached_in_mem
                if now - ts < ttl:
                    return value

            result = await fetch_fn()
            self._cache[key] = (result, time.time())

            try:
                serialized = json.dumps(result)
                async with write_connection() as session:
                    await session.execute(text("""
                        INSERT INTO cached_stats (key, value, updated_at)
                        VALUES (:key, :value, NOW())
                        ON CONFLICT (key) DO UPDATE 
                        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                    """), {"key": key, "value": serialized})
            except Exception:
                from log import log_exception
                log_exception(f"Failed to write cached_stat {key} to DB")

            return result

    async def _refresh_stat_in_db(self, key, fetch_fn):
        import json
        from db import write_connection
        try:
            lock = self._locks.setdefault(key, asyncio.Lock())
            if lock.locked():
                return
            async with lock:
                result = await fetch_fn()
                self._cache[key] = (result, time.time())
                serialized = json.dumps(result)
                async with write_connection() as session:
                    await session.execute(text("""
                        INSERT INTO cached_stats (key, value, updated_at)
                        VALUES (:key, :value, NOW())
                        ON CONFLICT (key) DO UPDATE 
                        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                    """), {"key": key, "value": serialized})
        except Exception:
            from log import log_exception
            log_exception(f"Failed to refresh cached_stat {key} in background")

    async def clear_system_logs(self):
        from db import write_connection
        async with write_connection() as session:
            await session.execute(text("DELETE FROM system_logs"))

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
        return await self.db_cached("summary", self._fetch_summary)

    async def _fetch_summary(self):
        async def get_total():
            async with self.connection_factory() as session:
                return await self.fetch_scalar("SELECT COUNT(*) FROM logs", session=session)
        async def get_errors():
            async with self.connection_factory() as session:
                return await self.fetch_scalar("SELECT COUNT(*) FROM logs WHERE status >= 400", session=session)
        async def get_ips():
            async with self.connection_factory() as session:
                return await self.fetch_scalar("SELECT COUNT(DISTINCT ip) FROM logs", session=session)

        total_reqs, errors, unique_ips = await asyncio.gather(get_total(), get_errors(), get_ips())
        return {
            "total_requests": total_reqs,
            "unique_ips": unique_ips,
            "errors": errors,
        }

    async def get_top_ips(self, limit: int):
        return await self.db_cached(f"top_ips:{limit}", lambda: self._fetch_top_ips(limit))

    async def _fetch_top_ips(self, limit: int):
        rows = await self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            GROUP BY ip ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"ip": row["ip"], "count": row["count"]} for row in rows]

    async def get_top_urls(self, limit: int):
        return await self.db_cached(f"top_urls:{limit}", lambda: self._fetch_top_urls(limit))

    async def _fetch_top_urls(self, limit: int):
        rows = await self.fetch_rows("""
            SELECT path, COUNT(*) as count FROM logs
            GROUP BY path ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"path": row["path"], "count": row["count"]} for row in rows]

    async def get_status_codes(self):
        return await self.db_cached("status_codes", self._fetch_status_codes)

    async def _fetch_status_codes(self):
        rows = await self.fetch_rows("""
            SELECT status, COUNT(*) as count FROM logs
            GROUP BY status ORDER BY count DESC
        """)
        return [{"status": row["status"], "count": row["count"]} for row in rows]

    async def get_traffic(self, granularity: str, ip: str | None, limit: int, offset: int = 0):
        key = f"traffic:{granularity}:{ip or ''}:{limit}:{offset}"
        if ip:
            return await self._cached(key, lambda: self._fetch_traffic(granularity, ip, limit, offset))
        return await self.db_cached(key, lambda: self._fetch_traffic(granularity, ip, limit, offset))

    async def _fetch_traffic(self, granularity: str, ip: str | None, limit: int, offset: int):
        unit = DATE_TRUNC_UNITS[granularity]
        unit_delta = TIMEDELTA_UNITS[granularity]
        
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
                    date_trunc('{unit}', l.time) AS period_raw,
                    COUNT(*) AS count
                FROM logs l
                WHERE l.time >= :start_time
                  AND l.time <= :end_time
                  {where_clause_ip_and}
                GROUP BY 1
                ORDER BY 1
            """
            rows = await self.fetch_rows(query, params, session=session)

        # Format period string in Python for faster performance
        def format_period_py(dt: datetime, gran: str) -> str:
            if gran == "minute":
                return dt.strftime("%Y-%m-%dT%H:%M")
            elif gran == "hour":
                return dt.strftime("%Y-%m-%dT%H")
            else: # day
                return dt.strftime("%Y-%m-%d")

        result = []
        for row in rows:
            p_raw = row["period_raw"]
            if p_raw:
                p_str = format_period_py(p_raw, granularity)
                result.append({"period": p_str, "count": row["count"]})
        return result

    async def get_anomalies(self):
        return await self.db_cached("anomalies", self._fetch_anomalies)

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
        return await self.db_cached(
            f"status_over_time:{granularity}:{limit}:{offset}",
            lambda: self._fetch_status_codes_over_time(granularity, limit, offset),
        )

    async def _fetch_status_codes_over_time(self, granularity: str, limit: int, offset: int):
        unit = DATE_TRUNC_UNITS[granularity]
        unit_delta = TIMEDELTA_UNITS[granularity]
        
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
            SELECT
                date_trunc('{unit}', l.time) AS period_raw,
                l.status,
                COUNT(*) AS count
            FROM logs l
            WHERE l.time >= :start_time
              AND l.time < :end_time_exclusive
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        params = {
            "start_time": start_time,
            "end_time_exclusive": end_time_exclusive
        }
        async with self.connection_factory() as session:
            rows = await self.fetch_rows(query, params, session=session)

        # Map DB results by formatted period string and status
        db_data = {}
        all_statuses = set()
        for row in rows:
            p_raw = row["period_raw"]
            if p_raw:
                # Format to YYYY-MM-DD"T"HH24 to match the expected frontend format
                p_str = p_raw.strftime("%Y-%m-%dT%H")
                status = str(row["status"]) if row["status"] is not None else "Unknown"
                all_statuses.add(status)
                db_data.setdefault(p_str, {})[status] = row["count"]

        # Generate expected time periods list in Python
        labels = []
        for i in range(limit):
            p = start_time + i * unit_delta
            p_str = p.strftime("%Y-%m-%dT%H")
            labels.append(p_str)

        # Build datasets padded with 0 for missing intervals
        datasets = []
        for status in sorted(all_statuses):
            status_data = []
            for label in labels:
                status_data.append(db_data.get(label, {}).get(status, 0))
            datasets.append({
                "label": status,
                "data": status_data
            })
            
        return {"labels": labels, "datasets": datasets}

    async def search_logs(self, ip, path, status, time_from, time_to, limit, offset=0):
        is_unfiltered = not ip and not path and status is None and not time_from and not time_to

        if not time_from and not time_to:
            from datetime import datetime, timedelta, timezone
            time_from = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
            
        params: dict[str, object] = {"limit": limit, "offset": offset}
        
        if is_unfiltered:
            # Unfiltered query using the fast pg_class estimate with count fallback in a single roundtrip
            query = """
                WITH paginated_rows AS (
                    SELECT ip, time, method, path, status, size
                    FROM logs
                    LIMIT :limit OFFSET :offset
                ),
                estimated_count AS (
                    SELECT reltuples::bigint AS total FROM pg_class WHERE relname = 'logs'
                ),
                final_count AS (
                    SELECT CASE 
                        WHEN total <= 0 THEN (SELECT COUNT(*) FROM logs)
                        ELSE total
                    END AS total
                    FROM estimated_count
                )
                SELECT 
                    (SELECT total FROM final_count) AS total_count,
                    (SELECT coalesce(json_agg(r), '[]'::json) FROM paginated_rows r) AS rows
            """
            async with self.connection_factory() as session:
                result = await session.execute(text(query), params)
                row = result.mappings().one()
                total = row["total_count"]
                rows = row["rows"]
                if isinstance(rows, str):
                    import json
                    rows = json.loads(rows)
        else:
            where_clauses = []
            if ip:
                where_clauses.append("ip LIKE :ip")
                params["ip"] = f"{ip}%"
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

            # Filtered query using a single roundtrip with key lookup to avoid Seq Scan on logs
            query = f"""
                WITH filtered_ids AS NOT MATERIALIZED (
                    SELECT id 
                    FROM logs 
                    WHERE {where_sql}
                ),
                total_count AS (
                    SELECT COUNT(*) AS total FROM filtered_ids
                ),
                paginated_ids AS (
                    SELECT id 
                    FROM filtered_ids
                    LIMIT :limit OFFSET :offset
                )
                SELECT 
                    (SELECT total FROM total_count) AS total_count,
                    (
                        SELECT coalesce(json_agg(r), '[]'::json) 
                        FROM (
                            SELECT l.ip, l.time, l.method, l.path, l.status, l.size
                            FROM paginated_ids p
                            JOIN logs l ON l.id = p.id
                        ) r
                    ) AS rows
            """
            async with self.connection_factory() as session:
                result = await session.execute(text(query), params)
                row = result.mappings().one()
                total = row["total_count"]
                rows = row["rows"]
                if isinstance(rows, str):
                    import json
                    rows = json.loads(rows)

        return {
            "rows": [
                {
                    "ip": r.get("ip"),
                    "time": r.get("time").isoformat() if hasattr(r.get("time"), "isoformat") else r.get("time"),
                    "method": r.get("method"),
                    "path": r.get("path"),
                    "status": r.get("status"),
                    "size": r.get("size"),
                }
                for r in rows
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
