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
        self._cache_ttl = 300
        self._locks = {}

    async def _execute_with_retry(self, operation_fn, is_write=False):
        import asyncio
        from sqlalchemy.exc import DBAPIError, OperationalError
        from log import log_exception

        max_retries = 3
        delay = 0.5
        for attempt in range(max_retries):
            try:
                return await operation_fn()
            except (DBAPIError, OperationalError, OSError, ConnectionError) as e:
                log_exception(
                    f"Database {'write' if is_write else 'read'} transient error "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(delay * (2 ** attempt))

    async def _write_with_retry(self, write_fn):
        return await self._execute_with_retry(write_fn, is_write=True)

    async def _cached(self, key, fn):
        now = time.time()
        cached = self._cache.get(key)
        if cached:
            value, ts = cached
            if now - ts < self._cache_ttl:
                return value

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

    async def db_cached(self, key, fetch_fn, ttl=300, force_refresh=False):
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
            row = await self.fetch_rows("SELECT value, updated_at FROM cached_stats WHERE key = :key", {"key": key})
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

        if db_val is not None and not force_refresh:
            parsed_val = json.loads(db_val)
            self._cache[key] = (parsed_val, now)
            if db_age >= ttl:
                asyncio.create_task(self._refresh_stat_in_db(key, fetch_fn))
            return parsed_val

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
                async def _write_op():
                    async with write_connection() as session:
                        await session.execute(text("""
                            INSERT INTO cached_stats (key, value, updated_at)
                            VALUES (:key, :value, NOW())
                            ON CONFLICT (key) DO UPDATE 
                            SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                        """), {"key": key, "value": serialized})
                await self._write_with_retry(_write_op)
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
                async def _write_op():
                    async with write_connection() as session:
                        await session.execute(text("""
                            INSERT INTO cached_stats (key, value, updated_at)
                            VALUES (:key, :value, NOW())
                            ON CONFLICT (key) DO UPDATE 
                            SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                        """), {"key": key, "value": serialized})
                await self._write_with_retry(_write_op)
        except Exception:
            from log import log_exception
            log_exception(f"Failed to refresh cached_stat {key} in background")

    async def _precompute_granularities(self):
        import json
        from db import write_connection

        async def _store(cache_key, result):
            self._cache[cache_key] = (result, time.time())
            serialized = json.dumps(result)
            async def _write_op():
                async with write_connection() as session:
                    await session.execute(text("""
                        INSERT INTO cached_stats (key, value, updated_at)
                        VALUES (:key, :value, NOW())
                        ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                    """), {"key": cache_key, "value": serialized})
            await self._write_with_retry(_write_op)

        try:
            for gran in ["hour", "day", "minute"]:
                key = f"status_over_time:{gran}:48:0"
                if key not in self._cache:
                    result = await self._fetch_status_codes_over_time(gran, 48, 0)
                    await _store(key, result)

                key = f"traffic:{gran}::48:0"
                if key not in self._cache:
                    result = await self._fetch_traffic(gran, None, 48, 0)
                    await _store(key, result)

            search_key = "search:default:15"
            if search_key not in self._cache:
                result = await self._search_logs_raw(None, None, None, None, None, 15, 0)
                await _store(search_key, result)
        except Exception:
            from log import log_exception
            log_exception("Failed to precompute granularity caches")

    async def clear_system_logs(self):
        from db import write_connection
        async def _write_op():
            async with write_connection() as session:
                await session.execute(text("DELETE FROM system_logs"))
        await self._write_with_retry(_write_op)

    async def fetch_scalar(self, query: str, params: dict | None = None, session=None) -> int:
        async def _op():
            if session:
                result = await session.execute(text(query), params or {})
                value = result.scalar()
                return int(value or 0)
            async with self.connection_factory() as session_ctx:
                result = await session_ctx.execute(text(query), params or {})
                value = result.scalar()
            return int(value or 0)
        return await self._execute_with_retry(_op)

    async def fetch_val(self, query: str, params: dict | None = None, session=None):
        async def _op():
            if session:
                result = await session.execute(text(query), params or {})
                return result.scalar()
            async with self.connection_factory() as session_ctx:
                result = await session_ctx.execute(text(query), params or {})
                return result.scalar()
        return await self._execute_with_retry(_op)

    async def fetch_rows(self, query: str, params: dict | None = None, session=None):
        async def _op():
            if session:
                result = await session.execute(text(query), params or {})
                return result.mappings().all()
            async with self.connection_factory() as session_ctx:
                result = await session_ctx.execute(text(query), params or {})
                return result.mappings().all()
        return await self._execute_with_retry(_op)

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
        query = """
            WITH unique_ips AS (
                SELECT ip FROM logs GROUP BY ip
            ),
            counts AS (
                SELECT 
                    COUNT(*) AS total_requests,
                    COUNT(*) FILTER (WHERE status >= 400) AS errors
                FROM logs
            )
            SELECT 
                c.total_requests,
                c.errors,
                (SELECT COUNT(*) FROM unique_ips) AS unique_ips
            FROM counts c
        """
        rows = await self.fetch_rows(query)
        row = rows[0]
        return {
            "total_requests": row["total_requests"],
            "unique_ips": row["unique_ips"],
            "errors": row["errors"],
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

        if ip:
            max_time_query = "SELECT time FROM logs WHERE ip = :ip ORDER BY time DESC LIMIT 1"
            max_time = await self.fetch_val(max_time_query, {"ip": ip})
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
        rows = await self.fetch_rows(query, params)

        def format_period_py(dt: datetime, gran: str) -> str:
            if gran == "minute":
                return dt.strftime("%Y-%m-%dT%H:%M")
            elif gran == "hour":
                return dt.strftime("%Y-%m-%dT%H")
            else:
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

        query = """
            WITH base_data AS (
                SELECT ip, status, date_trunc('minute', time) as minute
                FROM logs
                WHERE time >= :time_threshold
            ),
            high_freq AS (
                SELECT ip, minute, COUNT(*) as count
                FROM base_data
                GROUP BY ip, minute
                HAVING COUNT(*) > 100
            ),
            many_404 AS (
                SELECT ip, COUNT(*) as count
                FROM base_data
                WHERE status = 404
                GROUP BY ip
                HAVING COUNT(*) > 20
            ),
            many_500 AS (
                SELECT ip, COUNT(*) as count
                FROM base_data
                WHERE status = 500
                GROUP BY ip
                HAVING COUNT(*) > 10
            )
            SELECT 
                (SELECT coalesce(json_agg(h), '[]'::json) FROM (SELECT ip, to_char(minute, 'YYYY-MM-DD"T"HH24:MI') as minute, count FROM high_freq ORDER BY count DESC) h) AS high_frequency,
                (SELECT coalesce(json_agg(c4), '[]'::json) FROM (SELECT ip, count FROM many_404 ORDER BY count DESC) c4) AS many_404s,
                (SELECT coalesce(json_agg(c5), '[]'::json) FROM (SELECT ip, count FROM many_500 ORDER BY count DESC) c5) AS many_500s
        """
        import json
        rows = await self.fetch_rows(query, params)
        row = rows[0]
        high_freq = row["high_frequency"]
        many_404 = row["many_404s"]
        many_500 = row["many_500s"]

        if isinstance(high_freq, str):
            high_freq = json.loads(high_freq)
        if isinstance(many_404, str):
            many_404 = json.loads(many_404)
        if isinstance(many_500, str):
            many_500 = json.loads(many_500)

        return {
            "high_frequency": high_freq,
            "many_404s": many_404,
            "many_500s": many_500,
        }

    async def get_dashboard_data(self):
        return await self.db_cached("dashboard", self._fetch_dashboard_data)

    async def _fetch_dashboard_data(self):
        query = """
            WITH 
            max_time_cte AS (
                SELECT COALESCE(MAX(time), NOW()) AS val FROM logs
            ),
            params AS (
                SELECT 
                    val AS max_time,
                    val - interval '47 hours' AS start_time,
                    val AS end_time,
                    val + interval '1 hour' AS end_time_exclusive,
                    val - interval '24 hours' AS time_threshold
                FROM max_time_cte
            ),
            summary_cte AS (
                WITH unique_ips AS (
                    SELECT ip FROM logs GROUP BY ip
                ),
                counts AS (
                    SELECT 
                        COUNT(*) AS total_requests,
                        COUNT(*) FILTER (WHERE status >= 400) AS errors
                    FROM logs
                )
                SELECT 
                    c.total_requests,
                    c.errors,
                    (SELECT COUNT(*) FROM unique_ips) AS unique_ips
                FROM counts c
            ),
            top_ips_cte AS (
                SELECT json_agg(h) AS val FROM (
                    SELECT ip, COUNT(*) as count FROM logs
                    GROUP BY ip ORDER BY count DESC LIMIT 8
                ) h
            ),
            top_urls_cte AS (
                SELECT json_agg(h) AS val FROM (
                    SELECT path, COUNT(*) as count FROM logs
                    GROUP BY path ORDER BY count DESC LIMIT 8
                ) h
            ),
            status_codes_cte AS (
                SELECT json_agg(h) AS val FROM (
                    SELECT status, COUNT(*) as count FROM logs
                    GROUP BY status ORDER BY count DESC
                ) h
            ),
            traffic_cte AS (
                SELECT json_agg(h) AS val FROM (
                    SELECT
                        to_char(date_trunc('hour', l.time), 'YYYY-MM-DD"T"HH24') AS period,
                        COUNT(*) AS count
                    FROM logs l, params p
                    WHERE l.time >= p.start_time
                      AND l.time <= p.end_time
                    GROUP BY date_trunc('hour', l.time)
                    ORDER BY date_trunc('hour', l.time)
                ) h
            ),
            status_over_time_cte AS (
                SELECT json_agg(h) AS val FROM (
                    SELECT
                        date_trunc('hour', l.time) AS period_raw,
                        l.status,
                        COUNT(*) AS count
                    FROM logs l, params p
                    WHERE l.time >= p.start_time
                      AND l.time < p.end_time_exclusive
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                ) h
            ),
            anomalies_cte AS (
                WITH base_data AS (
                    SELECT ip, status, date_trunc('minute', time) as minute
                    FROM logs, params p
                    WHERE time >= p.time_threshold
                ),
                high_freq AS (
                    SELECT ip, minute, COUNT(*) as count
                    FROM base_data
                    GROUP BY ip, minute
                    HAVING COUNT(*) > 100
                ),
                many_404 AS (
                    SELECT ip, COUNT(*) as count
                    FROM base_data
                    WHERE status = 404
                    GROUP BY ip
                    HAVING COUNT(*) > 20
                ),
                many_500 AS (
                    SELECT ip, COUNT(*) as count
                    FROM base_data
                    WHERE status = 500
                    GROUP BY ip
                    HAVING COUNT(*) > 10
                )
                SELECT 
                    (SELECT coalesce(json_agg(h), '[]'::json) FROM (SELECT ip, to_char(minute, 'YYYY-MM-DD"T"HH24:MI') as minute, count FROM high_freq ORDER BY count DESC) h) AS high_frequency,
                    (SELECT coalesce(json_agg(c4), '[]'::json) FROM (SELECT ip, count FROM many_404 ORDER BY count DESC) c4) AS many_404s,
                    (SELECT coalesce(json_agg(c5), '[]'::json) FROM (SELECT ip, count FROM many_500 ORDER BY count DESC) c5) AS many_500s
            )
            SELECT json_build_object(
                'max_time', (SELECT val FROM max_time_cte),
                'summary', (SELECT row_to_json(s) FROM summary_cte s),
                'top_ips', (SELECT coalesce(val, '[]'::json) FROM top_ips_cte),
                'top_urls', (SELECT coalesce(val, '[]'::json) FROM top_urls_cte),
                'status_codes', (SELECT coalesce(val, '[]'::json) FROM status_codes_cte),
                'traffic', (SELECT coalesce(val, '[]'::json) FROM traffic_cte),
                'status_over_time', (SELECT coalesce(val, '[]'::json) FROM status_over_time_cte),
                'anomalies', (SELECT row_to_json(a) FROM anomalies_cte a)
            ) AS result;
        """
        raw_result = await self.fetch_val(query)

        max_time_str = raw_result.get("max_time")
        if max_time_str:
            max_time = datetime.fromisoformat(max_time_str)
        else:
            max_time = datetime.now(timezone.utc)

        end_period = max_time.replace(minute=0, second=0, microsecond=0)
        start_time = end_period - 47 * timedelta(hours=1)

        labels = []
        for i in range(48):
            p = start_time + i * timedelta(hours=1)
            labels.append(p.strftime("%Y-%m-%dT%H"))

        db_data = {}
        all_statuses = set()
        for row in raw_result.get("status_over_time", []) or []:
            p_raw_str = row.get("period_raw")
            if p_raw_str:
                p_dt = datetime.fromisoformat(p_raw_str)
                p_str = p_dt.strftime("%Y-%m-%dT%H")
                status = str(row.get("status")) if row.get("status") is not None else "Unknown"
                all_statuses.add(status)
                db_data.setdefault(p_str, {})[status] = row.get("count", 0)

        datasets = []
        for status in sorted(all_statuses):
            status_data = []
            for label in labels:
                status_data.append(db_data.get(label, {}).get(status, 0))
            datasets.append({
                "label": status,
                "data": status_data
            })

        return {
            "summary": raw_result.get("summary"),
            "top_ips": raw_result.get("top_ips"),
            "top_urls": raw_result.get("top_urls"),
            "status_codes": raw_result.get("status_codes"),
            "traffic": raw_result.get("traffic"),
            "status_over_time": {"labels": labels, "datasets": datasets},
            "anomalies": raw_result.get("anomalies"),
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
        else:
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
        rows = await self.fetch_rows(query, params)

        db_data = {}
        all_statuses = set()
        for row in rows:
            p_raw = row["period_raw"]
            if p_raw:
                p_str = p_raw.strftime("%Y-%m-%dT%H")
                status = str(row["status"]) if row["status"] is not None else "Unknown"
                all_statuses.add(status)
                db_data.setdefault(p_str, {})[status] = row["count"]

        labels = []
        for i in range(limit):
            p = start_time + i * unit_delta
            p_str = p.strftime("%Y-%m-%dT%H")
            labels.append(p_str)

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

        if is_unfiltered and offset == 0:
            return await self.db_cached(
                f"search:default:{limit}",
                lambda: self._search_logs_raw(ip, path, status, time_from, time_to, limit, offset),
            )
        return await self._search_logs_raw(ip, path, status, time_from, time_to, limit, offset)

    async def _run_page_query(self, query: str, params: dict) -> list:
        return await self.fetch_rows(query, params)

    async def _search_logs_raw(self, ip, path, status, time_from, time_to, limit, offset=0):
        is_unfiltered = not ip and not path and status is None and not time_from and not time_to

        params: dict[str, object] = {"limit": limit, "offset": offset}

        if is_unfiltered:
            page_query = """
                SELECT l.id, l.ip, l.time, l.method, l.path, l.status, l.size
                FROM logs l
                JOIN (
                    SELECT id FROM logs ORDER BY id DESC LIMIT :limit OFFSET :offset
                ) temp ON l.id = temp.id
                ORDER BY l.id DESC
            """

            async def _fetch_total():
                return await self.fetch_scalar("SELECT COUNT(*) FROM logs")

            total, rows_result = await asyncio.gather(
                self.db_cached("search_count_unfiltered", _fetch_total, ttl=60),
                self._run_page_query(page_query, params),
            )
            rows = rows_result
        else:
            where_clauses = []

            # IP search: dùng trigram index — hỗ trợ LIKE '%x%' trên 7M rows
            if ip:
                where_clauses.append("ip LIKE :ip")
                params["ip"] = f"%{ip}%"

            # Path search: dùng trigram index — hỗ trợ LIKE '%x%' trên 7M rows
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

            query = f"""
                SELECT l.id, l.ip, l.time, l.method, l.path, l.status, l.size
                FROM logs l
                JOIN (
                    SELECT id
                    FROM logs
                    WHERE {where_sql}
                    ORDER BY id DESC
                    LIMIT :limit OFFSET :offset
                ) temp ON l.id = temp.id
                ORDER BY l.id DESC
            """
            rows = await self.fetch_rows(query, params)
            total = None

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

    async def search_logs_count(self, ip, path, status, time_from, time_to):
        is_unfiltered = not ip and not path and status is None and not time_from and not time_to
        if is_unfiltered:
            async def _count_all():
                return await self.fetch_scalar("SELECT COUNT(*) FROM logs")
            return await self.db_cached("search_count_unfiltered", _count_all, ttl=60)

        cache_key = f"search_count:{ip or ''}:{path or ''}:{status or ''}:{time_from or ''}:{time_to or ''}"
        return await self.db_cached(
            cache_key,
            lambda: self._fetch_search_count_raw(ip, path, status, time_from, time_to),
            ttl=120
        )

    async def _fetch_search_count_raw(self, ip, path, status, time_from, time_to):
        params = {}
        where_clauses = []

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
        query = f"SELECT COUNT(*) FROM logs WHERE {where_sql}"

        return await self.fetch_scalar(query, params)

    async def search_logs_keyset(
        self,
        ip: str | None,
        path: str | None,
        status: int | None,
        time_from: str | None,
        time_to: str | None,
        limit: int,
        cursor: str | None = None,
        cursor_id: int | None = None,
    ):
        params: dict[str, object] = {"limit": limit}
        where_clauses = []

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

        if cursor_id is not None:
            where_clauses.append("id < :cursor_id")
            params["cursor_id"] = cursor_id

        where_sql = " AND ".join(where_clauses) or "1=1"

        query = f"""
            SELECT l.id, l.ip, l.time, l.method, l.path, l.status, l.size
            FROM logs l
            JOIN (
                SELECT id
                FROM logs
                WHERE {where_sql}
                ORDER BY id DESC
                LIMIT :limit
            ) temp ON l.id = temp.id
            ORDER BY l.id DESC
        """

        rows = await self.fetch_rows(query, params)

        if not rows:
            return {"rows": [], "next_cursor": None, "next_cursor_id": None, "has_more": False}

        last = rows[-1]
        next_cursor = last["time"].isoformat() if last["time"] else None
        next_cursor_id = last["id"]

        return {
            "rows": [
                {
                    "ip": r["ip"],
                    "time": r["time"].isoformat() if r["time"] else None,
                    "method": r["method"],
                    "path": r["path"],
                    "status": r["status"],
                    "size": r["size"],
                }
                for r in rows
            ],
            "next_cursor": next_cursor,
            "next_cursor_id": next_cursor_id,
            "has_more": len(rows) == limit,
        }

    async def jump_to_page(
        self,
        page: int,
        page_size: int,
        ip: str | None = None,
        path: str | None = None,
        status: int | None = None,
        time_from: str | None = None,
        time_to: str | None = None,
    ):
        offset = page * page_size

        if not ip and not path and status is None and not time_from and not time_to:
            async def _fetch_total():
                return await self.fetch_scalar("SELECT COUNT(*) FROM logs")

            total_count = await self.db_cached("search_count_unfiltered", _fetch_total, ttl=60)

            if offset >= total_count:
                return {"rows": [], "next_cursor": None, "next_cursor_id": None, "has_more": False}

            if offset > total_count / 2:
                reverse_offset = total_count - offset - 1
                if reverse_offset < 0:
                    reverse_offset = 0
                query = """
                    SELECT id FROM logs
                    ORDER BY id ASC
                    LIMIT 1 OFFSET :reverse_offset
                """
                params = {"reverse_offset": reverse_offset}
            else:
                query = """
                    SELECT id FROM logs
                    ORDER BY id DESC
                    LIMIT 1 OFFSET :offset
                """
                params = {"offset": offset}

            target_id = await self.fetch_val(query, params)
            if target_id is None:
                return {"rows": [], "next_cursor": None, "next_cursor_id": None, "has_more": False}

            return await self.search_logs_keyset(
                ip=None, path=None, status=None,
                time_from=None, time_to=None,
                limit=page_size,
                cursor_id=target_id + 1,
            )

        MAX_FILTERED_OFFSET = 10_000
        if offset > MAX_FILTERED_OFFSET:
            raise ValueError(
                f"Cannot jump past page {MAX_FILTERED_OFFSET // page_size} "
                f"with active filters. Remove filters to jump further."
            )

        return await self._search_logs_raw(
            ip, path, status, time_from, time_to, page_size, offset
        )

    def _period_expr(self, granularity: str, table_alias: str | None = None) -> str:
        unit = DATE_TRUNC_UNITS[granularity]
        fmt = TO_CHAR_FORMATS[granularity]
        time_column = f"{table_alias}.time" if table_alias else "time"
        return f"to_char(date_trunc('{unit}', {time_column}), '{fmt}')"

    def _parse_datetime(self, value: str) -> datetime:
        if len(value) > 6 and value[-6] == ' ' and value[-3] == ':':
            value = value[:-6] + '+' + value[-5:]
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