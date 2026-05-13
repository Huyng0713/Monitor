from datetime import datetime

from sqlalchemy import text

from db import read_connection

DATE_TRUNC_UNITS = {
    "minute": "minute",
    "hour": "hour",
    "day": "day",
}
TO_CHAR_FORMATS = {
    "minute": "YYYY-MM-DD\"T\"HH24:MI",
    "hour": "YYYY-MM-DD\"T\"HH24",
    "day": "YYYY-MM-DD",
}


class StatsService:
    def __init__(self, connection_factory=read_connection):
        self.connection_factory = connection_factory

    def fetch_scalar(self, query: str, params: dict | None = None) -> int:
        with self.connection_factory() as session:
            result = session.execute(text(query), params or {}).scalar()
        return int(result or 0)

    def fetch_rows(self, query: str, params: dict | None = None):
        with self.connection_factory() as session:
            return session.execute(text(query), params or {}).mappings().all()

    def get_summary(self):
        return {
            "total_requests": self.fetch_scalar("SELECT COUNT(*) FROM logs"),
            "unique_ips": self.fetch_scalar("SELECT COUNT(DISTINCT ip) FROM logs"),
            "errors": self.fetch_scalar("SELECT COUNT(*) FROM logs WHERE status >= 400"),
        }

    def get_top_ips(self, limit: int):
        rows = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            GROUP BY ip ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"ip": row["ip"], "count": row["count"]} for row in rows]

    def get_top_urls(self, limit: int):
        rows = self.fetch_rows("""
            SELECT path, COUNT(*) as count FROM logs
            GROUP BY path ORDER BY count DESC LIMIT :limit
        """, {"limit": limit})
        return [{"path": row["path"], "count": row["count"]} for row in rows]

    def get_status_codes(self):
        rows = self.fetch_rows("""
            SELECT status, COUNT(*) as count FROM logs
            GROUP BY status ORDER BY count DESC
        """)
        return [{"status": row["status"], "count": row["count"]} for row in rows]

    def get_traffic(self, granularity: str, ip: str | None):
        period_expr = self._period_expr(granularity)
        params: dict[str, object] = {}

        query = f"""
            SELECT {period_expr} as period, COUNT(*) as count
            FROM logs
        """
        if ip:
            query += " WHERE ip LIKE :ip"
            params["ip"] = f"%{ip}%"

        query += " GROUP BY period ORDER BY period"
        rows = self.fetch_rows(query, params)
        return [{"period": row["period"], "count": row["count"]} for row in rows]

    def get_anomalies(self):
        high_freq = self.fetch_rows("""
            SELECT ip, to_char(date_trunc('minute', time), 'YYYY-MM-DD"T"HH24:MI') as minute, COUNT(*) as count
            FROM logs GROUP BY ip, minute
            HAVING COUNT(*) > 100
            ORDER BY count DESC
        """)
        many_404 = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            WHERE status = 404
            GROUP BY ip HAVING COUNT(*) > 20
            ORDER BY count DESC
        """)
        many_500 = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            WHERE status = 500
            GROUP BY ip HAVING COUNT(*) > 10
            ORDER BY count DESC
        """)
        return {
            "high_frequency": [{"ip": row["ip"], "minute": row["minute"], "count": row["count"]} for row in high_freq],
            "many_404s": [{"ip": row["ip"], "count": row["count"]} for row in many_404],
            "many_500s": [{"ip": row["ip"], "count": row["count"]} for row in many_500],
        }

    def search_logs(
        self,
        ip: str | None,
        path: str | None,
        status: int | None,
        time_from: str | None,
        time_to: str | None,
        limit: int,
    ):
        query = "SELECT ip, time, method, path, status, size FROM logs WHERE 1=1"
        params: dict[str, object] = {"limit": limit}

        if ip:
            query += " AND ip LIKE :ip"
            params["ip"] = f"%{ip}%"
        if path:
            query += " AND path LIKE :path"
            params["path"] = f"%{path}%"
        if status is not None:
            query += " AND status = :status"
            params["status"] = status
        if time_from:
            query += " AND time >= :time_from"
            params["time_from"] = self._parse_datetime(time_from)
        if time_to:
            query += " AND time <= :time_to"
            params["time_to"] = self._parse_datetime(time_to)

        query += " ORDER BY time DESC LIMIT :limit"
        rows = self.fetch_rows(query, params)
        return [
            {
                "ip": row["ip"],
                "time": row["time"].isoformat() if hasattr(row["time"], "isoformat") else row["time"],
                "method": row["method"],
                "path": row["path"],
                "status": row["status"],
                "size": row["size"],
            }
            for row in rows
        ]

    def get_status_codes_over_time(self, granularity: str):
        period_expr = self._period_expr(granularity)
        rows = self.fetch_rows(f"""
            SELECT
                {period_expr} as period,
                status,
                COUNT(*) as count
            FROM logs
            GROUP BY period, status
            ORDER BY period
        """)

        grouped = {}
        all_statuses = set()
        for row in rows:
            period = row["period"]
            status = str(row["status"])
            all_statuses.add(status)
            grouped.setdefault(period, {})[status] = row["count"]

        labels = sorted(grouped.keys())
        datasets = [
            {"label": status, "data": [grouped[label].get(status, 0) for label in labels]}
            for status in sorted(all_statuses)
        ]
        return {"labels": labels, "datasets": datasets}

    def _period_expr(self, granularity: str) -> str:
        unit = DATE_TRUNC_UNITS[granularity]
        fmt = TO_CHAR_FORMATS[granularity]
        return f"to_char(date_trunc('{unit}', time), '{fmt}')"

    def _parse_datetime(self, value: str) -> datetime:
        return datetime.fromisoformat(value)
