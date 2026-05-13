from typing import Sequence

from db import read_connection


class StatsService:
    def __init__(self, connection_factory=read_connection):
        self.connection_factory = connection_factory

    def fetch_scalar(self, query: str, params: Sequence = ()) -> int:
        with self.connection_factory() as conn:
            row = conn.execute(query, params).fetchone()
        return row[0] if row else 0

    def fetch_rows(self, query: str, params: Sequence = ()):
        with self.connection_factory() as conn:
            return conn.execute(query, params).fetchall()

    def get_summary(self):
        return {
            "total_requests": self.fetch_scalar("SELECT COUNT(*) FROM logs"),
            "unique_ips": self.fetch_scalar("SELECT COUNT(DISTINCT ip) FROM logs"),
            "errors": self.fetch_scalar("SELECT COUNT(*) FROM logs WHERE status >= 400"),
        }

    def get_top_ips(self, limit: int):
        rows = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            GROUP BY ip ORDER BY count DESC LIMIT ?
        """, (limit,))
        return [{"ip": row["ip"], "count": row["count"]} for row in rows]

    def get_top_urls(self, limit: int):
        rows = self.fetch_rows("""
            SELECT path, COUNT(*) as count FROM logs
            GROUP BY path ORDER BY count DESC LIMIT ?
        """, (limit,))
        return [{"path": row["path"], "count": row["count"]} for row in rows]

    def get_status_codes(self):
        rows = self.fetch_rows("""
            SELECT status, COUNT(*) as count FROM logs
            GROUP BY status ORDER BY count DESC
        """)
        return [{"status": row["status"], "count": row["count"]} for row in rows]

    def get_traffic(self, time_format: str, ip: str | None):
        if ip:
            rows = self.fetch_rows("""
                SELECT strftime(?, time) as period, COUNT(*) as count
                FROM logs WHERE ip LIKE ?
                GROUP BY period ORDER BY period
            """, (time_format, f"%{ip}%"))
        else:
            rows = self.fetch_rows("""
                SELECT strftime(?, time) as period, COUNT(*) as count
                FROM logs GROUP BY period ORDER BY period
            """, (time_format,))
        return [{"period": row["period"], "count": row["count"]} for row in rows]

    def get_anomalies(self):
        high_freq = self.fetch_rows("""
            SELECT ip, strftime('%Y-%m-%dT%H:%M', time) as minute, COUNT(*) as count
            FROM logs GROUP BY ip, minute
            HAVING count > 100
            ORDER BY count DESC
        """)
        many_404 = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            WHERE status = 404
            GROUP BY ip HAVING count > 20
            ORDER BY count DESC
        """)
        many_500 = self.fetch_rows("""
            SELECT ip, COUNT(*) as count FROM logs
            WHERE status = 500
            GROUP BY ip HAVING count > 10
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
        params = []

        if ip:
            query += " AND ip LIKE ?"
            params.append(f"%{ip}%")
        if path:
            query += " AND path LIKE ?"
            params.append(f"%{path}%")
        if status is not None:
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
        rows = self.fetch_rows(query, tuple(params))
        return [
            {
                "ip": row["ip"],
                "time": row["time"],
                "method": row["method"],
                "path": row["path"],
                "status": row["status"],
                "size": row["size"],
            }
            for row in rows
        ]

    def get_status_codes_over_time(self, time_format: str):
        rows = self.fetch_rows("""
            SELECT
                strftime(?, time) as period,
                status,
                COUNT(*) as count
            FROM logs
            GROUP BY period, status
            ORDER BY period
        """, (time_format,))

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
