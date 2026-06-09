import os
import asyncio
import random
from datetime import datetime, timedelta, timezone
from log_parse import LogEntry
from log import log_activity, log_exception

# Pre-defined mock data to generate realistic logs
MOCK_IPS = [
    "8.8.8.8", "1.1.1.1", "113.161.4.5", "27.72.88.99", "14.226.11.22",
    "116.109.112.5", "127.0.0.1", "192.168.1.100", "52.221.43.12",
    "13.250.12.34", "18.136.21.99", "172.217.24.14", "142.250.66.46"
]
MOCK_METHODS = ["GET", "POST", "GET", "GET", "PUT", "DELETE"]
MOCK_PATHS = [
    "/", "/api/v1/users", "/login", "/dashboard", "/products", 
    "/static/css/style.css", "/static/js/main.js", "/search", 
    "/api/v1/checkout", "/cart", "/images/hero.jpg", "/about"
]
MOCK_STATUSES = [200, 200, 200, 201, 302, 404, 401, 500]
MOCK_REFERERS = ["-", "https://google.com", "https://github.com", "https://facebook.com"]
MOCK_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
]

class LogSimulator:
    def __init__(self):
        self.enabled = os.getenv("SIMULATION_ENABLED", "0") == "1"
        self.interval = int(os.getenv("SIMULATION_INTERVAL", "3"))
        self.batch_min = int(os.getenv("SIMULATION_BATCH_MIN", "80"))
        self.batch_max = int(os.getenv("SIMULATION_BATCH_MAX", "220"))
        self._task = None
        self._burst_chance = 0.04  # 4% chance of burst/attack
        self._burst_remaining = 0

    def start(self):
        if self._task is None or self._task.done():
            self.enabled = True
            self._task = asyncio.create_task(self._run_loop())
            log_activity(f"Log simulator started with interval={self.interval}s")

    def stop(self):
        if self._task is not None and not self._task.done():
            self.enabled = False
            self._task.cancel()
            log_activity("Log simulator stopped")
        self._task = None

    def get_status(self) -> dict:
        return {
            "enabled": self.enabled and self._task is not None and not self._task.done(),
            "interval": self.interval,
            "batch_min": self.batch_min,
            "batch_max": self.batch_max,
        }

    async def _run_loop(self):
        while self.enabled:
            try:
                await self.tick()
            except asyncio.CancelledError:
                break
            except Exception:
                log_exception("Unexpected error in simulation tick")
            await asyncio.sleep(self.interval)

    def _choose_batch_size(self) -> int:
        # Determine batch size: ordinary traffic or burst attack
        if self._burst_remaining > 0:
            batch_size = random.randint(
                max(self.batch_max, 200),
                max(self.batch_max * 2, 400),
            )
            self._burst_remaining -= 1
        else:
            if random.random() < self._burst_chance:
                # Trigger a new burst
                self._burst_remaining = random.randint(3, 8)
                batch_size = random.randint(
                    max(self.batch_max, 200),
                    max(self.batch_max * 2, 400),
                )
                log_activity(f"DDoS/Burst attack simulation triggered for {self._burst_remaining} ticks!")
            else:
                batch_size = random.randint(self.batch_min, self.batch_max)
        return batch_size

    def generate_entries(self, batch_size: int | None = None, spread_seconds: int = 59) -> list[LogEntry]:
        batch_size = batch_size or self._choose_batch_size()
        entries = []
        now = datetime.now(timezone.utc)

        # If it's a burst, we focus it on specific IPs/paths to trigger anomalies
        burst_ip = "182.21.45.99" if self._burst_remaining > 0 else None
        burst_path = "/login" if self._burst_remaining > 0 and random.random() < 0.7 else None
        burst_status = 404 if self._burst_remaining > 0 and random.random() < 0.4 else None

        for i in range(batch_size):
            ip = burst_ip or random.choice(MOCK_IPS)
            path = burst_path or random.choice(MOCK_PATHS)
            status = burst_status or random.choice(MOCK_STATUSES)

            # Spread entries across the last minute and give each row a distinct
            # microsecond so cron retries do not collapse into duplicate rows.
            event_time = now - timedelta(
                seconds=random.randint(0, max(spread_seconds, 0)),
                microseconds=i,
            )

            entry = LogEntry(
                ip=ip,
                time=event_time,
                method=random.choice(MOCK_METHODS),
                path=path,
                status=status,
                size=random.randint(100, 15000) if status == 200 else 0,
                referer=random.choice(MOCK_REFERERS),
                user_agent=random.choice(MOCK_USER_AGENTS)
            )
            entries.append(entry)
        return entries

    async def tick(
        self,
        batch_size: int | None = None,
        broadcast: bool = False,
        spread_seconds: int = 59,
    ) -> dict:
        entries = self.generate_entries(batch_size, spread_seconds)

        from db import insert_many
        inserted = await insert_many(entries)

        from routes import stats_service
        await stats_service.clear_realtime_cache()

        payload = {
            "generated": len(entries),
            "inserted": inserted,
            "sample": [self._entry_to_payload(e) for e in entries[:10]],
        }

        if broadcast:
            from ws_manager import manager as ws_manager
            await ws_manager.broadcast({
                "type": "new_logs",
                "count": inserted,
                "logs": [self._entry_to_payload(e) for e in entries],
            })

        return payload

    @staticmethod
    def _entry_to_payload(entry: LogEntry) -> dict:
        return {
            "ip": entry.ip,
            "time": entry.time.isoformat(),
            "method": entry.method,
            "path": entry.path,
            "status": entry.status,
            "size": entry.size,
            "country_code": "pending",
            "country_name": "Loading...",
            "isp": "Loading...",
        }

simulator = LogSimulator()
