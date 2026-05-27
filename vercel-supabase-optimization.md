# Tối ưu hóa cho Vercel + Supabase

> Port 6543 (transaction pooler) đã đúng. Áp dụng các thay đổi theo thứ tự ưu tiên dưới đây.

---

## 1. `db.py` — Thêm `command_timeout` và `ssl`

**Vấn đề:** Không có timeout cho kết nối DB, có thể treo function đến hết giới hạn Vercel (10s Hobby / 60s Pro). Supabase yêu cầu SSL.

```python
# db.py — thay thế toàn bộ hàm _engine_options()

def _engine_options() -> dict:
    from uuid import uuid4
    options = {
        "pool_pre_ping": False,
        "pool_recycle": 300,
        "connect_args": {
            "statement_cache_size": 0,
            "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
            "command_timeout": 8,   # <-- thêm: hủy query sau 8s
            "ssl": "require",       # <-- thêm: Supabase bắt buộc SSL
        },
    }
    if IS_VERCEL:
        options.update({"poolclass": NullPool})
    else:
        options.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
    return options
```

---

## 2. `db.py` — Sửa lỗi `engine` không tồn tại trong `prune_logs.py`

**Vấn đề:** `prune_logs.py` import `engine` nhưng `db.py` chỉ export `read_engine` và `write_engine` → crash khi chạy script.

```python
# prune_logs.py — dòng đầu, sửa import
# ❌ Cũ
from db import init_db, write_connection, engine

# ✅ Mới
from db import init_db, write_connection, write_engine as engine
```

---

## 3. `stats_service.py` — Tăng TTL cache và thêm `statement_timeout`

**Vấn đề:** TTL 120s quá ngắn cho serverless (mỗi cold start phải đợi DB). Query dashboard nặng có thể chạy quá 8s trên Hobby plan.

### 3a. Tăng TTL cho các endpoint quan trọng

```python
# stats_service.py — sửa các hàm get_* sau

async def get_summary(self):
    return await self.db_cached("summary", self._fetch_summary, ttl=300)          # 120 -> 300

async def get_top_ips(self, limit: int):
    return await self.db_cached(f"top_ips:{limit}", lambda: self._fetch_top_ips(limit), ttl=300)

async def get_top_urls(self, limit: int):
    return await self.db_cached(f"top_urls:{limit}", lambda: self._fetch_top_urls(limit), ttl=300)

async def get_status_codes(self):
    return await self.db_cached("status_codes", self._fetch_status_codes, ttl=300)

async def get_anomalies(self):
    return await self.db_cached("anomalies", self._fetch_anomalies, ttl=300)

async def get_dashboard_data(self):
    return await self.db_cached("dashboard", self._fetch_dashboard_data, ttl=300)
```

### 3b. Thêm `statement_timeout` cho dashboard query

```python
# stats_service.py — đầu hàm _fetch_dashboard_data(), thêm 1 dòng

async def _fetch_dashboard_data(self):
    query = """..."""  # giữ nguyên query hiện tại
    async with self.connection_factory() as session:
        await session.execute(text("SET LOCAL statement_timeout = '8000'"))  # <-- thêm dòng này
        res = await session.execute(text(query))
        raw_result = res.scalar()
    # ... phần còn lại giữ nguyên
```

---

## 4. `routes.py` — Thay `asyncio.create_task` bằng `BackgroundTasks`

**Vấn đề:** `asyncio.create_task()` có thể bị Vercel terminate ngay sau khi response trả về, trước khi task chạy xong. `BackgroundTasks` của FastAPI/Starlette được đảm bảo chạy hết trước khi đóng connection.

```python
# routes.py — thêm import
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Request

# Sửa endpoint /stats/dashboard
@app.get("/stats/dashboard")
async def get_dashboard(background_tasks: BackgroundTasks):
    data = await stats_service.get_dashboard_data()
    # Precompute chạy sau khi response đã gửi, nhưng trước khi Vercel đóng worker
    background_tasks.add_task(stats_service._precompute_granularities)
    return json_cached(data)
```

> **Lưu ý:** Xóa `asyncio.create_task(self._precompute_granularities())` trong `stats_service.py` ở các hàm `db_cached` và `_refresh_stat_in_db` nếu muốn quản lý tập trung ở routes. Hoặc giữ nguyên nếu muốn tiện lợi — rủi ro thực tế thấp vì `_precompute_granularities` chỉ ghi cache, không ảnh hưởng correctness.

---

## 5. `log.py` — Thêm stdout handler khi chạy trên Vercel

**Vấn đề:** `RotatingFileHandler` ghi vào `/tmp` — Vercel không đảm bảo `/tmp` tồn tại giữa các invocations. Không có log stdout thì Vercel Runtime Logs trống rỗng.

```python
# log.py — thêm hàm và gọi ở cuối file

import sys

def _add_stdout_handler(logger: logging.Logger, level: int) -> None:
    """Thêm StreamHandler vào stdout để Vercel thu thập qua Runtime Logs."""
    if any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in logger.handlers
    ):
        return  # Đã có rồi, không thêm nữa
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)


# Thêm vào cuối file log.py, sau khi các logger đã được tạo
IS_VERCEL = os.getenv("VERCEL") == "1"
if IS_VERCEL:
    _add_stdout_handler(app_logger, logging.INFO)
    _add_stdout_handler(error_logger, logging.ERROR)
    _add_stdout_handler(file_logger, logging.WARNING)
```

---

## 6. `vercel.json` — Đặt `maxDuration`

**Vấn đề:** Không có `maxDuration` thì Vercel dùng default (10s Hobby, 15s Pro). Dashboard query có thể cần hơn 10s lần đầu (cold start + query nặng).

```json
{
  "version": 2,
  "builds": [
    {
      "src": "api/index.py",
      "use": "@vercel/python"
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "api/index.py"
    }
  ],
  "functions": {
    "api/index.py": {
      "maxDuration": 30
    }
  }
}
```

> **Lưu ý:** `maxDuration` tối đa là 60s trên Pro, 10s trên Hobby. Nếu dùng Hobby, giữ nguyên `vercel.json` và đảm bảo `statement_timeout` ở bước 3b đã đặt ≤ 8s.

---

## Tóm tắt file cần sửa

| File | Thay đổi |
|------|----------|
| `db.py` | Thêm `command_timeout: 8` và `ssl: "require"` vào `connect_args` |
| `prune_logs.py` | Sửa import `engine` → `write_engine as engine` |
| `stats_service.py` | Tăng TTL `get_*` lên 300s; thêm `SET LOCAL statement_timeout` cho dashboard |
| `routes.py` | Import `BackgroundTasks`; sửa `/stats/dashboard` dùng `background_tasks.add_task` |
| `log.py` | Thêm `_add_stdout_handler` và gọi khi `IS_VERCEL` |
| `vercel.json` | Thêm block `functions` với `maxDuration: 30` |
