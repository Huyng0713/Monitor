import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import NullPool

from env import load_dotenv
from log import log_activity, log_exception
from log_parse import LogEntry

load_dotenv()

RAW_DATABASE_URL = os.getenv("DATABASE_URL")
BULK_INSERT_BATCH_SIZE = int(os.getenv("BULK_INSERT_BATCH_SIZE", "500"))
IS_VERCEL = os.getenv("VERCEL") == "1"
RUN_SCHEMA_CREATE = os.getenv("DB_CREATE_ALL_ON_STARTUP", "0") == "1"

if not RAW_DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")


def normalize_database_url(url: str) -> str:
    from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
    try:
        parsed = urlparse(url)
        if parsed.query:
            params = dict(parse_qsl(parsed.query))
            params.pop("sslmode", None)
            params.pop("sslrootcert", None)
            new_query = urlencode(params)
            parsed = parsed._replace(query=new_query)
            url = urlunparse(parsed)
    except Exception:
        pass

    is_cockroach = "cockroach" in url or "26257" in url or url.startswith("cockroachdb")

    if is_cockroach:
        for scheme in ["postgresql+asyncpg://", "postgresql+psycopg://", "postgresql://", "postgres://", "cockroachdb+asyncpg://", "cockroachdb://"]:
            if url.startswith(scheme):
                url = url.replace(scheme, "cockroachdb+asyncpg://", 1)
                break
    else:
        if url.startswith("postgresql+asyncpg://"):
            pass
        elif url.startswith("postgresql+psycopg://"):
            url = url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)

    target_dialect = "cockroachdb+asyncpg://" if is_cockroach else "postgresql+asyncpg://"
    if target_dialect in url and "prepared_statement_cache_size" not in url:
        if "?" in url:
            url += "&prepared_statement_cache_size=0"
        else:
            url += "?prepared_statement_cache_size=0"
    return url


DATABASE_URL = normalize_database_url(RAW_DATABASE_URL)


class Base(DeclarativeBase):
    pass


class LogRecord(Base):
    __tablename__ = "logs"
    __table_args__ = (
        # Giữ nguyên method trong constraint — tránh risk duplicate trên 7M rows
        UniqueConstraint("ip", "time", "method", "path", "status", name="uq_logs_identity"),

        # Composite index cho traffic và anomalies (query theo time range + status)
        Index("ix_logs_time_status", "time", "status"),

        # Index cho GROUP BY ip (Top IPs query)
        Index("ix_logs_ip", "ip"),

        # Index cho GROUP BY path (Top URLs query)
        Index("ix_logs_path", "path"),

        # Index cho filter theo status đơn lẻ
        Index("ix_logs_status", "status"),

        # Trigram indexes — tạo qua migration vì SQLAlchemy không hỗ trợ GIN trigram syntax
        # CREATE INDEX ix_logs_ip_trgm ON logs USING GIN (ip gin_trgm_ops)
        # CREATE INDEX ix_logs_path_trgm ON logs USING GIN (path gin_trgm_ops)
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ip: Mapped[str] = mapped_column(String, nullable=False)
    # Bỏ index=True — đã có ix_logs_time_status covering time
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    method: Mapped[str | None] = mapped_column(String, nullable=True)
    path: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    referer: Mapped[str | None] = mapped_column(String, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String, nullable=True)


class SystemLogRecord(Base):
    __tablename__ = "system_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, index=True)
    logger: Mapped[str] = mapped_column(String, nullable=False)
    level: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)
    traceback: Mapped[str | None] = mapped_column(String, nullable=True)


class CachedStatRecord(Base):
    __tablename__ = "cached_stats"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)


def _engine_options() -> dict:
    import ssl
    from uuid import uuid4

    if "cockroach" in DATABASE_URL or "26257" in DATABASE_URL:
        cert_path = os.path.expanduser("~/.postgresql/root.crt")
        if os.path.exists(cert_path):
            ssl_ctx = ssl.create_default_context(cafile=cert_path)
        else:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
    else:
        ssl_ctx = ssl.create_default_context()

    options = {
        "pool_pre_ping": True,
        "pool_recycle": 120,
        "connect_args": {
            "statement_cache_size": 0,
            "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
            "ssl": ssl_ctx,
        },
    }
    if IS_VERCEL:
        options.update({"poolclass": NullPool})
        options["connect_args"]["command_timeout"] = 120
    else:
        options.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
    return options


read_engine = create_async_engine(DATABASE_URL, **_engine_options(), execution_options={"isolation_level": "AUTOCOMMIT"})
write_engine = create_async_engine(DATABASE_URL, **_engine_options())

ReadSessionLocal = async_sessionmaker(bind=read_engine, autoflush=False, expire_on_commit=False)
WriteSessionLocal = async_sessionmaker(bind=write_engine, autoflush=False, expire_on_commit=False)


@asynccontextmanager
async def read_connection():
    async with ReadSessionLocal() as session:
        try:
            yield session
        except Exception:
            log_exception("Database read failed")
            raise


@asynccontextmanager
async def write_connection():
    async with WriteSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            log_exception("Database write failed")
            raise


async def init_db(force=False):
    if not force and not RUN_SCHEMA_CREATE:
        log_activity("Database runtime schema creation skipped")
        return
    try:
        async with write_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        log_activity("Database schema ready")
    except Exception:
        log_exception("Failed to initialize database schema")
        raise


async def dispose_engine():
    await read_engine.dispose()
    await write_engine.dispose()


def _entry_to_row(entry: LogEntry) -> dict:
    return {
        "ip": entry.ip,
        "time": entry.time,
        "method": entry.method,
        "path": entry.path,
        "status": entry.status,
        "size": entry.size,
        "referer": entry.referer,
        "user_agent": entry.user_agent,
    }


async def insert_entry(entry: LogEntry):
    async with write_connection() as session:
        statement = pg_insert(LogRecord).values(_entry_to_row(entry))
        # Giữ nguyên method trong on_conflict khớp với UniqueConstraint
        statement = statement.on_conflict_do_nothing(index_elements=["ip", "time", "method", "path", "status"])
        await session.execute(statement)


async def insert_many(entries):
    batch = []
    inserted_total = 0
    received_total = 0

    async def flush_batch(rows):
        if not rows:
            return 0
        async with write_connection() as session:
            statement = pg_insert(LogRecord).values(rows)
            # Giữ nguyên method trong on_conflict khớp với UniqueConstraint
            statement = statement.on_conflict_do_nothing(index_elements=["ip", "time", "method", "path", "status"])
            result = await session.execute(statement)
        return result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0

    for entry in entries:
        batch.append(_entry_to_row(entry))
        received_total += 1
        if len(batch) >= BULK_INSERT_BATCH_SIZE:
            inserted_total += await flush_batch(batch)
            batch = []

    inserted_total += await flush_batch(batch)

    if received_total == 0:
        log_activity("insert_many called with no entries")
        return 0

    log_activity("Bulk insert completed: received=%s inserted=%s", received_total, inserted_total)
    return inserted_total


async def insert_system_log(logger: str, level: str, message: str, traceback: str | None = None):
    async with write_connection() as session:
        log_record = SystemLogRecord(
            logger=logger,
            level=level,
            message=message,
            traceback=traceback,
            created_at=datetime.utcnow()
        )
        session.add(log_record)


if __name__ == "__main__":
    asyncio.run(init_db(force=True))
    print("Database initialized successfully")