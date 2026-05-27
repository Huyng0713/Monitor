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
    if url.startswith("postgresql+asyncpg://"):
        pass
    elif url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        
    if "postgresql+asyncpg://" in url and "prepared_statement_cache_size" not in url:
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
        UniqueConstraint("ip", "time", "method", "path", "status", name="uq_logs_identity"),
        Index("ix_logs_time_status", "time", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ip: Mapped[str] = mapped_column(String, nullable=False)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
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
    from uuid import uuid4
    options = {
        "pool_pre_ping": False,  # Disabled to save 400ms per checkout
        "pool_recycle": 300,
        "connect_args": {
            "statement_cache_size": 0,
            "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
            "ssl": "require",       # Supabase database requires or recommends SSL encryption
        },
    }
    if IS_VERCEL:
        options.update({"poolclass": NullPool})
        options["connect_args"]["command_timeout"] = 8  # Timeout only on Vercel
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


async def init_db():
    if not RUN_SCHEMA_CREATE:
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
    asyncio.run(init_db())
    print("Database initialized successfully")
