import os
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from env import load_dotenv
from log import log_activity, log_exception
from log_parse import LogEntry

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
BULK_INSERT_BATCH_SIZE = int(os.getenv("BULK_INSERT_BATCH_SIZE", "500"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)


class Base(DeclarativeBase):
    pass


class LogRecord(Base):
    __tablename__ = "logs"
    __table_args__ = (
        UniqueConstraint("ip", "time", "method", "path", "status", name="uq_logs_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ip: Mapped[str] = mapped_column(String, nullable=False, index=True)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    method: Mapped[str | None] = mapped_column(String, nullable=True)
    path: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    referer: Mapped[str | None] = mapped_column(String, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String, nullable=True)


# Singleton engine — created once, reused across requests
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=300,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@contextmanager
def read_connection():
    session = SessionLocal()
    try:
        yield session
    except Exception:
        log_exception("Database read failed")
        raise
    finally:
        session.close()


@contextmanager
def write_connection():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        log_exception("Database write failed")
        raise
    finally:
        session.close()


def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        log_activity("Database schema ready")
    except Exception:
        log_exception("Failed to initialize database schema")
        raise


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


def insert_entry(entry: LogEntry):
    with write_connection() as session:
        statement = pg_insert(LogRecord).values(_entry_to_row(entry))
        statement = statement.on_conflict_do_nothing(constraint="uq_logs_identity")
        session.execute(statement)


def insert_many(entries):
    rows = [_entry_to_row(entry) for entry in entries]
    if not rows:
        log_activity("insert_many called with no entries")
        return 0

    inserted_total = 0
    for start in range(0, len(rows), BULK_INSERT_BATCH_SIZE):
        batch = rows[start:start + BULK_INSERT_BATCH_SIZE]
        with write_connection() as session:
            statement = pg_insert(LogRecord).values(batch)
            statement = statement.on_conflict_do_nothing(constraint="uq_logs_identity")
            result = session.execute(statement)
        inserted = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0
        inserted_total += inserted

    log_activity("Bulk insert completed: received=%s inserted=%s", len(rows), inserted_total)
    return inserted_total


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully")