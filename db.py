import os
import sqlite3
from contextlib import contextmanager

from log import log_activity, log_exception
from log_parse import LogEntry

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "monitor.db")
SQLITE_TIMEOUT_SECONDS = 5


def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn
    except sqlite3.Error:
        log_exception("Failed to open SQLite connection: path=%s", DB_PATH)
        raise


@contextmanager
def read_connection():
    conn = get_connection()
    try:
        yield conn
    except sqlite3.Error:
        log_exception("Database read failed")
        raise
    finally:
        conn.close()


@contextmanager
def write_connection():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        log_exception("Database write failed")
        raise
    finally:
        conn.close()


def init_db():
    with write_connection() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT NOT NULL,
            time TEXT NOT NULL,
            method TEXT,
            path TEXT,
            status INTEGER,
            size INTEGER,
            referer TEXT,
            user_agent TEXT,
            UNIQUE(ip, time, method, path, status)
        )
    """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_time ON logs(time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ip ON logs(ip)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON logs(status)")

    log_activity("Database schema ready")


def insert_entry(entry: LogEntry):
    with write_connection() as conn:
        conn.execute("""
            INSERT INTO logs (ip, time, method, path, status, size, referer, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.ip,
            entry.time.isoformat(),
            entry.method,
            entry.path,
            entry.status,
            entry.size,
            entry.referer,
            entry.user_agent,
        ))


def insert_many(entries):
    """Faster bulk insert for loading existing log file."""
    rows = [
        (
            entry.ip,
            entry.time.isoformat(),
            entry.method,
            entry.path,
            entry.status,
            entry.size,
            entry.referer,
            entry.user_agent,
        )
        for entry in entries
    ]

    if not rows:
        log_activity("insert_many called with no entries")
        return 0

    with write_connection() as conn:
        cursor = conn.executemany("""
            INSERT OR IGNORE INTO logs (ip, time, method, path, status, size, referer, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

    inserted = cursor.rowcount if cursor.rowcount != -1 else 0
    log_activity("Bulk insert completed: received=%s inserted=%s", len(rows), inserted)
    return inserted


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully")
