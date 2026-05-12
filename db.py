import sqlite3
import os
from datetime import datetime
from log_parse import LogEntry

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "monitor.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
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
    conn.commit()
    conn.close()

def insert_entry(entry: LogEntry):
    conn = get_connection()
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
    conn.commit()
    conn.close()

def insert_many(entries):
    """Faster bulk insert for loading existing log file."""
    conn = get_connection()
    conn.executemany("""
        INSERT OR IGNORE INTO logs (ip, time, method, path, status, size, referer, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (e.ip, e.time.isoformat(), e.method, e.path,
         e.status, e.size, e.referer, e.user_agent)
        for e in entries
    ])
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
    init_db()
    print("Database initialized successfully")