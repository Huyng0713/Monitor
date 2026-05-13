from db import init_db, insert_many
from log import log_activity, log_exception
from log_sources import FileLogSource, collect_entries

try:
    init_db()
    log_activity("Database initialized")

    source = FileLogSource(name="default-access-log", filepath="access.log")
    entries = collect_entries(source)
    log_activity("Parsed %s entries from log source=%s", len(entries), source.name)

    inserted = insert_many(entries)
    log_activity("Inserted %s entries into DB", inserted)
except Exception:
    log_exception("Log import pipeline failed")
    raise
