import argparse

from db import init_db, insert_many
from log import log_activity, log_exception
from log_sources import FileLogSource, collect_entries


def parse_args():
    parser = argparse.ArgumentParser(description="Import an nginx access log file into monitor.db")
    parser.add_argument(
        "filepath",
        nargs="?",
        default="access.log",
        help="Path to the nginx access log file. Defaults to access.log",
    )
    parser.add_argument(
        "--source-name",
        default="cli-log-source",
        help="Logical source name used in logs",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        init_db()
        log_activity("Database initialized")

        source = FileLogSource(name=args.source_name, filepath=args.filepath)
        entries = collect_entries(source)
        log_activity(
            "Parsed %s entries from log source=%s path=%s",
            len(entries),
            source.name,
            source.filepath,
        )

        inserted = insert_many(entries)
        skipped = len(entries) - inserted if inserted <= len(entries) else 0
        log_activity(
            "Inserted %s entries into DB from source=%s skipped=%s",
            inserted,
            source.name,
            skipped,
        )

        print(f"Source: {source.name}")
        print(f"File: {source.filepath}")
        print(f"Parsed entries: {len(entries)}")
        print(f"Inserted entries: {inserted}")
        print(f"Skipped entries: {skipped}")
    except Exception:
        log_exception("Log import pipeline failed: source=%s path=%s", args.source_name, args.filepath)
        raise


if __name__ == "__main__":
    main()
