import argparse

from db import BULK_INSERT_BATCH_SIZE, init_db, insert_many
from log import log_activity, log_exception
from log_sources import FileLogSource, iter_entry_batches


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
        parsed = 0
        inserted = 0
        for batch in iter_entry_batches(source.read_entries(), BULK_INSERT_BATCH_SIZE):
            parsed += len(batch)
            inserted += insert_many(batch)
        skipped = parsed - inserted if inserted <= parsed else 0
        log_activity(
            "Imported log entries from source=%s path=%s parsed=%s inserted=%s skipped=%s",
            source.name,
            source.filepath,
            parsed,
            inserted,
            skipped,
        )
        log_activity(
            "Streaming import completed: source=%s batch_size=%s",
            source.name,
            BULK_INSERT_BATCH_SIZE,
        )

        print(f"Source: {source.name}")
        print(f"File: {source.filepath}")
        print(f"Parsed entries: {parsed}")
        print(f"Inserted entries: {inserted}")
        print(f"Skipped entries: {skipped}")
    except Exception:
        log_exception("Log import pipeline failed: source=%s path=%s", args.source_name, args.filepath)
        raise


if __name__ == "__main__":
    main()
