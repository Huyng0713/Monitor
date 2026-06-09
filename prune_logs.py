import asyncio
import os
from sqlalchemy import text

# Import database connection utilities from project
from db import DATABASE_URL, init_db, write_connection, write_engine as engine
from stats_service import StatsService

TARGET_LIMIT = int(os.getenv("PRUNE_LOGS_TARGET_LIMIT", "5000000"))
BATCH_SIZE = int(os.getenv("PRUNE_LOGS_BATCH_SIZE", "10000"))
DELETE_STATEMENT_TIMEOUT_MS = int(os.getenv("PRUNE_LOGS_DELETE_TIMEOUT_MS", "120000"))
RUN_VACUUM_FULL = os.getenv("PRUNE_LOGS_VACUUM_FULL", "1") == "1"
IS_COCKROACH = "cockroach" in DATABASE_URL.lower() or "26257" in DATABASE_URL


async def delete_older_logs_in_batches(cutoff_time, total_to_delete):
    deleted_total = 0

    while True:
        async with write_connection() as session:
            await session.execute(
                text("SELECT set_config('statement_timeout', :timeout_ms, true)"),
                {"timeout_ms": str(DELETE_STATEMENT_TIMEOUT_MS)},
            )
            result = await session.execute(
                text("""
                    WITH deleted AS (
                        DELETE FROM logs
                        WHERE id IN (
                            SELECT id
                            FROM logs
                            WHERE time < :cutoff
                            ORDER BY time ASC
                            LIMIT :batch_size
                        )
                        RETURNING 1
                    )
                    SELECT COUNT(*) FROM deleted
                """),
                {"cutoff": cutoff_time, "batch_size": BATCH_SIZE},
            )
            deleted_count = result.scalar_one()

        if deleted_count == 0:
            break

        deleted_total += deleted_count
        print(
            f"Deleted {deleted_total:,}/{total_to_delete:,} logs "
            f"(last batch: {deleted_count:,})",
            flush=True,
        )

    return deleted_total


async def vacuum_full_logs():
    if not RUN_VACUUM_FULL:
        print("Skipping VACUUM FULL because PRUNE_LOGS_VACUUM_FULL is not 1.")
        return

    if IS_COCKROACH:
        print("Skipping VACUUM FULL because CockroachDB does not support it.")
        return

    print("Running VACUUM FULL logs (rebuild table and indexes)...")
    try:
        async with engine.connect() as conn:
            conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(text("SET statement_timeout = 0"))
            await conn.execute(text("VACUUM FULL logs"))
        print("VACUUM FULL completed successfully.")
    except Exception as exc:
        print(f"VACUUM FULL failed after DELETE completed: {exc}")


async def main():
    await init_db()
    service = StatsService()

    # 1. Check current counts
    total_count = await service.fetch_scalar("SELECT COUNT(*) FROM logs")
    print(f"Current logs count: {total_count:,}")

    if total_count <= TARGET_LIMIT:
        print(f"Current count {total_count:,} is within target limit {TARGET_LIMIT:,}. No pruning needed.")
        return

    # 2. Find the timestamp of the 5,000,000th newest log
    cutoff_time = await service.fetch_val(
        "SELECT time FROM logs ORDER BY time DESC LIMIT 1 OFFSET :offset", 
        {"offset": TARGET_LIMIT - 1}
    )
    print(f"{TARGET_LIMIT:,}th newest log time: {cutoff_time}")

    if not cutoff_time:
        print("Could not retrieve cutoff time.")
        return

    # 3. Count logs older than the cutoff timestamp
    older_count = await service.fetch_scalar(
        "SELECT COUNT(*) FROM logs WHERE time < :cutoff", 
        {"cutoff": cutoff_time}
    )
    print(f"Number of logs older than {cutoff_time} (to be deleted): {older_count:,}")

    # 4. Perform DELETE
    if older_count > 0:
        print(f"Deleting {older_count:,} older logs in batches of {BATCH_SIZE:,}...")
        deleted_total = await delete_older_logs_in_batches(cutoff_time, older_count)
        print(f"DELETE completed successfully. Deleted {deleted_total:,} logs.")
    else:
        print("No older logs to delete.")

    # 5. Run VACUUM FULL to reclaim disk space where the database supports it.
    await vacuum_full_logs()

    # 6. Check new counts
    new_total = await service.fetch_scalar("SELECT COUNT(*) FROM logs")
    print(f"New logs count: {new_total:,}")

if __name__ == "__main__":
    asyncio.run(main())
