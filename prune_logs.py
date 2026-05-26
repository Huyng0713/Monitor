import asyncio
import sys
from sqlalchemy import text

# Import database connection utilities from project
from db import init_db, write_connection, engine
from stats_service import StatsService

async def main():
    await init_db()
    service = StatsService()

    # 1. Check current counts
    total_count = await service.fetch_scalar("SELECT COUNT(*) FROM logs")
    print(f"Current logs count: {total_count:,}")

    target_limit = 300000
    if total_count <= target_limit:
        print(f"Current count {total_count:,} is within target limit {target_limit:,}. No pruning needed.")
        return

    # 2. Find the timestamp of the 300,000th newest log
    cutoff_time = await service.fetch_val(
        "SELECT time FROM logs ORDER BY time DESC LIMIT 1 OFFSET :offset", 
        {"offset": target_limit}
    )
    print(f"300,000th newest log time: {cutoff_time}")

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
        print(f"Deleting {older_count:,} older logs...")
        async with write_connection() as session:
            await session.execute(text("DELETE FROM logs WHERE time < :cutoff"), {"cutoff": cutoff_time})
        print("DELETE completed successfully.")
    else:
        print("No older logs to delete.")

    # 5. Run VACUUM FULL to reclaim disk space
    print("Running VACUUM FULL logs (rebuild table and indexes)...")
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("VACUUM FULL logs"))
    print("VACUUM FULL completed successfully.")

    # 6. Check new counts
    new_total = await service.fetch_scalar("SELECT COUNT(*) FROM logs")
    print(f"New logs count: {new_total:,}")

if __name__ == "__main__":
    asyncio.run(main())
