import asyncio
import time
from db import read_connection
from stats_service import StatsService

async def main():
    service = StatsService()
    
    ip_filter = "172.16.1.21"
    
    print("Simulating user search:")
    print("1. User runs search. First page is loaded immediately, and count is requested in background.")
    
    # Simulate count request (cache miss on first request)
    start_time = time.perf_counter()
    count = await service.search_logs_count(ip_filter, None, None, None, None)
    print(f"Background Count fetched: {count} in {(time.perf_counter() - start_time) * 1000:.2f} ms")
    
    # 2. User jumps to page 500 (offset = 500 * 15 = 7500)
    print("\n2. User jumps to page 500 (offset 7500):")
    start_time = time.perf_counter()
    result_500 = await service.jump_to_page(500, 15, ip=ip_filter)
    duration_500 = (time.perf_counter() - start_time) * 1000
    print(f"Page 500 loaded in {duration_500:.2f} ms (Rows count: {len(result_500['rows'])})")
    
    # 3. User navigates to next page (page 501, offset 7515)
    print("\n3. User navigates to page 501 (offset 7515):")
    start_time = time.perf_counter()
    result_501 = await service.jump_to_page(501, 15, ip=ip_filter)
    duration_501 = (time.perf_counter() - start_time) * 1000
    print(f"Page 501 loaded in {duration_501:.2f} ms (Rows count: {len(result_501['rows'])})")

    # 4. User navigates to page 2 (offset 30)
    print("\n4. User navigates to page 2 (offset 30):")
    start_time = time.perf_counter()
    result_2 = await service.jump_to_page(2, 15, ip=ip_filter)
    duration_2 = (time.perf_counter() - start_time) * 1000
    print(f"Page 2 loaded in {duration_2:.2f} ms (Rows count: {len(result_2['rows'])})")

if __name__ == "__main__":
    asyncio.run(main())
