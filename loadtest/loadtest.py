import asyncio
import aiohttp
import time
import numpy as np

URL = "http://localhost:8080/check"
TOTAL_REQUESTS = 1000
CONCURRENCY = 10  # Кол-во параллельных корутин


async def send_request(session, latencies):
    # Тестируем как пробивающиеся пароли, так и безопасные
    payload = {"password": "password" if time.time() % 2 == 0 else "secure_abc123!"}

    start_time = time.time()
    try:
        async with session.post(URL, json=payload) as response:
            if response.status == 200:
                latency = (time.time() - start_time) * 1000  # в ms
                latencies.append(latency)
            else:
                print(f"Error status: {response.status}")
    except Exception as e:
        print(f"Request failed: {e}")


async def worker(session, queue, latencies):
    while not queue.empty():
        await queue.get()
        await send_request(session, latencies)
        queue.task_done()


async def main():
    latencies = []
    queue = asyncio.Queue()

    for _ in range(TOTAL_REQUESTS):
        queue.put_nowait(None)

    start_total = time.time()

    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(worker(session, queue, latencies)) for _ in range(CONCURRENCY)]
        await queue.join()
        for w in workers:
            w.cancel()

    total_time = time.time() - start_total

    if latencies:
        throughput = len(latencies) / total_time
        p50 = np.percentile(latencies, 50)
        p99 = np.percentile(latencies, 99)

        print("\n=== Результаты Тестирования ===")
        print(f"Throughput: {throughput:.2f} req/s")
        print(f"P50 Latency: {p50:.2f} ms")
        print(f"P99 Latency: {p99:.2f} ms")
        print("===============================\n")


if __name__ == "__main__":
    asyncio.run(main())