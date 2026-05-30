import os
import json
import signal
import sys
import asyncio
import redis.asyncio as aioredis

# Структура в памяти: prefix -> [{"suffix": "...", "count": ...}]
DB = {}
running = True


def load_database():
    file_path = "/data/breached_hashes.txt"
    if not os.path.exists(file_path):
        print(f"Database file {file_path} not found!")
        return

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue
            full_hash, count_str = line.split(":", 1)
            full_hash = full_hash.upper()
            try:
                count = int(count_str)
            except ValueError:
                continue

            prefix = full_hash[:5]
            suffix = full_hash[5:]

            if prefix not in DB:
                DB[prefix] = []
            DB[prefix].append({"suffix": suffix, "count": count})
    print(f"Loaded {len(DB)} unique prefixes from database.")


def handle_signal(signum, frame):
    global running
    print(f"Received signal {signum}, shutting down gracefully...")
    running = False


async def main():
    load_database()

    # Регистрация Graceful shutdown сигналов
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: handle_signal(sig, None))

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    print("Worker is ready and waiting for requests...")

    while running:
        try:
            # Таймаут 1 секунда, чтобы цикл мог проверять флаг `running`
            res = await redis_client.brpop("queue:lookup_requests", timeout=1)
            if res is None:
                continue

            _, msg_str = res
            req_data = json.loads(msg_str)

            req_id = req_data["request_id"]
            prefix = req_data["prefix"]

            # Поиск префикса в памяти
            matches = DB.get(prefix, [])

            # Отправка ответа в персональную очередь ответа gateway'я
            response_queue = f"queue:lookup_responses:{req_id}"
            await redis_client.lpush(response_queue, json.dumps({"matches": matches}))
            # Ставим небольшое TTL, чтобы очередь не зависла вечно в Redis, если gateway отвалился
            await redis_client.expire(response_queue, 10)

        except Exception as e:
            print(f"Error processing message: {e}")
            await asyncio.sleep(1)

    await redis_client.close()
    print("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())