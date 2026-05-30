import os
import json
import asyncio
import signal
from datetime import datetime
from fastapi import FastAPI
import redis.asyncio as aioredis

app = FastAPI(title="Audit Consumer Service")
redis_client = None
audit_task = None
running = True

# Храним последние 100 логов в in-memory списке для быстрого доступа через HTTP API
LOGS_BUFFER = []
STATS = {
    "total_checks": 0,
    "unique_prefixes": set()
}


@app.on_event("startup")
async def startup_event():
    global redis_client, audit_task
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    # Запускаем фоновый воркер чтения очереди внутри event-loop'а FastAPI
    audit_task = asyncio.create_task(audit_worker_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global running
    running = False
    if audit_task:
        audit_task.cancel()
    if redis_client:
        await redis_client.close()


async def audit_worker_loop():
    print("Audit consumer background loop started.")
    # Открываем лог-файл в режиме append
    log_file = open("audit.log", "a", encoding="utf-8")

    while running:
        try:
            res = await redis_client.brpop("queue:audit_events", timeout=1)
            if res is None:
                continue

            _, msg_str = res
            event = json.loads(msg_str)

            # Обогащаем событие читаемым временем
            event["readable_time"] = datetime.utcnow().isoformat()

            # 1. Запись в JSONL файл
            log_file.write(json.dumps(event) + "\n")
            log_file.flush()

            # 2. Обновление in-memory структуры для HTTP-endpoints
            LOGS_BUFFER.insert(0, event)
            if len(LOGS_BUFFER) > 100:
                LOGS_BUFFER.pop()

            STATS["total_checks"] += 1
            STATS["unique_prefixes"].add(event["prefix"])

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in audit consumer: {e}")
            await asyncio.sleep(1)

    log_file.close()


@app.get("/history")
async def get_history():
    return LOGS_BUFFER


@app.get("/stats")
async def get_stats():
    return {
        "total_checks": STATS["total_checks"],
        "unique_prefixes_count": len(STATS["unique_prefixes"])
    }