import os
import json
import hashlib
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as aioredis

app = FastAPI(title="Password Leak Gateway")
redis_client = None


@app.on_event("startup")
async def startup_event():
    global redis_client
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)


@app.on_event("shutdown")
async def shutdown_event():
    if redis_client:
        await redis_client.close()


class PasswordCheckRequest(BaseModel):
    password: str


@app.post("/check")
async def check_password(req: PasswordCheckRequest):
    # 1. Генерируем UUID
    request_id = str(uuid.uuid4())

    # 2. Вычисляем SHA-1 в верхнем регистре
    sha1_hash = hashlib.sha1(req.password.encode('utf-8')).hexdigest().upper()

    # 3. k-anonymity разделение
    prefix = sha1_hash[:5]
    suffix = sha1_hash[5:]

    # 4 & 5. Публикуем сообщения в очереди (асинхронно через pipeline)
    async with redis_client.pipeline(transaction=True) as pipe:
        lookup_msg = json.dumps({"request_id": request_id, "prefix": prefix})
        audit_msg = json.dumps(
            {"timestamp": asyncio.get_event_loop().time(), "prefix": prefix, "request_id": request_id})

        await pipe.lpush("queue:lookup_requests", lookup_msg)
        await pipe.lpush("queue:audit_events", audit_msg)
        await pipe.execute()

    # 6. Ожидаем ответ в персональной очереди с таймаутом 5 сек
    response_queue = f"queue:lookup_responses:{request_id}"
    try:
        # brpop возвращает кортеж (имя_очереди, значение)
        res = await redis_client.brpop(response_queue, timeout=5)
        if res is None:
            raise HTTPException(status_code=504, detail="Gateway Timeout - Worker did not respond")

        _, data_str = res
        data = json.loads(data_str)

        # 7. Финальное сравнение суффикса на клиенте (в gateway)
        matches = data.get("matches", [])
        matched_record = next((m for m in matches if m["suffix"] == suffix), None)

        if matched_record:
            return {"breached": True, "count": matched_record["count"]}
        return {"breached": False, "count": 0}

    finally:
        # Очищаем за собой временную очередь на случай, если ответ пришел после таймаута
        await redis_client.delete(response_queue)