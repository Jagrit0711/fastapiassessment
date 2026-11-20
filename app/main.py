import asyncio
import os
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel

from app.database import init_db, save_event, mark_delivered
from app.downstream import forward_event, close_client
from app.processor import scrub_and_fingerprint

_process_pool: ProcessPoolExecutor | None = None
_delivery_queue: asyncio.Queue = asyncio.Queue()
_seen_events: set[str] = set()
_workers: list[asyncio.Task] = []


async def delivery_worker():
    while True:
        try:
            event_id, cleaned_text, fingerprint = await _delivery_queue.get()
            try:
                await forward_event(event_id, cleaned_text, fingerprint)
                await mark_delivered(event_id)
            except Exception:
                # In production: log error or transfer to dead letter queue
                pass
            finally:
                _delivery_queue.task_done()
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _process_pool, _workers
    await init_db()

    # Pre-spawn ProcessPoolExecutor to avoid cold-start latency during burst
    cpu_count = os.cpu_count() or 4
    _process_pool = ProcessPoolExecutor(max_workers=min(8, cpu_count))

    # Concurrently consume downstream deliveries
    _workers = [asyncio.create_task(delivery_worker()) for _ in range(15)]

    try:
        yield
    finally:
        for w in _workers:
            w.cancel()
        await asyncio.gather(*_workers, return_exceptions=True)
        await close_client()
        if _process_pool is not None:
            _process_pool.shutdown(wait=False)


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/ingest", status_code=202)
async def ingest_webhook(
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    # Fast in-memory deduplication layer: short-circuits redundant CPU scrubbing during burst retries
    if payload.event_id in _seen_events:
        return Response(status_code=202, content='{"status":"accepted","duplicate":true}', media_type="application/json")
    _seen_events.add(payload.event_id)

    # CPU-bound preprocessing: offload to ProcessPoolExecutor (or thread pool fallback)
    loop = asyncio.get_running_loop()
    if _process_pool is not None:
        cleaned_text, fingerprint = await loop.run_in_executor(
            _process_pool, scrub_and_fingerprint, payload.content
        )
    else:
        cleaned_text, fingerprint = await asyncio.to_thread(
            scrub_and_fingerprint, payload.content
        )

    # Atomic SQLite insert with ON CONFLICT DO NOTHING
    is_new = await save_event(
        payload.event_id, payload.tenant_id, cleaned_text, fingerprint
    )

    if is_new:
        _delivery_queue.put_nowait((payload.event_id, cleaned_text, fingerprint))

    return Response(
        status_code=202,
        content='{"status":"accepted","duplicate":false}' if is_new else '{"status":"accepted","duplicate":true}',
        media_type="application/json",
    )
