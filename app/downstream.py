import asyncio
import os
import random
import httpx

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
    return _client


async def close_client():
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        _client = None


async def forward_event(
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
    max_retries: int = 5,
    base_delay: float = 0.05,
) -> dict:
    """
    Forwards a processed webhook event to the downstream consumer API with exponential backoff and jitter.
    """
    client = get_client()
    url = os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL)
    payload = {
        "event_id": event_id,
        "text": cleaned_text,
        "fingerprint": fingerprint,
    }

    last_exc = None
    for attempt in range(max_retries):
        try:
            response = await client.post(url, json=payload)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 503 or response.status_code >= 500:
                # Transient error - retry with backoff and jitter
                delay = base_delay * (2 ** attempt) + random.uniform(0.005, 0.02)
                await asyncio.sleep(delay)
                continue
            else:
                response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            delay = base_delay * (2 ** attempt) + random.uniform(0.005, 0.02)
            await asyncio.sleep(delay)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to forward event {event_id} after {max_retries} attempts")
