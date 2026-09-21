"""services/apify_receiver.py — Apify webhook ingestion consumer (Cloud Run).

Real Apify integration flow:
    Apify Actor run completes
        → ACTOR.RUN.SUCCEEDED webhook fires (small event payload)
        → This endpoint validates and parses the event
        → Extracts resource.defaultDatasetId
        → Fetches actual scraped items from Apify Dataset API (httpx async)
        → Normalizes items via services/normalizers/apify_career_scraper.py
        → Deduplicates against BigQuery (raw_job_postings)
        → Inserts only novel RawJob records into BigQuery
        → Publishes ONLY newly inserted job_ids to Pub/Sub (async-safe)

Idempotency strategy: OPTION 2 — job-level deduplication.
    Duplicate webhook deliveries are safe: the same run's dataset may be
    re-fetched, but BigQuery dedup ensures no duplicate rows are inserted
    and no duplicate Pub/Sub messages are published for already-existing jobs.
    This requires no extra storage and is consistent with the existing architecture.

Webhook authentication:
    Apify does not natively sign webhook requests with HMAC.
    The X-Apify-Signature header is supported as an OPTIONAL mechanism that
    requires manual configuration in Apify Console via the Headers template.
    When APIFY_WEBHOOK_SECRET is set, all incoming requests MUST carry a
    valid HMAC-SHA256(secret, raw_body) in the X-Apify-Signature header.
    When APIFY_WEBHOOK_SECRET is empty, signature validation is skipped
    (suitable for development; use URL-embedded token for basic auth in prod).

ARCHITECTURE.md: "dumb ingestion consumer" — no scoring logic here.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import pubsub_v1

from config.settings import get_settings
from observability.setup import flush_observability, setup_observability
from repositories.raw_jobs_repo import RawJobRepository
from services.normalizers import apify_career_scraper
from schemas.job_models import RawJob

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_APIFY_DATASET_API = "https://api.apify.com/v2/datasets/{dataset_id}/items"
_DATASET_FETCH_TIMEOUT_S = 60.0   # seconds; dataset fetch can be large
_PUBSUB_PUBLISH_TIMEOUT_S = 10.0  # seconds per message


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Startup and shutdown lifecycle."""
    setup_observability()
    logger.info("apify-receiver started")
    yield
    flush_observability()
    logger.info("apify-receiver shutting down")


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="apify-receiver",
    description="Ingestion webhook consumer for Apify actor job scraper payloads",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://api.apify.com"],
    allow_methods=["POST"],
)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint for Cloud Run liveness probe."""
    return {"status": "ok"}

#starting point 

@app.post("/webhook/apify", status_code=status.HTTP_202_ACCEPTED)
async def apify_webhook(request: Request) -> dict[str, Any]:
    """Receive an ACTOR.RUN.SUCCEEDED event from Apify.

    The webhook body is a small event payload — NOT the scraped data.
    This endpoint:
      1. Validates the HMAC signature (if APIFY_WEBHOOK_SECRET is configured).
      2. Parses the event and validates eventType == ACTOR.RUN.SUCCEEDED.
      3. Extracts resource.defaultDatasetId.
      4. Fetches the actual dataset items from the Apify Dataset API.
      5. Normalizes items into canonical RawJob records.
      6. Deduplicates and inserts novel jobs into BigQuery.
      7. Publishes only the newly inserted job_ids to Pub/Sub.

    Expected webhook payload (Apify default template):
    {
        "eventType": "ACTOR.RUN.SUCCEEDED",
        "eventData": {
            "actorId": "...",
            "actorRunId": "...",
            "actorTaskId": "..."    // optional
        },
        "resource": {
            "id": "<actorRunId>",
            "actId": "<actorId>",
            "status": "SUCCEEDED",
            "defaultDatasetId": "<datasetId>",
            ...
        }
    }
    """
    settings = get_settings()

    # ── Step 1: Read raw body before any parsing ───────────────────────────────
    body = await request.body()

    # ── Step 2: HMAC signature validation ─────────────────────────────────────
    # Apify does not natively add HMAC signatures. This validation is only
    # active when APIFY_WEBHOOK_SECRET is set, which requires configuring the
    # X-Apify-Signature header in the Apify Console Headers template.
    # Algorithm: HMAC-SHA256(key=secret, msg=raw_body), compared as hex digest.
    if settings.apify_webhook_secret:
        signature = request.headers.get("X-Apify-Signature", "")
        expected = hmac.new(
            settings.apify_webhook_secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            logger.warning(
                "Webhook signature mismatch from %s",
                request.client.host if request.client else "unknown",
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid webhook signature",
            )

    # ── Step 3: Parse JSON body ────────────────────────────────────────────────
    try:
        payload: dict[str, Any] = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON body: {exc}",
        ) from exc

    # ── Step 4: Validate event type ────────────────────────────────────────────
    event_type: str = payload.get("eventType", "")
    if event_type != "ACTOR.RUN.SUCCEEDED":
        logger.info("Ignoring non-success event type: %s", event_type)
        return {
            "received": event_type,
            "message": f"Event type '{event_type}' ignored; only ACTOR.RUN.SUCCEEDED is processed.",
        }

    # ── Step 5: Extract actor run metadata ────────────────────────────────────
    resource: dict[str, Any] = payload.get("resource", {})
    actor_run_id: str = resource.get("id", "") or payload.get("eventData", {}).get("actorRunId", "")
    actor_id: str = resource.get("actId", "") or payload.get("eventData", {}).get("actorId", "")
    dataset_id: str = resource.get("defaultDatasetId", "").strip()

    if not dataset_id:
        logger.error(
            "ACTOR.RUN.SUCCEEDED webhook missing resource.defaultDatasetId | actor_run_id=%s",
            actor_run_id,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing resource.defaultDatasetId",
        )

    logger.info(
        "Processing ACTOR.RUN.SUCCEEDED | actor_id=%s actor_run_id=%s dataset_id=%s",
        actor_id,
        actor_run_id,
        dataset_id,
    )

    # ── Step 6: Fetch dataset items from Apify Dataset API ────────────────────
    if not settings.apify_api_token:
        logger.error("apify_api_token is not configured; cannot fetch dataset items")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Apify API token is not configured. "
                "Set APIFY_API_TOKEN in .env (local) or the 'apify-api-token' "
                "Secret Manager secret (production)."
            ),
        )

    raw_items = await _fetch_dataset_items(dataset_id, settings.apify_api_token)

    if not raw_items:
        logger.info("Dataset %s returned 0 items; nothing to process.", dataset_id)
        return {"dataset_id": dataset_id, "received": 0, "inserted": 0, "published": 0}

    # ── Step 7: Normalize items into canonical RawJob records ─────────────────
    jobs: list[RawJob] = []
    skipped = 0
    for item in raw_items:
        normalized = apify_career_scraper.normalize(item)
        if normalized is not None:
            jobs.append(normalized)
        else:
            skipped += 1

    logger.info(
        "Normalization complete: total=%d valid=%d skipped=%d",
        len(raw_items),
        len(jobs),
        skipped,
    )

    if not jobs:
        return {
            "dataset_id": dataset_id,
            "received": len(raw_items),
            "inserted": 0,
            "published": 0,
            "message": "All items were malformed or had insufficient description",
        }

    # ── Step 8: Batch insert with deduplication ────────────────────────────────

    repo = RawJobRepository()
    try:
        inserted_ids: list[str] = await repo.batch_insert_jobs(jobs)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "BigQuery batch insert failed: dataset_id=%s error=%s",
            dataset_id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="BigQuery insert failed; see service logs",
        ) from exc

    inserted_count = len(inserted_ids)
    logger.info(
        "Deduplication result: received=%d valid=%d inserted=%d | dataset_id=%s",
        len(raw_items),
        len(jobs),
        inserted_count,
        dataset_id,
    )

    # ── Step 9: Publish ONLY the newly inserted job_ids to Pub/Sub ────────────

    published_count = 0
    publish_failures = 0
    if inserted_ids:
        published_count, publish_failures = await _publish_job_ids(
            inserted_ids,
            project_id=settings.gcp_project_id,
            topic=settings.pubsub_embedding_topic,
        )

    logger.info(
        "apify_webhook complete: dataset_id=%s received=%d inserted=%d published=%d publish_failures=%d",
        dataset_id,
        len(raw_items),
        inserted_count,
        published_count,
        publish_failures,
    )

    return {
        "dataset_id": dataset_id,
        "actor_run_id": actor_run_id,
        "received": len(raw_items),
        "normalized": len(jobs),
        "inserted": inserted_count,
        "published": published_count,
        "publish_failures": publish_failures,
    }


# ── Private helpers ────────────────────────────────────────────────────────────

async def _fetch_dataset_items(
    dataset_id: str,
    api_token: str,
    *,
    clean: bool = True,
) -> list[dict[str, Any]]:
    """Fetch all items from an Apify dataset via the Dataset Items API.

    Args:
        dataset_id: The Apify dataset ID (from resource.defaultDatasetId).
        api_token: Authenticated Apify API token. Never logged.
        clean: If True, request clean=true to omit Apify-internal metadata fields.

    Returns:
        List of raw dataset item dicts.

    Raises:
        HTTPException: On API auth failure, invalid dataset, timeout, or HTTP error.
    """
    url = _APIFY_DATASET_API.format(dataset_id=dataset_id)
    params: dict[str, str | int] = {
        "token": api_token,  # passed as query param per Apify API convention
        "format": "json",
    }
    if clean:
        params["clean"] = "true"

    try:
        async with httpx.AsyncClient(timeout=_DATASET_FETCH_TIMEOUT_S) as client:
            response = await client.get(url, params=params)
    except httpx.TimeoutException as exc:
        logger.error("Apify Dataset API timed out: dataset_id=%s", dataset_id)
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Apify Dataset API request timed out after {_DATASET_FETCH_TIMEOUT_S}s",
        ) from exc
    except httpx.RequestError as exc:
        logger.error("Apify Dataset API request error: dataset_id=%s error=%s", dataset_id, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not reach Apify Dataset API",
        ) from exc

    if response.status_code == 401:
        # Do NOT log the token — log only that authentication failed.
        logger.error(
            "Apify Dataset API authentication failed (401): dataset_id=%s | "
            "Check that APIFY_API_TOKEN is valid.",
            dataset_id,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Apify Dataset API authentication failed; check APIFY_API_TOKEN configuration",
        )

    if response.status_code == 404:
        logger.error("Apify dataset not found: dataset_id=%s", dataset_id)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Apify dataset '{dataset_id}' not found",
        )

    if not response.is_success:
        logger.error(
            "Apify Dataset API non-2xx response: dataset_id=%s status=%d",
            dataset_id,
            response.status_code,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Apify Dataset API returned HTTP {response.status_code}",
        )

    try:
        data = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Apify Dataset API returned malformed JSON: dataset_id=%s error=%s",
            dataset_id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Apify Dataset API returned non-JSON response",
        ) from exc

    # The Dataset Items API returns either a JSON array or {"items": [...]}
    if isinstance(data, list):
        return cast(list[dict[str, Any]], data)
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list):
            return cast(list[dict[str, Any]], items)

    logger.error(
        "Unexpected Apify Dataset API response shape: dataset_id=%s type=%s",
        dataset_id,
        type(data).__name__,
    )
    raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail="Apify Dataset API returned unexpected response structure",
    )


async def _publish_job_ids(
    job_ids: list[str],
    *,
    project_id: str,
    topic: str,
) -> tuple[int, int]:
    """Publish job_ids to Pub/Sub using a thread pool executor (async-safe).

    The synchronous pubsub_v1.PublisherClient is used because:
      - google-cloud-pubsub 2.40.0 includes PublisherAsyncClient, but it
        requires a different gRPC async transport that adds complexity.
      - Running the sync client in run_in_executor is the safe, minimal change
        that avoids blocking the FastAPI event loop without new dependencies.

    Each publish() call is awaited (via executor) and its future resolved so
    that publish failures are detected and logged individually, not silently
    dropped.

    Args:
        job_ids: List of newly inserted job_ids to publish.
        project_id: GCP project ID.
        topic: Pub/Sub topic name (not the full path).

    Returns:
        Tuple of (published_count, failure_count).
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)
    loop = asyncio.get_event_loop()

    published = 0
    failures = 0

    for job_id in job_ids:
        msg = json.dumps({"job_id": job_id}).encode("utf-8")

        def _do_publish(jid: str = job_id, message: bytes = msg) -> str:
            """Sync publish + wait for future resolution in the executor thread."""
            future = publisher.publish(topic_path, message)
            # Calling result() blocks until Pub/Sub confirms delivery or raises.
            return future.result(timeout=_PUBSUB_PUBLISH_TIMEOUT_S)

        try:
            message_id: str = await loop.run_in_executor(None, _do_publish)
            logger.debug("Published job_id=%s pubsub_message_id=%s", job_id, message_id)
            published += 1
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Pub/Sub publish failed: job_id=%s topic=%s error_type=%s error=%s",
                job_id,
                topic_path,
                type(exc).__name__,
                exc,
            )
            failures += 1

    return published, failures


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.apify_receiver:app", host="0.0.0.0", port=8080, workers=1)
