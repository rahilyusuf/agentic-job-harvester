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
import sys
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
from services.normalizers.batch import normalize_batch
from schemas.job_models import RawJob

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_APIFY_DATASET_API = "https://api.apify.com/v2/datasets/{dataset_id}/items"
_APIFY_ACTOR_RUN_API = "https://api.apify.com/v2/actor-runs/{actor_run_id}"
_DATASET_FETCH_TIMEOUT_S = 60.0  # seconds; dataset fetch can be large
_RUN_VERIFY_TIMEOUT_S = 15.0  # seconds; metadata check
_PUBSUB_PUBLISH_TIMEOUT_S = 10.0  # seconds per message


# ── Lifespan ───────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Startup and shutdown lifecycle."""
    # Enable INFO-level logging for all application modules.
    #
    # Why this approach (not basicConfig(force=True)):
    #   - Uvicorn installs handlers on its OWN named loggers (uvicorn,
    #     uvicorn.error, uvicorn.access) with propagate=False.  It leaves
    #     the root logger untouched (level=WARNING, zero handlers).
    #   - Our app loggers (services.*, repositories.*, etc.) propagate to
    #     root by default.  Raising root to INFO means they emit; adding a
    #     single root StreamHandler gives them somewhere to send records.
    #   - force=True would discard uvicorn’s handlers — avoided here so
    #     uvicorn’s own access/error logs continue working normally.
    _LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"
    _LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not root_logger.handlers:
        _handler = logging.StreamHandler(sys.stdout)
        _handler.setFormatter(logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        root_logger.addHandler(_handler)
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


# starting point


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

    logger.info("── APIFY WEBHOOK START ──────────────────────────────────────")

    # ── Step 1: Read raw body before any parsing ───────────────────────────────
    body = await request.body()

    # ── Step 2: Inbound Authentication Enforcement ────────────────────────────
    if settings.is_production and not settings.apify_webhook_secret:
        logger.error("APIFY_WEBHOOK_SECRET is not configured in production environment!")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Webhook authentication secret is not configured in production",
        )

    if settings.apify_webhook_secret:
        signature = request.headers.get("X-Apify-Signature", "")
        shared_secret = request.headers.get("X-Apify-Webhook-Secret", "")
        auth_header = request.headers.get("Authorization", "")
        bearer_secret = auth_header[7:].strip() if auth_header.startswith("Bearer ") else ""

        expected_hmac = hmac.new(
            settings.apify_webhook_secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()

        is_valid_hmac = bool(signature and hmac.compare_digest(signature, expected_hmac))
        is_valid_secret_header = bool(
            shared_secret and hmac.compare_digest(shared_secret, settings.apify_webhook_secret)
        )
        is_valid_bearer = bool(
            bearer_secret and hmac.compare_digest(bearer_secret, settings.apify_webhook_secret)
        )

        if not (is_valid_hmac or is_valid_secret_header or is_valid_bearer):
            client_ip = request.client.host if request.client else "unknown"
            logger.warning(
                "Unauthorized webhook request from %s (missing or invalid credentials)",
                client_ip,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid webhook credentials",
            )

    # ── Step 3: Parse JSON body ────────────────────────────────────────────────
    try:
        payload: dict[str, Any] = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON body: {exc}",
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook payload must be a JSON object",
        )

    # ── Step 4: Validate event type ────────────────────────────────────────────
    event_type: str = payload.get("eventType", "")
    if not event_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing eventType",
        )
    if event_type != "ACTOR.RUN.SUCCEEDED":
        logger.info("Ignoring non-success event type: %s", event_type)
        return {
            "received": event_type,
            "inserted": 0,
            "published": 0,
            "message": f"Event type '{event_type}' ignored; only ACTOR.RUN.SUCCEEDED is processed.",
        }

    # ── Step 5: Extract & validate actor run metadata ──────────────────────────
    resource: dict[str, Any] = payload.get("resource", {})
    if not isinstance(resource, dict) or not resource:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing resource object",
        )

    run_status: str = resource.get("status", "SUCCEEDED")
    if run_status != "SUCCEEDED":
        logger.info("Actor run status '%s' is not SUCCEEDED; skipping", run_status)
        return {
            "received": run_status,
            "inserted": 0,
            "published": 0,
            "message": f"Actor run status '{run_status}' is not SUCCEEDED; skipped.",
        }

    actor_run_id: str = (
        resource.get("id") or payload.get("eventData", {}).get("actorRunId") or ""
    ).strip()
    actor_id: str = (
        resource.get("actId") or payload.get("eventData", {}).get("actorId") or ""
    ).strip()
    dataset_id: str = resource.get("defaultDatasetId", "").strip()

    if not actor_run_id or not actor_id or not dataset_id:
        logger.error(
            "Malformed webhook payload: missing id, actId, or defaultDatasetId | run_id=%s act_id=%s dataset_id=%s",
            actor_run_id,
            actor_id,
            dataset_id,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing required fields (id, actId, defaultDatasetId)",
        )

    logger.info(
        "── WEBHOOK PARSED  actor_id=%s  actor_run_id=%s  dataset_id=%s",
        actor_id,
        actor_run_id,
        dataset_id,
    )

    # ── Step 5b: Actor allowlist authorization ────────────────────────────────
    if settings.apify_allowed_actor_ids and actor_id not in settings.apify_allowed_actor_ids:
        logger.warning(
            "Unauthorized actor_id=%s not in allowlist: %s",
            actor_id,
            settings.apify_allowed_actor_ids,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Actor ID '{actor_id}' is not authorized",
        )

    # ── Step 6: Verify actor run directly with Apify API ───────────────────────
    if not settings.apify_api_token:
        logger.error("apify_api_token is not configured; cannot verify run or fetch dataset items")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Apify API token is not configured. "
                "Set APIFY_API_TOKEN in .env (local) or the 'apify-api-token' "
                "Secret Manager secret (production)."
            ),
        )

    logger.info("── VERIFYING ACTOR RUN  actor_run_id=%s", actor_run_id)
    verified_run = await _verify_actor_run(actor_run_id, settings.apify_api_token)
    verified_status = verified_run.get("status")
    if verified_status != "SUCCEEDED":
        logger.warning(
            "Verified actor run status is not SUCCEEDED: status=%s actor_run_id=%s",
            verified_status,
            actor_run_id,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Verified actor run status is '{verified_status}', expected 'SUCCEEDED'",
        )

    verified_act_id = verified_run.get("actId") or verified_run.get("actorId")
    if settings.apify_allowed_actor_ids and verified_act_id not in settings.apify_allowed_actor_ids:
        logger.warning(
            "Verified actor run actId=%s is not in allowed actor IDs: %s",
            verified_act_id,
            settings.apify_allowed_actor_ids,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Verified actor ID '{verified_act_id}' is not authorized",
        )

    verified_dataset_id = (verified_run.get("defaultDatasetId") or "").strip()
    if verified_dataset_id and verified_dataset_id != dataset_id:
        logger.error(
            "Dataset ID mismatch: payload specifies '%s' but verified run produced '%s'",
            dataset_id,
            verified_dataset_id,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Dataset ID mismatch: payload specifies '{dataset_id}' but run produced '{verified_dataset_id}'",
        )

    logger.info("── FETCHING DATASET  dataset_id=%s", dataset_id)
    raw_items = await _fetch_dataset_items(dataset_id, settings.apify_api_token)

    if not raw_items:
        logger.info(
            "── DATASET FETCHED  item_count=0  dataset_id=%s — nothing to process", dataset_id
        )
        return {
            "dataset_id": dataset_id,
            "received": 0,
            "normalized": 0,
            "rejected": 0,
            "unique": 0,
            "in_batch_duplicates": 0,
            "inserted": 0,
            "published": 0,
        }

    logger.info("── DATASET FETCHED  item_count=%d  dataset_id=%s", len(raw_items), dataset_id)

    # ── Step 7: Normalize items into canonical RawJob records ─────────────────
    logger.info("── NORMALIZATION START  items=%d", len(raw_items))
    batch = normalize_batch(raw_items)
    jobs = batch.jobs
    normalized_count = batch.normalized
    in_batch_duplicates = batch.in_batch_duplicates
    skipped = batch.rejected
    apply_url_fallback_count = batch.apply_url_fallback

    logger.info(
        "── NORMALIZATION COMPLETE  received=%d  valid=%d  skipped=%d  apply_url_fallback=%d"
        "  unique=%d  in_batch_duplicates=%d",
        len(raw_items),
        normalized_count,
        skipped,
        apply_url_fallback_count,
        len(jobs),
        in_batch_duplicates,
    )

    if not jobs:
        return {
            "dataset_id": dataset_id,
            "received": len(raw_items),
            "normalized": normalized_count,
            "rejected": skipped,
            "unique": len(jobs),
            "in_batch_duplicates": in_batch_duplicates,
            "inserted": 0,
            "published": 0,
            "apply_url_fallback": apply_url_fallback_count,
            "message": "All items were malformed or had insufficient description",
        }

    # ── Step 8: Batch insert with deduplication ────────────────────────────────

    logger.info("── BIGQUERY PERSIST START  jobs_to_insert=%d", len(jobs))
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
    duplicates_count = len(jobs) - inserted_count
    logger.info(
        "── BIGQUERY PERSIST COMPLETE  novel_inserted=%d  duplicates_skipped=%d",
        inserted_count,
        duplicates_count,
    )

    # ── Step 8b: Recover saved-but-unembedded jobs from this batch on retry ─────
    # If any jobs in this batch already exist in BigQuery but are still in NEW_RAW
    # status (e.g. from an earlier webhook delivery where Pub/Sub publish failed),
    # recover them so that an Apify webhook retry automatically republishes them.
    jobs_to_publish = list(inserted_ids)
    recovered_retry_ids: list[str] = []
    if duplicates_count > 0:
        all_batch_ids = [j.job_id for j in jobs]
        existing_batch_ids = [jid for jid in all_batch_ids if jid not in set(inserted_ids)]
        if existing_batch_ids:
            try:
                unembedded_existing = await repo.get_unembedded_job_ids(job_ids=existing_batch_ids)
                if unembedded_existing:
                    logger.info(
                        "── RECOVERY: Found %d saved-but-unembedded job(s) in retried batch: %s",
                        len(unembedded_existing),
                        unembedded_existing,
                    )
                    recovered_retry_ids = unembedded_existing
                    for jid in recovered_retry_ids:
                        if jid not in jobs_to_publish:
                            jobs_to_publish.append(jid)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to query unembedded existing jobs during batch recovery: %s",
                    exc,
                )

    # ── Step 9: Publish jobs_to_publish to Pub/Sub ────────────────────────────

    published_count = 0
    publish_failures = 0
    if jobs_to_publish:
        logger.info("── PUBSUB PUBLISH START  job_ids_to_publish=%d", len(jobs_to_publish))
        published_count, publish_failures = await _publish_job_ids(
            jobs_to_publish,
            project_id=settings.gcp_project_id,
            topic=settings.pubsub_embedding_topic,
        )
        logger.info(
            "── PUBSUB PUBLISH COMPLETE  published=%d  failures=%d",
            published_count,
            publish_failures,
        )

    logger.info(
        "── APIFY WEBHOOK COMPLETE  dataset_id=%s  received=%d  normalized=%d"
        "  apply_url_fallback=%d  inserted=%d  published=%d  publish_failures=%d  recovered=%d",
        dataset_id,
        len(raw_items),
        normalized_count,
        apply_url_fallback_count,
        inserted_count,
        published_count,
        publish_failures,
        len(recovered_retry_ids),
    )

    return {
        "dataset_id": dataset_id,
        "actor_run_id": actor_run_id,
        "received": len(raw_items),
        "normalized": normalized_count,
        "rejected": skipped,
        "unique": len(jobs),
        "in_batch_duplicates": in_batch_duplicates,
        "apply_url_fallback": apply_url_fallback_count,
        "inserted": inserted_count,
        "published": published_count,
        "publish_failures": publish_failures,
        "recovered": len(recovered_retry_ids),
    }


@app.post("/jobs/reconcile", status_code=status.HTTP_200_OK)
@app.post("/reconcile", status_code=status.HTTP_200_OK)
async def trigger_reconciliation(
    limit: int = 100,
    min_age_seconds: int = 0,
) -> dict[str, Any]:
    """Endpoint to trigger out-of-band reconciliation of saved-but-unembedded jobs."""
    return await reconcile_unpublished_jobs(limit=limit, min_age_seconds=min_age_seconds)


async def reconcile_unpublished_jobs(
    *,
    limit: int = 100,
    min_age_seconds: int = 0,
    repo: RawJobRepository | None = None,
) -> dict[str, Any]:
    """Discover and publish saved jobs that remain unembedded (status=NEW_RAW).

    Guarantees that saved-but-unpublished jobs can be discovered and retried
    independently of the original webhook (e.g. via Cloud Scheduler or manual trigger).

    Args:
        limit: Maximum number of jobs to reconcile in one run.
        min_age_seconds: Minimum elapsed seconds since scraped_at (default 0).
        repo: Optional RawJobRepository instance for testing/dependency injection.

    Returns:
        Summary dict containing counts of discovered, published, and failed jobs.
    """
    settings = get_settings()
    active_repo = repo or RawJobRepository()

    logger.info("── RECONCILIATION START  limit=%d  min_age_seconds=%d", limit, min_age_seconds)
    unembedded_ids = await active_repo.get_unembedded_job_ids(
        job_ids=None,
        limit=limit,
        min_age_seconds=min_age_seconds,
    )

    if not unembedded_ids:
        logger.info("── RECONCILIATION COMPLETE  No unembedded jobs found")
        return {"reconciled": 0, "published": 0, "publish_failures": 0, "job_ids": []}

    logger.info(
        "── RECONCILIATION DISCOVERED  count=%d  job_ids=%s",
        len(unembedded_ids),
        unembedded_ids,
    )

    published_count, publish_failures = await _publish_job_ids(
        unembedded_ids,
        project_id=settings.gcp_project_id,
        topic=settings.pubsub_embedding_topic,
    )

    logger.info(
        "── RECONCILIATION COMPLETE  reconciled=%d  published=%d  publish_failures=%d",
        len(unembedded_ids),
        published_count,
        publish_failures,
    )

    return {
        "reconciled": len(unembedded_ids),
        "published": published_count,
        "publish_failures": publish_failures,
        "job_ids": unembedded_ids,
    }


# ── Private helpers ────────────────────────────────────────────────────────────


async def _verify_actor_run(
    actor_run_id: str,
    api_token: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Verify an Actor run directly with the Apify API.

    Protects against fabricated payloads by ensuring the run exists, succeeded,
    belongs to an authorized actor, and produced the claimed dataset ID.

    Args:
        actor_run_id: The Apify actor run ID.
        api_token: Authenticated Apify API token. Never logged.
        client: Optional httpx.AsyncClient for testing/dependency injection.

    Returns:
        Verified run data dict from Apify API.

    Raises:
        HTTPException: On network error, non-2xx status, or run not found.
    """
    url = _APIFY_ACTOR_RUN_API.format(actor_run_id=actor_run_id)
    headers = {
        "Authorization": f"Bearer {api_token.strip()}",
    }
    try:
        if client is not None:
            response = await client.get(url, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=_RUN_VERIFY_TIMEOUT_S) as http_client:
                response = await http_client.get(url, headers=headers)
    except httpx.TimeoutException as exc:
        logger.error("Apify Actor Run API timed out: actor_run_id=%s", actor_run_id)
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Apify Actor Run API request timed out",
        ) from exc
    except httpx.RequestError as exc:
        logger.error(
            "Apify Actor Run API request error: actor_run_id=%s error=%s", actor_run_id, exc
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not reach Apify Actor Run API",
        ) from exc

    if response.status_code == 404:
        logger.error("Apify actor run not found: actor_run_id=%s", actor_run_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Apify actor run '{actor_run_id}' not found",
        )

    if not response.is_success:
        logger.error(
            "Apify Actor Run API non-2xx response: actor_run_id=%s status=%d",
            actor_run_id,
            response.status_code,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Apify Actor Run API returned HTTP {response.status_code}",
        )

    try:
        data = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.error("Apify Actor Run API returned malformed JSON: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Apify Actor Run API returned non-JSON response",
        ) from exc

    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        return cast(dict[str, Any], data["data"])
    if isinstance(data, dict):
        return data

    raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail="Apify Actor Run API returned unexpected structure",
    )


async def _fetch_dataset_items(
    dataset_id: str,
    api_token: str,
    *,
    clean: bool = True,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """Fetch all items from an Apify dataset via the Dataset Items API.

    Args:
        dataset_id: The Apify dataset ID (from resource.defaultDatasetId).
        api_token: Authenticated Apify API token. Never logged or placed in URL.
        clean: If True, request clean=true to omit Apify-internal metadata fields.
        client: Optional httpx.AsyncClient for testing/dependency injection.

    Returns:
        List of raw dataset item dicts.

    Raises:
        HTTPException: On API auth failure, invalid dataset, timeout, or HTTP error.
    """
    url = _APIFY_DATASET_API.format(dataset_id=dataset_id)
    headers = {
        "Authorization": f"Bearer {api_token.strip()}",
    }
    params: dict[str, str | int] = {
        "format": "json",
    }
    if clean:
        params["clean"] = "true"

    try:
        if client is not None:
            response = await client.get(url, headers=headers, params=params)
        else:
            async with httpx.AsyncClient(timeout=_DATASET_FETCH_TIMEOUT_S) as http_client:
                response = await http_client.get(url, headers=headers, params=params)
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
