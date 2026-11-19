"""services/apify_receiver.py — Apify webhook ingestion consumer (Cloud Run).

Real Apify integration flow:
    Apify Actor run completes
        → ACTOR.RUN.SUCCEEDED (or opt-in ACTOR.RUN.ABORTED) webhook fires
        → This endpoint validates and parses the event
        → Extracts resource.defaultDatasetId
        → Fetches actual scraped items from Apify Dataset API (httpx async)
        → Normalizes items via services/normalizers/apify_career_scraper.py
        → Deduplicates against BigQuery (raw_job_postings)
        → Inserts only novel RawJob records into BigQuery
        → Publishes ONLY newly inserted job_ids to Pub/Sub (async-safe)

Idempotency strategy: OPTION 2 — job-level deduplication.
    Legacy recovery selects NEW_RAW and may republish previously published jobs.
    With PUBLICATION_LEDGER_ENABLED, guarded DML persists jobs and publication
    intentions atomically. Confirmed publications are excluded from recovery.
    Broker confirmation is independent of embedding progress; crash-window
    duplicates still require idempotent workers. See docs/ingestion-publications.md.

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
from time import perf_counter
from typing import Any, cast

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import pubsub_v1

from config.settings import get_settings
from observability.ingestion_logging import (
    COUNTERS,
    configure_ingestion_logging,
    context,
    emit,
    error_fields,
    request_context,
    stage,
)
from observability.setup import flush_observability, setup_observability
from repositories.raw_jobs_repo import RawJobRepository
from services.normalizers.batch import normalize_batch

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
    settings = get_settings()
    restore_logging = configure_ingestion_logging(
        environment=settings.environment, level=settings.ingestion_log_level
    )
    try:
        setup_observability()
        logger.info("apify-receiver started")
        emit(
            "ingestion.runtime.configured",
            bq_location=settings.bq_location,
            ledger_enabled=settings.publication_ledger_enabled,
        )
        yield
    finally:
        try:
            flush_observability()
        finally:
            logger.info("apify-receiver shutting down")
            restore_logging()


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


# 1-starting point


@app.post("/webhook/apify", status_code=status.HTTP_202_ACCEPTED)
async def apify_webhook(request: Request) -> dict[str, Any]:
    """Receive an Apify event with safe, request-scoped lifecycle logging."""
    with request_context(request.headers.get("X-Request-ID")) as state:
        emit("ingestion.webhook.received")
        try:
            result = await _ingest_webhook(request)
        except asyncio.CancelledError as exc:
            fields: dict[str, object] = {}
            if state.stage == "bigquery":
                fields["persistence_outcome"] = "unconfirmed"
            elif state.stage == "pubsub":
                fields["reason"] = "publication_confirmation_incomplete"
            emit(
                "ingestion.cancelled",
                logging.WARNING,
                outcome="cancelled",
                error_category="operation_cancelled",
                duration_ms=round((perf_counter() - state.started) * 1000),
                **state.counters,
                **error_fields(exc),
                **fields,
            )
            raise
        except Exception as exc:
            code = exc.status_code if isinstance(exc, HTTPException) else 500
            rejected = code < 500 and state.stage in {
                "authentication",
                "event_validation",
                "actor_authorization",
            }
            category = "request_rejected" if rejected else "operation_failed"
            fields = {}
            if state.stage == "bigquery":
                category = "persistence_unconfirmed"
                fields["persistence_outcome"] = "unconfirmed"
            emit(
                "ingestion.failed",
                logging.WARNING if rejected else logging.ERROR,
                error_category=category,
                status_code=code,
                duration_ms=round((perf_counter() - state.started) * 1000),
                **state.counters,
                **error_fields(exc),
                **fields,
            )
            raise
        if state.ignored:
            emit(
                "ingestion.ignored",
                logging.INFO,
                outcome="ignored",
                duration_ms=round((perf_counter() - state.started) * 1000),
                **state.counters,
            )
            return result
        counts = {key: result.get(key, state.counters.get(key) or 0) for key in COUNTERS}
        outcome = "success"
        if counts["publish_failures"] and not counts["published"]:
            outcome = "publication_unconfirmed"
        elif counts["publish_failures"] or state.incomplete:
            outcome = "partial"
        elif counts["rejected"]:
            outcome = "completed_with_rejections"
        elif counts["received"] == 0:
            outcome = "empty"
        emit(
            "ingestion.completed",
            logging.WARNING
            if counts["publish_failures"] or state.incomplete or counts["rejected"]
            else logging.INFO,
            outcome=outcome,
            subscriber_processing="not_observed",
            duration_ms=round((perf_counter() - state.started) * 1000),
            **counts,
        )
        return result


async def _ingest_webhook(request: Request) -> dict[str, Any]:
    """Receive a success event, or an explicitly enabled aborted-run event.

    The webhook body is a small event payload — NOT the scraped data.
    This endpoint:
      1. Validates the HMAC signature (if APIFY_WEBHOOK_SECRET is configured).
      2. Validates event eligibility and exact agreement with the verified run status.
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

    stage("authentication")
    state = context()
    if state:
        state.secrets = [
            value
            for name in (
                "apify_api_token",
                "apify_webhook_secret",
                "litellm_proxy_api_key",
                "telegram_bot_token",
                "e2b_api_key",
                "langfuse_secret_key",
                "reconciliation_secret",
            )
            if isinstance(value := getattr(settings, name, None), str) and value
        ]
        for header in (
            "Authorization",
            "X-Apify-Signature",
            "X-Apify-Webhook-Secret",
            "X-Dev-Token",
        ):
            value = request.headers.get(header, "")
            if value:
                state.secrets.append(value)
                if header == "Authorization" and value.startswith("Bearer "):
                    state.secrets.append(value[7:].strip())

    # ── Step 1: Read raw body before any parsing ───────────────────────────────
    body = await request.body()

    # ── Step 2: Inbound Authentication Enforcement ────────────────────────────
    if settings.is_production and not settings.apify_webhook_secret:
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
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid webhook credentials",
            )
        emit("ingestion.authentication.verified", verified=True, authentication_required=True)
    else:
        emit(
            "ingestion.authentication.skipped",
            verified=False,
            authentication_required=False,
            reason="authentication_not_configured",
        )

    # ── Step 3: Parse JSON body ────────────────────────────────────────────────
    stage("event_validation")
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
    expected_status = None
    if event_type == "ACTOR.RUN.SUCCEEDED":
        expected_status = "SUCCEEDED"
    elif event_type == "ACTOR.RUN.ABORTED" and settings.apify_ingest_aborted_runs:
        expected_status = "ABORTED"
    if expected_status is None:
        if state:
            state.ignored = True
        return {
            "received": event_type,
            "inserted": 0,
            "published": 0,
            "message": (
                "ABORTED-run ingestion is disabled."
                if event_type == "ACTOR.RUN.ABORTED"
                else f"Event type '{event_type}' ignored; only ACTOR.RUN.SUCCEEDED and enabled ACTOR.RUN.ABORTED events are processed."
            ),
        }

    # ── Step 5: Extract & validate actor run metadata ──────────────────────────
    resource: dict[str, Any] = payload.get("resource", {})
    if not isinstance(resource, dict) or not resource:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing resource object",
        )

    run_status: str = resource.get("status", expected_status)
    if run_status != expected_status:
        if state:
            state.ignored = True
        return {
            "received": run_status,
            "inserted": 0,
            "published": 0,
            "message": f"Actor run status '{run_status}' is not {expected_status}; skipped.",
        }

    actor_run_id: str = (
        resource.get("id") or payload.get("eventData", {}).get("actorRunId") or ""
    ).strip()
    actor_id: str = (
        resource.get("actId") or payload.get("eventData", {}).get("actorId") or ""
    ).strip()
    dataset_id: str = resource.get("defaultDatasetId", "").strip()

    if not actor_run_id or not actor_id or not dataset_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Webhook payload missing required fields (id, actId, defaultDatasetId)",
        )

    if state:
        state.actor_run_id = actor_run_id
        state.dataset_id = dataset_id
    emit("ingestion.event.validated")
    stage("actor_authorization")

    # ── Step 5b: Actor allowlist authorization ────────────────────────────────
    if settings.apify_allowed_actor_ids and actor_id not in settings.apify_allowed_actor_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Actor ID '{actor_id}' is not authorized",
        )

    # ── Step 6: Verify actor run directly with Apify API ───────────────────────
    emit("ingestion.actor.authorized", allowlist_enforced=bool(settings.apify_allowed_actor_ids))
    if not settings.apify_api_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Apify API token is not configured. "
                "Set APIFY_API_TOKEN in .env (local) or the 'apify-api-token' "
                "Secret Manager secret (production)."
            ),
        )

    stage("actor_verification")
    emit("ingestion.actor.verification_started")
    verified_run = await _verify_actor_run(actor_run_id, settings.apify_api_token)
    verified_status = verified_run.get("status")
    if verified_status != expected_status:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Verified actor run status is '{verified_status}', expected '{expected_status}'",
        )

    verified_act_id = verified_run.get("actId") or verified_run.get("actorId")
    if settings.apify_allowed_actor_ids and verified_act_id not in settings.apify_allowed_actor_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Verified actor ID '{verified_act_id}' is not authorized",
        )

    if state:
        state.source_run_status = verified_status
    emit(
        "ingestion.actor.verified",
        verified=True,
        allowlist_enforced=bool(settings.apify_allowed_actor_ids),
    )
    stage("dataset_verification")
    if expected_status == "ABORTED" and (
        not isinstance(verified_run.get("defaultDatasetId"), str)
        or not verified_run["defaultDatasetId"].strip()
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Verified aborted run is missing a valid defaultDatasetId",
        )
    verified_dataset_id = (verified_run.get("defaultDatasetId") or "").strip()
    if verified_dataset_id and verified_dataset_id != dataset_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Dataset ID mismatch: payload specifies '{dataset_id}' but run produced '{verified_dataset_id}'",
        )

    if not verified_dataset_id and state:
        state.incomplete = True
    emit(
        "ingestion.dataset.verified",
        logging.INFO if verified_dataset_id else logging.WARNING,
        verified=bool(verified_dataset_id),
    )
    if expected_status == "ABORTED":
        emit(
            "ingestion.actor.aborted",
            logging.WARNING,
            reason="partial_source_dataset",
        )
    stage("dataset_fetch")
    emit("ingestion.dataset.fetch_started")
    raw_items = await _fetch_dataset_items(dataset_id, settings.apify_api_token)

    if not raw_items:
        emit("ingestion.dataset.fetched", received=len(raw_items))
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

    emit("ingestion.dataset.fetched", received=len(raw_items))

    # ── Step 7: Normalize items into canonical RawJob records ─────────────────
    stage("normalization")
    emit("ingestion.normalization.started", received=len(raw_items))
    batch = normalize_batch(raw_items)
    jobs = batch.jobs
    normalized_count = batch.normalized
    in_batch_duplicates = batch.in_batch_duplicates
    skipped = batch.rejected
    apply_url_fallback_count = batch.apply_url_fallback

    emit(
        "ingestion.normalization.completed",
        logging.WARNING if skipped else logging.INFO,
        received=len(raw_items),
        normalized=normalized_count,
        rejected=skipped,
        unique=len(jobs),
        in_batch_duplicates=in_batch_duplicates,
        apply_url_fallback=apply_url_fallback_count,
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

    if settings.publication_ledger_enabled:
        from services.ingestion_publications import ingest_with_publications

        return await ingest_with_publications(
            batch,
            dataset_id=dataset_id,
            actor_run_id=actor_run_id,
            settings=settings,
        )

    # ── Step 8: Batch insert with deduplication ────────────────────────────────

    stage("bigquery")
    emit("ingestion.bigquery.started", unique=len(jobs))
    repo = RawJobRepository()
    try:
        inserted_ids: list[str] = await repo.batch_insert_jobs(jobs)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="BigQuery insert failed; see service logs",
        ) from exc

    inserted_count = len(inserted_ids)
    duplicates_count = len(jobs) - inserted_count
    emit(
        "ingestion.bigquery.completed",
        inserted=inserted_count,
        unique=len(jobs),
        existing_duplicates=duplicates_count,
    )

    # ── Step 8b: Recover saved-but-unembedded jobs from this batch on retry ─────
    # If any jobs in this batch already exist in BigQuery but are still in NEW_RAW
    # status (e.g. from an earlier webhook delivery where Pub/Sub publish failed),
    # recover them so that an Apify webhook retry automatically republishes them.
    jobs_to_publish = list(inserted_ids)
    recovered_retry_ids: list[str] = []
    recovery_unconfirmed = False
    stage("recovery")
    emit("ingestion.recovery.started")
    if duplicates_count > 0:
        all_batch_ids = [j.job_id for j in jobs]
        existing_batch_ids = [jid for jid in all_batch_ids if jid not in set(inserted_ids)]
        if existing_batch_ids:
            try:
                unembedded_existing = await repo.get_unembedded_job_ids(job_ids=existing_batch_ids)
                if unembedded_existing:
                    recovered_retry_ids = unembedded_existing
                    for jid in recovered_retry_ids:
                        if jid not in jobs_to_publish:
                            jobs_to_publish.append(jid)
            except Exception as exc:  # noqa: BLE001
                recovery_unconfirmed = True
                state = context()
                if state:
                    state.incomplete = True
                emit(
                    "ingestion.recovery.failed",
                    logging.WARNING,
                    error_category="recovery_unconfirmed",
                    **error_fields(exc),
                )

    emit(
        "ingestion.recovery.completed",
        logging.WARNING if recovery_unconfirmed else logging.INFO,
        recovered=len(recovered_retry_ids),
        outcome="unconfirmed" if recovery_unconfirmed else "success",
    )
    # ── Step 9: Publish jobs_to_publish to Pub/Sub ────────────────────────────

    published_count = 0
    publish_failures = 0
    if jobs_to_publish:
        stage("pubsub")
        emit("ingestion.pubsub.started")
        published_count, publish_failures = await _publish_job_ids(
            jobs_to_publish,
            project_id=settings.gcp_project_id,
            topic=settings.pubsub_embedding_topic,
        )
        emit(
            "ingestion.pubsub.completed",
            logging.WARNING if publish_failures else logging.INFO,
            published=published_count,
            publish_failures=publish_failures,
            outcome=("unconfirmed" if not published_count else "partial")
            if publish_failures
            else "success",
            subscriber_processing="not_observed",
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
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    min_age_seconds: int = Query(0, ge=0, le=31536000),
) -> dict[str, Any]:
    """Endpoint to trigger out-of-band reconciliation of saved-but-unembedded jobs."""
    secret = get_settings().reconciliation_secret
    if not secret:
        raise HTTPException(503, "Reconciliation authentication is not configured")
    header = request.headers.get("Authorization", "")
    if not hmac.compare_digest(header, f"Bearer {secret}"):
        raise HTTPException(401, "Invalid reconciliation credentials")
    try:
        return await reconcile_unpublished_jobs(limit=limit, min_age_seconds=min_age_seconds)
    except HTTPException:
        raise
    except Exception as exc:
        emit(
            "ingestion.recovery.failed",
            logging.ERROR,
            error_category="recovery_unconfirmed",
            **error_fields(exc),
        )
        raise HTTPException(502, "Reconciliation outcome unconfirmed") from exc


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

    if not 1 <= limit <= 1000 or not 0 <= min_age_seconds <= 31536000:
        raise ValueError("Invalid reconciliation bounds")
    if settings.publication_ledger_enabled:
        from services.publication_dispatcher import dispatch_pending

        publications = active_repo.publications
        if publications is None:
            raise ValueError("Publication ledger is not configured")
        published = failures = reconciled = 0
        ids = []
        # Offset is a claim-budget counter, not a SQL OFFSET. Claims leave the
        # eligible set while leased; bound the loop even when every send is skipped.
        for offset in range(0, limit, 100):
            page_limit = min(100, limit - offset)
            result = await dispatch_pending(
                publications,
                project_id=settings.gcp_project_id,
                topic=settings.pubsub_embedding_topic,
                limit=page_limit,
                min_age_seconds=min_age_seconds,
            )
            ids.extend(result.attempted_ids)
            reconciled += len(result.attempted_ids)
            published += len(result.confirmed)
            failures += len(result.unconfirmed_ids)
            claimed_count = (
                result.claimed_count
                if result.claimed_count is not None
                else len(result.attempted_ids)
            )
            if claimed_count < page_limit:
                break
        emit(
            "ingestion.recovery.completed",
            logging.WARNING if failures else logging.INFO,
            recovered=reconciled,
            published=published,
            publish_failures=failures,
        )
        return {
            "reconciled": reconciled,
            "published": published,
            "publish_failures": failures,
            "job_ids": ids,
        }

    emit("ingestion.recovery.started")
    unembedded_ids = await active_repo.get_unembedded_job_ids(
        job_ids=None,
        limit=limit,
        min_age_seconds=min_age_seconds,
    )

    if not unembedded_ids:
        emit("ingestion.recovery.completed", recovered=0, published=0, publish_failures=0)
        return {"reconciled": 0, "published": 0, "publish_failures": 0, "job_ids": []}

    emit("ingestion.recovery.discovered", recovered=len(unembedded_ids))

    published_count, publish_failures = await _publish_job_ids(
        unembedded_ids,
        project_id=settings.gcp_project_id,
        topic=settings.pubsub_embedding_topic,
    )

    emit(
        "ingestion.recovery.completed",
        logging.WARNING if publish_failures else logging.INFO,
        recovered=len(unembedded_ids),
        published=published_count,
        publish_failures=publish_failures,
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
        emit("ingestion.apify.request_failed", logging.DEBUG, error_category="upstream_timeout")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Apify Actor Run API request timed out",
        ) from exc
    except httpx.RequestError as exc:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not reach Apify Actor Run API",
        ) from exc

    if response.status_code == 404:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Apify actor run '{actor_run_id}' not found",
        )

    if not response.is_success:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Apify Actor Run API returned HTTP {response.status_code}",
        )

    try:
        data = response.json()
    except Exception as exc:  # noqa: BLE001
        emit(
            "ingestion.apify.request_failed", logging.DEBUG, error_category="invalid_upstream_json"
        )
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
        emit("ingestion.apify.request_failed", logging.DEBUG, error_category="upstream_timeout")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Apify Dataset API request timed out after {_DATASET_FETCH_TIMEOUT_S}s",
        ) from exc
    except httpx.RequestError as exc:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not reach Apify Dataset API",
        ) from exc

    if response.status_code == 401:
        # Do NOT log the token — log only that authentication failed.
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_authentication",
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Apify Dataset API authentication failed; check APIFY_API_TOKEN configuration",
        )

    if response.status_code == 404:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Apify dataset '{dataset_id}' not found",
        )

    if not response.is_success:
        emit(
            "ingestion.apify.request_failed",
            logging.DEBUG,
            error_category="upstream_request_failed",
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Apify Dataset API returned HTTP {response.status_code}",
        )

    try:
        data = response.json()
    except Exception as exc:  # noqa: BLE001
        emit(
            "ingestion.apify.request_failed", logging.DEBUG, error_category="invalid_upstream_json"
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

    emit("ingestion.apify.request_failed", logging.DEBUG, error_category="invalid_upstream_shape")
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
            await loop.run_in_executor(None, _do_publish)

            published += 1
        except Exception as exc:  # noqa: BLE001
            if failures == 0:
                emit(
                    "ingestion.pubsub.publish_unconfirmed",
                    logging.WARNING,
                    error_category="publication_unconfirmed",
                    **error_fields(exc),
                )
            failures += 1

    return published, failures


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("services.apify_receiver:app", host="0.0.0.0", port=8080, workers=1)
