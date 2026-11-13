"""services/apify_receiver.py — Apify/n8n webhook ingestion consumer (Cloud Run).

Receives raw job payloads from Apify actors or n8n workflows via HTTP POST.
Validates payload, deduplicates, inserts into raw_job_postings, and publishes
job_id to the job-embedding-queue Pub/Sub topic.

ARCHITECTURE.md: "dumb ingestion consumer" — no scoring logic here.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import pubsub_v1

from config.settings import get_settings
from observability.setup import setup_observability
from repositories.raw_jobs_repo import RawJobRepository
from schemas.job_models import RawJob
from utils.ids import compute_job_id
from utils.time import utcnow

logger = logging.getLogger(__name__)


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Startup and shutdown lifecycle."""
    setup_observability()
    logger.info("apify-receiver started")
    yield
    logger.info("apify-receiver shutting down")


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="apify-receiver",
    description="Ingestion webhook consumer for Apify/n8n job scraper payloads",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://api.apify.com"],
    allow_methods=["POST"],
)


# ── Dependencies ───────────────────────────────────────────────────────────────

def get_raw_jobs_repo() -> RawJobRepository:
    return RawJobRepository()


def get_publisher() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint for Cloud Run liveness probe."""
    return {"status": "ok"}


@app.post("/webhook/apify", status_code=status.HTTP_202_ACCEPTED)
async def apify_webhook(request: Request) -> dict[str, Any]:
    """Receive a batch of job postings from an Apify actor webhook.

    Expected payload format:
    {
        "jobs": [
            {
                "url": "https://...",
                "title": "...",
                "company": "...",
                "description": "...",
                "location": "...",
                "salary": "...",
                "ats_id": "optional-native-id",
                "ats_platform": "lever|workable|greenhouse|unknown"
            }, ...
        ]
    }
    """
    settings = get_settings()

    # HMAC validation
    signature = request.headers.get("X-Apify-Signature", "")
    body = await request.body()
    if settings.apify_webhook_secret:
        expected = hmac.new(
            settings.apify_webhook_secret.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid webhook signature",
            )

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON: {exc}",
        ) from exc

    raw_jobs_list: list[dict] = payload.get("jobs", [])
    if not raw_jobs_list:
        return {"inserted": 0, "message": "No jobs in payload"}

    # Build RawJob models
    jobs: list[RawJob] = []
    for item in raw_jobs_list:
        try:
            job_url = item.get("url", "")
            job_id = compute_job_id(job_url, ats_id=item.get("ats_id"))
            jobs.append(
                RawJob(
                    job_id=job_id,
                    source=item.get("source", "apify"),
                    job_url=job_url,
                    title=item.get("title", ""),
                    company=item.get("company", ""),
                    location=item.get("location"),
                    description=item.get("description", ""),
                    salary_raw=item.get("salary"),
                    employment_type_raw=item.get("employment_type"),
                    ats_platform=item.get("ats_platform", "unknown"),
                    scraped_at=utcnow(),
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping malformed job payload: %s | %s", item.get("url"), exc)

    if not jobs:
        return {"inserted": 0, "message": "All jobs in payload were malformed"}

    # Batch insert (dedup handled in repo)
    repo = get_raw_jobs_repo()
    inserted = await repo.batch_insert_jobs(jobs)

    # Publish job_ids to embedding queue
    if inserted > 0:
        publisher = get_publisher()
        topic_path = publisher.topic_path(
            settings.gcp_project_id, settings.pubsub_embedding_topic
        )
        inserted_jobs = [j for j in jobs]  # all attempted — repo deduped
        for job in inserted_jobs:
            msg = json.dumps({"job_id": job.job_id}).encode("utf-8")
            try:
                publisher.publish(topic_path, msg)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to publish job_id=%s: %s", job.job_id, exc)

    logger.info("apify_webhook: received=%d inserted=%d", len(jobs), inserted)
    return {"received": len(jobs), "inserted": inserted}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.apify_receiver:app", host="0.0.0.0", port=8080, workers=1)
