"""services/embedding_worker.py — Pre-computes embeddings (text-embedding-004).

Cloud Run service triggered by Pub/Sub job-embedding-queue messages.
Reads job from raw_job_postings, generates 768-dim embedding via gateway/client.py,
writes to job_embeddings, and updates job status to EMBEDDING_DONE.

ARCHITECTURE.md: "calls text-embedding-004 on f'{title} {company} {location} {description[:2000]}'"
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from config.settings import get_settings
from gateway.client import generate_embedding
from observability.ingestion_logging import emit
from observability.setup import setup_observability
from repositories.embeddings_repo import EmbeddingRepository
from repositories.raw_jobs_repo import RawJobRepository
from schemas.job_models import JobStatus, RawJob
from schemas.publication_models import QueryOutcomeUnconfirmed

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    setup_observability()
    logger.info("embedding-worker started")
    yield
    logger.info("embedding-worker shutting down")


app = FastAPI(
    title="embedding-worker",
    description="Pub/Sub consumer that pre-computes text-embedding-004 for new jobs",
    version="1.0.0",
    lifespan=lifespan,
)


def _build_embedding_text(job: RawJob) -> str:
    """Build the embedding input text per ARCHITECTURE.md contract."""
    return f"{job.title} {job.company} {job.location or ''} {job.description[:2000]}"


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def pubsub_push(request: Request) -> JSONResponse | None:
    """Handle Pub/Sub push message containing a job_id to embed."""
    try:
        envelope = await request.json()
        if not isinstance(envelope, dict) or not isinstance(envelope.get("message"), dict):
            raise ValueError("Invalid envelope")
        data_b64 = envelope["message"].get("data")
        if not isinstance(data_b64, str):
            raise ValueError("Invalid message data")
        data_str = base64.b64decode(data_b64, validate=True).decode("utf-8")
        payload = json.loads(data_str)
        if not isinstance(payload, dict):
            raise ValueError("Invalid payload")
        job_id = payload.get("job_id")
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("Invalid job identifier")
        job_id = job_id.strip()
    except (ValueError, binascii.Error):
        emit("embedding.message.invalid", logging.WARNING, error_category="malformed_message")
        # Return 204 to ACK and discard malformed messages in either mode.
        return

    if get_settings().publication_ledger_enabled:
        try:
            await _process_job(job_id)
        except Exception as exc:
            emit("embedding.processing.retry", logging.WARNING, error_type=type(exc).__name__)
            return JSONResponse(
                status_code=503, content={"detail": "Processing incomplete; retry delivery"}
            )
    else:
        await _process_job(job_id)


async def _process_job(job_id: str) -> None:
    """Fetch job, generate embedding, write to BQ."""
    if get_settings().publication_ledger_enabled:
        return await _process_job_with_claim(job_id)
    raw_repo = RawJobRepository()
    emb_repo = EmbeddingRepository()

    job = await raw_repo.get_job(job_id)
    if job is None:
        logger.warning("Job not found for embedding: job_id=%s", job_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Processing incomplete; retry delivery",
        )

    if job.status not in (JobStatus.NEW_RAW.value, JobStatus.EMBEDDING_QUEUED.value):
        logger.info("Job %s already processed (status=%s), skipping", job_id, job.status)
        return

    # Mark as in-progress
    await raw_repo.update_status(job_id, JobStatus.EMBEDDING_QUEUED.value)

    try:
        embedding_text = _build_embedding_text(job)
        embedding = await generate_embedding(embedding_text)

        await emb_repo.upsert_embedding(job_id=job_id, embedding=embedding)
        await raw_repo.update_status(job_id, JobStatus.EMBEDDING_DONE.value)

        logger.info("Embedded job_id=%s dims=%d", job_id, len(embedding))

    except Exception as exc:
        logger.error("Embedding failed for job_id=%s: %s", job_id, exc, exc_info=True)
        # Reset status so it can be retried
        await raw_repo.update_status(job_id, JobStatus.NEW_RAW.value)
        raise


async def _process_job_with_claim(
    job_id: str, *, raw_repo=None, emb_repo=None, publications=None
) -> None:
    """Concurrent duplicates retry; completed duplicates skip. No publication-state resets."""
    from repositories.publications_repo import PublicationRepository

    settings = get_settings()
    raw_repo = raw_repo if raw_repo is not None else RawJobRepository(settings=settings)
    emb_repo = emb_repo if emb_repo is not None else EmbeddingRepository()
    publications = (
        publications if publications is not None else PublicationRepository(settings=settings)
    )
    job = await raw_repo.get_job(job_id)
    if job is None:
        raise ValueError("Stored job is missing")
    if job.status not in (JobStatus.NEW_RAW, JobStatus.EMBEDDING_QUEUED):
        return
    claim = await publications.claim_processing(job_id)
    if claim.completed:
        return
    try:
        # initial_embedding_v1 has one artifact per canonical job. Recovery after an
        # older nontransactional worker write can reuse that artifact without paying again.
        embedding = await emb_repo.get_embedding(job_id)
        if embedding is None:
            embedding = await asyncio.wait_for(
                generate_embedding(_build_embedding_text(job)),
                timeout=min(
                    settings.publication_worker_timeout_seconds,
                    settings.publication_lease_seconds / 2,
                ),
            )
        await publications.finish_processing(claim, embedding)
        emit("embedding.processing.completed")
    except (asyncio.CancelledError, TimeoutError, QueryOutcomeUnconfirmed):
        # Provider/query work can still be running remotely. Preserve its lease;
        # immediate release would invite concurrent paid work or a stale commit.
        raise
    except Exception:
        try:
            await publications.release_processing(claim)
        except Exception:
            emit("embedding.processing.release_unconfirmed", logging.WARNING)
        raise


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("services.embedding_worker:app", host="0.0.0.0", port=8080, workers=1)
