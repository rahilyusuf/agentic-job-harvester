"""services/embedding_worker.py — Pre-computes embeddings (text-embedding-004).

Cloud Run service triggered by Pub/Sub job-embedding-queue messages.
Reads job from raw_job_postings, generates 768-dim embedding via gateway/client.py,
writes to job_embeddings, and updates job status to EMBEDDING_DONE.

ARCHITECTURE.md: "calls text-embedding-004 on f'{title} {company} {location} {description[:2000]}'"
"""

from __future__ import annotations

import base64
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from config.settings import get_settings
from gateway.client import generate_embedding
from observability.setup import setup_observability
from repositories.embeddings_repo import EmbeddingRepository
from repositories.raw_jobs_repo import RawJobRepository
from schemas.job_models import JobStatus, RawJob

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


@app.post("/", status_code=status.HTTP_204_NO_CONTENT)
async def pubsub_push(request: Request) -> None:
    """Handle Pub/Sub push message containing a job_id to embed."""
    envelope = await request.json()
    message = envelope.get("message", {})
    data_b64 = message.get("data", "")

    try:
        data_str = base64.b64decode(data_b64).decode("utf-8")
        payload = json.loads(data_str)
        job_id = payload["job_id"]
    except (KeyError, ValueError, Exception) as exc:  # noqa: BLE001
        logger.error("Failed to parse Pub/Sub message: %s", exc)
        # Return 200 to ack and discard malformed messages
        return

    await _process_job(job_id)


async def _process_job(job_id: str) -> None:
    """Fetch job, generate embedding, write to BQ."""
    raw_repo = RawJobRepository()
    emb_repo = EmbeddingRepository()

    job = await raw_repo.get_job(job_id)
    if job is None:
        logger.warning("Job not found for embedding: job_id=%s", job_id)
        return

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.embedding_worker:app", host="0.0.0.0", port=8080, workers=1)
