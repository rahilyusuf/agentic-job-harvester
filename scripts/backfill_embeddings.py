"""scripts/backfill_embeddings.py — One-off embedding backfill for existing raw_job_postings.

Run after initial data load to generate embeddings for all EMBEDDING_DONE-eligible jobs.
Usage:
    python scripts/backfill_embeddings.py [--limit 1000] [--batch-size 50]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


async def backfill(limit: int, batch_size: int) -> None:
    from config.settings import get_settings
    from gateway.client import generate_embedding
    from repositories.embeddings_repo import EmbeddingRepository
    from repositories.raw_jobs_repo import RawJobRepository
    from schemas.job_models import JobStatus

    settings = get_settings()
    raw_repo = RawJobRepository()
    emb_repo = EmbeddingRepository()

    logger.info("Starting embedding backfill (limit=%d, batch_size=%d)", limit, batch_size)

    total_processed = 0
    total_failed = 0
    offset = 0

    while total_processed < limit:
        batch = await raw_repo.get_jobs_pending_embedding(limit=min(batch_size, limit - total_processed))
        if not batch:
            logger.info("No more jobs to embed. Done.")
            break

        for job in batch:
            try:
                text = f"{job.title} {job.company} {job.location or ''} {job.description[:2000]}"
                embedding = await generate_embedding(text)
                await emb_repo.upsert_embedding(job.job_id, embedding)
                await raw_repo.update_status(job.job_id, JobStatus.EMBEDDING_DONE.value)
                total_processed += 1
                if total_processed % 100 == 0:
                    logger.info("Progress: %d/%d embedded", total_processed, limit)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to embed job_id=%s: %s", job.job_id, exc)
                total_failed += 1

    logger.info(
        "Backfill complete: %d embedded, %d failed.", total_processed, total_failed
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill job embeddings")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(backfill(limit=args.limit, batch_size=args.batch_size))
