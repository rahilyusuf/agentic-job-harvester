"""repositories/raw_jobs_repo.py — Concrete BigQuery adapter for raw_job_postings.

Implements IRawJobRepository. Only import this class in:
  - services/apify_receiver.py (for injection)
  - tests/repositories/  (for integration tests, marked @pytest.mark.integration)

All other code must reference IRawJobRepository from interfaces.py.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config.settings import get_settings
from schemas.job_models import JobStatus, RawJob
from utils.retry import async_retry
from utils.time import format_bq_timestamp, get_dedup_window_start

logger = logging.getLogger(__name__)


class RawJobRepository:
    """BigQuery adapter for raw_job_postings table."""

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
    ) -> None:
        settings = get_settings()
        self._client = bq_client or bigquery.Client(project=settings.gcp_project_id)
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.raw_job_postings"

    def _row_to_raw_job(self, row: bigquery.Row) -> RawJob:
        """Convert a BigQuery row to a RawJob model."""
        data = dict(row.items())
        return RawJob.model_validate(data)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def insert_job(self, job: RawJob) -> None:
        """Insert a new job. Skips silently if duplicate within dedup window."""
        if await self.exists(job.job_id):
            logger.debug("Skipping duplicate job_id=%s", job.job_id)
            return

        row = job.model_dump(mode="json")
        errors = self._client.insert_rows_json(self._table, [row])
        if errors:
            raise RuntimeError(f"BQ insert errors for job_id={job.job_id}: {errors}")
        logger.info("Inserted job_id=%s", job.job_id)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def batch_insert_jobs(self, jobs: list[RawJob]) -> int:
        """Batch insert jobs. Returns count of novel (non-duplicate) inserts."""
        if not jobs:
            return 0

        # Check all job_ids in one BQ query
        ids = [j.job_id for j in jobs]
        placeholder = ", ".join(f"'{jid}'" for jid in ids)
        window_start = format_bq_timestamp(get_dedup_window_start())
        query = f"""
            SELECT job_id FROM `{self._table}`
            WHERE job_id IN ({placeholder})
              AND scraped_at >= '{window_start}'
        """
        existing = {row.job_id for row in self._client.query_and_wait(query)}

        novel_jobs = [j for j in jobs if j.job_id not in existing]
        if not novel_jobs:
            logger.info("All %d jobs are duplicates; nothing inserted.", len(jobs))
            return 0

        rows = [j.model_dump(mode="json") for j in novel_jobs]
        errors = self._client.insert_rows_json(self._table, rows)
        if errors:
            raise RuntimeError(f"BQ batch insert errors: {errors}")

        logger.info("Batch inserted %d/%d novel jobs.", len(novel_jobs), len(jobs))
        return len(novel_jobs)

    async def exists(self, job_id: str) -> bool:
        """Check if job_id exists within the 90-day dedup window."""
        window_start = format_bq_timestamp(get_dedup_window_start())
        query = f"""
            SELECT COUNT(1) as cnt FROM `{self._table}`
            WHERE job_id = '{job_id}'
              AND scraped_at >= '{window_start}'
            LIMIT 1
        """
        rows = list(self._client.query_and_wait(query))
        return rows[0].cnt > 0 if rows else False

    @async_retry(max_attempts=3, base_delay=1.0)
    async def update_status(self, job_id: str, status: str) -> None:
        """Update the status column for a job record."""
        # Validate status value against the enum
        JobStatus(status)  # raises ValueError if invalid
        query = f"""
            UPDATE `{self._table}`
            SET status = '{status}'
            WHERE job_id = '{job_id}'
        """
        self._client.query_and_wait(query)

    async def get_job(self, job_id: str) -> RawJob | None:
        """Fetch a single job by ID."""
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE job_id = '{job_id}'
            LIMIT 1
        """
        rows = list(self._client.query_and_wait(query))
        return self._row_to_raw_job(rows[0]) if rows else None

    async def get_jobs_pending_embedding(self, limit: int = 100) -> list[RawJob]:
        """Return jobs with status=EMBEDDING_QUEUED, ordered by scraped_at ASC."""
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE status = 'EMBEDDING_QUEUED'
            ORDER BY scraped_at ASC
            LIMIT {limit}
        """
        return [self._row_to_raw_job(row) for row in self._client.query_and_wait(query)]
