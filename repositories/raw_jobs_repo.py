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

from config.settings import Settings, get_settings
from observability.ingestion_logging import emit
from schemas.job_models import JobStatus, RawJob
from schemas.publication_models import BatchInsertResult, QueryOutcomeUnconfirmed
from utils.retry import async_retry
from utils.time import format_bq_timestamp, get_dedup_window_start

logger = logging.getLogger(__name__)


class RawJobRepository:
    """BigQuery adapter for raw_job_postings table."""

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
        *,
        settings: Settings | None = None,
    ) -> None:
        settings = settings if settings is not None else get_settings()
        self._dedup_window_days = settings.dedup_window_days
        self._client = bq_client or bigquery.Client(
            project=settings.gcp_project_id,
            location=settings.bq_location,
        )
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.raw_job_postings"
        self._settings = settings
        self.publications = None
        if settings.publication_ledger_enabled:
            from repositories.publications_repo import PublicationRepository

            self.publications = PublicationRepository(
                self._client, settings=settings, dataset=self._dataset
            )

    def _row_to_raw_job(self, row: bigquery.Row) -> RawJob:
        """Convert a BigQuery row to a RawJob model."""
        data = dict(row.items())
        return RawJob.model_validate(data)

    async def insert_job(self, job: RawJob) -> None:
        """Insert a new job. Skips silently if duplicate within dedup window."""
        if self.publications is not None:
            result = await self.persist_jobs([job])
            if result.unconfirmed_ids or result.rejected_ids:
                raise QueryOutcomeUnconfirmed(result.query_job_id or "unconfirmed")
            return
        await self._insert_job_legacy(job)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def _insert_job_legacy(self, job: RawJob) -> None:
        if await self.exists(job.job_id):
            emit("ingestion.bigquery.record_existing", logging.DEBUG)
            return

        row = job.model_dump(mode="json")
        errors = self._client.insert_rows_json(self._table, [row])
        if errors:
            raise RuntimeError(f"BQ insert errors for job_id={job.job_id}: {errors}")
        emit("ingestion.bigquery.record_inserted", logging.DEBUG)

    async def batch_insert_jobs(self, jobs: list[RawJob]) -> list[str]:
        """Batch insert jobs. Returns list of newly inserted job_ids (empty if all duplicates)."""
        if self.publications is not None:
            result = await self.persist_jobs(jobs)
            if result.unconfirmed_ids or result.rejected_ids:
                raise QueryOutcomeUnconfirmed(result.query_job_id or "unconfirmed")
            return result.inserted_ids
        return await self._batch_insert_legacy(jobs)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def _batch_insert_legacy(self, jobs: list[RawJob]) -> list[str]:
        return await self._batch_insert_jobs(jobs, retry_insert=True)

    async def batch_insert_jobs_once(self, jobs: list[RawJob]) -> list[str]:
        """One attempt, with SDK insertion retries disabled; errors may mean partial writes."""
        if self.publications is not None:
            return await self.batch_insert_jobs(jobs)
        return await self._batch_insert_jobs(jobs, retry_insert=False)

    async def persist_jobs(self, jobs: list[RawJob]) -> BatchInsertResult:
        """Guarded DML: raw rows and publication intentions commit atomically.

        Existing rows without a ledger are legacy UNKNOWN, never assumed unpublished.
        The initial-embedding publication identity deliberately outlives raw dedup windows.
        """
        if self.publications is None:
            raise RuntimeError("Publication ledger is not enabled")
        unique = {}
        for job in jobs:
            unique.setdefault(job.job_id, job)
        jobs = list(unique.values())
        if not jobs:
            return BatchInsertResult()
        columns = list(RawJob.model_fields)
        timestamp_columns = {"scraped_at", "posted_at"}
        expressions = [
            f"TIMESTAMP(JSON_VALUE(item,'$.{column}')) AS {column}"
            if column in timestamp_columns
            else f"JSON_VALUE(item,'$.{column}') AS {column}"
            for column in columns
        ]
        ledger = self.publications
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{ledger.table}` WHERE FALSE;
        CREATE TEMP TABLE incoming AS SELECT {", ".join(expressions)}
            FROM UNNEST(JSON_QUERY_ARRAY(@jobs)) item;
        ASSERT NOT EXISTS (
            SELECT r.job_id FROM `{self._table}` r JOIN incoming i ON r.job_id=i.job_id
            WHERE r.scraped_at>=@window GROUP BY r.job_id HAVING COUNT(*)>1
        ) AS 'Ambiguous existing raw jobs';
        CREATE TEMP TABLE novel AS SELECT i.* FROM incoming i WHERE NOT EXISTS (
            SELECT 1 FROM `{self._table}` r WHERE r.job_id=i.job_id AND r.scraped_at>=@window
        );
        INSERT `{self._table}` ({", ".join(columns)}) SELECT {", ".join(columns)} FROM novel;
        INSERT `{ledger.table}` (publication_id,job_id,topic,processing_version,state,
            created_at,updated_at,attempt_count,last_error_code)
        SELECT UPPER(
            TO_HEX(SHA256(CONCAT(@topic,'\\n',i.job_id,'\\n',@version)))
        ),i.job_id,@topic,@version,
            IF(n.job_id IS NULL,'UNKNOWN','PENDING'),CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),0,
            IF(n.job_id IS NULL,'legacy_unknown',NULL)
        FROM incoming i LEFT JOIN novel n USING(job_id)
        WHERE NOT EXISTS (SELECT 1 FROM `{ledger.table}` p
            WHERE p.job_id=i.job_id AND p.topic=@topic AND p.processing_version=@version);
        COMMIT TRANSACTION;
        SELECT i.job_id,n.job_id IS NOT NULL AS inserted FROM incoming i LEFT JOIN novel n USING(job_id);
        """
        try:
            rows = await ledger.execute(
                sql,
                ledger.parameters(
                    bigquery.ScalarQueryParameter(
                        "jobs", "STRING", json.dumps([j.model_dump(mode="json") for j in jobs])
                    ),
                    bigquery.ScalarQueryParameter(
                        "window", "TIMESTAMP", get_dedup_window_start(self._dedup_window_days)
                    ),
                ),
            )
            if {row["job_id"] for row in rows} != {j.job_id for j in jobs} or len(rows) != len(
                jobs
            ):
                raise ValueError("Invalid persistence confirmation")
            inserted = {row["job_id"] for row in rows if row["inserted"] is True}
            return BatchInsertResult(
                inserted_ids=[j.job_id for j in jobs if j.job_id in inserted],
                existing_ids=[j.job_id for j in jobs if j.job_id not in inserted],
            )
        except QueryOutcomeUnconfirmed as exc:
            return BatchInsertResult(
                unconfirmed_ids=[j.job_id for j in jobs], query_job_id=exc.job_id
            )

    async def _batch_insert_jobs(self, jobs: list[RawJob], *, retry_insert: bool) -> list[str]:
        if not jobs:
            return []

        # Check all job_ids in one BQ query using array parameter
        ids = [j.job_id for j in jobs]
        window_start = format_bq_timestamp(get_dedup_window_start(self._dedup_window_days))
        query = f"""
            SELECT job_id FROM `{self._table}`
            WHERE job_id IN UNNEST(@ids)
              AND scraped_at >= '{window_start}'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "STRING", ids),
            ]
        )
        existing = {row.job_id for row in self._client.query_and_wait(query, job_config=job_config)}

        novel_jobs = [j for j in jobs if j.job_id not in existing]
        if not novel_jobs:
            emit(
                "ingestion.bigquery.batch_existing",
                logging.DEBUG,
                unique=len(jobs),
                existing_duplicates=len(jobs),
            )
            return []

        rows = [j.model_dump(mode="json") for j in novel_jobs]
        insert_options = {} if retry_insert else {"retry": None}
        errors = self._client.insert_rows_json(self._table, rows, **insert_options)
        if errors:
            raise RuntimeError(f"BQ batch insert errors: {errors}")

        novel_ids = [j.job_id for j in novel_jobs]
        emit(
            "ingestion.bigquery.batch_inserted",
            logging.DEBUG,
            inserted=len(novel_ids),
            unique=len(jobs),
            existing_duplicates=len(existing),
        )
        return novel_ids

    async def exists(self, job_id: str) -> bool:
        """Check if job_id exists within the 90-day dedup window."""
        window_start = format_bq_timestamp(get_dedup_window_start())
        query = f"""
            SELECT COUNT(1) as cnt FROM `{self._table}`
            WHERE job_id = @job_id
              AND scraped_at >= '{window_start}'
            LIMIT 1
        """
        rows = list(
            self._client.query_and_wait(
                query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
                ),
            )
        )
        return rows[0].cnt > 0 if rows else False

    @async_retry(max_attempts=3, base_delay=1.0)
    async def update_status(self, job_id: str, status: str) -> None:
        """Update the status column for a job record."""
        # Validate status value against the enum
        JobStatus(status)  # raises ValueError if invalid
        query = f"""
            UPDATE `{self._table}`
            SET status = @status
            WHERE job_id = @job_id
        """
        self._client.query_and_wait(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                ]
            ),
        )

    async def get_job(self, job_id: str) -> RawJob | None:
        """Fetch a single job by ID."""
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE job_id = @job_id
            LIMIT 1
        """
        rows = list(
            self._client.query_and_wait(
                query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
                ),
            )
        )
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

    async def get_unembedded_job_ids(
        self,
        job_ids: list[str] | None = None,
        limit: int = 100,
        min_age_seconds: int = 0,
    ) -> list[str]:
        """Fetch job IDs that are saved in raw_job_postings but remain unembedded (status=NEW_RAW).

        Args:
            job_ids: Optional list of candidate job IDs to filter against (e.g. from a retried batch).
            limit: Maximum number of job IDs to return.
            min_age_seconds: Minimum age in seconds since scraped_at (to avoid racing with fresh writes).

        Returns:
            List of job_id strings with status = 'NEW_RAW'.
        """
        window_start = format_bq_timestamp(get_dedup_window_start())
        if job_ids is not None:
            if not job_ids:
                return []
            query = f"""
                SELECT job_id FROM `{self._table}`
                WHERE job_id IN UNNEST(@ids)
                  AND status = '{JobStatus.NEW_RAW.value}'
                  AND scraped_at >= '{window_start}'
                LIMIT {limit}
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ids", "STRING", job_ids),
                ]
            )
            rows = self._client.query_and_wait(query, job_config=job_config)
        else:
            age_filter = ""
            if min_age_seconds > 0:
                age_filter = f"AND scraped_at <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {min_age_seconds} SECOND)"
            query = f"""
                SELECT job_id FROM `{self._table}`
                WHERE status = '{JobStatus.NEW_RAW.value}'
                  AND scraped_at >= '{window_start}'
                  {age_filter}
                ORDER BY scraped_at ASC
                LIMIT {limit}
            """
            rows = self._client.query_and_wait(query)

        return [row.job_id for row in rows]
