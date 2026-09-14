"""repositories/embeddings_repo.py — Concrete BigQuery adapter for job_embeddings.

Implements IEmbeddingRepository. Contains the BQ VECTOR_SEARCH implementation
for Tier 1 Step A vector similarity retrieval.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from config.settings import get_settings
from utils.retry import async_retry
from utils.time import format_bq_timestamp, utcnow

logger = logging.getLogger(__name__)


class EmbeddingRepository:
    """BigQuery adapter for job_embeddings table with native vector search."""

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
    ) -> None:
        settings = get_settings()
        self._client = bq_client or bigquery.Client(project=settings.gcp_project_id)
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.job_embeddings"
        self._raw_table = f"{self._project}.{self._dataset}.raw_job_postings"

    @async_retry(max_attempts=3, base_delay=1.0)
    async def upsert_embedding(
        self,
        job_id: str,
        embedding: list[float],
        embedded_at: datetime | None = None,
    ) -> None:
        """Insert or update a 768-dim embedding for a job.

        Uses MERGE to handle race conditions from concurrent embedding workers.
        """
        ts = format_bq_timestamp(embedded_at or utcnow())
        embedding_json = json.dumps(embedding)

        query = f"""
            MERGE `{self._table}` T
            USING (SELECT '{job_id}' AS job_id) S
            ON T.job_id = S.job_id
            WHEN MATCHED THEN
              UPDATE SET embedding = {embedding_json}, embedded_at = '{ts}'
            WHEN NOT MATCHED THEN
              INSERT (job_id, embedding, embedded_at)
              VALUES ('{job_id}', {embedding_json}, '{ts}')
        """
        self._client.query_and_wait(query)
        logger.info("Upserted embedding for job_id=%s", job_id)

    async def get_embedding(self, job_id: str) -> list[float] | None:
        """Fetch the embedding vector for a job_id."""
        query = f"""
            SELECT embedding FROM `{self._table}`
            WHERE job_id = '{job_id}'
            LIMIT 1
        """
        rows = list(self._client.query_and_wait(query))
        if not rows:
            return None
        return list(rows[0].embedding)

    async def vector_search(
        self,
        query_embedding: list[float],
        top_k: int = 50,
        dataset: str | None = None,
    ) -> list[tuple[str, float]]:
        """Run BQ VECTOR_SEARCH cosine similarity via ML.DISTANCE.

        Implements ARCHITECTURE.md Step A:
            1 - ML.DISTANCE(e_active_user, e_job, 'COSINE'), top 50, <15ms.

        Only searches embeddings for jobs with status='EMBEDDING_DONE' in raw_job_postings
        to avoid scoring stale or in-flight records.

        Args:
            query_embedding: 768-dim user active centroid vector.
            top_k: Maximum number of candidates to return (default 50).
            dataset: Override dataset name.

        Returns:
            List of (job_id, similarity_score) tuples ordered by score DESC.
        """
        ds = dataset or self._dataset
        embedding_json = json.dumps(query_embedding)

        query = f"""
            SELECT
                e.job_id,
                1 - ML.DISTANCE(
                    e.embedding,
                    {embedding_json},
                    'COSINE'
                ) AS similarity_score
            FROM `{self._project}.{ds}.job_embeddings` e
            INNER JOIN `{self._project}.{ds}.raw_job_postings` r
                ON e.job_id = r.job_id
            WHERE r.status = 'EMBEDDING_DONE'
            ORDER BY similarity_score DESC
            LIMIT {top_k}
        """
        rows = self._client.query_and_wait(query)
        results = [(row.job_id, float(row.similarity_score)) for row in rows]
        logger.info("Vector search returned %d candidates (top_k=%d)", len(results), top_k)
        return results
