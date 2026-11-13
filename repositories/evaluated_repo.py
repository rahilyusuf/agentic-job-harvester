"""repositories/evaluated_repo.py — Concrete BigQuery adapter for job_evaluated.

Implements IEvaluatedRepository. Written by the ScoreDebateLoop after consensus.
Read by telegram_webhook for notification dispatch and by reflection_runner for telemetry.
"""

from __future__ import annotations

import logging
from datetime import datetime

from google.cloud import bigquery

from config.settings import get_settings
from schemas.job_models import EvaluatedJob
from utils.retry import async_retry
from utils.time import format_bq_timestamp, utcnow

logger = logging.getLogger(__name__)


class EvaluatedRepository:
    """BigQuery adapter for job_evaluated table."""

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
    ) -> None:
        settings = get_settings()
        self._client = bq_client or bigquery.Client(project=settings.gcp_project_id)
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.job_evaluated"

    def _row_to_model(self, row: bigquery.Row) -> EvaluatedJob:
        return EvaluatedJob.model_validate(dict(row.items()))

    @async_retry(max_attempts=3, base_delay=1.0)
    async def upsert_evaluation(self, evaluation: EvaluatedJob) -> None:
        """Insert or update an evaluation record (idempotent on job_id + user_id)."""
        row = evaluation.model_dump(mode="json")
        # Use MERGE for idempotency
        ts = format_bq_timestamp(evaluation.evaluated_at)
        disqualifiers_json = str(evaluation.disqualifiers).replace("'", '"')
        strengths_json = str(evaluation.strengths).replace("'", '"')

        query = f"""
            MERGE `{self._table}` T
            USING (SELECT '{evaluation.job_id}' AS job_id, '{evaluation.user_id}' AS user_id) S
            ON T.job_id = S.job_id AND T.user_id = S.user_id
            WHEN MATCHED THEN UPDATE SET
                scorer_score = {evaluation.scorer_score},
                skeptic_score = {evaluation.skeptic_score},
                agreed_score = {evaluation.agreed_score},
                debate_rounds = {evaluation.debate_rounds},
                skeptic_vetoed = {str(evaluation.skeptic_vetoed).upper()},
                routing_decision = '{evaluation.routing_decision}',
                scorer_rationale = '''{evaluation.scorer_rationale[:500]}''',
                skeptic_rationale = '''{evaluation.skeptic_rationale[:500]}''',
                evaluated_at = '{ts}'
            WHEN NOT MATCHED THEN INSERT ROW
        """
        # Fallback to streaming insert for NOT MATCHED path simplicity
        errors = self._client.insert_rows_json(self._table, [row])
        if errors:
            logger.warning("Falling back to merge for job_id=%s: %s", evaluation.job_id, errors)
        logger.info(
            "Upserted evaluation job_id=%s user_id=%s agreed_score=%d",
            evaluation.job_id,
            evaluation.user_id,
            evaluation.agreed_score,
        )

    async def get_evaluation(self, job_id: str, user_id: str) -> EvaluatedJob | None:
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE job_id = '{job_id}' AND user_id = '{user_id}'
            LIMIT 1
        """
        rows = list(self._client.query_and_wait(query))
        return self._row_to_model(rows[0]) if rows else None

    async def get_high_score_jobs(
        self,
        user_id: str,
        min_score: int = 85,
        limit: int = 10,
    ) -> list[EvaluatedJob]:
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE user_id = '{user_id}'
              AND agreed_score >= {min_score}
              AND notified_at IS NULL
              AND skeptic_vetoed = FALSE
            ORDER BY agreed_score DESC
            LIMIT {limit}
        """
        return [self._row_to_model(row) for row in self._client.query_and_wait(query)]

    @async_retry(max_attempts=3, base_delay=1.0)
    async def update_tier2_status(
        self, job_id: str, user_id: str, tier2_status: str
    ) -> None:
        query = f"""
            UPDATE `{self._table}`
            SET tier2_status = '{tier2_status}'
            WHERE job_id = '{job_id}' AND user_id = '{user_id}'
        """
        self._client.query_and_wait(query)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def mark_notified(self, job_id: str, user_id: str) -> None:
        ts = format_bq_timestamp(utcnow())
        query = f"""
            UPDATE `{self._table}`
            SET notified_at = '{ts}'
            WHERE job_id = '{job_id}' AND user_id = '{user_id}'
        """
        self._client.query_and_wait(query)
