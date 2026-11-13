"""repositories/actions_repo.py — Concrete BigQuery adapter for job_user_actions.

Implements IActionsRepository. Records HITL telemetry events (apply/pass/analyze).
Read by ReflectionAgent for the active learning flywheel.
"""

from __future__ import annotations

import logging

from google.cloud import bigquery

from config.settings import get_settings
from utils.retry import async_retry
from utils.time import format_bq_timestamp, utcnow

logger = logging.getLogger(__name__)


class ActionsRepository:
    """BigQuery adapter for job_user_actions table."""

    _VALID_ACTIONS = frozenset({"apply", "pass", "analyze"})

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
    ) -> None:
        settings = get_settings()
        self._client = bq_client or bigquery.Client(project=settings.gcp_project_id)
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.job_user_actions"

    @async_retry(max_attempts=3, base_delay=1.0)
    async def record_action(
        self,
        job_id: str,
        user_id: str,
        action: str,
        trace_id: str | None = None,
    ) -> None:
        """Record a user action event.

        Args:
            job_id: The job the user acted on.
            user_id: The user who performed the action.
            action: One of "apply" | "pass" | "analyze".
            trace_id: LangFuse trace ID for correlation with scoring trace.

        Raises:
            ValueError: If action is not a valid value.
        """
        if action.lower() not in self._VALID_ACTIONS:
            raise ValueError(f"Invalid action '{action}'. Must be one of {self._VALID_ACTIONS}")

        row = {
            "job_id": job_id,
            "user_id": user_id,
            "action": action.lower(),
            "trace_id": trace_id,
            "acted_at": format_bq_timestamp(utcnow()),
        }
        errors = self._client.insert_rows_json(self._table, [row])
        if errors:
            raise RuntimeError(f"BQ insert error for action record: {errors}")
        logger.info("Recorded action=%s for job_id=%s user_id=%s", action, job_id, user_id)

    async def get_accepted_jobs(
        self,
        user_id: str,
        since_days: int = 30,
    ) -> list[str]:
        """Return job_ids where action='apply' within the last N days."""
        query = f"""
            SELECT DISTINCT job_id FROM `{self._table}`
            WHERE user_id = '{user_id}'
              AND action = 'apply'
              AND acted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {since_days} DAY)
            ORDER BY acted_at DESC
        """
        return [row.job_id for row in self._client.query_and_wait(query)]

    async def get_action_counts(
        self,
        user_id: str,
        since_days: int = 30,
    ) -> dict[str, int]:
        """Return counts per action type."""
        query = f"""
            SELECT action, COUNT(*) AS cnt FROM `{self._table}`
            WHERE user_id = '{user_id}'
              AND acted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {since_days} DAY)
            GROUP BY action
        """
        counts: dict[str, int] = {"apply": 0, "pass": 0, "analyze": 0}
        for row in self._client.query_and_wait(query):
            counts[row.action] = row.cnt
        return counts
