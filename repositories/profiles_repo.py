"""repositories/profiles_repo.py — Concrete BigQuery adapter for user_profiles.

Implements IProfileRepository.
Stores UserProfile including the 768-dim e_resume and e_active_user centroid vectors.
Updated by user_profile_service.py and reflection_runner.py.
"""

from __future__ import annotations

import json
import logging

from google.cloud import bigquery

from config.settings import get_settings
from schemas.profile_models import ResumeMetadata, UserProfile
from utils.retry import async_retry
from utils.time import format_bq_timestamp, utcnow

logger = logging.getLogger(__name__)


class ProfileRepository:
    """BigQuery adapter for user_profiles table."""

    def __init__(
        self,
        bq_client: bigquery.Client | None = None,
        dataset: str | None = None,
    ) -> None:
        settings = get_settings()
        self._client = bq_client or bigquery.Client(project=settings.gcp_project_id)
        self._dataset = dataset or settings.bq_dataset
        self._project = settings.gcp_project_id
        self._table = f"{self._project}.{self._dataset}.user_profiles"

    def _row_to_profile(self, row: bigquery.Row) -> UserProfile:
        data = dict(row.items())
        # BQ FLOAT64 REPEATED comes back as a list — convert for Pydantic
        data["e_resume"] = list(data.get("e_resume", []))
        data["e_active_user"] = list(data.get("e_active_user", []))
        # resume_metadata is stored as a JSON STRING in BQ
        if isinstance(data.get("resume_metadata"), str):
            data["resume_metadata"] = json.loads(data["resume_metadata"])
        return UserProfile.model_validate(data)

    @async_retry(max_attempts=3, base_delay=1.0)
    async def upsert_profile(self, profile: UserProfile) -> None:
        """Insert or update a user profile (idempotent on user_id)."""
        row = profile.model_dump(mode="json")
        # Serialize resume_metadata as JSON string for BQ
        row["resume_metadata"] = json.dumps(row["resume_metadata"])
        row["updated_at"] = format_bq_timestamp(utcnow())

        errors = self._client.insert_rows_json(self._table, [row])
        if errors:
            # For updates, try a DML query
            logger.warning("Streaming insert failed; attempting DML for user_id=%s", profile.user_id)
            e_active = json.dumps(profile.e_active_user)
            e_resume = json.dumps(profile.e_resume)
            query = f"""
                UPDATE `{self._table}`
                SET
                    e_resume = {e_resume},
                    e_active_user = {e_active},
                    active_vector_alpha = {profile.active_vector_alpha},
                    updated_at = '{row["updated_at"]}',
                    is_active = {str(profile.is_active).upper()}
                WHERE user_id = '{profile.user_id}'
            """
            self._client.query_and_wait(query)
        logger.info("Upserted profile for user_id=%s", profile.user_id)

    async def get_profile(self, user_id: str) -> UserProfile | None:
        """Fetch a user profile by user_id."""
        query = f"""
            SELECT * FROM `{self._table}`
            WHERE user_id = '{user_id}'
            LIMIT 1
        """
        rows = list(self._client.query_and_wait(query))
        return self._row_to_profile(rows[0]) if rows else None

    @async_retry(max_attempts=3, base_delay=1.0)
    async def update_active_centroid(
        self,
        user_id: str,
        new_centroid: list[float],
        alpha: float,
    ) -> None:
        """Update e_active_user and active_vector_alpha after a reflection run.

        Implements the centroid-drift formula:
            e_active_new = (1 - α) * e_resume + α * e_accepted_centroid
        The formula itself is computed by ReflectionAgent; this method persists the result.
        """
        centroid_json = json.dumps(new_centroid)
        ts = format_bq_timestamp(utcnow())
        query = f"""
            UPDATE `{self._table}`
            SET
                e_active_user = {centroid_json},
                active_vector_alpha = {alpha},
                last_reflection_at = '{ts}',
                updated_at = '{ts}'
            WHERE user_id = '{user_id}'
        """
        self._client.query_and_wait(query)
        logger.info("Updated active centroid for user_id=%s alpha=%.3f", user_id, alpha)

    async def get_active_users(self) -> list[str]:
        """Return user_ids where is_active=True."""
        query = f"""
            SELECT user_id FROM `{self._table}`
            WHERE is_active = TRUE
            ORDER BY created_at ASC
        """
        return [row.user_id for row in self._client.query_and_wait(query)]
