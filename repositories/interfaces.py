"""repositories/interfaces.py — Protocol ABCs for all BigQuery repositories.

These protocols are the dependency-inversion boundary:
  - Agents and orchestration IMPORT and TYPE-HINT against these protocols.
  - Concrete *_repo.py classes IMPLEMENT these protocols.
  - Tests MOCK these protocols without touching real BigQuery.

All methods are async to avoid blocking the event loop in Cloud Run services.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from schemas.job_models import EvaluatedJob, RawJob
from schemas.profile_models import UserProfile
from schemas.publication_models import (
    BatchInsertResult,
    CheckpointResult,
    ProcessingClaim,
    PublicationClaim,
    PublicationOwnership,
)

# ── Raw Jobs ───────────────────────────────────────────────────────────────────


@runtime_checkable
class IRawJobRepository(Protocol):
    """Protocol for raw_job_postings table access."""

    async def insert_job(self, job: RawJob) -> None:
        """Insert a new job record. No-op for an existing retained job ID, independently of age."""
        ...

    async def batch_insert_jobs(self, jobs: list[RawJob]) -> list[str]:
        """Batch insert jobs. Returns newly inserted IDs."""
        ...

    async def persist_jobs(self, jobs: list[RawJob]) -> BatchInsertResult:
        """Atomically persist validated jobs and publication intentions."""
        ...

    async def exists(self, job_id: str) -> bool:
        """Check if a job_id exists among all retained raw rows."""
        ...

    async def update_status(self, job_id: str, status: str) -> None:
        """Update the status column for a job record."""
        ...

    async def get_job(self, job_id: str) -> RawJob | None:
        """Fetch a single job by ID."""
        ...

    async def get_jobs_pending_embedding(self, limit: int = 100) -> list[RawJob]:
        """Return jobs with status=EMBEDDING_QUEUED, ordered by scraped_at ASC."""
        ...


# ── Embeddings ─────────────────────────────────────────────────────────────────


class IPublicationRepository(Protocol):
    async def publication_counts(self, job_ids: list[str]) -> dict[str, int]: ...
    async def claim_pending(
        self, job_ids: list[str] | None = None, limit: int = 100, min_age_seconds: int = 0,
        *, deadline: float | None = None,
    ) -> list[PublicationClaim]: ...
    async def check_ownership(
        self, claims: list[PublicationClaim], *, deadline: float | None = None,
    ) -> list[PublicationOwnership]: ...
    async def record_outcomes(
        self, claims: list[PublicationClaim], confirmed: dict[str, str], unconfirmed_ids: list[str],
        *, deadline: float | None = None,
    ) -> CheckpointResult: ...
    async def claim_processing(self, job_id: str) -> ProcessingClaim: ...
    async def finish_processing(self, claim: ProcessingClaim, embedding: list[float]) -> None: ...
    async def release_processing(self, claim: ProcessingClaim) -> None: ...


@runtime_checkable
class IEmbeddingRepository(Protocol):
    """Protocol for job_embeddings table access."""

    async def upsert_embedding(
        self,
        job_id: str,
        embedding: list[float],
        embedded_at: datetime | None = None,
    ) -> None:
        """Insert or update a 768-dim embedding for a job."""
        ...

    async def get_embedding(self, job_id: str) -> list[float] | None:
        """Fetch the embedding vector for a job_id. Returns None if not found."""
        ...

    async def vector_search(
        self,
        query_embedding: list[float],
        top_k: int = 50,
        dataset: str | None = None,
    ) -> list[tuple[str, float]]:
        """Run BQ VECTOR_SEARCH cosine similarity.

        Returns:
            List of (job_id, similarity_score) tuples, ordered by score DESC.
        """
        ...


# ── Evaluated Jobs ─────────────────────────────────────────────────────────────


@runtime_checkable
class IEvaluatedRepository(Protocol):
    """Protocol for job_evaluated table access."""

    async def upsert_evaluation(self, evaluation: EvaluatedJob) -> None:
        """Insert or update an evaluation record (idempotent on job_id + user_id)."""
        ...

    async def get_evaluation(self, job_id: str, user_id: str) -> EvaluatedJob | None:
        """Fetch an evaluation for a specific job_id/user_id pair."""
        ...

    async def get_high_score_jobs(
        self,
        user_id: str,
        min_score: int = 85,
        limit: int = 10,
    ) -> list[EvaluatedJob]:
        """Return top-scoring jobs for a user above the notification threshold."""
        ...

    async def update_tier2_status(self, job_id: str, user_id: str, tier2_status: str) -> None:
        """Update the tier2_status field for optimistic locking."""
        ...

    async def mark_notified(self, job_id: str, user_id: str) -> None:
        """Set notified_at to now() for a job/user pair."""
        ...


# ── User Actions (HITL Telemetry) ─────────────────────────────────────────────


@runtime_checkable
class IActionsRepository(Protocol):
    """Protocol for job_user_actions table access (HITL telemetry)."""

    async def record_action(
        self,
        job_id: str,
        user_id: str,
        action: str,
        trace_id: str | None = None,
    ) -> None:
        """Record a user action (apply | pass | analyze) against a job."""
        ...

    async def get_accepted_jobs(
        self,
        user_id: str,
        since_days: int = 30,
    ) -> list[str]:
        """Return job_ids where action='apply' within the last N days."""
        ...

    async def get_action_counts(
        self,
        user_id: str,
        since_days: int = 30,
    ) -> dict[str, int]:
        """Return counts per action type: {'apply': N, 'pass': M, 'analyze': K}."""
        ...


# ── User Profiles ──────────────────────────────────────────────────────────────


@runtime_checkable
class IProfileRepository(Protocol):
    """Protocol for user_profiles table access."""

    async def upsert_profile(self, profile: UserProfile) -> None:
        """Insert or update a user profile record."""
        ...

    async def get_profile(self, user_id: str) -> UserProfile | None:
        """Fetch a user profile. Returns None if not found."""
        ...

    async def update_active_centroid(
        self,
        user_id: str,
        new_centroid: list[float],
        alpha: float,
    ) -> None:
        """Update e_active_user and active_vector_alpha after reflection."""
        ...

    async def get_active_users(self) -> list[str]:
        """Return user_ids where is_active=True."""
        ...
