"""retrieval/vector_matcher.py — Tier 1 Step A: BQ VECTOR_SEARCH cosine similarity.

Implements ARCHITECTURE.md Step A:
    1 - ML.DISTANCE(e_active_user, e_job, 'COSINE'), top 50 roles, <15ms target.

Depends on:
    repositories/interfaces.IEmbeddingRepository   (vector_search method)
    repositories/interfaces.IProfileRepository     (get_profile for e_active_user)

Never imports concrete repo classes directly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from repositories.interfaces import IEmbeddingRepository, IProfileRepository
from config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VectorMatchResult:
    """A single candidate job from the vector search."""

    job_id: str
    similarity_score: float


class VectorMatcher:
    """Tier 1 Step A: retrieve top-K similar job embeddings for a user.

    Usage:
        matcher = VectorMatcher(embedding_repo, profile_repo)
        candidates = await matcher.retrieve(user_id="user_123")
        # Returns list[VectorMatchResult], len <= settings.vector_search_top_k
    """

    def __init__(
        self,
        embedding_repo: IEmbeddingRepository,
        profile_repo: IProfileRepository,
    ) -> None:
        self._embedding_repo = embedding_repo
        self._profile_repo = profile_repo
        self._settings = get_settings()

    async def retrieve(
        self,
        user_id: str,
        top_k: int | None = None,
    ) -> list[VectorMatchResult]:
        """Retrieve top-K jobs by cosine similarity to the user's active centroid.

        Args:
            user_id: The user to retrieve candidates for.
            top_k: Override; defaults to settings.vector_search_top_k (50).

        Returns:
            List of VectorMatchResult sorted by similarity_score DESC.

        Raises:
            ValueError: If the user profile is not found or has no active centroid.
        """
        k = top_k if top_k is not None else self._settings.vector_search_top_k

        # Fetch the user's current active centroid
        profile = await self._profile_repo.get_profile(user_id)
        if profile is None:
            raise ValueError(f"User profile not found for user_id={user_id!r}")

        if not profile.e_active_user:
            raise ValueError(
                f"User profile for user_id={user_id!r} has no active centroid (e_active_user)."
                " Run the user profile service to initialise embeddings."
            )

        logger.info(
            "Running vector search for user_id=%s top_k=%d alpha=%.3f",
            user_id,
            k,
            profile.active_vector_alpha,
        )

        # Execute BQ VECTOR_SEARCH via repository
        raw_results = await self._embedding_repo.vector_search(
            query_embedding=profile.e_active_user,
            top_k=k,
        )

        results = [
            VectorMatchResult(job_id=job_id, similarity_score=score)
            for job_id, score in raw_results
        ]

        logger.info(
            "Vector search completed: %d candidates for user_id=%s",
            len(results),
            user_id,
        )
        return results
