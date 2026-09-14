"""tests/retrieval/test_vector_matcher.py — Unit tests for VectorMatcher."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from retrieval.vector_matcher import VectorMatcher, VectorMatchResult
from schemas.profile_models import ResumeMetadata, UserProfile
from utils.time import utcnow


def make_profile(user_id: str, has_centroid: bool = True) -> UserProfile:
    e_vec = [0.1] * 768 if has_centroid else []
    return UserProfile(
        user_id=user_id,
        resume_gcs_uri="gs://bucket/resume.pdf",
        resume_metadata=ResumeMetadata(
            total_yoe=4.0,
            current_title="ML Engineer",
            target_titles=["Senior ML Engineer"],
            top_skills=["Python", "PyTorch"],
            resume_text_hash="abc123",
        ),
        e_resume=[0.1] * 768,
        e_active_user=e_vec,
        active_vector_alpha=0.0,
    )


@pytest.fixture
def mock_embedding_repo() -> MagicMock:
    repo = MagicMock()
    repo.vector_search = AsyncMock(return_value=[
        ("job-001", 0.95),
        ("job-002", 0.88),
        ("job-003", 0.75),
    ])
    return repo


@pytest.fixture
def mock_profile_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_profile = AsyncMock(return_value=make_profile("user_test"))
    return repo


@pytest.mark.unit
@pytest.mark.asyncio
async def test_vector_matcher_returns_results(
    mock_embedding_repo: MagicMock,
    mock_profile_repo: MagicMock,
) -> None:
    """VectorMatcher returns VectorMatchResult list from embedding repo."""
    matcher = VectorMatcher(
        embedding_repo=mock_embedding_repo,
        profile_repo=mock_profile_repo,
    )
    results = await matcher.retrieve(user_id="user_test")

    assert len(results) == 3
    assert isinstance(results[0], VectorMatchResult)
    assert results[0].job_id == "job-001"
    assert results[0].similarity_score == pytest.approx(0.95)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_vector_matcher_raises_if_no_profile(
    mock_embedding_repo: MagicMock,
) -> None:
    """VectorMatcher raises ValueError when user profile is not found."""
    mock_profile_repo = MagicMock()
    mock_profile_repo.get_profile = AsyncMock(return_value=None)

    matcher = VectorMatcher(
        embedding_repo=mock_embedding_repo,
        profile_repo=mock_profile_repo,
    )
    with pytest.raises(ValueError, match="User profile not found"):
        await matcher.retrieve(user_id="unknown_user")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_vector_matcher_raises_if_no_centroid(
    mock_embedding_repo: MagicMock,
) -> None:
    """VectorMatcher raises ValueError when user has no active centroid."""
    profile_with_no_centroid = make_profile("user_test", has_centroid=False)
    mock_profile_repo = MagicMock()
    mock_profile_repo.get_profile = AsyncMock(return_value=profile_with_no_centroid)

    matcher = VectorMatcher(
        embedding_repo=mock_embedding_repo,
        profile_repo=mock_profile_repo,
    )
    with pytest.raises(ValueError, match="no active centroid"):
        await matcher.retrieve(user_id="user_test")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_vector_matcher_passes_top_k_to_repo(
    mock_embedding_repo: MagicMock,
    mock_profile_repo: MagicMock,
) -> None:
    """VectorMatcher passes correct top_k override to embedding repo."""
    matcher = VectorMatcher(
        embedding_repo=mock_embedding_repo,
        profile_repo=mock_profile_repo,
    )
    await matcher.retrieve(user_id="user_test", top_k=10)

    mock_embedding_repo.vector_search.assert_called_once()
    call_kwargs = mock_embedding_repo.vector_search.call_args.kwargs
    assert call_kwargs.get("top_k") == 10
