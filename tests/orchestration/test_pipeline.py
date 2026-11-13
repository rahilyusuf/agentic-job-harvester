"""tests/orchestration/test_pipeline.py — Tier 1 pipeline integration tests (mocked repos)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from schemas.job_models import EvaluatedJob, RawJob
from schemas.routing_models import RouterDecision, RoutingDecision
from schemas.scoring_models import ScorerOutput, SkepticOutput
from utils.time import utcnow


@pytest.fixture
def sample_job() -> RawJob:
    return RawJob(
        job_id="pipeline-job-001",
        source="test",
        job_url="https://example.com/job/pipeline",
        title="ML Engineer",
        company="TestAI",
        description="A fantastic ML engineering opportunity. " * 10,
        scraped_at=utcnow(),
    )


@pytest.fixture
def mock_raw_repo() -> MagicMock:
    repo = MagicMock()
    repo.update_status = AsyncMock()
    repo.get_job = AsyncMock()
    return repo


@pytest.fixture
def mock_evaluated_repo() -> MagicMock:
    repo = MagicMock()
    repo.upsert_evaluation = AsyncMock()
    repo.update_tier2_status = AsyncMock()
    return repo


@pytest.fixture
def mock_profile_repo() -> MagicMock:
    repo = MagicMock()
    return repo


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_skip_halts_immediately(
    sample_job: RawJob,
    mock_raw_repo: MagicMock,
    mock_evaluated_repo: MagicMock,
    mock_profile_repo: MagicMock,
) -> None:
    """SKIP routing decision halts pipeline — no scoring happens."""
    from unittest.mock import patch
    from orchestration.pipeline import build_tier1_pipeline

    skip_decision = RouterDecision(
        job_id=sample_job.job_id,
        decision=RoutingDecision.SKIP,
        confidence=0.99,
        reasoning="Title is VP-level which is excluded by user preference rules.",
        skip_reason="VP title excluded",
    )

    with patch("orchestration.pipeline.JobIntakeRouter.route", new_callable=AsyncMock) as mock_route, \
         patch("orchestration.pipeline.ScorerAgent.score", new_callable=AsyncMock) as mock_score:

        mock_route.return_value = skip_decision

        pipeline = build_tier1_pipeline(
            raw_jobs_repo=mock_raw_repo,
            evaluated_repo=mock_evaluated_repo,
            profile_repo=mock_profile_repo,
        )
        result = await pipeline.run(
            job=sample_job, user_id="user_test", resume_summary="Senior AI Engineer"
        )

    assert result.skipped is True
    assert result.evaluation is None
    assert result.routing_decision == "SKIP"
    # Scorer must NOT have been called
    mock_score.assert_not_called()
    # Status should have been updated to SKIPPED
    mock_raw_repo.update_status.assert_called_once_with(sample_job.job_id, "SKIPPED")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_produces_evaluated_job_on_process(
    sample_job: RawJob,
    mock_raw_repo: MagicMock,
    mock_evaluated_repo: MagicMock,
    mock_profile_repo: MagicMock,
) -> None:
    """PROCESS routing decision runs full debate and writes EvaluatedJob."""
    from unittest.mock import patch
    from orchestration.pipeline import build_tier1_pipeline

    process_decision = RouterDecision(
        job_id=sample_job.job_id,
        decision=RoutingDecision.PROCESS,
        confidence=0.80,
        reasoning="Good match for ML engineer role based on preference rules.",
    )
    scorer_out = ScorerOutput(
        job_id=sample_job.job_id,
        user_id="user_test",
        round_number=1,
        initial_score=78,
        strengths=["Strong ML alignment"],
        rationale="Good fit based on skill match and target role preferences." * 2,
    )
    skeptic_out = SkepticOutput(
        job_id=sample_job.job_id,
        user_id="user_test",
        round_number=1,
        agreed_score=75,
        veto=False,
        rationale="Slight reduction for location ambiguity, no critical flags detected." * 2,
        request_round_2=False,
    )

    with patch("orchestration.pipeline.JobIntakeRouter.route", new_callable=AsyncMock) as mock_route, \
         patch("orchestration.pipeline.ScorerAgent.score", new_callable=AsyncMock) as mock_score, \
         patch("orchestration.pipeline.SkepticAgent.audit", new_callable=AsyncMock) as mock_audit:

        mock_route.return_value = process_decision
        mock_score.return_value = scorer_out
        mock_audit.return_value = skeptic_out

        pipeline = build_tier1_pipeline(
            raw_jobs_repo=mock_raw_repo,
            evaluated_repo=mock_evaluated_repo,
            profile_repo=mock_profile_repo,
        )
        result = await pipeline.run(
            job=sample_job, user_id="user_test", resume_summary="Senior AI Engineer"
        )

    assert result.skipped is False
    assert result.evaluation is not None
    assert result.evaluation.agreed_score == 75
    assert result.evaluation.debate_rounds == 1
    assert result.evaluation.skeptic_vetoed is False
    mock_evaluated_repo.upsert_evaluation.assert_called_once()
