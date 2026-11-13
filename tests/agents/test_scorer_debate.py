"""tests/agents/test_scorer_debate.py — Regression tests for Tier 1 debate loop.

Tests the consensus rule: if |scorer_score - skeptic_agreed_score| > 10 → round 2.
Tests the veto rule: skeptic veto bypasses score and forces SKIP.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from schemas.job_models import RawJob
from schemas.scoring_models import ScorerOutput, SkepticOutput
from utils.time import utcnow


@pytest.fixture
def sample_job() -> RawJob:
    return RawJob(
        job_id="debate-job-001",
        source="test",
        job_url="https://example.com/job/1",
        title="AI Engineer",
        company="TestCorp",
        description="A great AI engineering role. " * 10,
        scraped_at=utcnow(),
    )


@pytest.fixture
def scorer_output_high() -> ScorerOutput:
    return ScorerOutput(
        job_id="debate-job-001",
        user_id="user_test",
        round_number=1,
        initial_score=90,
        strengths=["Strong ML fit", "Remote role"],
        concerns=[],
        rationale="Excellent match for AI engineer role with required PyTorch skills." * 2,
        preference_rules_applied=[],
    )


@pytest.fixture
def skeptic_output_no_delta(scorer_output_high: ScorerOutput) -> SkepticOutput:
    """Skeptic agrees — no round 2 needed (delta = 0)."""
    return SkepticOutput(
        job_id="debate-job-001",
        user_id="user_test",
        round_number=1,
        agreed_score=88,  # delta = 2, no round 2
        veto=False,
        hidden_disqualifiers=[],
        rationale="Scorer assessment is accurate. Minor adjustment for location ambiguity." * 2,
        request_round_2=False,
    )


@pytest.fixture
def skeptic_output_large_delta(scorer_output_high: ScorerOutput) -> SkepticOutput:
    """Skeptic disagrees significantly — round 2 should be triggered (delta > 10)."""
    return SkepticOutput(
        job_id="debate-job-001",
        user_id="user_test",
        round_number=1,
        agreed_score=70,  # delta = 20, triggers round 2
        veto=False,
        hidden_disqualifiers=["Job has been open 120 days — possible ghost job"],
        rationale="Ghost job signals reduce score significantly from Scorer's assessment." * 2,
        request_round_2=True,
    )


@pytest.fixture
def skeptic_output_veto() -> SkepticOutput:
    """Skeptic exercises hard veto."""
    return SkepticOutput(
        job_id="debate-job-001",
        user_id="user_test",
        round_number=1,
        agreed_score=30,
        veto=True,
        veto_reason="Confirmed staffing agency front — job is misrepresented as direct hire.",
        hidden_disqualifiers=["Staffing agency front", "Contact is recruiter not hiring manager"],
        rationale="Clear evidence of agency misrepresentation in job description." * 2,
        request_round_2=False,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_debate_no_round_2_when_delta_small(
    sample_job: RawJob,
    scorer_output_high: ScorerOutput,
    skeptic_output_no_delta: SkepticOutput,
) -> None:
    """When |scorer - skeptic| <= 10, debate stays at 1 round."""
    from agents.scorer_agent import ScorerAgent
    from agents.skeptic_agent import SkepticAgent

    with patch.object(ScorerAgent, "_call_llm", new_callable=AsyncMock) as mock_scorer_llm, \
         patch.object(SkepticAgent, "_call_llm", new_callable=AsyncMock) as mock_skeptic_llm:

        mock_scorer_llm.return_value = scorer_output_high
        mock_skeptic_llm.return_value = skeptic_output_no_delta

        scorer = ScorerAgent()
        skeptic = SkepticAgent()

        s_out = await scorer.score(
            job=sample_job, user_id="user_test",
            resume_summary="Senior ML engineer", preference_rules=[], round_number=1,
        )
        sk_out = await skeptic.audit(job=sample_job, user_id="user_test", scorer_output=s_out)

        assert not sk_out.request_round_2
        assert abs(s_out.initial_score - sk_out.agreed_score) <= 10


@pytest.mark.unit
@pytest.mark.asyncio
async def test_debate_triggers_round_2_on_large_delta(
    sample_job: RawJob,
    scorer_output_high: ScorerOutput,
    skeptic_output_large_delta: SkepticOutput,
) -> None:
    """When |scorer - skeptic| > 10, request_round_2 is set to True."""
    from agents.skeptic_agent import SkepticAgent

    with patch.object(SkepticAgent, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = skeptic_output_large_delta

        skeptic = SkepticAgent()
        sk_out = await skeptic.audit(
            job=sample_job, user_id="user_test",
            scorer_output=scorer_output_high, round_number=1,
        )

    assert sk_out.request_round_2 is True
    delta = abs(scorer_output_high.initial_score - sk_out.agreed_score)
    assert delta > 10


@pytest.mark.unit
@pytest.mark.asyncio
async def test_skeptic_veto_is_authoritative(
    sample_job: RawJob,
    scorer_output_high: ScorerOutput,
    skeptic_output_veto: SkepticOutput,
) -> None:
    """Skeptic veto=True means the pipeline must treat as SKIP."""
    from agents.skeptic_agent import SkepticAgent

    with patch.object(SkepticAgent, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = skeptic_output_veto

        skeptic = SkepticAgent()
        sk_out = await skeptic.audit(
            job=sample_job, user_id="user_test",
            scorer_output=scorer_output_high, round_number=1,
        )

    assert sk_out.veto is True
    assert sk_out.veto_reason is not None
    # Veto must not trigger round 2 (that would be contradictory)
    assert not sk_out.request_round_2


@pytest.mark.unit
def test_skeptic_output_veto_requires_reason() -> None:
    """SkepticOutput model_validator rejects veto=True without veto_reason."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SkepticOutput(
            job_id="test",
            user_id="user_test",
            round_number=1,
            agreed_score=20,
            veto=True,
            veto_reason=None,  # should fail
            rationale="Some rationale here that is at least fifty characters long for testing.",
        )


@pytest.mark.unit
def test_round2_flag_only_in_round1() -> None:
    """request_round_2=True is invalid in round 2."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SkepticOutput(
            job_id="test",
            user_id="user_test",
            round_number=2,
            agreed_score=60,
            veto=False,
            rationale="Some rationale here that is at least fifty characters long for testing.",
            request_round_2=True,  # invalid in round 2
        )
