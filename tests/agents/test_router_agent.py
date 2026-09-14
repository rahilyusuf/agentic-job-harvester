"""tests/agents/test_router_agent.py — Unit tests for JobIntakeRouter.

Uses unittest.mock to patch gateway.client.GatewayClient so no real LLM calls are made.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from schemas.job_models import RawJob, JobStatus
from schemas.routing_models import RouterDecision, RoutingDecision
from utils.time import utcnow


@pytest.fixture
def sample_job() -> RawJob:
    return RawJob(
        job_id="test-job-001",
        source="apify-linkedin",
        job_url="https://www.linkedin.com/jobs/view/123456",
        title="Senior Machine Learning Engineer",
        company="DeepMind",
        location="London, UK",
        description=(
            "We are looking for a Senior ML Engineer to join our research team. "
            "You will work on large-scale deep learning systems. "
            "Requirements: 3+ years ML engineering, PyTorch, distributed training. "
            "This is a full-time role with competitive compensation."
        ),
        scraped_at=utcnow(),
    )


@pytest.fixture
def mock_skip_decision() -> RouterDecision:
    return RouterDecision(
        job_id="test-job-001",
        decision=RoutingDecision.SKIP,
        confidence=0.95,
        reasoning="Title requires Director-level seniority which exceeds user preference.",
        matched_rules=["rule_no_director"],
        skip_reason="Director title excluded by preference rules",
    )


@pytest.fixture
def mock_process_decision() -> RouterDecision:
    return RouterDecision(
        job_id="test-job-001",
        decision=RoutingDecision.PROCESS,
        confidence=0.85,
        reasoning="Strong ML engineering role matching user skills and targets.",
        matched_rules=["rule_ml_boost"],
    )


@pytest.fixture
def mock_high_priority_decision() -> RouterDecision:
    return RouterDecision(
        job_id="test-job-001",
        decision=RoutingDecision.HIGH_PRIORITY,
        confidence=0.97,
        reasoning="Exceptional match: DeepMind, ML role, target company on whitelist.",
        matched_rules=["rule_deepmind_priority", "rule_ml_boost"],
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_router_returns_skip(sample_job: RawJob, mock_skip_decision: RouterDecision) -> None:
    """Router returns SKIP when job matches exclusion rules."""
    with patch("agents.router_agent.JobIntakeRouter._call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = mock_skip_decision
        with patch("agents.router_agent.JobIntakeRouter._load_preference_rules", new_callable=AsyncMock) as mock_rules:
            mock_rules.return_value = [{"condition": "director title", "action": "SKIP"}]

            from agents.router_agent import JobIntakeRouter
            router = JobIntakeRouter()
            decision = await router.route(job=sample_job, user_id="user_test")

    assert decision.decision == RoutingDecision.SKIP
    assert decision.skip_reason is not None
    assert decision.confidence > 0.0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_router_returns_process(sample_job: RawJob, mock_process_decision: RouterDecision) -> None:
    """Router returns PROCESS for a standard match."""
    with patch("agents.router_agent.JobIntakeRouter._call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = mock_process_decision
        with patch("agents.router_agent.JobIntakeRouter._load_preference_rules", new_callable=AsyncMock) as mock_rules:
            mock_rules.return_value = []

            from agents.router_agent import JobIntakeRouter
            router = JobIntakeRouter()
            decision = await router.route(job=sample_job, user_id="user_test")

    assert decision.decision == RoutingDecision.PROCESS
    assert decision.confidence >= 0.0
    assert len(decision.reasoning) >= 20


@pytest.mark.unit
@pytest.mark.asyncio
async def test_router_returns_high_priority(
    sample_job: RawJob, mock_high_priority_decision: RouterDecision
) -> None:
    """Router returns HIGH_PRIORITY for exceptional matches."""
    with patch("agents.router_agent.JobIntakeRouter._call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = mock_high_priority_decision
        with patch("agents.router_agent.JobIntakeRouter._load_preference_rules", new_callable=AsyncMock) as mock_rules:
            mock_rules.return_value = [{"condition": "deepmind", "action": "PRIORITIZE"}]

            from agents.router_agent import JobIntakeRouter
            router = JobIntakeRouter()
            decision = await router.route(job=sample_job, user_id="user_test")

    assert decision.decision == RoutingDecision.HIGH_PRIORITY
    assert len(decision.matched_rules) > 0


@pytest.mark.unit
def test_router_decision_schema_validates() -> None:
    """RouterDecision rejects invalid decision values."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        RouterDecision(
            job_id="test",
            decision="INVALID_VALUE",  # not in enum
            confidence=0.5,
            reasoning="x" * 20,
        )


@pytest.mark.unit
def test_router_decision_skip_has_no_reason_raises() -> None:
    """SKIP decisions without skip_reason are still valid (reason optional)."""
    decision = RouterDecision(
        job_id="test",
        decision=RoutingDecision.SKIP,
        confidence=0.9,
        reasoning="Excluded by rule matching.",
        skip_reason=None,  # optional
    )
    assert decision.decision == RoutingDecision.SKIP
