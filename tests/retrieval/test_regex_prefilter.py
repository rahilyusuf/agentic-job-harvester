"""tests/retrieval/test_regex_prefilter.py — Unit tests for RegexPrefilter."""

from __future__ import annotations

import pytest
from retrieval.regex_prefilter import RegexPrefilter
from schemas.job_models import RawJob
from utils.time import utcnow


def make_job(job_id: str, title: str) -> RawJob:
    return RawJob(
        job_id=job_id,
        source="test",
        job_url=f"https://example.com/job/{job_id}",
        title=title,
        company="TestCorp",
        description="A " * 60,
        scraped_at=utcnow(),
    )


@pytest.fixture
def prefilter() -> RegexPrefilter:
    from unittest.mock import MagicMock
    settings = MagicMock()
    settings.prefilter_max_yoe = 5
    settings.prefilter_max_results = 15
    return RegexPrefilter(settings=settings)


@pytest.mark.unit
@pytest.mark.parametrize("title", [
    "VP of Engineering",
    "Vice President, AI",
    "Director of Machine Learning",
    "Head of Data Science",
    "Chief AI Officer",
    "CTO",
    "Engineering Manager",
    "Staff Engineer",
    "Distinguished Engineer",
])
def test_excluded_titles_are_rejected(prefilter: RegexPrefilter, title: str) -> None:
    """All management/seniored titles must be excluded by prefilter."""
    job = make_job("test-job", title)
    passed, rejected = prefilter.run([(job, 0.9)])
    assert len(passed) == 0, f"'{title}' should have been excluded"
    assert len(rejected) == 1
    assert "Excluded title" in rejected[0].rejection_reason or "management" in rejected[0].rejection_reason


@pytest.mark.unit
@pytest.mark.parametrize("title", [
    "Senior Machine Learning Engineer",
    "AI Engineer",
    "Software Engineer, ML Platform",
    "Research Engineer",
    "Data Scientist",
    "ML Platform Engineer",
])
def test_valid_titles_pass(prefilter: RegexPrefilter, title: str) -> None:
    """Valid (non-management) titles must pass the prefilter."""
    job = make_job("test-job", title)
    passed, rejected = prefilter.run([(job, 0.85)])
    assert len(passed) == 1, f"'{title}' should have passed the prefilter"
    assert len(rejected) == 0


@pytest.mark.unit
def test_yoe_ceiling_excludes_high_yoe_roles(prefilter: RegexPrefilter) -> None:
    """Jobs requiring YOE > 5 must be excluded when extraction is available."""
    from schemas.job_models import JobStructuredExtraction

    job = make_job("high-yoe-job", "Senior ML Engineer")
    extraction = JobStructuredExtraction(
        job_id="high-yoe-job",
        title_normalized="Senior ML Engineer",
        company_normalized="TestCorp",
        yoe_required_max=8,  # exceeds ceiling of 5
    )
    passed, rejected = prefilter.run([(job, 0.85)], extractions={"high-yoe-job": extraction})
    assert len(passed) == 0
    assert "YOE" in rejected[0].rejection_reason


@pytest.mark.unit
def test_max_results_cap_enforced(prefilter: RegexPrefilter) -> None:
    """Prefilter must not return more than max_results candidates."""
    jobs = [(make_job(f"job-{i}", "ML Engineer"), 0.9 - i * 0.01) for i in range(30)]
    passed, rejected = prefilter.run(jobs)
    assert len(passed) <= 15


@pytest.mark.unit
def test_empty_input_returns_empty(prefilter: RegexPrefilter) -> None:
    passed, rejected = prefilter.run([])
    assert passed == []
    assert rejected == []
