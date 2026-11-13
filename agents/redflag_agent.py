"""agents/redflag_agent.py — RedFlagDetectorAgent (Tier 2, DueDiligenceSuite sub-agent 4b).

Scans for ghost jobs (>60 days active), Glassdoor toxicity, unrealistic skill demands,
and staffing agency fronts. Runs in parallel with CompanyVerificationAgent and SkillGapAgent.

Model: "fast-flash"
"""

from __future__ import annotations

import logging

from agents.base import BaseIntelligenceAgent
from observability.setup import observe
from schemas.diligence_models import RedFlagAnalysis
from schemas.job_models import RawJob
from utils.time import days_since

logger = logging.getLogger(__name__)


class RedFlagDetectorAgent(BaseIntelligenceAgent):
    """Tier 2 sub-agent 4b: detect red flags in a job posting and company.

    Analyses job age, skill demand realism, Glassdoor signals, and company
    authenticity. Sets has_critical_flags=True for blocking disqualifiers.
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = RedFlagAnalysis

    def _system_prompt(self) -> str:
        return (
            "You are a job market fraud and toxicity analyst. "
            "Detect red flags in job postings: ghost/evergreen listings, staffing agency fronts, "
            "salary bait-and-switch, unrealistic skill stacks (5 years of a 2-year-old framework), "
            "Glassdoor toxicity patterns, and fake urgency. "
            "Be specific — cite evidence for each flag. "
            "Use severity CRITICAL only for hard disqualifiers (fraud, confirmed toxic culture). "
            "Set ghost_job_probability based on job age and listing patterns."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        research_text: str,
        job_age_days: float,
    ) -> list[dict[str, str]]:
        return [
            self._system_message(),
            self._user_message(
                f"## Job Posting\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Posted: {job_age_days:.0f} days ago\n"
                f"ATS Platform: {job.ats_platform or 'unknown'}\n"
                f"Salary: {job.salary_raw or 'Not specified'}\n"
                f"Description:\n{job.description[:3000]}\n\n"
                f"## Company Research Context\n{research_text[:2000]}\n\n"
                f"Identify all red flags. job_id={job.job_id}"
            ),
        ]

    @observe(name="RedFlagDetectorAgent")
    async def detect(
        self,
        job: RawJob,
        research_text: str = "",
        trace_id: str | None = None,
    ) -> RedFlagAnalysis:
        """Detect red flags in a job posting.

        Args:
            job: The RawJob to analyse.
            research_text: Company research context (from CompanyResearchAgent).
            trace_id: LangFuse trace ID.

        Returns:
            RedFlagAnalysis with flags, ghost probability, and overall recommendation.
        """
        job_age_days = days_since(job.scraped_at) if job.scraped_at else 0.0

        messages = self._build_messages(
            job=job,
            research_text=research_text,
            job_age_days=job_age_days,
        )
        analysis: RedFlagAnalysis = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=trace_id,
        )
        logger.info(
            "RedFlagDetector: job_id=%s flags=%d critical=%s recommendation=%s",
            job.job_id,
            len(analysis.flags),
            analysis.has_critical_flags,
            analysis.overall_recommendation,
        )
        return analysis
