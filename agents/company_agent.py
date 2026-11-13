"""agents/company_agent.py — CompanyVerificationAgent (Tier 2, DueDiligenceSuite sub-agent 4a).

Runs in parallel with RedFlagDetectorAgent and SkillGapAgent inside the
DueDiligenceSuite ParallelAgent. Verifies headcount, entity type, funding stage,
and recent news.

Model: "fast-flash"
"""

from __future__ import annotations

import logging

from agents.base import BaseIntelligenceAgent
from observability.setup import observe
from schemas.job_models import RawJob
from schemas.research_models import CompanyProfile

logger = logging.getLogger(__name__)


class CompanyVerificationAgent(BaseIntelligenceAgent):
    """Tier 2 sub-agent 4a: verify company facts from research data.

    Receives pre-fetched research text (from CompanyResearchAgent or raw search)
    and extracts structured CompanyProfile facts via structured LLM call.
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = CompanyProfile

    def _system_prompt(self) -> str:
        return (
            "You are a company due-diligence analyst. "
            "Extract structured facts from research text about a company. "
            "Be precise about entity type (MNC vs VC startup vs staffing agency vs consulting). "
            "Use UNKNOWN for fields you cannot confidently determine from the text. "
            "Do not guess — only state what the evidence supports."
        )

    def _build_messages(  # type: ignore[override]
        self,
        company_name: str,
        research_text: str,
    ) -> list[dict[str, str]]:
        return [
            self._system_message(),
            self._user_message(
                f"## Company: {company_name}\n\n"
                f"## Research Text\n{research_text[:4000]}\n\n"
                f"Extract a structured CompanyProfile from this information."
            ),
        ]

    @observe(name="CompanyVerificationAgent")
    async def verify(
        self,
        job: RawJob,
        research_text: str,
        trace_id: str | None = None,
    ) -> CompanyProfile:
        """Verify and structure company facts from research text.

        Args:
            job: The job posting (used for company name reference).
            research_text: Raw research text (from search/fetch results).
            trace_id: LangFuse trace ID.

        Returns:
            Structured CompanyProfile with entity type, headcount, funding, etc.
        """
        messages = self._build_messages(
            company_name=job.company,
            research_text=research_text,
        )
        profile: CompanyProfile = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=trace_id,
        )
        logger.info(
            "CompanyVerificationAgent: company=%s entity=%s funding=%s headcount=%s",
            job.company,
            profile.entity_type,
            profile.funding_stage,
            profile.headcount_bracket,
        )
        return profile
