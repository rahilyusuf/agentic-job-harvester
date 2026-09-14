"""agents/research_agent.py — CompanyResearchAgent (Tier 2, ReAct).

Implements ARCHITECTURE.md Agent 3:
    ADK LlmAgent, ReAct loop, max 5 iterations.
    Tools: lookup_research_cache, google_search, fetch_url, check_application_history.
    Triggered only on user [🔍 Analyze] action — never automatic.

Model: "fast-flash"
"""

from __future__ import annotations

import logging

from agents.base import BaseIntelligenceAgent
from observability.setup import observe
from schemas.job_models import RawJob
from schemas.research_models import CompanyResearch
from tools.cache_tools import lookup_research_cache, write_research_cache
from tools.history_tools import check_application_history
from tools.search_tools import fetch_url, google_search

logger = logging.getLogger(__name__)

_MAX_ITERATIONS = 5


class CompanyResearchAgent(BaseIntelligenceAgent):
    """Tier 2 Agent 3: ReAct loop for deep company research.

    Orchestrates up to 5 research iterations using search and cache tools
    before synthesising a CompanyResearch output.

    NOTE: This agent implements its own ReAct loop rather than using ADK's
    LlmAgent directly, so it integrates cleanly with the Instructor/LiteLLM
    pipeline for structured output enforcement.

    Usage:
        agent = CompanyResearchAgent()
        research = await agent.research(job=job, user_id=user_id)
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = CompanyResearch

    def _system_prompt(self) -> str:
        return (
            "You are a company research specialist conducting due diligence on a company "
            "for a senior AI/ML engineer considering applying. "
            "Investigate: entity type (startup/MNC/staffing), funding stage, headcount, "
            "recent news (layoffs, pivots, Series round), Glassdoor signals, and whether "
            "the hiring manager/team seems credible. "
            "Use your tools methodically. Start with the cache, then search, then fetch pages. "
            "After gathering facts, synthesise them into a clear, opinionated summary. "
            "Maximum 5 tool calls total."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        user_id: str,
        tool_results: list[str],
    ) -> list[dict[str, str]]:
        tools_context = (
            "\n\n".join(tool_results) if tool_results else "No tool results yet."
        )
        return [
            self._system_message(),
            self._user_message(
                f"## Job to Research\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Location: {job.location or 'Not specified'}\n"
                f"Job URL: {job.job_url}\n"
                f"Description excerpt:\n{job.description[:1000]}\n\n"
                f"## Research Results So Far\n{tools_context}\n\n"
                f"Synthesise everything into a CompanyResearch output. "
                f"job_id={job.job_id} user_id={user_id}"
            ),
        ]

    @observe(name="CompanyResearchAgent")
    async def research(self, job: RawJob, user_id: str) -> CompanyResearch:
        """Conduct full company research using a ReAct-style tool loop.

        Args:
            job: The job posting to research.
            user_id: The user requesting research (for history lookup).

        Returns:
            CompanyResearch aggregating all findings.
        """
        tool_results: list[str] = []
        iterations = 0
        cache_hit = False

        # Iteration 1: Check cache first
        cached = await lookup_research_cache(job.company)
        if cached is not None:
            cache_hit = True
            tool_results.append(f"[CACHE HIT] {job.company} research:\n{str(cached)[:2000]}")
            iterations = 1
        else:
            # Iteration 2: Google search
            search_results = await google_search(
                f"{job.company} company funding headcount recent news Glassdoor", num_results=5
            )
            tool_results.append(
                "[SEARCH] " + "\n".join(
                    f"- {r['title']}: {r['snippet']} ({r['url']})"
                    for r in search_results
                )
            )
            iterations += 1

            # Iteration 3: Check application history
            if iterations < _MAX_ITERATIONS:
                history = await check_application_history(job.company, user_id)
                tool_results.append(f"[HISTORY] {history}")
                iterations += 1

            # Iteration 4: Fetch company website if URL is short/clean
            if iterations < _MAX_ITERATIONS and job.job_url:
                # Try fetching the company homepage (strip job path)
                from urllib.parse import urlparse
                parsed = urlparse(str(job.job_url))
                homepage = f"{parsed.scheme}://{parsed.netloc}"
                page_content = await fetch_url(homepage)
                tool_results.append(f"[WEBPAGE] {homepage}:\n{page_content[:1500]}")
                iterations += 1

            # Iteration 5: Fetch Glassdoor if found in search
            if iterations < _MAX_ITERATIONS:
                glassdoor_result = next(
                    (r for r in search_results if "glassdoor.com" in r.get("url", "")), None
                )
                if glassdoor_result:
                    gd_content = await fetch_url(glassdoor_result["url"])
                    tool_results.append(f"[GLASSDOOR]:\n{gd_content[:1500]}")
                    iterations += 1

        # Synthesise via LLM
        messages = self._build_messages(job=job, user_id=user_id, tool_results=tool_results)
        research: CompanyResearch = await self._call_llm(  # type: ignore[assignment]
            messages=messages, trace_id=self._new_trace_id()
        )

        # Override computed fields
        object.__setattr__(research, "cache_hit", cache_hit)
        object.__setattr__(research, "iterations_used", iterations)

        # Write to cache (best-effort, non-blocking)
        if not cache_hit:
            await write_research_cache(job.company, research.model_dump(mode="json"))

        logger.info(
            "CompanyResearchAgent completed: job_id=%s company=%s cache_hit=%s iterations=%d",
            job.job_id, job.company, cache_hit, iterations,
        )
        return research
