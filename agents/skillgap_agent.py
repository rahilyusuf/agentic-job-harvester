"""agents/skillgap_agent.py — SkillGapAgent + E2B sandbox (Tier 2, sub-agent 4c).

Identifies blocking vs learnable skill gaps between job requirements and the user's resume.
For each blocking gap, generates and executes a Python practice challenge in E2B.

Model: "fast-flash"
"""

from __future__ import annotations

import asyncio
import logging

from agents.base import BaseIntelligenceAgent
from observability.setup import observe
from schemas.diligence_models import CodeChallenge, SkillGap, SkillGapReport
from schemas.job_models import RawJob
from tools.e2b_tools import execute_in_sandbox

logger = logging.getLogger(__name__)

# Intermediate schema for LLM gap identification before E2B execution
from pydantic import BaseModel, Field


class _GapIdentificationOutput(BaseModel):
    """Intermediate LLM output for gap identification before E2B execution."""

    gaps: list[SkillGap] = Field(default_factory=list)
    overall_readiness: str = Field(
        ..., description="READY | MINOR_GAPS | SIGNIFICANT_GAPS | NOT_QUALIFIED"
    )
    analysis_summary: str


class SkillGapAgent(BaseIntelligenceAgent):
    """Tier 2 sub-agent 4c: identify skill gaps and run E2B practice challenges.

    Workflow:
      1. LLM identifies blocking vs learnable gaps from resume + JD.
      2. For each blocking gap, generates a Python challenge via LLM.
      3. Executes the challenge in E2B AsyncSandbox.
      4. Returns full SkillGapReport with CodeChallenge results.
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = _GapIdentificationOutput

    def _system_prompt(self) -> str:
        return (
            "You are a technical skills assessor for an AI/ML engineering role. "
            "Compare the candidate's resume skills against the job requirements. "
            "Classify each gap as: "
            "  blocking = required skill the candidate clearly lacks "
            "  learnable = nice-to-have or acquirable quickly. "
            "For blocking gaps, describe a practical Python coding challenge to assess/practice it. "
            "Be realistic — do not invent gaps not mentioned in the JD."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        resume_summary: str,
        user_id: str,
    ) -> list[dict[str, str]]:
        return [
            self._system_message(),
            self._user_message(
                f"## Job Requirements\n"
                f"Title: {job.title}\n"
                f"Description:\n{job.description[:3000]}\n\n"
                f"## Candidate Resume Summary\n{resume_summary}\n\n"
                f"Identify skill gaps. job_id={job.job_id} user_id={user_id}"
            ),
        ]

    def _build_challenge_messages(self, skill: str, context: str) -> list[dict[str, str]]:
        """Build messages for generating a Python challenge for a specific blocking skill."""
        return [
            {"role": "system", "content": (
                "Generate a concise Python coding challenge (20-40 lines) that demonstrates "
                f"practical competency in {skill}. "
                "The code must be self-contained, runnable, and produce clear output. "
                "Include comments explaining what it demonstrates."
            )},
            {"role": "user", "content": (
                f"Create a runnable Python challenge for: {skill}\n"
                f"Job context: {context[:500]}\n"
                "Return only executable Python code, no markdown fences."
            )},
        ]

    @observe(name="SkillGapAgent")
    async def analyze(
        self,
        job: RawJob,
        user_id: str,
        resume_summary: str,
        trace_id: str | None = None,
    ) -> SkillGapReport:
        """Identify skill gaps and execute E2B challenges for blocking gaps.

        Args:
            job: The job posting to analyse.
            user_id: The candidate user ID.
            resume_summary: Condensed resume text.
            trace_id: LangFuse trace ID.

        Returns:
            SkillGapReport with all gaps and CodeChallenge results.
        """
        tid = trace_id or self._new_trace_id()

        # Step 1: Identify gaps via LLM
        gap_messages = self._build_messages(job=job, resume_summary=resume_summary, user_id=user_id)
        gap_output: _GapIdentificationOutput = await self._call_llm(  # type: ignore[assignment]
            messages=gap_messages,
            trace_id=tid,
        )

        # Step 2: Generate and execute E2B challenges for blocking gaps (concurrently)
        blocking_gaps = [g for g in gap_output.gaps if g.is_blocking]
        challenges: list[CodeChallenge] = []

        if blocking_gaps:
            # Generate challenge code for each blocking gap
            challenge_tasks = []
            for gap in blocking_gaps:
                challenge_msgs = self._build_challenge_messages(gap.skill, job.description[:500])
                challenge_tasks.append(
                    self._generate_and_execute_challenge(gap.skill, challenge_msgs)
                )
            # Run all challenge generations concurrently
            challenges = list(await asyncio.gather(*challenge_tasks, return_exceptions=False))

        report = SkillGapReport(
            job_id=job.job_id,
            user_id=user_id,
            gaps=gap_output.gaps,
            code_challenges=challenges,
            overall_readiness=gap_output.overall_readiness,
        )
        logger.info(
            "SkillGapAgent: job_id=%s blocking=%d learnable=%d challenges=%d readiness=%s",
            job.job_id,
            report.blocking_gap_count,
            report.learnable_gap_count,
            len(challenges),
            report.overall_readiness,
        )
        return report

    async def _generate_and_execute_challenge(
        self, skill: str, challenge_messages: list[dict[str, str]]
    ) -> CodeChallenge:
        """Generate Python challenge code and execute in E2B."""
        from pydantic import BaseModel as _BM, Field as _F

        class _CodeOutput(_BM):
            python_code: str = _F(..., description="Runnable Python code for the challenge")
            challenge_description: str = _F(..., description="Human-readable challenge description")

        # Temporarily swap OUTPUT_SCHEMA for challenge generation
        original_schema = self.__class__.OUTPUT_SCHEMA
        self.__class__.OUTPUT_SCHEMA = _CodeOutput  # type: ignore[assignment]
        try:
            code_output: _CodeOutput = await self._call_llm(  # type: ignore[assignment]
                messages=challenge_messages, trace_id=self._new_trace_id()
            )
        finally:
            self.__class__.OUTPUT_SCHEMA = original_schema  # type: ignore[assignment]

        return await execute_in_sandbox(
            skill=skill,
            challenge_description=code_output.challenge_description,
            python_code=code_output.python_code,
        )
