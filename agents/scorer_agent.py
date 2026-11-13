"""agents/scorer_agent.py — ScorerAgent.

Tier 1 Step E, debate round 1 (and optional round 2).
Scores job fit against user resume + Firestore dynamic_preference_rules.
Model: "fast-flash"
"""

from __future__ import annotations

import logging

from agents.base import BaseIntelligenceAgent
from observability.setup import observe
from schemas.job_models import RawJob
from schemas.scoring_models import ScorerOutput

logger = logging.getLogger(__name__)


class ScorerAgent(BaseIntelligenceAgent):
    """Tier 1 debate participant — scores job fit (0-100) against user resume.

    Called per debate round (max 2). In round 2, receives the Skeptic's
    feedback to refine the score if debate was triggered.

    Usage:
        scorer = ScorerAgent()
        output = await scorer.score(
            job=job,
            user_id=user_id,
            resume_summary=resume_text,
            preference_rules=rules,
            round_number=1,
        )
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = ScorerOutput

    def _system_prompt(self) -> str:
        return (
            "You are a senior technical recruiter and career advisor scoring job-to-candidate fit. "
            "Your scores reflect how well this role matches the candidate's skills, experience, "
            "target roles, salary expectations, and location preferences. "
            "Score 0-100: 90+ = exceptional fit, 70-89 = good fit, 50-69 = moderate, <50 = weak. "
            "Be honest about weaknesses. Do not inflate scores. "
            "Flag any concerns the Skeptic should investigate."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        resume_summary: str,
        preference_rules: list[dict],
        round_number: int,
        skeptic_feedback: str | None,
        user_id: str,
    ) -> list[dict[str, str]]:
        rules_text = "\n".join(
            f"- {r.get('condition', '')} → {r.get('action', '')} ({r.get('score_delta', 0):+d})"
            for r in preference_rules
        ) or "No active preference rules."

        round_context = ""
        if round_number == 2 and skeptic_feedback:
            round_context = (
                f"\n## Skeptic Feedback (Round 1)\n{skeptic_feedback}\n"
                "Revise your score taking this feedback into account."
            )

        return [
            self._system_message(),
            self._user_message(
                f"## Candidate Resume Summary\n{resume_summary}\n\n"
                f"## Active Preference Rules\n{rules_text}\n\n"
                f"## Job Posting\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Location: {job.location or 'Not specified'}\n"
                f"Description:\n{job.description[:3000]}\n"
                f"{round_context}\n"
                f"Score this role. job_id={job.job_id} user_id={user_id} round={round_number}"
            ),
        ]

    @observe(name="ScorerAgent")
    async def score(
        self,
        job: RawJob,
        user_id: str,
        resume_summary: str,
        preference_rules: list[dict],
        round_number: int = 1,
        skeptic_feedback: str | None = None,
        trace_id: str | None = None,
    ) -> ScorerOutput:
        """Score a job for a user.

        Args:
            job: The RawJob to score.
            user_id: The candidate user ID.
            resume_summary: Condensed resume text (used as scoring anchor).
            preference_rules: List of Firestore preference rule dicts.
            round_number: Debate round (1 or 2).
            skeptic_feedback: Skeptic's round 1 feedback (passed in round 2).
            trace_id: LangFuse trace ID for correlation.

        Returns:
            ScorerOutput with initial_score, strengths, concerns, and rationale.
        """
        messages = self._build_messages(
            job=job,
            resume_summary=resume_summary,
            preference_rules=preference_rules,
            round_number=round_number,
            skeptic_feedback=skeptic_feedback,
            user_id=user_id,
        )
        output: ScorerOutput = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=trace_id,
        )
        logger.info(
            "ScorerAgent: job_id=%s round=%d initial_score=%d",
            job.job_id,
            round_number,
            output.initial_score,
        )
        return output
