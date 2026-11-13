"""agents/skeptic_agent.py — SkepticAgent.

Tier 1 Step E, debate round 1 (and optional round 2).
Audits the Scorer's output for hidden disqualifiers (ghost jobs, recruiter spam,
unrealistic demands). Holds FINAL veto authority.

If |scorer_score - agreed_score| > 10 in round 1 → triggers round 2.
Skeptic's agreed_score is the authoritative final score in all cases.

Model: "fast-flash"
"""

from __future__ import annotations

import logging

from agents.base import BaseIntelligenceAgent
from config.settings import get_settings
from observability.setup import observe
from schemas.job_models import RawJob
from schemas.scoring_models import ScorerOutput, SkepticOutput

logger = logging.getLogger(__name__)


class SkepticAgent(BaseIntelligenceAgent):
    """Tier 1 debate auditor — challenges the Scorer and holds veto authority.

    Usage:
        skeptic = SkepticAgent()
        output = await skeptic.audit(
            job=job,
            user_id=user_id,
            scorer_output=scorer_output,
            round_number=1,
        )
        if output.veto:
            # pipeline must treat as SKIP
            ...
    """

    MODEL_ALIAS = "fast-flash"
    OUTPUT_SCHEMA = SkepticOutput

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._settings = get_settings()

    def _system_prompt(self) -> str:
        return (
            "You are a sceptical career advisor reviewing a job scoring decision. "
            "Your role is to find what the Scorer missed: ghost jobs (posted >60 days), "
            "staffing agency fronts, salary bait-and-switch, unrealistic skill demands, "
            "or cultural toxicity signals. "
            "You have FINAL VETO authority. If you detect a critical disqualifier, "
            "set veto=True and explain why. "
            "Your agreed_score is the authoritative final score — adjust the Scorer's score "
            "only if you have concrete evidence. Do not adjust without evidence."
        )

    def _build_messages(  # type: ignore[override]
        self,
        job: RawJob,
        scorer_output: ScorerOutput,
        round_number: int,
        user_id: str,
    ) -> list[dict[str, str]]:
        return [
            self._system_message(),
            self._user_message(
                f"## Job Posting\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Location: {job.location or 'Not specified'}\n"
                f"Salary: {job.salary_raw or 'Not specified'}\n"
                f"Description:\n{job.description[:3000]}\n\n"
                f"## Scorer's Assessment (Round {round_number})\n"
                f"Score: {scorer_output.initial_score}/100\n"
                f"Strengths: {', '.join(scorer_output.strengths)}\n"
                f"Concerns: {', '.join(scorer_output.concerns)}\n"
                f"Rationale: {scorer_output.rationale}\n\n"
                f"Audit this scoring. job_id={job.job_id} user_id={user_id} round={round_number}\n"
                f"Score delta threshold for round 2: "
                f"{self._settings.debate_score_delta_threshold} points."
            ),
        ]

    @observe(name="SkepticAgent")
    async def audit(
        self,
        job: RawJob,
        user_id: str,
        scorer_output: ScorerOutput,
        round_number: int = 1,
        trace_id: str | None = None,
    ) -> SkepticOutput:
        """Audit a ScorerAgent output and return the final score with veto capability.

        Args:
            job: The RawJob being evaluated.
            user_id: The candidate user ID.
            scorer_output: The Scorer's output from this round.
            round_number: Debate round (1 or 2).
            trace_id: LangFuse trace ID for correlation.

        Returns:
            SkepticOutput with agreed_score (final), veto flag, and hidden disqualifiers.
        """
        messages = self._build_messages(
            job=job,
            scorer_output=scorer_output,
            round_number=round_number,
            user_id=user_id,
        )
        output: SkepticOutput = await self._call_llm(  # type: ignore[assignment]
            messages=messages,
            trace_id=trace_id,
        )

        # Enforce round_2 trigger logic based on score delta
        delta = abs(scorer_output.initial_score - output.agreed_score)
        should_request_r2 = (
            delta > self._settings.debate_score_delta_threshold and round_number == 1
        )
        if should_request_r2 and not output.request_round_2:
            # Override model's decision — the delta rule is deterministic
            object.__setattr__(output, "request_round_2", True)

        logger.info(
            "SkepticAgent: job_id=%s round=%d agreed_score=%d veto=%s delta=%d r2=%s",
            job.job_id,
            round_number,
            output.agreed_score,
            output.veto,
            delta,
            output.request_round_2,
        )
        return output
