"""orchestration/pipeline.py — Tier 1 and Tier 2 pipeline graphs.

Implements ARCHITECTURE.md:
  Tier 1: Sequential → Router → LoopAgent(Scorer ↔ Skeptic, max 2 rounds) → Write to BQ
  Tier 2: Sequential → CompanyResearch → ParallelAgent(Verify | RedFlags | SkillGap)

All agents receive dependencies via constructor injection.
Repositories are injected as interface types (IRawJobRepository, etc.),
never as concrete *_repo.py classes.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from agents.company_agent import CompanyVerificationAgent
from agents.redflag_agent import RedFlagDetectorAgent
from agents.reflection_agent import ReflectionAgent
from agents.research_agent import CompanyResearchAgent
from agents.router_agent import JobIntakeRouter
from agents.scorer_agent import ScorerAgent
from agents.skillgap_agent import SkillGapAgent
from agents.skeptic_agent import SkepticAgent
from orchestration.output_validator import OutputValidator
from repositories.interfaces import (
    IActionsRepository,
    IEmbeddingRepository,
    IEvaluatedRepository,
    IProfileRepository,
    IRawJobRepository,
)
from schemas.diligence_models import RedFlagAnalysis, SkillGapReport
from schemas.job_models import EvaluatedJob, RawJob
from schemas.research_models import CompanyProfile, CompanyResearch
from schemas.routing_models import RoutingDecision

logger = logging.getLogger(__name__)


# ── Tier 1 ─────────────────────────────────────────────────────────────────────

@dataclass
class Tier1PipelineResult:
    """Output from a single Tier 1 pipeline run for one job + user."""

    job_id: str
    user_id: str
    routing_decision: str
    agreed_score: int | None
    skipped: bool
    evaluation: EvaluatedJob | None
    error: str | None = None


class Tier1Pipeline:
    """Tier 1: Router → ScoreDebateLoop → Write EvaluatedJob to BQ.

    Implements the SequentialAgent + LoopAgent pattern from ARCHITECTURE.md Step D-E.
    """

    def __init__(
        self,
        raw_jobs_repo: IRawJobRepository,
        evaluated_repo: IEvaluatedRepository,
        profile_repo: IProfileRepository,
    ) -> None:
        self._raw_jobs_repo = raw_jobs_repo
        self._evaluated_repo = evaluated_repo
        self._profile_repo = profile_repo
        self._router = JobIntakeRouter()
        self._scorer = ScorerAgent()
        self._skeptic = SkepticAgent()
        self._validator = OutputValidator()

    async def run(
        self,
        job: RawJob,
        user_id: str,
        resume_summary: str,
        preference_rules: list[dict] | None = None,
    ) -> Tier1PipelineResult:
        """Run Tier 1 for a single (job, user) pair.

        Step D: JobIntakeRouter → SKIP/PROCESS/HIGH_PRIORITY
        Step E: ScoreDebateLoop (LoopAgent, max 2 rounds)
                  ScorerAgent → SkepticAgent → [Round 2 if delta > threshold]

        Returns:
            Tier1PipelineResult with final evaluation or skip marker.
        """
        rules = preference_rules or []
        trace_id = self._scorer._new_trace_id()

        try:
            # ── Step D: Route ─────────────────────────────────────────────
            routing = await self._router.route(job=job, user_id=user_id)
            self._validator.validate(routing)

            if routing.decision == RoutingDecision.SKIP:
                logger.info("SKIP: job_id=%s reason=%s", job.job_id, routing.skip_reason)
                await self._raw_jobs_repo.update_status(job.job_id, "SKIPPED")
                return Tier1PipelineResult(
                    job_id=job.job_id,
                    user_id=user_id,
                    routing_decision=routing.decision.value,
                    agreed_score=None,
                    skipped=True,
                    evaluation=None,
                )

            # ── Step E: Score Debate Loop (max 2 rounds) ──────────────────
            scorer_output = await self._scorer.score(
                job=job,
                user_id=user_id,
                resume_summary=resume_summary,
                preference_rules=rules,
                round_number=1,
                trace_id=trace_id,
            )
            self._validator.validate(scorer_output)

            skeptic_output = await self._skeptic.audit(
                job=job,
                user_id=user_id,
                scorer_output=scorer_output,
                round_number=1,
                trace_id=trace_id,
            )
            self._validator.validate(skeptic_output)

            debate_rounds = 1

            # If skeptic requests round 2 (score delta > threshold)
            if skeptic_output.request_round_2 and not skeptic_output.veto:
                logger.info(
                    "Debate round 2 triggered: job_id=%s delta=%d",
                    job.job_id,
                    abs(scorer_output.initial_score - skeptic_output.agreed_score),
                )
                scorer_output_r2 = await self._scorer.score(
                    job=job,
                    user_id=user_id,
                    resume_summary=resume_summary,
                    preference_rules=rules,
                    round_number=2,
                    skeptic_feedback=skeptic_output.rationale,
                    trace_id=trace_id,
                )
                self._validator.validate(scorer_output_r2)

                skeptic_output = await self._skeptic.audit(
                    job=job,
                    user_id=user_id,
                    scorer_output=scorer_output_r2,
                    round_number=2,
                    trace_id=trace_id,
                )
                self._validator.validate(skeptic_output)
                debate_rounds = 2
                scorer_output = scorer_output_r2

            # Build EvaluatedJob
            evaluation = EvaluatedJob(
                job_id=job.job_id,
                user_id=user_id,
                scorer_score=scorer_output.initial_score,
                skeptic_score=skeptic_output.agreed_score,
                agreed_score=skeptic_output.agreed_score,
                debate_rounds=debate_rounds,
                skeptic_vetoed=skeptic_output.veto,
                routing_decision=routing.decision.value,
                scorer_rationale=scorer_output.rationale,
                skeptic_rationale=skeptic_output.rationale,
                disqualifiers=skeptic_output.hidden_disqualifiers,
                strengths=scorer_output.strengths,
            )

            # Persist to BigQuery
            await self._evaluated_repo.upsert_evaluation(evaluation)
            await self._raw_jobs_repo.update_status(job.job_id, "SCORED")

            logger.info(
                "Tier1 complete: job_id=%s agreed_score=%d rounds=%d vetoed=%s",
                job.job_id,
                evaluation.agreed_score,
                debate_rounds,
                evaluation.skeptic_vetoed,
            )
            return Tier1PipelineResult(
                job_id=job.job_id,
                user_id=user_id,
                routing_decision=routing.decision.value,
                agreed_score=evaluation.agreed_score,
                skipped=False,
                evaluation=evaluation,
            )

        except Exception as exc:
            logger.error("Tier1 pipeline error: job_id=%s error=%s", job.job_id, exc, exc_info=True)
            return Tier1PipelineResult(
                job_id=job.job_id,
                user_id=user_id,
                routing_decision="ERROR",
                agreed_score=None,
                skipped=False,
                evaluation=None,
                error=str(exc),
            )


# ── Tier 2 ─────────────────────────────────────────────────────────────────────

@dataclass
class Tier2PipelineResult:
    """Output from the Tier 2 DueDiligenceSuite for one job + user."""

    job_id: str
    user_id: str
    research: CompanyResearch | None
    company_profile: CompanyProfile | None
    red_flags: RedFlagAnalysis | None
    skill_gaps: SkillGapReport | None
    error: str | None = None


class Tier2Pipeline:
    """Tier 2: CompanyResearch → ParallelAgent(Verify | RedFlags | SkillGap).

    Triggered ONLY on user [🔍 Analyze] action — never automatic.
    Uses optimistic locking via tier2_status in job_evaluated.
    """

    def __init__(
        self,
        evaluated_repo: IEvaluatedRepository,
    ) -> None:
        self._evaluated_repo = evaluated_repo
        self._research_agent = CompanyResearchAgent()
        self._verify_agent = CompanyVerificationAgent()
        self._redflag_agent = RedFlagDetectorAgent()
        self._skillgap_agent = SkillGapAgent()
        self._validator = OutputValidator()

    async def run(
        self,
        job: RawJob,
        user_id: str,
        resume_summary: str,
    ) -> Tier2PipelineResult:
        """Run Tier 2 DueDiligenceSuite.

        Acquires optimistic lock on tier2_status before running.
        Runs verify + redflag + skillgap in parallel (ParallelAgent pattern).

        Args:
            job: The job to analyse.
            user_id: The requesting user.
            resume_summary: User's resume for skill gap analysis.

        Returns:
            Tier2PipelineResult with all diligence outputs.
        """
        try:
            # Optimistic lock
            await self._evaluated_repo.update_tier2_status(
                job.job_id, user_id, "IN_PROGRESS"
            )

            # Agent 3: Company Research (sequential, inputs to parallel agents)
            research = await self._research_agent.research(job=job, user_id=user_id)
            self._validator.validate(research)
            research_text = research.research_summary

            # Agent 4 (ParallelAgent): run all three sub-agents concurrently
            verify_task = self._verify_agent.verify(job=job, research_text=research_text)
            redflag_task = self._redflag_agent.detect(job=job, research_text=research_text)
            skillgap_task = self._skillgap_agent.analyze(
                job=job, user_id=user_id, resume_summary=resume_summary
            )

            profile, red_flags, skill_gaps = await asyncio.gather(
                verify_task, redflag_task, skillgap_task
            )
            self._validator.validate(profile)
            self._validator.validate(red_flags)
            self._validator.validate(skill_gaps)

            # Update tier2 status
            await self._evaluated_repo.update_tier2_status(job.job_id, user_id, "COMPLETE")

            logger.info(
                "Tier2 complete: job_id=%s critical_flags=%s readiness=%s",
                job.job_id,
                red_flags.has_critical_flags,
                skill_gaps.overall_readiness,
            )
            return Tier2PipelineResult(
                job_id=job.job_id,
                user_id=user_id,
                research=research,
                company_profile=profile,
                red_flags=red_flags,
                skill_gaps=skill_gaps,
            )

        except Exception as exc:
            await self._evaluated_repo.update_tier2_status(job.job_id, user_id, "ERROR")
            logger.error("Tier2 pipeline error: job_id=%s: %s", job.job_id, exc, exc_info=True)
            return Tier2PipelineResult(
                job_id=job.job_id,
                user_id=user_id,
                research=None,
                company_profile=None,
                red_flags=None,
                skill_gaps=None,
                error=str(exc),
            )


# ── Factory functions ──────────────────────────────────────────────────────────

def build_tier1_pipeline(
    raw_jobs_repo: IRawJobRepository,
    evaluated_repo: IEvaluatedRepository,
    profile_repo: IProfileRepository,
) -> Tier1Pipeline:
    """Build and return a configured Tier1Pipeline instance."""
    return Tier1Pipeline(
        raw_jobs_repo=raw_jobs_repo,
        evaluated_repo=evaluated_repo,
        profile_repo=profile_repo,
    )


def build_tier2_pipeline(
    evaluated_repo: IEvaluatedRepository,
) -> Tier2Pipeline:
    """Build and return a configured Tier2Pipeline instance."""
    return Tier2Pipeline(evaluated_repo=evaluated_repo)
