"""services/tier1_matcher_runner.py — Daily Cloud Scheduler Tier 1 entry point.

Cloud Run job triggered by Cloud Scheduler (daily).
Runs the full Tier 1 pipeline for all active users:
  Step A: Vector search (top 50 per user)
  Step B: Regex prefilter (→ ~15 per user)
  Step C-E: Router → ScoreDebateLoop → Write to BQ → Notify if score >= threshold

ARCHITECTURE.md: "O(jobs) pre-computation + O(users * top_k) LLM scoring"
"""

from __future__ import annotations

import asyncio
import logging
import sys

from config.settings import get_settings
from observability.setup import setup_observability
from orchestration.pipeline import build_tier1_pipeline
from repositories.actions_repo import ActionsRepository
from repositories.embeddings_repo import EmbeddingRepository
from repositories.evaluated_repo import EvaluatedRepository
from repositories.profiles_repo import ProfileRepository
from repositories.raw_jobs_repo import RawJobRepository
from retrieval.regex_prefilter import RegexPrefilter
from retrieval.vector_matcher import VectorMatcher
from schemas.job_models import RawJob

logger = logging.getLogger(__name__)


async def run_tier1_for_user(
    user_id: str,
    raw_repo: RawJobRepository,
    embedding_repo: EmbeddingRepository,
    evaluated_repo: EvaluatedRepository,
    profile_repo: ProfileRepository,
    settings: object,
) -> dict[str, int]:
    """Run Tier 1 pipeline for a single user. Returns stats dict."""
    matcher = VectorMatcher(embedding_repo=embedding_repo, profile_repo=profile_repo)
    prefilter = RegexPrefilter()
    pipeline = build_tier1_pipeline(
        raw_jobs_repo=raw_repo,
        evaluated_repo=evaluated_repo,
        profile_repo=profile_repo,
    )

    profile = await profile_repo.get_profile(user_id)
    if profile is None:
        logger.warning("No profile for user_id=%s, skipping.", user_id)
        return {"skipped": 0, "scored": 0, "errors": 0}

    # Load resume summary for scoring
    resume_summary = (
        f"Title: {profile.resume_metadata.current_title}\n"
        f"YOE: {profile.resume_metadata.total_yoe}\n"
        f"Skills: {', '.join(profile.resume_metadata.top_skills[:20])}\n"
        f"Targets: {', '.join(profile.resume_metadata.target_titles)}"
    )

    # Step A: Vector search
    vector_results = await matcher.retrieve(user_id=user_id)

    # Fetch RawJob objects for matched job_ids
    job_objects: list[RawJob] = []
    for vr in vector_results:
        job = await raw_repo.get_job(vr.job_id)
        if job:
            job_objects.append(job)

    candidates_with_scores = list(zip(job_objects, [vr.similarity_score for vr in vector_results if any(j.job_id == vr.job_id for j in job_objects)]))

    # Step B: Regex prefilter
    passed, rejected = prefilter.run(candidates_with_scores)
    logger.info("User %s: %d vector results → %d passed prefilter", user_id, len(vector_results), len(passed))

    # Step C-E: Run Tier 1 pipeline for each passed candidate
    stats = {"skipped": 0, "scored": 0, "errors": 0}
    for pr in passed:
        job = next((j for j in job_objects if j.job_id == pr.job_id), None)
        if job is None:
            continue
        result = await pipeline.run(job=job, user_id=user_id, resume_summary=resume_summary)
        if result.error:
            stats["errors"] += 1
        elif result.skipped:
            stats["skipped"] += 1
        else:
            stats["scored"] += 1

    return stats


async def main() -> None:
    """Entry point for Cloud Scheduler-triggered daily run."""
    setup_observability()
    settings = get_settings()

    logger.info("Tier1 matcher runner started.")

    raw_repo = RawJobRepository()
    embedding_repo = EmbeddingRepository()
    evaluated_repo = EvaluatedRepository()
    profile_repo = ProfileRepository()

    # Get all active users
    active_users = await profile_repo.get_active_users()
    logger.info("Processing %d active users", len(active_users))

    total_stats = {"skipped": 0, "scored": 0, "errors": 0}
    for user_id in active_users:
        try:
            stats = await run_tier1_for_user(
                user_id=user_id,
                raw_repo=raw_repo,
                embedding_repo=embedding_repo,
                evaluated_repo=evaluated_repo,
                profile_repo=profile_repo,
                settings=settings,
            )
            for k, v in stats.items():
                total_stats[k] += v
            logger.info("User %s done: %s", user_id, stats)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed Tier1 run for user_id=%s: %s", user_id, exc, exc_info=True)
            total_stats["errors"] += 1

    logger.info("Tier1 runner complete: %s", total_stats)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
