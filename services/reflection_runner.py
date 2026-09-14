"""services/reflection_runner.py — Weekly Cloud Scheduler entry point.

Runs the ReflectionAgent for all active users.
Triggered by Cloud Scheduler (weekly, e.g. Sunday 02:00 UTC).
Writes updated preference rules to Firestore and recalculates active centroids.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from config.settings import get_settings
from observability.setup import setup_observability
from agents.reflection_agent import ReflectionAgent
from repositories.actions_repo import ActionsRepository
from repositories.embeddings_repo import EmbeddingRepository
from repositories.profiles_repo import ProfileRepository

logger = logging.getLogger(__name__)


async def main() -> None:
    """Entry point for weekly Cloud Scheduler reflection run."""
    setup_observability()
    settings = get_settings()

    logger.info("Reflection runner started.")

    actions_repo = ActionsRepository()
    embedding_repo = EmbeddingRepository()
    profile_repo = ProfileRepository()

    agent = ReflectionAgent(
        actions_repo=actions_repo,
        embedding_repo=embedding_repo,
        profile_repo=profile_repo,
    )

    active_users = await profile_repo.get_active_users()
    logger.info("Running reflection for %d active users.", len(active_users))

    success_count = 0
    error_count = 0

    for user_id in active_users:
        try:
            rules = await agent.reflect(user_id=user_id)
            logger.info(
                "Reflection complete: user_id=%s rules=%d centroid_updated=%s",
                user_id,
                len(rules.rules),
                rules.centroid_updated,
            )
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Reflection failed for user_id=%s: %s", user_id, exc, exc_info=True)
            error_count += 1

    logger.info(
        "Reflection runner complete: success=%d errors=%d",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
