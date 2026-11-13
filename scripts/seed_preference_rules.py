"""scripts/seed_preference_rules.py — Seed initial Firestore dynamic_preference_rules.

Seeds a user's preference rules with sensible defaults before the first
ReflectionAgent run. Useful for bootstrapping a new user.

Usage:
    python scripts/seed_preference_rules.py --user-id YOUR_TELEGRAM_CHAT_ID
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

DEFAULT_RULES = [
    {
        "rule_id": "rule_ml_engineer_boost",
        "category": "TITLE_PATTERN",
        "action": "BOOST",
        "condition": "Title contains 'ML Engineer', 'AI Engineer', 'Machine Learning Engineer'",
        "score_delta": 15,
        "confidence": 0.9,
        "signal_count": 1,
        "examples": [],
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    },
    {
        "rule_id": "rule_staffing_skip",
        "category": "COMPANY_TYPE",
        "action": "SKIP",
        "condition": "Company is a staffing agency or recruiter front",
        "score_delta": None,
        "confidence": 0.95,
        "signal_count": 1,
        "examples": [],
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    },
    {
        "rule_id": "rule_startup_boost",
        "category": "COMPANY_TYPE",
        "action": "BOOST",
        "condition": "Company is a VC-backed startup at Series A-C with AI/ML focus",
        "score_delta": 10,
        "confidence": 0.8,
        "signal_count": 1,
        "examples": [],
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    },
    {
        "rule_id": "rule_remote_preferred",
        "category": "REMOTE_PREFERENCE",
        "action": "BOOST",
        "condition": "Role is fully remote or hybrid",
        "score_delta": 8,
        "confidence": 0.85,
        "signal_count": 1,
        "examples": [],
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    },
]


async def seed_rules(user_id: str) -> None:
    from config.settings import get_settings
    from google.cloud import firestore

    settings = get_settings()
    fs = firestore.AsyncClient(
        project=settings.gcp_project_id,
        database=settings.firestore_database_id,
    )

    doc_ref = fs.collection("config_cache").document(user_id)
    existing = await doc_ref.get()

    if existing.exists:
        logger.warning(
            "Rules already exist for user_id=%s. Use --force to overwrite.", user_id
        )
        return

    data = {
        "user_id": user_id,
        "rules": DEFAULT_RULES,
        "synthesis_period_days": 30,
        "centroid_updated": False,
        "alpha_used": 0.0,
        "accepted_jobs_count": 0,
        "passed_jobs_count": 0,
        "synthesized_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    await doc_ref.set(data)
    logger.info("Seeded %d default rules for user_id=%s", len(DEFAULT_RULES), user_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed initial preference rules")
    parser.add_argument("--user-id", required=True, help="User ID (Telegram chat ID or UUID)")
    args = parser.parse_args()
    asyncio.run(seed_rules(user_id=args.user_id))
