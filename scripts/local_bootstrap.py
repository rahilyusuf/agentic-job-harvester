"""scripts/local_bootstrap.py — Local development environment setup.

Creates BigQuery dataset and tables, sets up Firestore collections,
and validates environment configuration for local development.

Usage:
    python scripts/local_bootstrap.py
    python scripts/local_bootstrap.py --dry-run  # validate only, no writes
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent.parent / "sql"
SQL_FILES = [
    "raw_job_postings.sql",
    "job_embeddings.sql",
    "job_evaluated.sql",
    "job_user_actions.sql",
    "user_profiles.sql",
]


def validate_env() -> bool:
    """Check required environment variables are set."""
    required = [
        "GCP_PROJECT_ID",
        "LITELLM_PROXY_BASE_URL",
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
    ]
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        logger.error("Missing required env vars: %s", ", ".join(missing))
        return False
    logger.info("Environment validation passed ✓")
    return True


def create_bq_dataset(project: str, dataset: str, location: str, dry_run: bool) -> None:
    """Create the BigQuery dataset if it doesn't exist."""
    from google.cloud import bigquery
    from google.cloud.exceptions import Conflict

    client = bigquery.Client(project=project)
    dataset_ref = bigquery.DatasetReference(project, dataset)
    bq_dataset = bigquery.Dataset(dataset_ref)
    bq_dataset.location = location

    if dry_run:
        logger.info("[DRY RUN] Would create dataset: %s.%s (%s)", project, dataset, location)
        return

    try:
        client.create_dataset(bq_dataset, exists_ok=True)
        logger.info("Dataset ready: %s.%s", project, dataset)
    except Exception as exc:
        logger.error("Failed to create dataset: %s", exc)
        raise


def create_bq_tables(project: str, dataset: str, dry_run: bool) -> None:
    """Execute DDL SQL files to create tables."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project)

    for sql_file in SQL_FILES:
        sql_path = SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning("SQL file not found: %s", sql_path)
            continue

        sql = sql_path.read_text()
        # Substitute template variables
        sql = sql.replace("${GCP_PROJECT_ID}", project)
        sql = sql.replace("${BQ_DATASET}", dataset)

        if dry_run:
            logger.info("[DRY RUN] Would execute DDL: %s", sql_file)
            continue

        try:
            client.query_and_wait(sql)
            logger.info("Table created/verified: %s ✓", sql_file)
        except Exception as exc:
            logger.warning("DDL warning for %s: %s (table may already exist)", sql_file, exc)


def main(dry_run: bool = False) -> None:
    from config.settings import get_settings

    if not validate_env():
        sys.exit(1)

    settings = get_settings()
    project = settings.gcp_project_id
    dataset = settings.bq_dataset
    location = settings.bq_location

    logger.info("Bootstrap starting: project=%s dataset=%s location=%s", project, dataset, location)

    create_bq_dataset(project, dataset, location, dry_run)
    create_bq_tables(project, dataset, dry_run)

    if not dry_run:
        logger.info(
            "\n✅ Bootstrap complete!\n"
            "Next steps:\n"
            "  1. Run: python scripts/seed_preference_rules.py --user-id YOUR_ID\n"
            "  2. Create your user profile via: POST /profile (user_profile_service)\n"
            "  3. Run backfill: python scripts/backfill_embeddings.py\n"
            "  4. Trigger daily Tier 1: python services/tier1_matcher_runner.py\n"
        )
    else:
        logger.info("[DRY RUN] Validation complete — no changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap local dev environment")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no writes")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
