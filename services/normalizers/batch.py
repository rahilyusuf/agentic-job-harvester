"""Shared ordered normalization and first-valid-occurrence deduplication."""

import logging
from dataclasses import dataclass

from schemas.job_models import RawJob
from services.normalizers import apify_career_scraper

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NormalizedBatch:
    jobs: list[RawJob]
    received: int
    normalized: int
    rejected: int
    in_batch_duplicates: int
    apply_url_fallback: int


def normalize_batch(items: list[object]) -> NormalizedBatch:
    jobs: dict[str, RawJob] = {}
    normalized = rejected = duplicates = fallbacks = 0
    for item in items:
        try:
            _, fallback = apify_career_scraper.resolve_job_url(item)
            job = apify_career_scraper.normalize(item)
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping malformed dataset item: %s", type(exc).__name__)
            rejected += 1
            continue
        if job is None:
            rejected += 1
            continue
        normalized += 1
        fallbacks += int(fallback)
        if job.job_id in jobs:
            duplicates += 1
        else:
            jobs[job.job_id] = job
    return NormalizedBatch(
        list(jobs.values()), len(items), normalized, rejected, duplicates, fallbacks
    )
