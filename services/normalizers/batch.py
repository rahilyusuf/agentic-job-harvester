"""Shared ordered normalization and first-valid-occurrence deduplication."""

import logging
from dataclasses import dataclass

from schemas.job_models import RawJob
from observability.ingestion_logging import context, emit
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
    state = context()
    for index, item in enumerate(items):
        if state:
            state.record_index = index
        try:
            _, fallback = apify_career_scraper.resolve_job_url(item)
            job = apify_career_scraper.normalize(item)
        except (TypeError, ValueError) as exc:
            emit(
                "ingestion.record.rejected",
                logging.DEBUG,
                reason="invalid_field",
                error_type=type(exc).__name__,
            )
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
    if state:
        state.record_index = None
    return NormalizedBatch(
        list(jobs.values()), len(items), normalized, rejected, duplicates, fallbacks
    )
