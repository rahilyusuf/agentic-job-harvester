"""services/normalizers/apify_career_scraper.py

Normalizes one raw dataset item produced by the Apify
Company Career Page Scraper actor into the canonical RawJob model.

Scraper output fields (verified from dataset sample):
    companyDomain, companyName, source (ATS name), atsBoardToken,
    careersUrl, jobId, title, location, locationCity, locationRegion,
    locationCountry, remote, employmentType, department, team, seniority,
    requisitionId, postedAt, updatedAt, descriptionHtml, descriptionText,
    jobUrl, applyUrl, skills, requirements, responsibilities, benefits,
    salary, salaryMin, salaryMax, salaryCurrency, salaryPeriod, scrapedAt

Fields NOT currently representable in RawJob (source-specific):
    companyDomain, atsBoardToken, careersUrl, applyUrl,
    locationCity, locationRegion, locationCountry, remote,
    department, team, seniority, requisitionId, updatedAt,
    skills (list), requirements (list), responsibilities (list),
    benefits (list), salaryMin, salaryMax, salaryCurrency, salaryPeriod,
    descriptionHtml, scrapedAt (source-provided — we set our own scraped_at)

Follow-up architectural consideration:
    A raw_source_data JSON column in raw_job_postings would allow
    preservation of the above source-specific fields without
    polluting the canonical schema.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from schemas.job_models import RawJob
from utils.ids import compute_job_id
from utils.time import utcnow

logger = logging.getLogger(__name__)

# Identifier used as the ``source`` column in BigQuery.
# This distinguishes which Apify actor produced the record.
_SOURCE_LABEL = "apify-company-career-scraper"

# Minimum description length required by RawJob.description (min_length=50)
_MIN_DESCRIPTION_LEN = 50


def normalize(item: dict[str, Any]) -> RawJob | None:
    """Map one company-career-page-scraper dataset item to a canonical RawJob.

    Returns:
        A validated RawJob, or None if the item is malformed/unrepresentable.

    Never raises — validation failures are logged as warnings and skipped.
    """
    job_url: str = item.get("jobUrl", "").strip()
    if not job_url:
        logger.warning("Skipping item with missing jobUrl: title=%s", item.get("title"))
        return None

    # Determine the stable job identifier.
    # The scraper provides its own jobId (e.g. "workable-<uuid>") which is
    # already an ATS-native stable ID — pass it as ats_id so compute_job_id
    # uses it directly instead of falling back to MD5(url).
    ats_id: str | None = item.get("jobId", "").strip() or None
    job_id = compute_job_id(job_url, ats_id=ats_id)

    # Prefer plain-text description for embedding; fall back to HTML as last resort.
    description: str = (
        item.get("descriptionText", "") or item.get("descriptionHtml", "")
    ).strip()

    if len(description) < _MIN_DESCRIPTION_LEN:
        logger.warning(
            "Skipping item with description too short (%d chars): job_url=%s",
            len(description),
            job_url,
        )
        return None

    # Parse postedAt — ISO 8601 from the scraper, optional.
    posted_at: datetime | None = _parse_iso_dt(item.get("postedAt"))

    # salary is a raw string from the scraper (e.g. "4,485–5,950 EUR/month").
    # salaryMin/salaryMax/salaryCurrency/salaryPeriod are source-specific structured
    # fields not represented in RawJob — preserved here as context for the follow-up
    # architectural gap (raw_source_data JSON column).
    salary_raw: str | None = item.get("salary", "").strip() or None

    # ats_platform is the ATS system name as reported by the scraper
    # (e.g. "workable", "lever", "greenhouse").
    ats_platform: str | None = item.get("source", "").strip() or None

    try:
        return RawJob(
            job_id=job_id,
            source=_SOURCE_LABEL,
            scraped_at=utcnow(),
            job_url=job_url,  # type: ignore[arg-type]  # Pydantic coerces str -> HttpUrl
            title=item.get("title", "").strip(),
            company=item.get("companyName", "").strip(),
            location=item.get("location", "").strip() or None,
            description=description,
            salary_raw=salary_raw,
            employment_type_raw=item.get("employmentType", "").strip() or None,
            posted_at=posted_at,
            ats_platform=ats_platform,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Skipping malformed item: job_url=%s error=%s",
            job_url,
            exc,
        )
        return None


def _parse_iso_dt(value: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string (possibly with trailing Z) to a
    timezone-aware UTC datetime.  Returns None on any parse failure.
    """
    if not value:
        return None
    try:
        # Replace trailing Z with +00:00 for fromisoformat compatibility.
        normalized = value.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except (ValueError, TypeError):
        logger.debug("Could not parse datetime value: %r", value)
        return None
