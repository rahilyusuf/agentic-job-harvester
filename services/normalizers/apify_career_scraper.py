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

from orchestration.input_sanitizer import sanitize_job_fields
from schemas.job_models import RawJob
from utils.ids import compute_job_id
from utils.time import utcnow

logger = logging.getLogger(__name__)

# Identifier used as the ``source`` column in BigQuery.
# This distinguishes which Apify actor produced the record.
_SOURCE_LABEL = "apify-company-career-scraper"

# Minimum description length required by RawJob.description (min_length=50)
_MIN_DESCRIPTION_LEN = 50


def resolve_job_url(item: dict[str, Any]) -> tuple[str, bool]:
    """Resolve a URL for a job posting from scraper output.

    Precedence:
        1. jobUrl: Direct link to posting (e.g. Workable, SmartRecruiters, Lever).
        2. applyUrl: Direct link to application flow (e.g. Workday career portals
           where jobUrl is omitted in Apify scraper output).

    Returns:
        tuple[url, used_fallback]:
            - url: Stripped jobUrl or applyUrl, or "" if neither is present.
            - used_fallback: True if jobUrl was missing/blank and applyUrl was used.
    """
    job_url = (item.get("jobUrl") or "").strip()
    if job_url:
        return job_url, False
    apply_url = (item.get("applyUrl") or "").strip()
    if apply_url:
        return apply_url, True
    return "", False


def normalize(item: dict[str, Any]) -> RawJob | None:
    """Map one company-career-page-scraper dataset item to a canonical RawJob.

    Returns:
        A validated RawJob, or None if the item is malformed/unrepresentable.

    Never raises — validation failures are logged as warnings and skipped.
    """
    job_url, used_fallback = resolve_job_url(item)
    if not job_url:
        logger.warning(
            "Skipping item with missing jobUrl and applyUrl: title=%s",
            item.get("title"),
        )
        return None

    if used_fallback:
        logger.debug(
            "Using applyUrl fallback for item: title=%s url=%s",
            item.get("title"),
            job_url,
        )

    # Determine the stable job identifier.
    # The scraper provides its own jobId (e.g. "workable-<uuid>") which is
    # already an ATS-native stable ID — pass it as ats_id so compute_job_id
    # uses it directly instead of falling back to MD5(url).
    ats_id: str | None = item.get("jobId", "").strip() or None
    job_id = compute_job_id(job_url, ats_id=ats_id)

    # Prefer plain-text description for embedding; fall back to HTML as last resort.
    description: str = (item.get("descriptionText", "") or item.get("descriptionHtml", "")).strip()

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

    raw_location = (item.get("location") or "").strip() or None
    raw_title = (item.get("title") or "").strip()
    raw_company = (item.get("companyName") or item.get("companyDomain") or "").strip()
    raw_employment_type = (item.get("employmentType") or "").strip() or None

    # Sanitize untrusted scraped text (Option A: Layer 1 length cap + Layer 2 pattern strip & tag escape)
    # for safe storage in BigQuery, clean vector embeddings, and regex prefiltering.
    sanitized = sanitize_job_fields(
        title=raw_title,
        company=raw_company,
        location=raw_location,
        description=description,
        salary_raw=salary_raw,
        employment_type_raw=raw_employment_type,
        wrap=False,
    )

    clean_title = (sanitized["title"] or "").strip()
    clean_company = (sanitized["company"] or "").strip()
    if not clean_title or not clean_company:
        logger.warning(
            "Skipping item with empty or invalidated title/company: title=%r company=%r job_url=%s",
            clean_title,
            clean_company,
            job_url,
        )
        return None

    clean_description = (sanitized["description"] or "").strip()
    # Content quality check: ensure residual content beyond removed patterns meets the length threshold
    effective_content = clean_description.replace("[REMOVED]", "").strip()
    if len(effective_content) < _MIN_DESCRIPTION_LEN:
        logger.warning(
            "Skipping item with sanitized description too short (%d effective chars, min %d): job_url=%s",
            len(effective_content),
            _MIN_DESCRIPTION_LEN,
            job_url,
        )
        return None

    try:
        return RawJob(
            job_id=job_id,
            source=_SOURCE_LABEL,
            scraped_at=utcnow(),
            job_url=job_url,  # type: ignore[arg-type]  # Pydantic coerces str -> HttpUrl
            title=clean_title,
            company=clean_company,
            location=sanitized["location"],
            description=clean_description,
            salary_raw=sanitized["salary_raw"],
            employment_type_raw=sanitized["employment_type_raw"],
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
