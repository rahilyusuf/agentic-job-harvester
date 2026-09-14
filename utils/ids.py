"""utils/ids.py — Job ID computation.

job_id precedence (per ARCHITECTURE.md):
  1. ATS-native UUID from Lever / Workable / Greenhouse (preferred — stable across re-scrapes)
  2. MD5(job_url) fallback for non-ATS sources

The ID must be deterministic: re-scraping the same job always produces the same ID.
"""

from __future__ import annotations

import hashlib
import re
import uuid


_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

# ATS URL patterns that embed a UUID we can extract
_ATS_UUID_PATTERNS: list[re.Pattern[str]] = [
    # Lever:  https://jobs.lever.co/company/UUID
    re.compile(r"jobs\.lever\.co/[^/]+/([0-9a-f\-]{36})", re.IGNORECASE),
    # Workable: https://apply.workable.com/company/j/UUID
    re.compile(r"apply\.workable\.com/[^/]+/j/([A-Z0-9]{8,})", re.IGNORECASE),
    # Greenhouse: https://boards.greenhouse.io/company/jobs/12345678
    re.compile(r"boards\.greenhouse\.io/[^/]+/jobs/(\d{6,})", re.IGNORECASE),
    # SmartRecruiters: https://jobs.smartrecruiters.com/Company/UUID
    re.compile(r"jobs\.smartrecruiters\.com/[^/]+/([0-9a-f\-]{36})", re.IGNORECASE),
]


def compute_job_id(job_url: str, ats_id: str | None = None) -> str:
    """Return a stable, unique job identifier.

    Args:
        job_url: The canonical job posting URL.
        ats_id: Optional native ATS UUID if already extracted by the scraper.

    Returns:
        A string job_id — either the ATS UUID or MD5(job_url).
    """
    # 1. Explicit ATS ID from scraper payload (most reliable)
    if ats_id and ats_id.strip():
        return ats_id.strip()

    # 2. Try to extract a UUID from the URL structure
    for pattern in _ATS_UUID_PATTERNS:
        match = pattern.search(job_url)
        if match:
            return match.group(1)

    # 3. Fallback: MD5 of the normalised URL (lowercase, strip trailing slash)
    normalised_url = job_url.lower().rstrip("/")
    return hashlib.md5(normalised_url.encode("utf-8"), usedforsecurity=False).hexdigest()


def is_valid_uuid(value: str) -> bool:
    """Return True if value is a well-formed UUID v4 string."""
    try:
        uuid.UUID(value, version=4)
        return True
    except ValueError:
        return False
