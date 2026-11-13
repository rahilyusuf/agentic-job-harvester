"""tools/cache_tools.py — Firestore research cache tools for CompanyResearchAgent.

lookup_research_cache is called first in each ReAct iteration to avoid
redundant web searches for companies already researched in the last 72h.

write_research_cache persists the CompanyResearch result to Firestore
after a successful research run.

Cache key: `research_cache/{company_name_normalised}`
TTL: configurable per-record (default 72 hours) via CompanyResearch.cache_ttl_hours
"""

from __future__ import annotations

import json
import logging
from datetime import timezone

from google.cloud import firestore

from config.settings import get_settings
from utils.time import utcnow

logger = logging.getLogger(__name__)

_CACHE_COLLECTION = "research_cache"
_fs_client: firestore.AsyncClient | None = None


def _get_firestore() -> firestore.AsyncClient:
    """Return a singleton async Firestore client."""
    global _fs_client  # noqa: PLW0603
    if _fs_client is None:
        settings = get_settings()
        _fs_client = firestore.AsyncClient(
            project=settings.gcp_project_id,
            database=settings.firestore_database_id,
        )
    return _fs_client


def _normalise_company_name(company: str) -> str:
    """Normalise company name to a safe Firestore document ID."""
    import re
    return re.sub(r"[^a-z0-9_-]", "_", company.lower().strip())[:100]


async def lookup_research_cache(company_name: str) -> dict | None:
    """Look up cached company research from Firestore.

    Call this at the START of each CompanyResearchAgent run.
    Returns the cached research dict if found and not expired; None otherwise.

    Args:
        company_name: The company name to look up (normalised internally).

    Returns:
        Dict representation of CompanyResearch if cache hit and fresh;
        None if cache miss or expired.
    """
    fs = _get_firestore()
    doc_id = _normalise_company_name(company_name)
    doc_ref = fs.collection(_CACHE_COLLECTION).document(doc_id)

    try:
        doc = await doc_ref.get()
        if not doc.exists:
            logger.debug("Cache miss for company='%s'", company_name)
            return None

        data = doc.to_dict()
        if data is None:
            return None

        # Check TTL
        cached_at_str = data.get("researched_at")
        cache_ttl_hours = data.get("cache_ttl_hours", 72)

        if cached_at_str:
            from datetime import datetime
            cached_at = datetime.fromisoformat(str(cached_at_str))
            if cached_at.tzinfo is None:
                cached_at = cached_at.replace(tzinfo=timezone.utc)
            age_hours = (utcnow() - cached_at).total_seconds() / 3600
            if age_hours > cache_ttl_hours:
                logger.info(
                    "Cache expired for company='%s' (age=%.1fh ttl=%dh)",
                    company_name, age_hours, cache_ttl_hours,
                )
                return None

        logger.info("Cache hit for company='%s'", company_name)
        return data

    except Exception as exc:  # noqa: BLE001
        logger.warning("Cache lookup failed for company='%s': %s", company_name, exc)
        return None


async def write_research_cache(company_name: str, research_data: dict) -> None:
    """Persist company research to Firestore cache.

    Call this AFTER a successful CompanyResearchAgent run.

    Args:
        company_name: The company name (used as document ID).
        research_data: Dict representation of a CompanyResearch model.
    """
    fs = _get_firestore()
    doc_id = _normalise_company_name(company_name)
    doc_ref = fs.collection(_CACHE_COLLECTION).document(doc_id)

    try:
        await doc_ref.set(research_data, merge=True)
        logger.info("Wrote research cache for company='%s'", company_name)
    except Exception as exc:  # noqa: BLE001
        # Non-fatal: caching is best-effort
        logger.warning("Failed to write research cache for company='%s': %s", company_name, exc)
