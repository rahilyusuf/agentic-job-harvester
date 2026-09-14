"""utils/time.py — Timestamp and time-window helpers.

Centralises the 90-day deduplication window logic so it cannot drift
between services (apify_receiver, embedding_worker, etc.).

All functions use UTC. Never use naive datetimes in production code.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from config.settings import get_settings


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware).

    Use this instead of ``datetime.utcnow()`` (which is naive and deprecated in 3.12+).
    """
    return datetime.now(tz=timezone.utc)


def get_dedup_window_start(window_days: int | None = None) -> datetime:
    """Return the start of the deduplication window (UTC).

    Any job scraped more recently than this timestamp is considered a
    potential duplicate and must be checked against BigQuery before insert.

    Args:
        window_days: Override; if None, reads from settings.dedup_window_days.

    Returns:
        Timezone-aware UTC datetime marking the start of the dedup window.
    """
    days = window_days if window_days is not None else get_settings().dedup_window_days
    return utcnow() - timedelta(days=days)


def is_within_dedup_window(scraped_at: datetime, window_days: int | None = None) -> bool:
    """Return True if ``scraped_at`` falls within the dedup window.

    Args:
        scraped_at: The datetime the job was scraped (must be timezone-aware).
        window_days: Override; if None, reads from settings.dedup_window_days.

    Returns:
        True if the job was scraped within the window and could be a duplicate.
    """
    if scraped_at.tzinfo is None:
        # Assume UTC for naive datetimes (legacy data safety)
        scraped_at = scraped_at.replace(tzinfo=timezone.utc)
    return scraped_at >= get_dedup_window_start(window_days)


def format_bq_timestamp(dt: datetime) -> str:
    """Format a datetime as a BigQuery TIMESTAMP literal string.

    Args:
        dt: Timezone-aware datetime.

    Returns:
        ISO 8601 string compatible with BQ TIMESTAMP type.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def days_since(dt: datetime) -> float:
    """Return the number of days elapsed since ``dt`` (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = utcnow() - dt
    return delta.total_seconds() / 86400
