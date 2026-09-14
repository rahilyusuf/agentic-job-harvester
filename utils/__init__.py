"""utils — Generic, domain-free helpers.

Only pure utility functions live here. If a function encodes a business rule
(e.g., "skip VP titles"), it belongs in retrieval/ not utils/.
"""

from utils.ids import compute_job_id
from utils.retry import async_retry, with_retry
from utils.time import get_dedup_window_start, is_within_dedup_window, utcnow

__all__ = [
    "compute_job_id",
    "async_retry",
    "with_retry",
    "utcnow",
    "get_dedup_window_start",
    "is_within_dedup_window",
]
