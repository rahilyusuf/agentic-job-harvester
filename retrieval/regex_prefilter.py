"""retrieval/regex_prefilter.py — Tier 1 Step B: $0 regex gate.

Implements ARCHITECTURE.md Step B:
    Drop YOE > 5yrs, exclude VP/Director/Manager titles → yields ~15 roles.

This is a pure Python filter — zero BQ cost, zero LLM calls.
Operates on the list[RawJob] output that comes from resolving vector match job_ids.

Business rules encoded here (update this file + ARCHITECTURE.md if they change):
  - Max YOE: settings.prefilter_max_yoe (default 5)
  - Excluded title patterns: see EXCLUDED_TITLE_PATTERNS below
  - Max output: settings.prefilter_max_results (default 15)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from config.settings import get_settings
from schemas.job_models import JobStructuredExtraction, RawJob

logger = logging.getLogger(__name__)

# Title patterns that are immediately excluded (management / over-seniored roles)
# Add patterns here when new exclusion rules are identified.
EXCLUDED_TITLE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bvp\b", re.IGNORECASE),
    re.compile(r"\bvice\s+president\b", re.IGNORECASE),
    re.compile(r"\bdirector\b", re.IGNORECASE),
    re.compile(r"\bhead\s+of\b", re.IGNORECASE),
    re.compile(r"\bchief\b", re.IGNORECASE),
    re.compile(r"\bc[etos]o\b", re.IGNORECASE),           # CTO, CEO, COO, CSO
    re.compile(r"\bmanager\b", re.IGNORECASE),
    re.compile(r"\bprincipal\b", re.IGNORECASE),          # Principal Engineer is fine but filtered
    re.compile(r"\bstaff\s+engineer\b", re.IGNORECASE),   # too senior
    re.compile(r"\bdistinguished\b", re.IGNORECASE),
    re.compile(r"\bfellow\b", re.IGNORECASE),
    re.compile(r"\bsenior\s+director\b", re.IGNORECASE),
]


@dataclass(frozen=True)
class PrefilterResult:
    """A job that passed the regex prefilter."""

    job_id: str
    title: str
    similarity_score: float
    yoe_required_max: int | None
    passed_reason: str = "OK"


@dataclass(frozen=True)
class PrefilterRejection:
    """A job that was rejected by the regex prefilter (for audit trail)."""

    job_id: str
    title: str
    similarity_score: float
    rejection_reason: str


class RegexPrefilter:
    """Tier 1 Step B: deterministic, $0 rule-based filter.

    Receives the ~50 vector-search candidates and drops roles that violate
    hard rules (seniority ceiling, excluded title patterns).
    Emits ≤15 candidates for downstream LLM scoring.

    Usage:
        prefilter = RegexPrefilter()
        passed, rejected = prefilter.run(jobs_with_scores)
        # passed: list[PrefilterResult], len <= settings.prefilter_max_results
        # rejected: list[PrefilterRejection] (for audit/debug)
    """

    def __init__(self, settings: object | None = None) -> None:
        _settings = settings or get_settings()
        self._max_yoe: int = _settings.prefilter_max_yoe  # type: ignore[attr-defined]
        self._max_results: int = _settings.prefilter_max_results  # type: ignore[attr-defined]

    def run(
        self,
        candidates: list[tuple[RawJob, float]],
        extractions: dict[str, JobStructuredExtraction] | None = None,
    ) -> tuple[list[PrefilterResult], list[PrefilterRejection]]:
        """Filter a list of (RawJob, similarity_score) tuples.

        Args:
            candidates: List of (RawJob, similarity_score) sorted by score DESC.
            extractions: Optional dict of job_id → JobStructuredExtraction for richer YOE data.

        Returns:
            Tuple of (passed_list, rejected_list).
            passed_list is capped at settings.prefilter_max_results and sorted by score DESC.
        """
        passed: list[PrefilterResult] = []
        rejected: list[PrefilterRejection] = []

        for job, score in candidates:
            extraction = extractions.get(job.job_id) if extractions else None

            # ── Rule 1: Title exclusion (management / over-seniored) ──────────
            if self._is_excluded_title(job.title):
                rejected.append(PrefilterRejection(
                    job_id=job.job_id,
                    title=job.title,
                    similarity_score=score,
                    rejection_reason=f"Excluded title pattern matched: '{job.title}'",
                ))
                continue

            # ── Rule 2: YOE ceiling ───────────────────────────────────────────
            yoe_max = None
            if extraction:
                yoe_max = extraction.yoe_required_max
                # Skip if management role flag set by extraction
                if extraction.is_management_role:
                    rejected.append(PrefilterRejection(
                        job_id=job.job_id,
                        title=job.title,
                        similarity_score=score,
                        rejection_reason="LLM extraction flagged as management role",
                    ))
                    continue

            if yoe_max is not None and yoe_max > self._max_yoe:
                rejected.append(PrefilterRejection(
                    job_id=job.job_id,
                    title=job.title,
                    similarity_score=score,
                    rejection_reason=f"YOE required ({yoe_max}) exceeds ceiling ({self._max_yoe})",
                ))
                continue

            passed.append(PrefilterResult(
                job_id=job.job_id,
                title=job.title,
                similarity_score=score,
                yoe_required_max=yoe_max,
            ))

            if len(passed) >= self._max_results:
                # Remaining candidates skipped (budget cap)
                logger.debug(
                    "Prefilter reached max_results=%d; dropping remaining candidates.",
                    self._max_results,
                )
                break

        logger.info(
            "Prefilter: %d passed / %d rejected (from %d candidates)",
            len(passed),
            len(rejected),
            len(candidates),
        )
        return passed, rejected

    def _is_excluded_title(self, title: str) -> bool:
        """Return True if the title matches any exclusion pattern."""
        return any(pattern.search(title) for pattern in EXCLUDED_TITLE_PATTERNS)
