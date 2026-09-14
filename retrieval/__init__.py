"""retrieval — Tier 1 candidate retrieval (business logic layer).

Step A: vector_matcher.py  — BQ VECTOR_SEARCH cosine, top 50
Step B: regex_prefilter.py — $0 gate: YOE, title exclusions → ~15 roles

This is business logic, NOT raw data access.
The retrieval layer depends on repositories/interfaces.py,
never on concrete *_repo.py classes.
"""

from retrieval.vector_matcher import VectorMatcher
from retrieval.regex_prefilter import RegexPrefilter

__all__ = ["VectorMatcher", "RegexPrefilter"]
