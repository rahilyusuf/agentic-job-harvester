"""schemas/reflection_models.py — PreferenceRules contract.

ReflectionAgent (weekly Cloud Scheduler) synthesizes user HITL telemetry into
Firestore dynamic_preference_rules. These rules are read by JobIntakeRouter
and ScorerAgent at scoring time.
"""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field


class RuleAction(str, enum.Enum):
    BOOST = "BOOST"         # Increase score for matching jobs
    PENALIZE = "PENALIZE"   # Decrease score for matching jobs
    SKIP = "SKIP"           # Immediately skip matching jobs (hard filter)
    PRIORITIZE = "PRIORITIZE"  # Mark as HIGH_PRIORITY routing


class RuleCategory(str, enum.Enum):
    COMPANY_TYPE = "COMPANY_TYPE"
    TITLE_PATTERN = "TITLE_PATTERN"
    LOCATION = "LOCATION"
    SKILL_MATCH = "SKILL_MATCH"
    SALARY_BAND = "SALARY_BAND"
    INDUSTRY = "INDUSTRY"
    COMPANY_SIZE = "COMPANY_SIZE"
    REMOTE_PREFERENCE = "REMOTE_PREFERENCE"
    CUSTOM = "CUSTOM"


class PreferenceRule(BaseModel):
    """A single synthesized preference rule stored in Firestore."""

    rule_id: str = Field(..., description="Unique ID, e.g. 'rule_startup_boost_001'")
    category: RuleCategory
    action: RuleAction
    condition: str = Field(
        ...,
        min_length=5,
        description="Natural-language or regex-style condition, e.g. 'company is VC startup'",
    )
    score_delta: int | None = Field(
        None,
        ge=-50,
        le=50,
        description="Score adjustment for BOOST/PENALIZE. None for SKIP/PRIORITIZE.",
    )
    confidence: float = Field(
        ..., ge=0.0, le=1.0,
        description="Agent confidence in this rule, based on signal strength in telemetry"
    )
    signal_count: int = Field(
        ..., ge=1,
        description="Number of user actions that support this rule"
    )
    examples: list[str] = Field(
        default_factory=list,
        max_length=5,
        description="Sample job_ids that evidence this rule",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime | None = Field(
        None, description="Optional TTL; None means the rule persists until overwritten"
    )


class PreferenceRules(BaseModel):
    """Full set of synthesized preference rules, written to Firestore by ReflectionAgent."""

    user_id: str
    synthesis_period_days: int = Field(30, ge=7, le=90)
    rules: list[PreferenceRule] = Field(default_factory=list)
    centroid_updated: bool = Field(
        False,
        description="True if e_active_user centroid was recalculated in this run",
    )
    alpha_used: float = Field(
        ..., ge=0.0, le=1.0,
        description="α used in: e_active_new = (1-α)*e_resume + α*e_accepted_centroid"
    )
    accepted_jobs_count: int = Field(..., ge=0)
    passed_jobs_count: int = Field(..., ge=0)
    synthesized_at: datetime = Field(default_factory=datetime.utcnow)
    reflection_trace_id: str | None = Field(
        None, description="LangFuse trace ID for this reflection run"
    )
