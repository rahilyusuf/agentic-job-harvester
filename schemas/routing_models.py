"""schemas/routing_models.py — RouterDecision contract.

JobIntakeRouter returns one of three decisions:
  SKIP          → halt immediately, ~$0.0001 cost, no further agents run
  PROCESS       → standard Tier 1 scoring pipeline
  HIGH_PRIORITY → Tier 1 scoring + immediate Telegram notification on high score
"""

from __future__ import annotations

import enum

from pydantic import BaseModel, Field


class RoutingDecision(str, enum.Enum):
    SKIP = "SKIP"
    PROCESS = "PROCESS"
    HIGH_PRIORITY = "HIGH_PRIORITY"


class RouterDecision(BaseModel):
    """Structured output from JobIntakeRouter agent."""

    model_config = {"str_strip_whitespace": True}

    job_id: str
    decision: RoutingDecision
    confidence: float = Field(..., ge=0.0, le=1.0, description="Model confidence in this routing")
    reasoning: str = Field(
        ...,
        min_length=20,
        description="Brief rationale matching against dynamic_preference_rules",
    )
    matched_rules: list[str] = Field(
        default_factory=list,
        description="IDs or names of Firestore preference rules that influenced this decision",
    )
    skip_reason: str | None = Field(
        None,
        description="If SKIP, the primary disqualifier (title mismatch, seniority, location, etc.)",
    )
