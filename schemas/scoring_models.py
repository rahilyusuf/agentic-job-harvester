"""schemas/scoring_models.py — ScorerAgent & SkepticAgent output contracts.

Tier 1 debate loop:
  Round 1: ScorerAgent scores → SkepticAgent audits
  If |scorer_score - skeptic_agreed_score| > 10 → Round 2
  SkepticAgent holds final veto authority.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class ScorerOutput(BaseModel):
    """Structured output from ScorerAgent (Tier 1 debate, round N)."""

    model_config = {"str_strip_whitespace": True}

    job_id: str
    user_id: str
    round_number: int = Field(..., ge=1, le=2)
    initial_score: int = Field(
        ..., ge=0, le=100, description="Fit score against resume + preference rules"
    )
    strengths: list[str] = Field(
        ..., min_length=1, description="Top reasons this role is a good fit"
    )
    concerns: list[str] = Field(
        default_factory=list, description="Potential friction points before Skeptic audits"
    )
    rationale: str = Field(..., min_length=50, description="Full scoring rationale")
    preference_rules_applied: list[str] = Field(
        default_factory=list, description="Firestore rule IDs used in scoring"
    )


class SkepticOutput(BaseModel):
    """Structured output from SkepticAgent (Tier 1 debate, round N).

    The Skeptic's agreed_score is the authoritative final score.
    If veto=True, the pipeline must treat this as a hard SKIP regardless of agreed_score.
    """

    model_config = {"str_strip_whitespace": True}

    job_id: str
    user_id: str
    round_number: int = Field(..., ge=1, le=2)
    agreed_score: int = Field(
        ..., ge=0, le=100, description="Skeptic's revised score — this is the FINAL score"
    )
    veto: bool = Field(
        False,
        description="If True, Skeptic exercises hard veto. Job is SKIPPED regardless of score.",
    )
    veto_reason: str | None = Field(
        None, description="Required if veto=True. The blocking disqualifier."
    )
    hidden_disqualifiers: list[str] = Field(
        default_factory=list,
        description="Issues the Scorer may have missed (ghost job signals, recruiter spam, etc.)",
    )
    rationale: str = Field(..., min_length=50)
    request_round_2: bool = Field(
        False,
        description="True if |scorer_score - agreed_score| > 10 and round_number == 1",
    )

    @model_validator(mode="after")
    def veto_requires_reason(self) -> "SkepticOutput":
        if self.veto and not self.veto_reason:
            raise ValueError("veto_reason is required when veto=True")
        return self

    @model_validator(mode="after")
    def round2_only_from_round1(self) -> "SkepticOutput":
        if self.request_round_2 and self.round_number != 1:
            raise ValueError("request_round_2 can only be True in round 1")
        return self
