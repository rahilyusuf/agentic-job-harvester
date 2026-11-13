"""orchestration — ADK graph wiring and non-agent pipeline logic."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orchestration.hitl_controller import HITLController
    from orchestration.input_sanitizer import (
        format_job_for_prompt,
        sanitize_job_fields,
        sanitize_scraped_text,
    )
    from orchestration.output_validator import OutputValidator
    from orchestration.pipeline import (
        Tier1Pipeline,
        Tier1PipelineResult,
        Tier2Pipeline,
        Tier2PipelineResult,
        build_tier1_pipeline,
        build_tier2_pipeline,
    )

__all__ = [
    "HITLController",
    "OutputValidator",
    "Tier1Pipeline",
    "Tier1PipelineResult",
    "Tier2Pipeline",
    "Tier2PipelineResult",
    "build_tier1_pipeline",
    "build_tier2_pipeline",
    "format_job_for_prompt",
    "sanitize_job_fields",
    "sanitize_scraped_text",
]


def __getattr__(name: str) -> Any:
    """Lazy-load package exports to prevent circular imports during agent initialization."""
    if name in {
        "Tier1Pipeline",
        "Tier1PipelineResult",
        "Tier2Pipeline",
        "Tier2PipelineResult",
        "build_tier1_pipeline",
        "build_tier2_pipeline",
    }:
        from orchestration import pipeline

        return getattr(pipeline, name)
    if name == "OutputValidator":
        from orchestration.output_validator import OutputValidator

        return OutputValidator
    if name == "HITLController":
        from orchestration.hitl_controller import HITLController

        return HITLController
    if name in {"format_job_for_prompt", "sanitize_job_fields", "sanitize_scraped_text"}:
        from orchestration import input_sanitizer

        return getattr(input_sanitizer, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

