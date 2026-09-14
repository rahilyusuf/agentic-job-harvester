"""orchestration/output_validator.py — Pydantic Output Validation Gateway.

Gates all agent outputs before persistence. Raises ValidationError on failure.
All pipeline code calls validator.validate(output) between every agent step.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


class OutputValidationError(Exception):
    """Raised when an agent output fails Pydantic validation."""

    def __init__(self, agent_output: object, error: ValidationError) -> None:
        self.agent_output = agent_output
        self.pydantic_error = error
        super().__init__(
            f"Agent output validation failed for {type(agent_output).__name__}: {error}"
        )


class OutputValidator:
    """Validates that agent outputs conform to their declared Pydantic v2 schemas.

    Usage:
        validator = OutputValidator()
        validated = validator.validate(some_pydantic_model_instance)

    The validate() method is a no-op if the output is already a valid Pydantic model
    instance. It re-validates from model_dump to catch any post-construction mutations.

    Raises:
        OutputValidationError: If validation fails.
        TypeError: If the output is not a Pydantic BaseModel instance.
    """

    def validate(self, output: BaseModel) -> BaseModel:
        """Validate an agent output against its schema.

        Args:
            output: The Pydantic model instance to validate.

        Returns:
            The same instance if valid.

        Raises:
            TypeError: If output is not a Pydantic BaseModel.
            OutputValidationError: If re-validation from model_dump fails.
        """
        if not isinstance(output, BaseModel):
            raise TypeError(
                f"Expected a Pydantic BaseModel instance, got {type(output).__name__}. "
                "All agent outputs must be validated Pydantic v2 models."
            )

        try:
            # Re-validate from model_dump to catch any mutations after __init__
            output.__class__.model_validate(output.model_dump())
            logger.debug("Validated %s ✓", output.__class__.__name__)
            return output

        except ValidationError as exc:
            logger.error(
                "Validation FAILED for %s: %s",
                output.__class__.__name__,
                exc,
            )
            raise OutputValidationError(output, exc) from exc
