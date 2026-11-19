"""utils/retry.py — Generic retry/backoff decorators.

Provides both sync and async retry wrappers with exponential backoff.
These are domain-free: no project-specific exceptions or business logic here.

Usage:
    @async_retry(
        max_attempts=3, base_delay=1.0,
        exceptions=(google.api_core.exceptions.ServiceUnavailable,),
    )
    async def my_bq_call() -> ...:
        ...

    @with_retry(max_attempts=3)
    def my_sync_call() -> ...:
        ...
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from observability.diagnostics import caller_source, exception_fields

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])
AF = TypeVar("AF", bound=Callable[..., Coroutine[Any, Any, Any]])


def _log_retry(
    operation: str, attempt: int, maximum: int, error: Exception, delay: float | None
) -> None:
    """Log metadata only; exception messages can contain credentials or row contents."""
    fields = {
        **exception_fields(error),
        "source": caller_source(2),
        "operation": operation,
        "attempt": attempt,
        "max_attempts": maximum,
        "error_type": type(error).__name__,
        "retry_delay_ms": round(delay * 1000) if delay is not None else 0,
        "outcome": "exhausted" if delay is None else "retrying",
        "error_category": "operation_retry",
    }
    logger.log(
        logging.ERROR if delay is None else logging.WARNING,
        "operation.retry operation=%s attempt=%d/%d error_type=%s retry_delay_ms=%d outcome=%s",
        operation,
        attempt,
        maximum,
        fields["error_type"],
        fields["retry_delay_ms"],
        fields["outcome"],
        extra={"retry_fields": fields},
        stacklevel=2,
    )


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    reraise: bool = True,
) -> Callable[[AF], AF]:
    """Decorator for async functions with exponential backoff retry.

    Args:
        max_attempts: Total number of attempts (1 = no retry).
        base_delay: Initial delay in seconds before first retry.
        backoff_factor: Multiplier applied to delay on each retry.
        max_delay: Maximum delay cap in seconds.
        exceptions: Tuple of exception types that trigger a retry.
        reraise: If True, re-raise the last exception after exhausting attempts.
    """

    def decorator(func: AF) -> AF:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        _log_retry(func.__qualname__, attempt, max_attempts, exc, None)
                        break
                    actual_delay = min(delay, max_delay)
                    _log_retry(func.__qualname__, attempt, max_attempts, exc, actual_delay)
                    await asyncio.sleep(actual_delay)
                    delay *= backoff_factor

            if reraise and last_exc is not None:
                raise last_exc
            return None

        return wrapper  # type: ignore[return-value]

    return decorator


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    reraise: bool = True,
) -> Callable[[F], F]:
    """Decorator for synchronous functions with exponential backoff retry."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        _log_retry(func.__qualname__, attempt, max_attempts, exc, None)
                        break
                    actual_delay = min(delay, max_delay)
                    _log_retry(func.__qualname__, attempt, max_attempts, exc, actual_delay)
                    time.sleep(actual_delay)
                    delay *= backoff_factor

            if reraise and last_exc is not None:
                raise last_exc
            return None

        return wrapper  # type: ignore[return-value]

    return decorator
