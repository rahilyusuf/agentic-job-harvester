"""tools/e2b_tools.py — E2B AsyncSandbox execution tool for SkillGapAgent.

Executes Python code challenges in an isolated E2B micro-VM.
NEVER use exec() locally — all code execution goes through this function.

Security constraints (per guardrails.md G-6):
  - Hard timeout: settings.e2b_sandbox_timeout_seconds (default 30s)
  - Output capped at 2000 chars (sandbox_stdout) and 500 chars (sandbox_stderr)
  - Sandbox ID is recorded in CodeChallenge for audit trail

ADK tool function — called by SkillGapAgent when generating practice challenges
for blocking skill gaps.
"""

from __future__ import annotations

import logging
import time

from e2b_code_interpreter import AsyncSandbox

from config.settings import get_settings
from schemas.diligence_models import CodeChallenge

logger = logging.getLogger(__name__)

_MAX_STDOUT_CHARS = 2000
_MAX_STDERR_CHARS = 500


async def execute_in_sandbox(
    skill: str,
    challenge_description: str,
    python_code: str,
) -> CodeChallenge:
    """Execute a Python code challenge in an E2B AsyncSandbox micro-VM.

    Use this tool to verify that a generated practice challenge actually runs
    correctly before presenting it to the user. Pass the skill name, a human-
    readable challenge description, and the Python code to execute.

    The sandbox is automatically destroyed after execution (or timeout).
    Never execute code locally — always use this tool for code challenges.

    Args:
        skill: The skill being tested (e.g., "Docker", "Kubernetes", "SQL").
        challenge_description: Human-readable description of what the challenge tests.
        python_code: Valid Python 3 code implementing the challenge.

    Returns:
        CodeChallenge with stdout, stderr, success flag, and sandbox ID.
    """
    settings = get_settings()
    timeout = settings.e2b_sandbox_timeout_seconds

    start_ms = int(time.monotonic() * 1000)
    sandbox_id = "unknown"

    try:
        async with AsyncSandbox(api_key=settings.e2b_api_key, timeout=timeout) as sandbox:
            sandbox_id = sandbox.sandbox_id
            logger.info(
                "E2B sandbox created: id=%s skill='%s' timeout=%ds",
                sandbox_id,
                skill,
                timeout,
            )

            execution = await sandbox.run_code(python_code)

            elapsed_ms = int(time.monotonic() * 1000) - start_ms
            stdout = "\n".join(execution.logs.stdout or [])[:_MAX_STDOUT_CHARS]
            stderr = "\n".join(execution.logs.stderr or [])[:_MAX_STDERR_CHARS]
            success = not execution.error and not stderr

            logger.info(
                "E2B execution complete: sandbox=%s skill='%s' success=%s elapsed=%dms",
                sandbox_id,
                skill,
                success,
                elapsed_ms,
            )

            return CodeChallenge(
                skill=skill,
                challenge_description=challenge_description,
                generated_code=python_code,
                sandbox_stdout=stdout,
                sandbox_stderr=stderr,
                execution_success=success,
                execution_time_ms=elapsed_ms,
                sandbox_id=sandbox_id,
            )

    except TimeoutError:
        elapsed_ms = int(time.monotonic() * 1000) - start_ms
        logger.error("E2B sandbox timeout for skill='%s' sandbox=%s", skill, sandbox_id)
        return CodeChallenge(
            skill=skill,
            challenge_description=challenge_description,
            generated_code=python_code,
            sandbox_stdout="",
            sandbox_stderr=f"TIMEOUT: Execution exceeded {timeout}s limit.",
            execution_success=False,
            execution_time_ms=elapsed_ms,
            sandbox_id=sandbox_id,
        )

    except Exception as exc:  # noqa: BLE001
        elapsed_ms = int(time.monotonic() * 1000) - start_ms
        logger.error("E2B sandbox error for skill='%s': %s", skill, exc)
        return CodeChallenge(
            skill=skill,
            challenge_description=challenge_description,
            generated_code=python_code,
            sandbox_stdout="",
            sandbox_stderr=f"ERROR: {exc}"[:_MAX_STDERR_CHARS],
            execution_success=False,
            execution_time_ms=elapsed_ms,
            sandbox_id=sandbox_id,
        )
