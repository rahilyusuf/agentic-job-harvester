---
name: e2b-sandboxing
description: Guidance on E2B code interpreter sandboxing with AsyncSandbox for SkillGapAgent coding challenges. Covers secure micro-VM execution, 30-second timeout constraints, stdout/stderr length truncation, and audit logging.
---

# E2B Code Interpreter Sandboxing

This skill guides the implementation, execution, and security constraints for running LLM-generated code in isolated E2B micro-VM sandboxes.

---

## 1. Context & Purpose

In **Tier 2 (DueDiligenceSuite)**, the `SkillGapAgent` analyzes missing technical skills:
1. Distinguishes between **learnable gaps** (e.g., familiar with PyTorch, job asks for TensorFlow) and **blocking gaps** (e.g., missing distributed training or CUDA requirements).
2. For blocking gaps, generates a targeted Python coding challenge with test assertions.
3. **Executes the challenge in an E2B micro-VM** to verify syntax, execution, and test cases *before* presenting it to the user.

---

## 2. Security Guardrail G-6: Isolation Rules

- **Zero Local Execution:** Never execute LLM-generated code locally via `exec()`, `eval()`, or local subprocesses (`subprocess.run`). All dynamic code MUST execute inside an E2B sandbox.
- **Hard Timeout Limit:** Every sandbox execution must respect `SANDBOX_TIMEOUT_SECONDS = 30` (from `config/settings.py`).
- **Output Length Limits:** Truncate stdout and stderr to prevent memory flooding:
  - `_MAX_STDOUT_CHARS = 2000`
  - `_MAX_STDERR_CHARS = 500`
- **Audit Logging:** Record the `sandbox_id`, execution duration (`execution_time_ms`), and exit status in `schemas.diligence_models.CodeChallenge`.

---

## 3. Tool Implementation (`tools/e2b_tools.py`)

Always use the standard `execute_in_sandbox()` tool function:

```python
from e2b_code_interpreter import AsyncSandbox
from config.settings import get_settings
from schemas.diligence_models import CodeChallenge

async def execute_in_sandbox(
    skill: str,
    challenge_description: str,
    python_code: str,
) -> CodeChallenge:
    settings = get_settings()
    timeout = settings.e2b_sandbox_timeout_seconds  # 30 seconds

    async with AsyncSandbox(api_key=settings.e2b_api_key) as sandbox:
        execution = await sandbox.run_code(
            python_code,
            timeout=timeout,
        )
        stdout = (execution.text or "")[:_MAX_STDOUT_CHARS]
        stderr = (execution.error.value if execution.error else "")[:_MAX_STDERR_CHARS]
        passed = execution.error is None

        return CodeChallenge(
            skill=skill,
            challenge_description=challenge_description,
            python_code=python_code,
            sandbox_id=sandbox.sandbox_id,
            execution_time_ms=...,
            passed=passed,
            stdout=stdout,
            stderr=stderr,
        )
```

---

## 4. Lifecycle & Error Handling

1. **Context Management:** Always use `async with AsyncSandbox(...)` or a robust `try...finally` block calling `await sandbox.kill()` to prevent orphaned micro-VMs.
2. **Timeout Handling:** Catch `TimeoutError` from E2B gracefully, mark `passed=False`, and populate `stderr="Execution timed out after 30 seconds"`.
3. **Syntax / Runtime Errors:** Failure in test assertions should not crash the agent; the failed execution details are captured in `CodeChallenge` so the agent can evaluate candidate readiness accurately.

