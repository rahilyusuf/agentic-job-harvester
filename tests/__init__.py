"""tests — Test suite mirroring the source tree.

- tests/agents/        ← Agent logic with mocked LiteLLM (fixture-based)
- tests/orchestration/ ← LoopAgent consensus-threshold regression tests
- tests/retrieval/     ← Vector matcher and prefilter unit tests
- tests/repositories/  ← Against interfaces.py mocks, not live BigQuery

Run: pytest tests/ -q --tb=short
Run unit only: pytest tests/ -q -m unit
"""
