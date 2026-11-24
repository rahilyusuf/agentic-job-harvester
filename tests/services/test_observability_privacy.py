"""Offline SDK/API probes: no Langfuse constructor, exporter or provider calls."""

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import litellm
import pytest
from langfuse import Langfuse
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from pydantic import BaseModel, ValidationError

from gateway import client as gateway
from observability import setup


@pytest.fixture
def sdk(monkeypatch):
    """Exercise real SDK context code with local OTel and an inert observation wrapper."""
    records, observations, calls = [], [], []

    class Collector(SpanProcessor):
        def on_end(self, span):
            records.append(span)

    class Observation:
        def __init__(self, **kwargs):
            self.fields = kwargs
            self.updates = []
            observations.append(self)

        def update(self, **kwargs):
            self.updates.append(kwargs)

    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(Collector())
    # Bypass __init__: it can create an exporter even when using fake credentials.
    client = object.__new__(Langfuse)
    client._otel_tracer = provider.get_tracer("offline-privacy-test")
    client._environment = "test"
    client._release = "test"
    monkeypatch.setattr(client, "_get_span_class", lambda kind: Observation)
    original = client.start_as_current_observation

    def start(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(client, "start_as_current_observation", start)
    monkeypatch.setattr(setup, "get_langfuse", lambda: client)
    yield SimpleNamespace(client=client, records=records, observations=observations, calls=calls)
    provider.shutdown()


def test_installed_sdk_defaults_and_exception_exit_are_not_privacy_controls(sdk):
    signature = inspect.signature(Langfuse.start_as_current_observation)
    assert signature.parameters["input"].default is None
    assert signature.parameters["output"].default is None
    # Native context managers record exception content unless it is kept out of __exit__.
    error = RuntimeError("EXCEPTION_CONTENT_CANARY")
    with pytest.raises(RuntimeError) as raised:
        with sdk.client.start_as_current_observation(name="direct", as_type="generation"):
            raise error
    assert raised.value is error
    assert any(
        "EXCEPTION_CONTENT_CANARY" in str(event.attributes)
        for record in sdk.records for event in record.events
    )
    assert sdk.observations[0].fields["input"] is None
    assert sdk.observations[0].fields["output"] is None


def test_adapter_blocks_io_and_exception_capture_even_when_capture_is_requested(sdk, monkeypatch):
    monkeypatch.setenv("LANGFUSE_OBSERVE_DECORATOR_IO_CAPTURE_ENABLED", "true")
    result = {"resume_summary": "OUTPUT_RESUME_CANARY"}

    @setup.observe(name="ScorerAgent", capture_input=True, capture_output=True)
    def success(resume_summary):
        return result

    assert success("INPUT_RESUME_CANARY") is result
    error = ValueError("ERROR_RESUME_CANARY")

    @setup.observe(name="SkepticAgent")
    def failure():
        raise error

    with pytest.raises(ValueError) as raised:
        failure()
    assert raised.value is error
    assert not any(record.events for record in sdk.records)
    captured = str(sdk.calls) + str([o.fields for o in sdk.observations])
    captured += str([o.updates for o in sdk.observations])
    assert "CANARY" not in captured
    assert all(call["input"] is None and call["output"] is None for call in sdk.calls)
    assert sdk.observations[-1].updates[-1]["metadata"]["outcome"] == "error"


@pytest.mark.asyncio
async def test_nested_sync_async_context_and_concurrent_requests(sdk):
    @setup.observe(name="SkepticAgent")
    def child():
        return trace.get_current_span().get_span_context()

    @setup.observe(name="ScorerAgent")
    async def parent(value):
        current = trace.get_current_span().get_span_context()
        await asyncio.sleep(0)
        nested = child()
        assert nested.trace_id == current.trace_id
        assert nested.span_id != current.span_id
        return value, current

    initial = otel_context.get_current()
    first, second = await asyncio.gather(parent("one"), parent("two"))
    assert (first[0], second[0]) == ("one", "two")
    assert first[1].trace_id != second[1].trace_id
    for record in sdk.records:
        if record.name == "SkepticAgent":
            assert record.parent.span_id in {first[1].span_id, second[1].span_id}
    assert otel_context.get_current() is initial


@pytest.mark.parametrize("phase", ["client", "create", "enter", "update", "exit"])
@pytest.mark.parametrize("business_fails", [False, True])
def test_telemetry_failure_never_reexecutes_or_replaces_sync_business(
    monkeypatch, phase, business_fails, caplog
):
    telemetry_error = RuntimeError("TELEMETRY_SECRET_CANARY")
    span = MagicMock()
    manager = MagicMock()
    manager.__enter__.return_value = span
    client = MagicMock()
    client.start_as_current_observation.return_value = manager
    getter = MagicMock(return_value=client)
    target = {
        "client": getter, "create": client.start_as_current_observation,
        "enter": manager.__enter__, "update": span.update, "exit": manager.__exit__,
    }[phase]
    target.side_effect = telemetry_error
    monkeypatch.setattr(setup, "get_langfuse", getter)
    count = 0
    business_error = ValueError("BUSINESS_SECRET_CANARY")
    initial = otel_context.get_current()

    @setup.observe(name="ScorerAgent")
    def operation():
        nonlocal count
        count += 1
        if business_fails:
            raise business_error
        return 42

    if business_fails:
        with pytest.raises(ValueError) as raised:
            operation()
        assert raised.value is business_error
    else:
        assert operation() == 42
    assert count == 1 and otel_context.get_current() is initial
    assert "CANARY" not in caplog.text
    if manager.__exit__.called:
        manager.__exit__.assert_called_once_with(None, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["client", "enter", "update", "exit"])
async def test_cancellation_is_original_and_business_executes_once(monkeypatch, phase):
    span, manager, client = MagicMock(), MagicMock(), MagicMock()
    manager.__enter__.return_value = span
    client.start_as_current_observation.return_value = manager
    getter = MagicMock(return_value=client)
    target = {
        "client": getter, "enter": manager.__enter__, "update": span.update,
        "exit": manager.__exit__,
    }[phase]
    target.side_effect = RuntimeError("TELEMETRY_CANARY")
    monkeypatch.setattr(setup, "get_langfuse", getter)
    cancelled = asyncio.CancelledError("CANCELLATION_CANARY")
    count = 0

    @setup.observe(name="ScorerAgent")
    async def operation():
        nonlocal count
        count += 1
        raise cancelled

    with pytest.raises(asyncio.CancelledError) as raised:
        await operation()
    assert raised.value is cancelled and count == 1


def test_metadata_is_controlled_and_identifiers_are_never_raw(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(setup, "get_langfuse", lambda: client)
    metadata = {
        "operation": "structured_completion", "model_alias": "fast-flash",
        "retry_limit": 3, "prompt": "PROMPT_CANARY", "nested": {"resume": "CANARY"},
        "job_id": "JOB_CANARY", "correlation_id": "CORRELATION_CANARY",
    }
    original = dict(metadata)
    setup.update_current_span(user_id="USER_CANARY", session_id="SESSION_CANARY", metadata=metadata)
    safe = client.update_current_span.call_args.kwargs["metadata"]
    assert set(safe) == {
        "operation", "model_alias", "retry_limit", "job_id_hash", "user_id_hash",
        "correlation_id_hash",
    }
    assert "CANARY" not in str(safe) and metadata == original


@pytest.mark.parametrize("first", [True, False])
def test_setup_and_gateway_order_preserves_unrelated_callbacks(monkeypatch, first):
    other = object()
    for name in ("callbacks", "success_callback", "failure_callback"):
        monkeypatch.setattr(litellm, name, [other, "langfuse", "langfuse_otel", "other"])
    monkeypatch.setattr(gateway, "_litellm_configured", False)
    monkeypatch.setattr(gateway.instructor, "from_litellm", MagicMock())
    settings = SimpleNamespace(
        litellm_proxy_base_url="http://127.0.0.1:9", litellm_proxy_api_key="dummy",
        langfuse_public_key="", langfuse_secret_key="", langfuse_host="http://127.0.0.1:9",
    )
    monkeypatch.setattr(setup, "get_settings", lambda: settings)
    monkeypatch.setattr(gateway, "get_settings", lambda: settings)
    monkeypatch.setattr(litellm, "api_base", None)
    monkeypatch.setattr(litellm, "api_key", None)
    if first:
        setup.setup_observability()
    gateway.GatewayClient()
    setup.setup_observability()
    setup.setup_observability()
    for name in ("callbacks", "success_callback", "failure_callback"):
        assert getattr(litellm, name) == [other, "other"]
    assert litellm.api_base == settings.litellm_proxy_base_url
    assert litellm.api_key == settings.litellm_proxy_api_key


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "model", "validation"])
async def test_gateway_is_one_logical_generation_and_preserves_model_behavior(sdk, failure, monkeypatch):
    class Result(BaseModel):
        value: int

    result = Result(value=1)
    error = RuntimeError("MODEL_CANARY")
    if failure == "validation":
        try:
            Result.model_validate({"value": "invalid"})
        except ValidationError as exc:
            error = exc
    create = AsyncMock(return_value=result)
    if failure:
        create.side_effect = error
    client = object.__new__(gateway.GatewayClient)
    monkeypatch.setattr(
        client, "_client",
        SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create))),
        raising=False,
    )
    messages = [{"role": "user", "content": "RESUME_PROMPT_CANARY"}]
    metadata = {"provider_metadata": "unchanged"}

    @setup.observe(name="ScorerAgent")
    async def agent():
        return await client.create(
            "fast-flash", Result, messages, trace_id="CORRELATION_CANARY",
            max_retries=3, metadata=metadata, temperature=0.1,
        )

    if failure:
        with pytest.raises(type(error)) as raised:
            await agent()
        assert raised.value is error
    else:
        assert await agent() is result
    create.assert_awaited_once_with(
        model="fast-flash", response_model=Result, messages=messages, max_retries=3,
        metadata={**metadata, "trace_id": "CORRELATION_CANARY"}, temperature=0.1,
    )
    assert metadata == {"provider_metadata": "unchanged"}
    assert [call["as_type"] for call in sdk.calls] == ["span", "generation"]
    generation = next(record for record in sdk.records if record.name == "structured_completion")
    parent = next(record for record in sdk.records if record.name == "ScorerAgent")
    assert generation.parent.span_id == parent.context.span_id
    assert not generation.events
    assert "CANARY" not in str(sdk.calls)


def test_disabled_tracing_never_discovers_client_or_exporter(monkeypatch):
    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")
    getter = MagicMock(side_effect=AssertionError("SDK construction forbidden"))
    monkeypatch.setattr(setup, "get_client", getter)
    with setup.safe_observation(name="ScorerAgent"):
        pass
    setup.flush_observability()
    getter.assert_not_called()


@pytest.mark.asyncio
async def test_actual_task_cancellation_closes_native_context_without_exception_capture(sdk):
    entered = asyncio.Event()
    errors = []
    count = 0

    @setup.observe(name="ScorerAgent")
    async def operation():
        nonlocal count
        count += 1
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as error:
            errors.append(error)
            raise

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel("CANCELLATION_CANARY")
    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value is errors[0] and count == 1
    assert not sdk.records[0].events
    assert sdk.observations[0].updates[-1]["metadata"]["outcome"] == "cancelled"
    assert "CANARY" not in str(sdk.calls)


def test_disabled_tracing_is_rechecked_after_an_enabled_call(monkeypatch):
    getter = MagicMock()
    monkeypatch.setattr(setup, "get_client", getter)
    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "true")
    assert setup.get_langfuse() is getter.return_value
    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")
    assert setup.get_langfuse() is None
    getter.assert_called_once_with()


@pytest.mark.parametrize("method", ["create_score", "update_current_span", "flush"])
def test_feedback_update_and_flush_failures_are_best_effort(monkeypatch, method, caplog):
    client = MagicMock()
    getattr(client, method).side_effect = RuntimeError("SENSITIVE_CANARY")
    monkeypatch.setattr(setup, "get_langfuse", lambda: client)
    setup.score_trace("trace", "JOB_CANARY", "analyze", comment="RESUME_CANARY")
    setup.update_current_span(user_id="USER_CANARY")
    setup.flush_observability()
    assert "CANARY" not in str(client.create_score.call_args)
    assert "CANARY" not in caplog.text


def test_proxy_changes_only_owned_telemetry_callbacks():
    from pathlib import Path

    import yaml

    config = yaml.safe_load(Path("gateway/litellm_config.yaml").read_text(encoding="utf-8"))
    assert config["litellm_settings"]["success_callback"] == []
    assert config["litellm_settings"]["failure_callback"] == []
    assert {m["model_name"] for m in config["model_list"]} == {
        "router-flash", "fast-flash", "deep-pro",
    }
