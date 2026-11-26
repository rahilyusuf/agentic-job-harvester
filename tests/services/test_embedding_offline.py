"""HTTP → real Vertex/BQ adapters with fake SDK clients; no SQL emulation claim."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from google.cloud import aiplatform_v1beta1, bigquery
from google.cloud.aiplatform_v1beta1.types import PredictRequest, PredictResponse
from google.protobuf import json_format, struct_pb2

from config.settings import Settings
from gateway import client as gateway
from repositories import embeddings_repo, publications_repo, raw_jobs_repo
from schemas.job_models import JobStatus
from schemas.publication_models import ProcessingClaim
from services import embedding_worker as worker
from tests.services.test_embedding_worker import _make_dummy_job

VECTOR = [0.125] * 768


def response_with(prediction: Any) -> PredictResponse:
    """Build the wire protobuf directly to avoid repeated-Value constructor coercion."""
    pb = PredictResponse.pb()()
    pb.predictions.add().CopyFrom(json_format.ParseDict(prediction, struct_pb2.Value()))
    return PredictResponse.wrap(pb)


def envelope(payload: Any) -> dict[str, Any]:
    return {"message": {"data": base64.b64encode(json.dumps(payload).encode()).decode()}}


class FakeBigQuery:
    """Stateful expected SDK outcomes, not a BigQuery transaction/SQL interpreter."""

    def __init__(self) -> None:
        self.job = _make_dummy_job("synthetic'job")
        self.vector = None
        self.completed = False
        self.token = None
        self.calls = []
        self.configs = []
        self.fail = None
        self.busy = False
        self.missing = False

    def query_and_wait(self, sql: str, *, job_config: bigquery.QueryJobConfig) -> list[Any]:
        params = {p.name: p.value if hasattr(p, "value") else p.values
                  for p in job_config.query_parameters}
        self.calls.append((sql, params))
        self.configs.append(job_config)
        if "SELECT *" in sql:
            assert params["job_id"] == self.job.job_id
            return [] if self.missing else [self.job.model_dump()]
        if "SELECT embedding" in sql:
            return [] if self.vector is None else [SimpleNamespace(embedding=self.vector)]
        if "MERGE" in sql:
            if self.fail == "vector":
                raise RuntimeError("synthetic write failure")
            assert params["job_id"] == self.job.job_id
            self.vector = params["embedding"]
        elif "SET status = @status" in sql:
            if self.fail == "status" and params["status"] == "EMBEDDING_DONE":
                raise RuntimeError("synthetic completion failure")
            self.job.status = JobStatus(params["status"])
        else:
            raise AssertionError("Unexpected legacy SQL")
        return []

    def query(self, sql: str, *, job_config: bigquery.QueryJobConfig,
              **kwargs: Any) -> SimpleNamespace:
        params = {p.name: p.value if hasattr(p, "value") else p.values
                  for p in job_config.query_parameters}
        self.calls.append((sql, params))
        self.configs.append(job_config)
        rows = []
        if "CREATE TEMP TABLE claimed" in sql:
            if not self.busy:
                self.token = params["token"]
                self.job.status = JobStatus.EMBEDDING_QUEUED
                rows = [{"publication_id": params["key"], "job_id": params["id"],
                         "lease_token": self.token, "completed": self.completed}]
        elif "MERGE" in sql:
            assert params["token"] == self.token
            if self.fail == "vector":
                raise RuntimeError("synthetic uncertain completion")
            self.vector = params["embedding"]
            self.job.status = JobStatus.EMBEDDING_DONE
            self.completed = True
            self.token = None
        elif "processing_completed_at IS NULL" in sql:
            self.token = None
            self.job.status = JobStatus.NEW_RAW
        else:
            raise AssertionError("Unexpected ledger SQL")
        return SimpleNamespace(result=lambda **options: rows)

    def get_job(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("synthetic outcome remains unconfirmed")


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    settings = Settings.model_construct(
        gcp_project_id="offline-project", bq_dataset="offline_dataset",
        litellm_proxy_base_url="http://127.0.0.1:9", publication_ledger_enabled=False,
    )
    for module in (worker, gateway, embeddings_repo, raw_jobs_repo, publications_repo):
        monkeypatch.setattr(module, "get_settings", lambda: settings)
    bq = FakeBigQuery()
    monkeypatch.setattr(bigquery, "Client", lambda **kwargs: bq)
    prediction = MagicMock(return_value=response_with({"embeddings": {"values": VECTOR}}))
    constructor = MagicMock(return_value=SimpleNamespace(predict=prediction))
    monkeypatch.setattr(aiplatform_v1beta1, "PredictionServiceClient", constructor)
    init = MagicMock()
    monkeypatch.setattr(gateway.aiplatform, "init", init)
    return SimpleNamespace(settings=settings, bq=bq, predict=prediction,
                           constructor=constructor, init=init)


async def post(body: Any) -> httpx.Response:
    transport = httpx.ASGITransport(app=worker.app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://offline") as client:
        return await client.post("/", json=body)


@pytest.mark.asyncio
@pytest.mark.parametrize("ledger", [False, True])
async def test_http_to_persistence_and_completed_duplicate(
    harness: SimpleNamespace, ledger: bool,
) -> None:
    h = harness
    h.settings.publication_ledger_enabled = ledger
    result = await post(envelope({"job_id": "  synthetic'job  "}))
    assert result.status_code == 204
    assert h.bq.vector == VECTOR
    assert h.bq.job.status == JobStatus.EMBEDDING_DONE
    request = h.predict.call_args.kwargs["request"]
    assert isinstance(request, PredictRequest)
    assert request.endpoint == (
        "projects/offline-project/locations/us-central1/publishers/google/models/text-embedding-004"
    )
    assert request.instances[0]["content"] == worker._build_embedding_text(h.bq.job)
    h.init.assert_called_once_with(project="offline-project", location="us-central1")
    h.constructor.assert_called_once_with(
        client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"}
    )
    if ledger:
        assert h.bq.completed and h.bq.token is None
        sql, params = next((sql, p) for sql, p in h.bq.calls if "MERGE" in sql)
        assert "COMMIT TRANSACTION" in sql and "ASSERT @@row_count=1" in sql
        assert params["id"] == h.bq.job.job_id and params["embedding"] == VECTOR
    else:
        assert [p["status"] for sql, p in h.bq.calls if "SET status = @status" in sql] == [
            "EMBEDDING_QUEUED", "EMBEDDING_DONE",
        ]
        sql, params = next((sql, p) for sql, p in h.bq.calls if "MERGE" in sql)
        assert params["job_id"] == h.bq.job.job_id and params["embedding"] == VECTOR
        config = next(c for c in h.bq.configs if len(c.query_parameters) == 3)
        assert [p.to_api_repr()["parameterType"] for p in config.query_parameters] == [
            {"type": "STRING"}, {"type": "ARRAY", "arrayType": {"type": "FLOAT64"}},
            {"type": "TIMESTAMP"},
        ]
    assert all(h.bq.job.job_id not in sql for sql, _ in h.bq.calls)
    writes = len(h.bq.calls)
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 204
    assert len(h.bq.calls) == writes + 1
    assert h.predict.call_count == 1


def test_sdk_native_response_parsing_independent_of_request_constructor(
    harness: SimpleNamespace,
) -> None:
    assert gateway._parse_embedding_response(harness.predict.return_value) == VECTOR


def test_sdk_native_request_construction(harness: SimpleNamespace) -> None:
    assert gateway._blocking_embed("synthetic", "offline-project") == VECTOR
    assert isinstance(harness.predict.call_args.kwargs["request"], PredictRequest)


@pytest.mark.parametrize("location", [None, "Dubai"])
def test_text_format_and_description_cap(harness: SimpleNamespace, location: str | None) -> None:
    job = harness.bq.job.model_copy(update={"location": location, "description": "x" * 2100})
    expected = f"AI Engineer TechCorp {location or ''} " + "x" * 2000
    assert worker._build_embedding_text(job) == expected


@pytest.mark.asyncio
async def test_gateway_input_cap_and_existing_region_semantics(harness: SimpleNamespace) -> None:
    harness.settings.gcp_region = "europe-west1"
    await gateway.generate_embedding("x" * 10001)
    harness.init.assert_called_once_with(project="offline-project", location="europe-west1")
    request = harness.predict.call_args.kwargs["request"]
    assert request.instances[0]["content"] == "x" * 10000
    assert "/locations/us-central1/" in request.endpoint


@pytest.mark.parametrize("prediction", [None, {}, {"embeddings": None},
                                     {"embeddings": {}}, {"embeddings": {"values": "bad"}}])
def test_malformed_prediction_rejected(harness: SimpleNamespace, prediction: Any) -> None:
    with pytest.raises(ValueError, match="Invalid embedding"):
        gateway._parse_embedding_response(response_with(prediction))


def test_empty_predictions_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid embedding"):
        gateway._parse_embedding_response(PredictResponse())


BAD_VECTORS = [[], [0.0] * 767, [0.0] * 769, *[
    [value] + [0.0] * 767 for value in (True, "0.1", None, float("nan"),
                                      float("inf"), float("-inf"))
]]


@pytest.mark.parametrize("vector", BAD_VECTORS)
@pytest.mark.parametrize("ledger", [False, True])
@pytest.mark.asyncio
async def test_invalid_vector_rejected_before_persistence(
    harness: SimpleNamespace, vector: Any, ledger: bool,
) -> None:
    h = harness
    with pytest.raises(ValueError, match="Invalid embedding vector"):
        if ledger:
            repo = publications_repo.PublicationRepository(h.bq, settings=h.settings)
            await repo.finish_processing(ProcessingClaim(
                publication_id="key", job_id=h.bq.job.job_id, lease_token="owner",
            ), vector)
        else:
            repo = embeddings_repo.EmbeddingRepository(bq_client=h.bq)
            # Exercise the write boundary without waiting through retries of invalid input.
            await repo.upsert_embedding.__wrapped__(repo, h.bq.job.job_id, vector)
    assert h.bq.calls == []


@pytest.mark.parametrize("vector", BAD_VECTORS)
def test_gateway_rejects_invalid_numeric_vectors(vector: Any) -> None:
    # JSON/protobuf preserves bool, string, null and non-finite Value variants.
    with pytest.raises(ValueError, match="Invalid embedding vector"):
        gateway._parse_embedding_response(response_with({"embeddings": {"values": vector}}))


@pytest.mark.asyncio
async def test_numeric_vector_normalized_and_timestamp_preserved(harness: SimpleNamespace) -> None:
    from datetime import UTC, datetime

    ts = datetime(2026, 1, 1, tzinfo=UTC)
    repo = embeddings_repo.EmbeddingRepository(bq_client=harness.bq)
    await repo.upsert_embedding(harness.bq.job.job_id, [1] * 768, ts)
    assert all(type(value) is float for value in harness.bq.vector)
    assert harness.bq.calls[0][1]["embedded_at"] == ts


def malformed_bodies() -> list[bytes]:
    bodies = [b"{", b"null", b"[]", b'"PRIVATE_PAYLOAD_CANARY"', b"{}"]
    for message in (None, [], "bad", {}, {"data": None}, {"data": 7},
                    {"data": "!!!"},
                    {"data": envelope({"job_id": "synthetic-job"})["message"]["data"] + "\n"},
                    {"data": "a"},
                    {"data": base64.b64encode(b"\xff").decode()},
                    {"data": base64.b64encode(b"{").decode()}):
        bodies.append(json.dumps({"message": message}).encode())
    for payload in (None, [], "bad", 1, {}, *[
        {"job_id": value} for value in (None, True, 123, [], {}, "", "   ")
    ]):
        bodies.append(json.dumps(envelope(payload)).encode())
    return bodies


@pytest.mark.parametrize("body", malformed_bodies())
@pytest.mark.parametrize("ledger", [False, True])
@pytest.mark.asyncio
async def test_malformed_envelopes_ack_without_dependencies(
    harness: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, body: bytes, ledger: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness.settings.publication_ledger_enabled = ledger

    def unexpected() -> None:
        raise AssertionError("Malformed messages must not load settings")

    monkeypatch.setattr(worker, "get_settings", unexpected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=worker.app),
                                base_url="http://offline") as client:
        result = await client.post("/", content=body)
    assert result.status_code == 204 and result.content == b""
    assert harness.bq.calls == []
    harness.predict.assert_not_called()
    assert "Traceback" not in caplog.text
    assert "PRIVATE_PAYLOAD_CANARY" not in caplog.text


@pytest.mark.parametrize("failure", ["provider", "vector", "status"])
@pytest.mark.asyncio
async def test_legacy_failure_retries_without_false_ack(
    harness: SimpleNamespace, failure: str,
) -> None:
    h = harness
    if failure == "provider":
        h.predict.side_effect = RuntimeError("synthetic provider failure")
    else:
        h.bq.fail = failure
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 500
    assert h.bq.job.status == JobStatus.NEW_RAW
    assert (h.bq.vector is not None) == (failure == "status")
    h.bq.fail = None
    h.predict.side_effect = None
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 204
    assert h.bq.vector == VECTOR and h.bq.job.status == JobStatus.EMBEDDING_DONE


@pytest.mark.asyncio
async def test_ledger_existing_artifact_reused(harness: SimpleNamespace) -> None:
    h = harness
    h.settings.publication_ledger_enabled = True
    h.bq.vector = VECTOR.copy()
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 204
    h.predict.assert_not_called()
    assert h.bq.completed and h.bq.job.status == JobStatus.EMBEDDING_DONE


@pytest.mark.asyncio
async def test_ledger_invalid_existing_artifact_is_not_committed(harness: SimpleNamespace) -> None:
    h = harness
    h.settings.publication_ledger_enabled = True
    h.bq.vector = [0.0] * 767
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 503
    h.predict.assert_not_called()
    assert not h.bq.completed and h.bq.job.status == JobStatus.NEW_RAW
    assert not any("MERGE" in sql for sql, _ in h.bq.calls)


@pytest.mark.asyncio
async def test_ledger_busy_returns_retry(harness: SimpleNamespace) -> None:
    h = harness
    h.settings.publication_ledger_enabled = True
    h.bq.busy = True
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 503
    h.predict.assert_not_called()
    assert not h.bq.completed and h.bq.vector is None


@pytest.mark.parametrize("uncertain", [False, True])
@pytest.mark.asyncio
async def test_ledger_failure_releases_only_certain_work(
    harness: SimpleNamespace, uncertain: bool,
) -> None:
    h = harness
    h.settings.publication_ledger_enabled = True
    if uncertain:
        h.bq.fail = "vector"
    else:
        h.predict.side_effect = RuntimeError("synthetic provider failure")
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 503
    assert not h.bq.completed and h.bq.vector is None
    assert (h.bq.token is not None) == uncertain
    assert h.bq.job.status == (JobStatus.EMBEDDING_QUEUED if uncertain else JobStatus.NEW_RAW)


@pytest.mark.parametrize("ledger", [False, True])
@pytest.mark.asyncio
async def test_missing_raw_job_is_retryable_in_both_modes(
    harness: SimpleNamespace, ledger: bool,
) -> None:
    h = harness
    h.settings.publication_ledger_enabled = ledger
    h.bq.missing = True
    assert (await post(envelope({"job_id": h.bq.job.job_id}))).status_code == 503
    h.predict.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_missing_job_succeeds_when_visible_on_redelivery(
    harness: SimpleNamespace,
) -> None:
    h = harness
    h.settings.publication_ledger_enabled = False
    message = envelope({"job_id": h.bq.job.job_id})
    h.bq.missing = True

    first = await post(message)
    assert first.status_code == 503
    h.predict.assert_not_called()
    assert h.bq.vector is None and h.bq.job.status == JobStatus.NEW_RAW
    assert len(h.bq.calls) == 1 and "SELECT *" in h.bq.calls[0][0]

    h.bq.missing = False
    second = await post(message)
    assert second.status_code == 204
    h.predict.assert_called_once()
    assert h.bq.vector == VECTOR and h.bq.job.status == JobStatus.EMBEDDING_DONE
    assert sum("MERGE" in sql for sql, _ in h.bq.calls) == 1
