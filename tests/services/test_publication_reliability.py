"""Offline contract/fault tests. In-memory state models are not a BigQuery emulator."""

import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from starlette.responses import JSONResponse

from config.settings import Settings
from repositories.publications_repo import PublicationRepository
from repositories.raw_jobs_repo import RawJobRepository
from schemas.publication_models import (
    BatchInsertResult,
    CheckpointResult,
    ProcessingBusy,
    ProcessingClaim,
    ProcessingLeaseLost,
    PublicationClaim,
    PublicationOwnership,
    QueryFailed,
    QueryOutcomeUnconfirmed,
    QueryTransactionAborted,
    publication_id,
)
from scripts.plan_publication_migration import build_manifest
from services import apify_receiver, embedding_worker
from services.ingestion_publications import ingest_with_publications
from services.normalizers.batch import normalize_batch
from services.publication_dispatcher import dispatch_pending
from tests.services.test_embedding_worker import _build_pubsub_request, _make_dummy_job


def settings():
    return Settings.model_construct(
        gcp_project_id="test-project",
        bq_dataset="test_dataset",
        bq_location="US",
        pubsub_embedding_topic="test-topic",
        publication_ledger_enabled=True,
    )


class MemoryLedger:
    """Explicit fake for concurrency and crash-window contracts, not SQL validation."""

    def __init__(self):
        self.rows = {}
        self.now = 0
        self.lock = asyncio.Lock()
        self.checkpoint_fails = False
        self.completed = set()
        self.processing = {}
        self.artifacts = {}
        self.checkpoints = []

    def add(self, job_id, state="PENDING"):
        self.rows[job_id] = dict(state=state, until=0, token=None, attempts=0, message=None)

    async def claim_pending(self, job_ids=None, limit=100, min_age_seconds=0, *, deadline=None):
        async with self.lock:
            claims = []
            for job_id, row in sorted(self.rows.items()):
                if job_ids is not None and job_id not in job_ids:
                    continue
                if row["state"] in ("PUBLISHED", "UNKNOWN") or row["until"] > self.now:
                    continue
                if row["attempts"] >= 5:
                    row["state"] = "UNKNOWN"
                    continue
                token = f"token-{row['attempts']}"
                row.update(
                    state="IN_FLIGHT",
                    until=self.now + 1800,
                    token=token,
                    attempts=row["attempts"] + 1,
                )
                claims.append(
                    PublicationClaim(
                        publication_id=job_id, job_id=job_id, lease_token=token,
                        lease_expires_at=datetime.fromtimestamp(row["until"], timezone.utc),
                    )
                )
                if len(claims) == limit:
                    break
            return claims

    async def check_ownership(self, claims, *, deadline=None):
        return [
            PublicationOwnership(
                **{**c.model_dump(), "lease_expires_at": datetime.fromtimestamp(
                    self.rows[c.job_id]["until"], timezone.utc)},
                server_now=datetime.fromtimestamp(self.now, timezone.utc),
            ) for c in claims if self.rows[c.job_id]["token"] == c.lease_token
            and self.rows[c.job_id]["until"] > self.now
            and self.rows[c.job_id]["state"] == "IN_FLIGHT"
        ]

    async def record_outcomes(self, claims, confirmed, unconfirmed_ids, *, deadline=None):
        if self.checkpoint_fails:
            raise QueryOutcomeUnconfirmed("checkpoint")
        self.checkpoints.append([c.job_id for c in claims])
        saved, lost = [], []
        async with self.lock:
            for claim in claims:
                row = self.rows[claim.job_id]
                if (row["state"] == "PUBLISHED" and row["message"] is not None
                    and row["message"] == confirmed.get(claim.job_id)):
                    saved.append(claim.job_id)
                    continue
                if (row["token"] != claim.lease_token or row["until"] <= self.now
                    or row["state"] != "IN_FLIGHT"):
                    lost.append(claim.job_id)
                    continue
                row["state"] = "PUBLISHED" if claim.job_id in confirmed else "PENDING"
                row["message"] = confirmed.get(claim.job_id)
                row["token"] = None
                if claim.job_id in confirmed:
                    row["until"] = 0
                else:
                    row["until"] = max(row["until"], self.now + 60)
                saved.append(claim.job_id)
        return CheckpointResult(checkpointed_ids=saved, lease_lost_ids=lost)

    async def publication_counts(self, job_ids):
        counts = {}
        for job_id in job_ids:
            state = self.rows[job_id]["state"]
            counts[state] = counts.get(state, 0) + 1
        return counts

    async def claim_processing(self, job_id):
        async with self.lock:
            if job_id in self.completed:
                return ProcessingClaim(publication_id=job_id, job_id=job_id, completed=True)
            if job_id in self.processing:
                raise ProcessingBusy()
            self.processing[job_id] = "owner"
            return ProcessingClaim(publication_id=job_id, job_id=job_id, lease_token="owner")

    async def finish_processing(self, claim, embedding):
        async with self.lock:
            if self.processing.get(claim.job_id) != claim.lease_token:
                raise ProcessingLeaseLost()
            self.artifacts[claim.job_id] = embedding
            self.completed.add(claim.job_id)
            self.processing.pop(claim.job_id)

    async def release_processing(self, claim):
        if self.processing.get(claim.job_id) == claim.lease_token:
            self.processing.pop(claim.job_id)


class MemoryRaw:
    def __init__(self, ledger):
        self.publications = ledger
        self.jobs = {}

    async def persist_jobs(self, jobs):
        async with self.publications.lock:
            inserted, existing = [], []
            for job in jobs:
                if job.job_id in self.jobs:
                    existing.append(job.job_id)
                    if job.job_id not in self.publications.rows:
                        self.publications.add(job.job_id, "UNKNOWN")
                else:
                    if job.job_id in self.publications.rows:
                        raise ValueError("Retained identity has no raw job")
                    inserted.append(job.job_id)
                    self.jobs[job.job_id] = job
                    self.publications.add(job.job_id)
            return BatchInsertResult(inserted_ids=inserted, existing_ids=existing)


@pytest.fixture
def full_batch():
    path = (
        Path(__file__).resolve().parents[2]
        / "Input/dataset_company-career-page-scraper_2026-09-17_06-15-27-909.json"
    )
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    original = deepcopy(data)
    batch = normalize_batch(data)
    assert batch.received == 215 and len(batch.jobs) == 113
    assert batch.in_batch_duplicates == 102 and data == original
    return batch


@pytest.mark.asyncio
async def test_full_fixture_first_and_repeated_webhooks(full_batch):
    ledger, sent = MemoryLedger(), []
    raw = MemoryRaw(ledger)
    # Three explicitly known pending jobs, rather than legacy NEW_RAW assumptions.
    for job in full_batch.jobs[:3]:
        raw.jobs[job.job_id] = job
        ledger.add(job.job_id)

    async def publish(job_id):
        sent.append(job_id)
        return f"ack-{job_id}"

    async def dispatcher(repository, **kwargs):
        return await dispatch_pending(repository, publish_one=publish, **kwargs)

    first = await ingest_with_publications(
        full_batch,
        dataset_id="dataset",
        actor_run_id="run",
        settings=settings(),
        repository=raw,
        dispatcher=dispatcher,
    )
    second = await ingest_with_publications(
        full_batch,
        dataset_id="dataset",
        actor_run_id="run",
        settings=settings(),
        repository=raw,
        dispatcher=dispatcher,
    )
    assert (first["inserted"], first["recovered"], first["published"]) == (110, 3, 113)
    assert (second["inserted"], second["recovered"], second["published"]) == (0, 0, 0)
    assert len(raw.jobs) == len(set(sent)) == len(sent) == 113


@pytest.mark.asyncio
async def test_legacy_rows_are_held_without_automatic_replay(full_batch):
    ledger = MemoryLedger()
    raw = MemoryRaw(ledger)
    raw.jobs = {job.job_id: job for job in full_batch.jobs}
    await raw.persist_jobs(full_batch.jobs)
    send = AsyncMock()
    result = await dispatch_pending(
        ledger, project_id="test-project", topic="test-topic", publish_one=send
    )
    assert result.attempted_ids == []
    send.assert_not_awaited()
    assert {row["state"] for row in ledger.rows.values()} == {"UNKNOWN"}


@pytest.mark.asyncio
async def test_concurrent_delivery_and_100_plus_13_fairness():
    ledger = MemoryLedger()
    for i in range(113):
        ledger.add(f"job-{i:03}")
    send = AsyncMock(side_effect=lambda job_id: f"ack-{job_id}")

    async def dispatch():
        return await dispatch_pending(
            ledger, project_id="test-project", topic="test-topic", publish_one=send
        )

    results = await asyncio.gather(dispatch(), dispatch())
    assert sorted(len(result.confirmed) for result in results) == [13, 100]
    assert send.await_count == 113
    assert (await dispatch()).attempted_ids == []


@pytest.mark.asyncio
async def test_partial_publication_recovers_only_due_unconfirmed_ids():
    ledger = MemoryLedger()
    for job_id in ("good", "uncertain"):
        ledger.add(job_id)

    async def publish(job_id):
        if job_id == "uncertain":
            raise TimeoutError("PRIVATE")
        return "ack"

    first = await dispatch_pending(ledger, project_id="p", topic="t", publish_one=publish)
    assert first.confirmed == {"good": "ack"} and first.unconfirmed_ids == ["uncertain"]
    send = AsyncMock(return_value="ack-recovered")
    assert not (
        await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    ).attempted_ids
    ledger.now += 1801
    recovered = await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    assert recovered.attempted_ids == ["uncertain"]


@pytest.mark.asyncio
async def test_broker_ack_checkpoint_crash_recovers_at_least_once():
    ledger = MemoryLedger()
    ledger.add("job")
    ledger.checkpoint_fails = True
    send = AsyncMock(return_value="broker-ack")
    with pytest.raises(QueryOutcomeUnconfirmed):
        await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    assert ledger.rows["job"]["state"] == "IN_FLIGHT"
    ledger.checkpoint_fails = False
    assert not (
        await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    ).attempted_ids
    ledger.now += 1801
    await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    assert send.await_count == 2  # Explicit unavoidable crash-window duplicate.


@pytest.mark.asyncio
async def test_partial_or_unconfirmed_persistence_never_publishes(full_batch):
    repo = MagicMock()
    repo.persist_jobs = AsyncMock(
        return_value=BatchInsertResult(unconfirmed_ids=[j.job_id for j in full_batch.jobs])
    )
    dispatcher = AsyncMock()
    with pytest.raises(HTTPException) as error:
        await ingest_with_publications(
            full_batch,
            dataset_id="d",
            actor_run_id="r",
            settings=settings(),
            repository=repo,
            dispatcher=dispatcher,
        )
    assert error.value.status_code == 502
    dispatcher.assert_not_awaited()


@pytest.mark.asyncio
async def test_atomic_adapter_uses_parameters_and_returns_actual_confirmations():
    client = MagicMock()
    client.query.return_value.result.return_value = [{"job_id": "job", "inserted": True}]
    repo = RawJobRepository(bq_client=client, settings=settings())
    result = await repo.persist_jobs([_make_dummy_job("job")])
    assert result.inserted_ids == ["job"]
    sql = client.query.call_args.args[0]
    assert (
        sql.index("DELETE FROM")
        < sql.index("INSERT `test-project.test_dataset.raw_job_postings`")
        < sql.index("COMMIT TRANSACTION")
    )
    assert "INSERT `test-project.test_dataset.job_publications`" in sql
    client.insert_rows_json.assert_not_called()
    assert client.query.call_args.kwargs["job_retry"] is None
    values = {p.name: p.value for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert json.loads(values["jobs"])[0]["job_id"] == "job"


@pytest.mark.asyncio
async def test_uncertain_query_resolves_same_job_without_resubmission():
    client = MagicMock()
    client.query.side_effect = TimeoutError("PRIVATE")
    client.get_job.return_value.result.return_value = [{"job_id": "job", "inserted": True}]
    repo = RawJobRepository(bq_client=client, settings=settings())
    assert (await repo.persist_jobs([_make_dummy_job("job")])).inserted_ids == ["job"]
    assert client.get_job.call_args.args[0] == client.query.call_args.kwargs["job_id"]
    assert client.query.call_count == 1
    client.get_job.side_effect = TimeoutError("PRIVATE")
    result = await repo.persist_jobs([_make_dummy_job("other")])
    assert result.unconfirmed_ids == ["other"] and result.inserted_ids == []
    assert result.query_job_id


@pytest.mark.asyncio
async def test_sql_claims_and_completion_are_fenced_and_ordered():
    client = MagicMock()
    client.query.return_value.result.return_value = []
    repo = PublicationRepository(client, settings=settings())
    assert await repo.claim_pending() == []
    sql = client.query.call_args.args[0]
    assert "ORDER BY created_at, publication_id LIMIT @limit" in sql
    assert "attempt_count >= 5" in sql and "state='UNKNOWN'" in sql
    assert "lease_expires_at<=CURRENT_TIMESTAMP()" in sql
    claim = ProcessingClaim(publication_id="key", job_id="job", lease_token="token")
    await repo.finish_processing(claim, [0.0] * 768)
    sql = client.query.call_args.args[0]
    assert (
        "processing_token=@token" in sql
        and "processing_lease_expires_at>CURRENT_TIMESTAMP()" in sql
    )
    assert sql.index("ASSERT") < sql.index("MERGE") < sql.index("processing_completed_at=")


@pytest.mark.asyncio
async def test_worker_busy_duplicate_does_not_generate_twice(monkeypatch):
    monkeypatch.setattr(embedding_worker, "get_settings", settings)
    ledger = MemoryLedger()
    raw = MagicMock(get_job=AsyncMock(return_value=_make_dummy_job("job")))
    emb = MagicMock(get_embedding=AsyncMock(return_value=None))
    started, finish = asyncio.Event(), asyncio.Event()

    async def generate(text):
        started.set()
        await finish.wait()
        return [0.0] * 768

    model = AsyncMock(side_effect=generate)
    monkeypatch.setattr(embedding_worker, "generate_embedding", model)
    first = asyncio.create_task(
        embedding_worker._process_job_with_claim(
            "job", raw_repo=raw, emb_repo=emb, publications=ledger
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    try:
        with pytest.raises(ProcessingBusy):
            await embedding_worker._process_job_with_claim(
                "job", raw_repo=raw, emb_repo=emb, publications=ledger
            )
    finally:
        finish.set()
        await first
    await embedding_worker._process_job_with_claim(
        "job", raw_repo=raw, emb_repo=emb, publications=ledger
    )
    assert model.await_count == 1 and "job" in ledger.completed


@pytest.mark.asyncio
async def test_worker_reuses_existing_artifact_and_failure_does_not_clear_publication(monkeypatch):
    monkeypatch.setattr(embedding_worker, "get_settings", settings)
    ledger = MemoryLedger()
    ledger.add("job", "PUBLISHED")
    raw = MagicMock(get_job=AsyncMock(return_value=_make_dummy_job("job")))
    emb = MagicMock(get_embedding=AsyncMock(return_value=[0.0] * 768))
    model = AsyncMock()
    monkeypatch.setattr(embedding_worker, "generate_embedding", model)
    await embedding_worker._process_job_with_claim(
        "job", raw_repo=raw, emb_repo=emb, publications=ledger
    )
    model.assert_not_awaited()
    assert ledger.rows["job"]["state"] == "PUBLISHED"


@pytest.mark.asyncio
async def test_worker_busy_delivery_is_retryable_not_acknowledged(monkeypatch):
    monkeypatch.setattr(embedding_worker, "get_settings", settings)
    monkeypatch.setattr(embedding_worker, "_process_job", AsyncMock(side_effect=ProcessingBusy()))
    response = await embedding_worker.pubsub_push(_build_pubsub_request("job"))
    assert isinstance(response, JSONResponse)
    assert response.status_code == 503


@pytest.mark.asyncio
async def test_worker_cancel_holds_lease_for_possible_background_work(monkeypatch):
    monkeypatch.setattr(embedding_worker, "get_settings", settings)
    ledger = MemoryLedger()
    raw = MagicMock(get_job=AsyncMock(return_value=_make_dummy_job("job")))
    emb = MagicMock(get_embedding=AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await embedding_worker._process_job_with_claim(
            "job", raw_repo=raw, emb_repo=emb, publications=ledger
        )
    assert "job" in ledger.processing


def test_evidence_first_manifest_never_invents_confirmation():
    topic = "projects/test-project/topics/test-topic"
    jobs = [{"job_id": f"job-{i}", "status": "NEW_RAW"} for i in range(113)]
    original = deepcopy(jobs)
    unknown = build_manifest(jobs, [], topic=topic)
    assert unknown["unknown"] == 113 and unknown["published"] == 0
    proof = [
        {
            "job_id": "job-1",
            "topic": topic,
            "publication_confirmed": True,
            "evidence_ref": "reviewed-run-manifest",
        }
    ]
    manifest = build_manifest(jobs, proof, topic=topic)
    assert manifest["published"] == 1 and manifest["unknown"] == 112
    row = next(r for r in manifest["rows"] if r["job_id"] == "job-1")
    assert row["message_id"] is None and row["published_at"] is None
    assert row["publication_id"] == publication_id(topic, "job-1")
    assert jobs == original and not manifest["applied"] and manifest["automatic_replays"] == 0
    with pytest.raises(ValueError):
        build_manifest(jobs, [{**proof[0], "topic": "wrong"}], topic=topic)
    with pytest.raises(ValueError):
        build_manifest(jobs[:-1], [], topic=topic)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "secret,header,status",
    [
        ("", "", 503),
        ("configured", "", 401),
        ("configured", "Bearer wrong", 401),
        ("configured", "Bearer configured", 200),
    ],
)
@pytest.mark.parametrize("enabled", [False, True])
async def test_reconciliation_requires_explicit_authorization(
    monkeypatch, secret, header, status, enabled,
):
    configured = settings()
    configured.publication_ledger_enabled = enabled
    configured.reconciliation_secret = secret
    monkeypatch.setattr(apify_receiver, "get_settings", lambda: configured)
    reconcile = AsyncMock(
        return_value={"reconciled": 0, "published": 0, "publish_failures": 0, "job_ids": []}
    )
    monkeypatch.setattr(apify_receiver, "reconcile_unpublished_jobs", reconcile)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=apify_receiver.app), base_url="http://test"
    ) as client:
        response = await client.post("/jobs/reconcile", headers={"Authorization": header})
    assert response.status_code == status
    assert reconcile.await_count == int(status == 200)


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["limit=0", "limit=1001", "min_age_seconds=-1"])
async def test_reconciliation_bounds_reject_before_cloud_calls(monkeypatch, query):
    reconcile = AsyncMock()
    monkeypatch.setattr(apify_receiver, "reconcile_unpublished_jobs", reconcile)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=apify_receiver.app), base_url="http://test"
    ) as client:
        assert (await client.post("/reconcile?" + query)).status_code == 422
    reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_existing_jobs_are_logged_as_held_not_success(full_batch):
    from observability.ingestion_logging import request_context

    ledger = MemoryLedger()
    raw = MemoryRaw(ledger)
    raw.jobs = {job.job_id: job for job in full_batch.jobs}
    with request_context(None) as state:
        result = await ingest_with_publications(
            full_batch, dataset_id="d", actor_run_id="r", settings=settings(), repository=raw
        )
        assert state.incomplete and result["published"] == 0


@pytest.mark.asyncio
async def test_duplicate_claim_set_never_reaches_publisher():
    repo = MagicMock()
    claim = PublicationClaim(
        publication_id="key", job_id="job", lease_token="token",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=1800),
    )
    repo.claim_pending = AsyncMock(return_value=[claim, claim])
    publish = AsyncMock()
    with pytest.raises(ValueError):
        await dispatch_pending(repo, project_id="p", topic="t", publish_one=publish)
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_raw_write_entrypoints_never_stream_in_ledger_mode():
    client = MagicMock()
    client.query.return_value.result.return_value = [{"job_id": "job", "inserted": True}]
    repo = RawJobRepository(bq_client=client, settings=settings())
    job = _make_dummy_job("job")
    await repo.insert_job(job)
    assert await repo.batch_insert_jobs([job]) == ["job"]
    assert await repo.batch_insert_jobs_once([job]) == ["job"]
    assert client.query.call_count == 3
    client.insert_rows_json.assert_not_called()
    for call in client.query.call_args_list:
        sql = call.args[0]
        assert "UPPER(" in sql and "TO_HEX(SHA256" in sql
        # Catch broken SQL delimiters offline without pretending to execute BigQuery.
        depth = 0
        for char in sql:
            depth += (char == "(") - (char == ")")
            assert depth >= 0
        assert depth == 0


@pytest.mark.asyncio
async def test_worker_failure_releases_only_owned_processing_lease(monkeypatch):
    monkeypatch.setattr(embedding_worker, "get_settings", settings)
    ledger = MemoryLedger()
    ledger.add("job", "PUBLISHED")
    raw = MagicMock(get_job=AsyncMock(return_value=_make_dummy_job("job")))
    emb = MagicMock(get_embedding=AsyncMock(return_value=None))
    monkeypatch.setattr(
        embedding_worker, "generate_embedding", AsyncMock(side_effect=ValueError("PRIVATE"))
    )
    with pytest.raises(ValueError):
        await embedding_worker._process_job_with_claim(
            "job", raw_repo=raw, emb_repo=emb, publications=ledger
        )
    assert not ledger.processing and ledger.rows["job"]["state"] == "PUBLISHED"
    stale = ProcessingClaim(publication_id="job", job_id="job", lease_token="stale")
    ledger.processing["job"] = "new-owner"
    await ledger.release_processing(stale)
    assert ledger.processing["job"] == "new-owner"


def test_dev_ledger_configuration_never_resolves_production_secrets(monkeypatch):
    from devtools.adapters import _publication_settings

    monkeypatch.setenv("PUBLICATION_LEDGER_ENABLED", "true")
    with patch(
        "config.settings.get_settings", side_effect=AssertionError("No production settings")
    ):
        configured = _publication_settings()
    assert (
        configured.publication_ledger_enabled
        and configured.gcp_project_id == "agentic-job-harvester-v2"
    )


@pytest.mark.asyncio
async def test_reconciliation_pages_do_not_reselect_confirmed_jobs(monkeypatch):
    from services import publication_dispatcher

    ledger = MemoryLedger()
    for i in range(113):
        ledger.add(f"job-{i:03}")
    real_dispatch = publication_dispatcher.dispatch_pending
    send = AsyncMock(return_value="broker-ack")

    async def dispatch(repository, **kwargs):
        return await real_dispatch(repository, publish_one=send, **kwargs)

    monkeypatch.setattr(publication_dispatcher, "dispatch_pending", dispatch)
    monkeypatch.setattr(apify_receiver, "get_settings", settings)
    repo = MagicMock(publications=ledger)
    first = await apify_receiver.reconcile_unpublished_jobs(repo=repo)
    second = await apify_receiver.reconcile_unpublished_jobs(repo=repo)
    third = await apify_receiver.reconcile_unpublished_jobs(repo=repo)
    assert [r["reconciled"] for r in (first, second, third)] == [100, 13, 0]
    assert first["published"] == 100 and second["published"] == 13
    assert set(first["job_ids"]).isdisjoint(second["job_ids"])
    assert send.await_count == 113


@pytest.mark.asyncio
async def test_unconfirmed_query_logs_safe_lookup_identity(caplog):
    client = MagicMock()
    client.query.side_effect = TimeoutError("SECRET_QUERY_CANARY")
    client.get_job.side_effect = TimeoutError("SECRET_QUERY_CANARY")
    repo = PublicationRepository(client, settings=settings())
    with pytest.raises(QueryOutcomeUnconfirmed) as error:
        await repo.execute("SELECT @secret", [], job_id="safe-query-id")
    assert error.value.job_id == "safe-query-id"
    events = [r.event_fields for r in caplog.records if hasattr(r, "event_fields")]
    assert events[-1]["query_job_id"] == "safe-query-id"
    assert "SECRET_QUERY_CANARY" not in caplog.text


@pytest.mark.asyncio
async def test_worker_artifact_lookup_parameterizes_untrusted_identity():
    from repositories.embeddings_repo import EmbeddingRepository

    client = MagicMock()
    client.query_and_wait.return_value = []
    with patch("repositories.embeddings_repo.get_settings", settings):
        repo = EmbeddingRepository(bq_client=client)
    job_id = "source-id' OR TRUE --"
    assert await repo.get_embedding(job_id) is None
    sql = client.query_and_wait.call_args.args[0]
    assert job_id not in sql and "job_id = @job_id" in sql
    assert client.query_and_wait.call_args.kwargs["job_config"].query_parameters[0].value == job_id


def test_manifest_requires_explicit_boolean_and_timezone():
    topic = "projects/p/topics/t"
    jobs = [{"job_id": "job", "status": "NEW_RAW"}]
    proof = {
        "job_id": "job",
        "topic": topic,
        "publication_confirmed": True,
        "evidence_ref": "reviewed",
    }
    for invalid in (
        {"publication_confirmed": 1},
        {"published_at": "2026-11-16T12:00:00"},
        {"message_id": ""},
    ):
        with pytest.raises(ValueError):
            build_manifest(jobs, [{**proof, **invalid}], topic=topic, expected_count=1)


@pytest.mark.asyncio
async def test_checkpoints_are_bounded_and_isolate_one_lost_lease():
    ledger = MemoryLedger()
    for i in range(23):
        ledger.add(f"job-{i:02}")

    async def publish(job_id):
        if job_id == "job-09":
            ledger.rows["job-00"]["token"] = "new-owner"
        return f"ack-{job_id}"

    result = await dispatch_pending(ledger, project_id="p", topic="t", publish_one=publish)
    assert list(map(len, ledger.checkpoints)) == [10, 10, 3]
    assert len(result.confirmed) == 22 and result.unconfirmed_ids == ["job-00"]
    assert ledger.rows["job-00"]["state"] == "IN_FLIGHT"
    assert ledger.rows["job-01"]["state"] == "PUBLISHED"


@pytest.mark.asyncio
async def test_checkpoint_is_idempotent_for_the_same_confirmation():
    ledger = MemoryLedger()
    ledger.add("job")
    claims = await ledger.claim_pending()
    first = await ledger.record_outcomes(claims, {"job": "ack"}, [])
    second = await ledger.record_outcomes(claims, {"job": "ack"}, [])
    other = await ledger.record_outcomes(claims, {"job": "different-ack"}, [])
    assert first == second == CheckpointResult(checkpointed_ids=["job"])
    assert other.lease_lost_ids == ["job"]
    assert ledger.rows["job"]["message"] == "ack"


@pytest.mark.asyncio
@pytest.mark.parametrize("days", [89, 91, 366])
async def test_recrawls_preserve_original_content_and_identity(days):
    ledger = MemoryLedger()
    raw = MemoryRaw(ledger)
    original = _make_dummy_job("job")
    await raw.persist_jobs([original])
    ledger.rows["job"]["state"] = "PUBLISHED"
    recrawl = original.model_copy(update={
        "title": "Changed title", "scraped_at": original.scraped_at + timedelta(days=days),
    })
    result = await raw.persist_jobs([recrawl])
    assert result.existing_ids == ["job"] and not result.inserted_ids
    assert raw.jobs["job"] == original and ledger.rows["job"]["state"] == "PUBLISHED"
    send = AsyncMock()
    await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_identity_without_raw_row_is_not_restored():
    ledger = MemoryLedger()
    ledger.add("job", "PUBLISHED")
    raw = MemoryRaw(ledger)
    with pytest.raises(ValueError, match="Retained identity"):
        await raw.persist_jobs([_make_dummy_job("job")])
    assert not raw.jobs and ledger.rows["job"]["state"] == "PUBLISHED"


@pytest.mark.asyncio
async def test_identity_queries_have_no_freshness_cutoff_and_reads_reject_duplicates():
    client = MagicMock()
    client.query.return_value.result.return_value = [{"job_id": "job", "inserted": False}]
    repo = RawJobRepository(bq_client=client, settings=settings())
    await repo.persist_jobs([_make_dummy_job("job")])
    sql = client.query.call_args.args[0]
    assert "@window" not in sql and "scraped_at>=" not in sql
    assert "Retained identity has no raw job" in sql
    assert "HAVING COUNT(*)>1" in sql
    assert "p.job_id=i.job_id" in sql
    assert "UPDATE `test-project.test_dataset.raw_job_postings`" not in sql
    client.query_and_wait.return_value = [MagicMock(), MagicMock()]
    with pytest.raises(ValueError, match="Ambiguous raw job"):
        await repo.get_job("job")
    assert "LIMIT 2" in client.query_and_wait.call_args.args[0]


@pytest.mark.asyncio
async def test_disabled_flag_keeps_streaming_without_ledger_or_identity_window():
    configured = settings().model_copy(update={"publication_ledger_enabled": False})
    client = MagicMock()
    client.query_and_wait.return_value = []
    client.insert_rows_json.return_value = []
    repo = RawJobRepository(bq_client=client, settings=configured)
    assert repo.publications is None
    assert await repo.batch_insert_jobs_once([_make_dummy_job("job")]) == ["job"]
    assert "scraped_at" not in client.query_and_wait.call_args.args[0]
    assert client.insert_rows_json.call_args.kwargs == {"retry": None}
    client.query.assert_not_called()
    client.query_and_wait.return_value = [MagicMock(cnt=1)]
    assert await repo.exists("job")
    assert "scraped_at" not in client.query_and_wait.call_args.args[0]
    client.query_and_wait.return_value = []
    await repo.get_unembedded_job_ids()
    assert "scraped_at >=" in client.query_and_wait.call_args.args[0]


def _failed_query(reason="invalidQuery", message="Transaction is aborted due to concurrent update"):
    job = MagicMock(state="DONE", error_result={"reason": reason, "message": message})
    job.result.side_effect = RuntimeError("PRIVATE")
    return job


@pytest.mark.asyncio
async def test_confirmed_abort_retries_new_job_only_after_original_is_resolved():
    client = MagicMock()
    successful = MagicMock()
    successful.result.return_value = [{"ok": True}]
    client.query.side_effect = [RuntimeError("lost"), successful]
    client.get_job.return_value = _failed_query()
    repo = PublicationRepository(client, settings=settings())
    with patch("repositories.publications_repo.asyncio.sleep", new_callable=AsyncMock):
        assert await repo.execute("BEGIN TRANSACTION; COMMIT TRANSACTION;", []) == [{"ok": True}]
    first, second = client.query.call_args_list
    assert first.kwargs["job_id"] != second.kwargs["job_id"]
    assert client.get_job.call_args.args[0] == first.kwargs["job_id"]
    assert all(call.kwargs["job_retry"] is None for call in client.query.call_args_list)


@pytest.mark.asyncio
async def test_abort_retry_budget_and_permanent_or_postcommit_failures():
    client = MagicMock()
    client.query.side_effect = RuntimeError("PRIVATE")
    client.get_job.return_value = _failed_query()
    repo = PublicationRepository(client, settings=settings())
    with patch("repositories.publications_repo.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(QueryTransactionAborted):
            await repo.execute("BEGIN TRANSACTION; COMMIT TRANSACTION;", [])
    assert client.query.call_count == 3
    client.reset_mock()
    client.get_job.return_value = _failed_query(message="Syntax error")
    with pytest.raises(QueryFailed):
        await repo.execute("SELECT broken", [])
    assert client.query.call_count == 1
    client.reset_mock()
    with pytest.raises(QueryOutcomeUnconfirmed):
        await repo.execute("BEGIN TRANSACTION; COMMIT TRANSACTION; SELECT broken", [])
    assert client.query.call_count == 1


@pytest.mark.asyncio
async def test_checkpoint_sql_commits_valid_outcomes_without_batch_lease_assertion():
    client = MagicMock()
    client.query.return_value.result.return_value = [
        {"job_id": "good", "checkpointed": True}, {"job_id": "stale", "checkpointed": False},
    ]
    repo = PublicationRepository(client, settings=settings())
    claims = [PublicationClaim(
        publication_id=i, job_id=i, lease_token="old",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=1800),
    ) for i in ("good", "stale")]
    result = await repo.record_outcomes(claims, {"good": "ack1", "stale": "ack2"}, [])
    assert result == CheckpointResult(checkpointed_ids=["good"], lease_lost_ids=["stale"])
    sql = client.query.call_args.args[0]
    assert "ASSERT @@row_count" not in sql
    assert "p.lease_token=o.token" in sql and "p.lease_expires_at>CURRENT_TIMESTAMP()" in sql
    assert "p.message_id=o.message_id" in sql and "SELECT * FROM results" in sql


@pytest.mark.asyncio
async def test_checkpoint_abort_retries_without_another_broker_send():
    ledger = MemoryLedger()
    ledger.add("job")
    client = MagicMock()
    success = MagicMock()
    success.result.return_value = [{"job_id": "job", "checkpointed": True}]
    client.query.side_effect = [RuntimeError("PRIVATE"), success]
    client.get_job.return_value = _failed_query()
    repo = PublicationRepository(client, settings=settings())
    original = ledger.record_outcomes

    async def checkpoint(claims, confirmed, unconfirmed_ids, **kwargs):
        await repo.record_outcomes(claims, confirmed, unconfirmed_ids, **kwargs)
        return await original(claims, confirmed, unconfirmed_ids, **kwargs)

    ledger.record_outcomes = checkpoint
    send = AsyncMock(return_value="ack")
    with patch("repositories.publications_repo.asyncio.sleep", new_callable=AsyncMock):
        result = await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    assert result.confirmed == {"job": "ack"}
    assert send.await_count == 1 and client.query.call_count == 2


@pytest.mark.asyncio
async def test_pause_during_ownership_check_prevents_any_send(monkeypatch):
    from services import publication_dispatcher as dispatcher
    ledger = MemoryLedger()
    ledger.add("job")
    now = [0.0]
    original = ledger.check_ownership

    async def paused(*args, **kwargs):
        owners = await original(*args, **kwargs)
        now[0] = 1801.0
        return owners

    ledger.check_ownership = paused
    monkeypatch.setattr(dispatcher, "monotonic", lambda: now[0])
    send = AsyncMock()
    result = await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    send.assert_not_awaited()
    assert not result.attempted_ids and not ledger.checkpoints


@pytest.mark.asyncio
async def test_expired_or_reassigned_claims_are_not_sent():
    ledger = MemoryLedger()
    ledger.add("job")
    original = ledger.check_ownership

    async def reassigned(claims, **kwargs):
        ledger.rows["job"]["token"] = "someone-else"
        return await original(claims, **kwargs)

    ledger.check_ownership = reassigned
    send = AsyncMock()
    await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_crash_after_first_checkpoint_preserves_earlier_confirmations():
    ledger = MemoryLedger()
    for i in range(20):
        ledger.add(f"job-{i:02}")
    original = ledger.record_outcomes

    async def crash(claims, confirmed, unconfirmed_ids, **kwargs):
        if len(ledger.checkpoints) == 1:
            raise QueryOutcomeUnconfirmed("lost-second-checkpoint")
        return await original(claims, confirmed, unconfirmed_ids, **kwargs)

    ledger.record_outcomes = crash
    send = AsyncMock(return_value="ack")
    with pytest.raises(QueryOutcomeUnconfirmed):
        await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
    assert sum(r["state"] == "PUBLISHED" for r in ledger.rows.values()) == 10
    assert sum(r["state"] == "IN_FLIGHT" for r in ledger.rows.values()) == 10


@pytest.mark.asyncio
async def test_five_expired_claims_are_held_not_published_forever():
    ledger = MemoryLedger()
    ledger.add("job")
    send = AsyncMock(side_effect=TimeoutError())
    for _ in range(6):
        await dispatch_pending(ledger, project_id="p", topic="t", publish_one=send)
        ledger.now += 1801
    assert ledger.rows["job"]["state"] == "UNKNOWN"
    assert send.await_count == 5


@pytest.mark.asyncio
async def test_sdk_thread_rechecks_deadline_before_publish(monkeypatch):
    from services import publication_dispatcher as dispatcher
    ledger = MemoryLedger()
    ledger.add("job")
    publisher = MagicMock()
    now = [0.0]
    monkeypatch.setattr(dispatcher, "monotonic", lambda: now[0])
    monkeypatch.setattr(dispatcher.pubsub_v1, "PublisherClient", lambda: publisher)

    async def queued(call, *args):
        if call.__name__ == "send":
            now[0] = 2000.0  # Executor delay AFTER the event-loop lease check.
        return call(*args)

    monkeypatch.setattr(dispatcher.asyncio, "to_thread", queued)
    result = await dispatch_pending(ledger, project_id="p", topic="t")
    publisher.publish.assert_not_called()
    assert not result.attempted_ids and not result.unconfirmed_ids
    assert result.claimed_count == 1 and not ledger.checkpoints


@pytest.mark.asyncio
async def test_claim_count_keeps_later_pages_reachable_when_sends_are_skipped(monkeypatch):
    from services import publication_dispatcher as dispatcher
    ledger = MemoryLedger()
    for i in range(113):
        ledger.add(f"job-{i:03}")
    # Lose the first page after claiming it; the next page must still be reached.
    original = ledger.check_ownership

    async def ownership(claims, **kwargs):
        if claims[0].job_id < "job-100":
            return []
        return await original(claims, **kwargs)

    ledger.check_ownership = ownership
    real_dispatch = dispatcher.dispatch_pending
    send = AsyncMock(return_value="ack")

    async def dispatch(repository, **kwargs):
        return await real_dispatch(repository, publish_one=send, **kwargs)

    monkeypatch.setattr(dispatcher, "dispatch_pending", dispatch)
    monkeypatch.setattr(apify_receiver, "get_settings", settings)
    result = await apify_receiver.reconcile_unpublished_jobs(
        repo=MagicMock(publications=ledger), limit=200,
    )
    assert result["published"] == 13 and send.await_count == 13
    assert ledger.rows["job-000"]["state"] == "IN_FLIGHT"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempted_on_first_page,expire_before_claim,limit,expected_limits",
    [
        (100, False, 200, [100, 100]),
        (40, False, 200, [100, 100]),
        (0, False, 200, [100, 100]),
        (0, True, 200, [100, 100]),
        (40, True, 113, [100, 13]),
    ],
)
async def test_reconciliation_pages_by_claims_not_send_attempts(
    monkeypatch, attempted_on_first_page, expire_before_claim, limit, expected_limits,
):
    from services import publication_dispatcher as dispatcher

    ledger = MemoryLedger()
    for index in range(113):
        job_id = f"job-{index:03}"
        ledger.add(job_id)
        if expire_before_claim:
            ledger.rows[job_id].update(state="IN_FLIGHT", until=-1, token="expired", attempts=1)
    original_ownership = ledger.check_ownership
    original_claim = ledger.claim_pending
    pages, limits = [], []

    async def claim(job_ids=None, limit=100, min_age_seconds=0, **kwargs):
        limits.append(limit)
        result = await original_claim(job_ids, limit, min_age_seconds, **kwargs)
        pages.append([item.job_id for item in result])
        return result

    async def ownership(claims, **kwargs):
        owners = await original_ownership(claims, **kwargs)
        return [owner for owner in owners if
                int(owner.job_id.removeprefix("job-")) >= 100 or
                int(owner.job_id.removeprefix("job-")) < attempted_on_first_page]

    ledger.claim_pending = claim
    ledger.check_ownership = ownership
    real_dispatch = dispatcher.dispatch_pending
    send = AsyncMock(side_effect=lambda job_id: f"ack-{job_id}")

    async def dispatch(repository, **kwargs):
        return await real_dispatch(repository, publish_one=send, **kwargs)

    monkeypatch.setattr(dispatcher, "dispatch_pending", dispatch)
    monkeypatch.setattr(apify_receiver, "get_settings", settings)
    result = await apify_receiver.reconcile_unpublished_jobs(
        repo=MagicMock(publications=ledger), limit=limit,
    )
    expected_attempts = attempted_on_first_page + 13
    assert limits == expected_limits
    assert [len(page) for page in pages] == [100, 13]
    assert len(set(pages[0] + pages[1])) == 113
    assert result["reconciled"] == result["published"] == send.await_count == expected_attempts
    assert result["publish_failures"] == 0
    assert len(result["job_ids"]) == len(set(result["job_ids"])) == expected_attempts
    expected_claims = 2 if expire_before_claim else 1
    assert all(row["attempts"] == expected_claims for row in ledger.rows.values())
    assert all(ledger.rows[job_id]["state"] == "PUBLISHED" for job_id in pages[1])


@pytest.mark.asyncio
async def test_zero_send_pages_exhaust_only_bounded_claim_budget_without_reselection(monkeypatch):
    from services import publication_dispatcher as dispatcher

    ledger = MemoryLedger()
    for index in range(1200):
        ledger.add(f"job-{index:04}")
    original_claim = ledger.claim_pending
    selected = []

    async def claim(*args, **kwargs):
        result = await original_claim(*args, **kwargs)
        selected.append([item.job_id for item in result])
        return result

    ledger.claim_pending = claim
    ledger.check_ownership = AsyncMock(return_value=[])
    real_dispatch = dispatcher.dispatch_pending
    send = AsyncMock()

    async def dispatch(repository, **kwargs):
        return await real_dispatch(repository, publish_one=send, **kwargs)

    monkeypatch.setattr(dispatcher, "dispatch_pending", dispatch)
    monkeypatch.setattr(apify_receiver, "get_settings", settings)
    repo = MagicMock(publications=ledger)
    first = await apify_receiver.reconcile_unpublished_jobs(repo=repo, limit=1000)
    assert first == {"reconciled": 0, "published": 0, "publish_failures": 0, "job_ids": []}
    assert len(selected) == 10 and all(len(page) == 100 for page in selected)
    assert sum(row["state"] == "PENDING" for row in ledger.rows.values()) == 200
    await apify_receiver.reconcile_unpublished_jobs(repo=repo, limit=200)
    assert len(selected) == 12
    ids = [job_id for page in selected for job_id in page]
    assert len(ids) == len(set(ids)) == 1200
    await apify_receiver.reconcile_unpublished_jobs(repo=repo, limit=1000)
    assert len(selected) == 13 and selected[-1] == []
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_publications_do_not_reselect_deferred_ids_on_remaining_pages(monkeypatch):
    from services import publication_dispatcher as dispatcher

    ledger = MemoryLedger()
    for index in range(113):
        ledger.add(f"job-{index:03}")
    send = AsyncMock(side_effect=TimeoutError("unconfirmed"))
    real_dispatch = dispatcher.dispatch_pending
    page_sizes = []

    async def dispatch(repository, **kwargs):
        result = await real_dispatch(repository, publish_one=send, **kwargs)
        page_sizes.append(result.claimed_count)
        return result

    monkeypatch.setattr(dispatcher, "dispatch_pending", dispatch)
    monkeypatch.setattr(apify_receiver, "get_settings", settings)
    result = await apify_receiver.reconcile_unpublished_jobs(
        repo=MagicMock(publications=ledger), limit=1000,
    )
    assert page_sizes == [100, 13]
    assert result["reconciled"] == result["publish_failures"] == send.await_count == 113
    assert result["published"] == 0 and len(set(result["job_ids"])) == 113
    assert all(row["state"] == "PENDING" and row["until"] > ledger.now
               for row in ledger.rows.values())


@pytest.mark.asyncio
async def test_raw_sql_expressions_match_every_checked_in_field():
    import re

    from schemas.job_models import RawJob
    client = MagicMock()
    client.query.return_value.result.return_value = [{"job_id": "job", "inserted": True}]
    repo = RawJobRepository(bq_client=client, settings=settings())
    await repo.persist_jobs([_make_dummy_job("job")])
    sql = client.query.call_args.args[0]
    ddl = Path("sql/raw_job_postings.sql").read_text(encoding="utf-8")
    fields = dict(re.findall(r"^\s+(\w+)\s+(STRING|TIMESTAMP)\b", ddl, re.MULTILINE))
    assert set(fields) == set(RawJob.model_fields) and len(fields) == 13
    for name, kind in fields.items():
        value = f"JSON_VALUE(item,'$.{name}')"
        if kind == "TIMESTAMP":
            value = f"TIMESTAMP({value})"
        assert f"{value} AS {name}" in sql


@pytest.mark.asyncio
async def test_preflight_adapter_checks_token_expiry_and_unique_raw_record():
    client = MagicMock()
    repo = PublicationRepository(client, settings=settings())
    claim = PublicationClaim(
        publication_id="pub", job_id="job", lease_token="token",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=1800),
    )
    client.query.return_value.result.return_value = [{
        **claim.model_dump(), "server_now": datetime.now(timezone.utc),
    }]
    assert (await repo.check_ownership([claim]))[0].job_id == "job"
    sql = client.query.call_args.args[0]
    assert "p.lease_token=r.token" in sql and "p.lease_expires_at>CURRENT_TIMESTAMP()" in sql
    assert "j.job_id=p.job_id)=1" in sql
    assert "CURRENT_TIMESTAMP() AS server_now" in sql


@pytest.mark.asyncio
async def test_raw_transaction_abort_retries_without_streaming_or_blind_append():
    client = MagicMock()
    success = MagicMock()
    success.result.return_value = [{"job_id": "job", "inserted": False}]
    client.query.side_effect = [RuntimeError("PRIVATE"), success]
    client.get_job.return_value = _failed_query()
    repo = RawJobRepository(client, settings=settings())
    with patch("repositories.publications_repo.asyncio.sleep", new_callable=AsyncMock):
        result = await repo.persist_jobs([_make_dummy_job("job")])
    assert result.existing_ids == ["job"] and not result.inserted_ids
    assert client.query.call_count == 2
    client.insert_rows_json.assert_not_called()


@pytest.mark.asyncio
async def test_repeated_http_webhook_uses_full_fixture_without_republication(monkeypatch):
    from services import ingestion_publications as ingestion
    path = Path("Input/dataset_company-career-page-scraper_2026-09-17_06-15-27-909.json")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    raw = MemoryRaw(MemoryLedger())
    configured = settings()
    configured.apify_api_token = "offline-token"
    monkeypatch.setattr(apify_receiver, "get_settings", lambda: configured)
    monkeypatch.setattr(apify_receiver, "_verify_actor_run", AsyncMock(return_value={
        "id": "run", "actId": "actor", "status": "SUCCEEDED", "defaultDatasetId": "dataset",
    }))
    monkeypatch.setattr(apify_receiver, "_fetch_dataset_items", AsyncMock(return_value=data))
    send = AsyncMock(return_value="ack")

    async def dispatcher(repository, **kwargs):
        return await dispatch_pending(repository, publish_one=send, **kwargs)

    real_ingest = ingestion.ingest_with_publications

    async def ingest(batch, **kwargs):
        return await real_ingest(batch, repository=raw, dispatcher=dispatcher, **kwargs)

    monkeypatch.setattr(ingestion, "ingest_with_publications", ingest)
    payload = {"eventType": "ACTOR.RUN.SUCCEEDED", "resource": {
        "id": "run", "actId": "actor", "status": "SUCCEEDED", "defaultDatasetId": "dataset",
    }}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=apify_receiver.app), base_url="http://test",
    ) as client:
        first = await client.post("/webhook/apify", json=payload)
        second = await client.post("/webhook/apify", json=payload)
    assert first.status_code == second.status_code == 202
    a, b = first.json(), second.json()
    assert (a["received"], a["normalized"], a["unique"], a["in_batch_duplicates"]) == (215, 215, 113, 102)
    assert a["inserted"] == a["published"] == 113
    assert b["inserted"] == b["published"] == b["recovered"] == 0
    assert len(raw.jobs) == send.await_count == 113 and set(a) == set(b)


@pytest.mark.asyncio
async def test_confirmed_identity_conflict_logs_only_a_safe_category(caplog):
    client = MagicMock()
    client.query.side_effect = RuntimeError("PRIVATE_PAYLOAD_CANARY")
    client.get_job.return_value = _failed_query(
        message="Query error: Retained identity has no raw job at [12:3]",
    )
    repo = RawJobRepository(client, settings=settings())
    with pytest.raises(QueryFailed) as error:
        await repo.persist_jobs([_make_dummy_job("job")])
    assert error.value.category == "raw_identity_conflict"
    events = [r.event_fields for r in caplog.records if hasattr(r, "event_fields")]
    assert events[-1]["event"] == "ingestion.bigquery.failed"
    assert events[-1]["error_category"] == "raw_identity_conflict"
    assert "PRIVATE_PAYLOAD_CANARY" not in caplog.text
    assert client.query.call_count == 1
    client.insert_rows_json.assert_not_called()


def test_publication_ledger_remains_disabled_by_default():
    assert Settings.model_fields["publication_ledger_enabled"].default is False
