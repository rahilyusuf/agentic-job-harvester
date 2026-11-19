"""Opt-in, isolated GCP integration checks. Import and `plan` are network-free.

Never invokes Apify, a worker, Secret Manager, or subscription consumption. Resource
provisioning, checking, execution and cleanup each require an exact confirmation.
IAM setup/review and production migration/activation remain operator procedures.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import Literal
from unittest.mock import patch

from pydantic import BaseModel, ConfigDict, Field, model_validator

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "agentic-job-harvester-v2"
LOCATION = "US"
LABEL_PURPOSE = "publication_ledger_it"
MAX_QUERIES = 100
MAX_PUBLICATIONS = 16
MAX_SECONDS = 900
TTL_MS = 7 * 86400 * 1000
SOURCE_FILES = (
    "scripts/verify_publication_ledger.py",
    ".dockerignore",
    "infra/apify_receiver.Dockerfile",
    "requirements.txt",
    "config/settings.py",
    "repositories/raw_jobs_repo.py",
    "repositories/publications_repo.py",
    "repositories/interfaces.py",
    "schemas/job_models.py",
    "schemas/publication_models.py",
    "services/publication_dispatcher.py",
    "services/ingestion_publications.py",
    "services/normalizers/batch.py",
    "services/normalizers/apify_career_scraper.py",
    "orchestration/input_sanitizer.py",
    "sql/raw_job_postings.sql",
    "sql/job_publications.sql",
)


class SafetyError(RuntimeError):
    """A failed gate; only fixed, non-sensitive messages reach the console."""


class Resources(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    project: Literal["agentic-job-harvester-v2"] = PROJECT
    location: Literal["US"] = LOCATION
    dataset: str
    topic: str
    subscription: str
    service_account: str


def resources_for(run_id: str) -> Resources:
    return Resources(
        dataset=f"ledger_it_{run_id}",
        topic=f"projects/{PROJECT}/topics/ledger-it-{run_id}",
        subscription=f"projects/{PROJECT}/subscriptions/ledger-it-{run_id}-sub",
        service_account=f"ledger-it-{run_id}@{PROJECT}.iam.gserviceaccount.com",
    )


def source_fingerprint() -> str:
    digest = hashlib.sha256()
    for name in SOURCE_FILES:
        digest.update(name.encode())
        digest.update(b"\0")
        # Stable across Windows checkout line endings; still detects code changes.
        digest.update((ROOT / name).read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\0")
    return digest.hexdigest()


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: str = Field(pattern=r"^\d{14}$")
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    resources: Resources
    iam_review_reference: str = ""

    @model_validator(mode="after")
    def exact_resources(self):
        datetime.strptime(self.run_id, "%Y%m%d%H%M%S")
        if self.resources != resources_for(self.run_id):
            raise ValueError("Resources must exactly match the isolated run namespace")
        return self

    @property
    def labels(self) -> dict[str, str]:
        return {"purpose": LABEL_PURPOSE, "run_id": self.run_id}

    @property
    def dataset_id(self) -> str:
        return f"{PROJECT}.{self.resources.dataset}"

    @property
    def tables(self) -> tuple[str, str]:
        return tuple(
            f"{self.dataset_id}.{name}"
            for name in (
                "raw_job_postings",
                "job_publications",
            )
        )

    @property
    def job_ids(self) -> tuple[str, str]:
        return (f"ledger-it-{self.run_id}-a", f"ledger-it-{self.run_id}-b")


def validate_execution(manifest: Manifest, phase: str, execute: bool, confirm: str) -> None:
    if not execute or confirm != f"{phase.upper()} {manifest.run_id}":
        raise SafetyError("Explicit execution and exact phase/run confirmation required")
    if manifest.source_sha256 != source_fingerprint():
        raise SafetyError("Source changed; regenerate and review the manifest")
    if phase in {"check", "run"} and not manifest.iam_review_reference.strip():
        raise SafetyError("Scoped IAM review reference required")
    if phase == "run" and sys.version_info[:2] != (3, 11):
        raise SafetyError("Run integration scenarios with the production Python 3.11 runtime")


class Journal:
    def __init__(self, path: Path):
        self.file = path.open("x", encoding="utf-8")
        self.lock = threading.Lock()

    def record(self, event: str, **fields) -> None:
        with self.lock:
            self.file.write(
                json.dumps(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event": event,
                        **fields,
                    },
                    default=str,
                )
                + "\n"
            )
            self.file.flush()

    def close(self) -> None:
        self.file.close()


class Budget:
    def __init__(self):
        self.deadline = time.monotonic() + MAX_SECONDS
        self.queries = self.publications = 0
        self.lock = threading.Lock()

    def check(self) -> None:
        if time.monotonic() >= self.deadline:
            raise SafetyError("Integration time budget exhausted")

    def spend(self, kind: str) -> None:
        with self.lock:
            self.check()
            limit = MAX_QUERIES if kind == "queries" else MAX_PUBLICATIONS
            if getattr(self, kind) >= limit:
                raise SafetyError("Integration operation budget exhausted")
            setattr(self, kind, getattr(self, kind) + 1)


def schema_contract(name: str) -> list[tuple[str, str, str]]:
    sql = (ROOT / "sql" / f"{name}.sql").read_text(encoding="utf-8-sig")
    fields = re.findall(r"^    (\w+)\s+(STRING|TIMESTAMP|INT64)(\s+NOT NULL)?\s*,?", sql, re.M)
    if len(fields) != {"raw_job_postings": 13, "job_publications": 18}[name]:
        raise SafetyError("Checked-in schema requires review")
    return [
        (n, "INTEGER" if t == "INT64" else t, "REQUIRED" if req else "NULLABLE")
        for n, t, req in fields
    ]


@dataclass
class Clients:
    bq: object
    publisher: object
    subscriber: object
    publisher_factory: object

    def close(self):
        try:
            self.bq.close()
        finally:
            try:
                self.publisher.transport.close()
            finally:
                self.subscriber.close()


def create_clients(manifest: Manifest, *, operator: bool) -> Clients:
    # Deliberately lazy: neither import nor offline plan performs ADC discovery.
    import google.auth
    from google.auth import impersonated_credentials
    from google.cloud import bigquery, pubsub_v1

    source, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials = (
        source
        if operator
        else impersonated_credentials.Credentials(
            source_credentials=source,
            target_principal=manifest.resources.service_account,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=900,
        )
    )
    publisher_type = pubsub_v1.PublisherClient
    return Clients(
        bq=bigquery.Client(project=PROJECT, location=LOCATION, credentials=credentials),
        publisher=publisher_type(credentials=credentials),
        subscriber=pubsub_v1.SubscriberClient(credentials=credentials),
        publisher_factory=lambda: publisher_type(credentials=credentials),
    )


def owned(resource, manifest: Manifest) -> None:
    if any(dict(resource.labels).get(k) != v for k, v in manifest.labels.items()):
        raise SafetyError("Resource ownership labels do not match the reviewed manifest")


def metadata(manifest: Manifest, clients: Clients, *, allow_missing=False) -> dict:
    from google.api_core.exceptions import NotFound

    r = manifest.resources
    operations = {
        "dataset": lambda: clients.bq.get_dataset(manifest.dataset_id, retry=None, timeout=10),
        "topic": lambda: clients.publisher.get_topic(
            request={"topic": r.topic}, retry=None, timeout=10
        ),
        "subscription": lambda: clients.subscriber.get_subscription(
            request={"subscription": r.subscription},
            retry=None,
            timeout=10,
        ),
    }
    result = {}
    for name, get in operations.items():
        try:
            value = get()
        except NotFound:
            if not allow_missing:
                raise SafetyError("An isolated resource is missing") from None
        else:
            owned(value, manifest)
            result[name] = value
    return result


def preflight(manifest: Manifest, clients: Clients) -> None:
    resources = metadata(manifest, clients)
    r = manifest.resources
    dataset, topic, sub = (resources[k] for k in ("dataset", "topic", "subscription"))
    if (
        dataset.full_dataset_id.replace(":", ".") != manifest.dataset_id
        or dataset.location != LOCATION
        or dataset.default_table_expiration_ms != TTL_MS
        or topic.name != r.topic
        or sub.name != r.subscription
        or sub.topic != r.topic
        or sub.ack_deadline_seconds != 60
        or sub.message_retention_duration.total_seconds() != 86400
        or sub.state.name != "ACTIVE"
        or sub.push_config.push_endpoint
        or sub.bigquery_config.table
        or sub.cloud_storage_config.bucket
        or sub.detached
        or sub.filter
    ):
        raise SafetyError("Unsafe isolated resource configuration")
    attached = list(
        islice(
            clients.publisher.list_topic_subscriptions(
                request={"topic": r.topic},
                retry=None,
                timeout=10,
            ),
            2,
        )
    )
    if attached != [r.subscription]:
        raise SafetyError("Unexpected subscriptions attached to the isolated topic")
    for table_id in manifest.tables:
        table = clients.bq.get_table(table_id, retry=None, timeout=10)
        owned(table, manifest)
        actual = [
            (f.name, {"INT64": "INTEGER"}.get(f.field_type, f.field_type), f.mode)
            for f in table.schema
        ]
        name = table_id.rsplit(".", 1)[1]
        cluster = (
            ["status", "company"] if name == "raw_job_postings" else ["state", "topic", "job_id"]
        )
        if (
            actual != schema_contract(name)
            or table.table_type != "TABLE"
            or table.full_table_id.replace(":", ".") != table_id
            or list(table.clustering_fields or []) != cluster
            or table.streaming_buffer
            or table.expires is None
            or table.created is None
            or not timedelta(0) < table.expires - table.created <= timedelta(days=7)
            or table.location != LOCATION
            or table.require_partition_filter
        ):
            raise SafetyError("Isolated table metadata differs from the reviewed contract")
        if name == "raw_job_postings":
            partition = table.time_partitioning
            if (
                not partition
                or partition.field != "scraped_at"
                or partition.type_ != "DAY"
                or partition.expiration_ms != 365 * 86400 * 1000
            ):
                raise SafetyError("Raw table partition contract differs")
        elif table.time_partitioning or table.range_partitioning:
            raise SafetyError("Unexpected ledger partitioning")


def provision(manifest: Manifest, clients: Clients, journal: Journal) -> None:
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, pubsub_v1

    # Verify ALL names are absent before creating anything. No adoption or upserts.
    probes = (
        lambda: clients.bq.get_dataset(manifest.dataset_id, retry=None, timeout=10),
        lambda: clients.publisher.get_topic(
            request={"topic": manifest.resources.topic}, retry=None, timeout=10
        ),
        lambda: clients.subscriber.get_subscription(
            request={"subscription": manifest.resources.subscription}, retry=None, timeout=10
        ),
    )
    for get in probes:
        try:
            get()
        except NotFound:
            continue
        raise SafetyError("A proposed resource already exists; use a fresh run namespace")
    dataset = bigquery.Dataset(manifest.dataset_id)
    dataset.location, dataset.labels = LOCATION, manifest.labels
    dataset.default_table_expiration_ms = TTL_MS
    clients.bq.create_dataset(dataset, exists_ok=False, retry=None, timeout=10)
    journal.record("created", resource=manifest.dataset_id)
    for table_id in manifest.tables:
        name = table_id.rsplit(".", 1)[1]
        table = bigquery.Table(
            table_id,
            schema=[bigquery.SchemaField(n, t, mode=m) for n, t, m in schema_contract(name)],
        )
        table.labels = manifest.labels
        table.expires = datetime.now(timezone.utc) + timedelta(days=7)
        if name == "raw_job_postings":
            table.time_partitioning = bigquery.TimePartitioning(
                type_="DAY",
                field="scraped_at",
                expiration_ms=365 * 86400 * 1000,
            )
            table.clustering_fields = ["status", "company"]
        else:
            table.clustering_fields = ["state", "topic", "job_id"]
        clients.bq.create_table(table, exists_ok=False, retry=None, timeout=10)
        journal.record("created", resource=table_id)
    clients.publisher.create_topic(
        request={
            "name": manifest.resources.topic,
            "labels": manifest.labels,
        },
        retry=None,
        timeout=10,
    )
    journal.record("created", resource=manifest.resources.topic)
    clients.subscriber.create_subscription(
        request={
            "name": manifest.resources.subscription,
            "topic": manifest.resources.topic,
            "labels": manifest.labels,
            "ack_deadline_seconds": 60,
            "message_retention_duration": {"seconds": 86400},
            "expiration_policy": {"ttl": {"seconds": 7 * 86400}},
            "push_config": pubsub_v1.types.PushConfig(),
        },
        retry=None,
        timeout=10,
    )
    journal.record("created", resource=manifest.resources.subscription)


def cleanup(manifest: Manifest, clients: Clients, journal: Journal) -> None:
    resources = metadata(manifest, clients, allow_missing=True)
    # Verify every existing resource before deleting ANY, including partial setups.
    if "dataset" in resources:
        for item in clients.bq.list_tables(manifest.dataset_id, retry=None, timeout=10):
            if item.full_table_id.replace(":", ".") not in manifest.tables:
                raise SafetyError(
                    "Unexpected table in test dataset; manual cleanup review required"
                )
            owned(clients.bq.get_table(item.reference, retry=None, timeout=10), manifest)
    if "topic" in resources:
        attached = list(
            islice(
                clients.publisher.list_topic_subscriptions(
                    request={"topic": manifest.resources.topic},
                    retry=None,
                    timeout=10,
                ),
                2,
            )
        )
        if set(attached) - {manifest.resources.subscription}:
            raise SafetyError("Unexpected attached subscription; cleanup stopped")
    if "subscription" in resources:
        if resources["subscription"].topic != manifest.resources.topic:
            raise SafetyError("Subscription topic differs; cleanup stopped")
        clients.subscriber.delete_subscription(
            request={
                "subscription": manifest.resources.subscription,
            },
            retry=None,
            timeout=10,
        )
        journal.record("deleted", resource=manifest.resources.subscription)
    if "topic" in resources:
        clients.publisher.delete_topic(
            request={"topic": manifest.resources.topic}, retry=None, timeout=10
        )
        journal.record("deleted", resource=manifest.resources.topic)
    if "dataset" in resources:
        clients.bq.delete_dataset(
            manifest.dataset_id, delete_contents=True, not_found_ok=False, retry=None, timeout=10
        )
        journal.record("deleted", resource=manifest.dataset_id)


class QueryResult:
    def __init__(self, job, owner, query_id, lose_reply=False):
        self.job, self.owner, self.query_id, self.lose_reply = job, owner, query_id, lose_reply

    def __getattr__(self, name):
        return getattr(self.job, name)

    def result(self, **kwargs):
        try:
            rows = list(self.job.result(**kwargs))
        except Exception:
            error = self.job.error_result or {}
            self.owner.journal.record(
                "query_failed",
                query_job_id=self.query_id,
                reason=error.get("reason"),
                phase=self.owner.phase,
            )
            raise
        self.owner.journal.record(
            "query_completed",
            query_job_id=self.query_id,
            bytes_billed=self.job.total_bytes_billed,
            phase=self.owner.phase,
        )
        if self.lose_reply:
            self.lose_reply = False
            raise TimeoutError("Injected loss after real query completion")
        return rows


class GuardedBigQuery:
    """Instrument real SDK jobs; faults are confined to this test client, never app code."""

    def __init__(self, client, manifest, journal, budget):
        self.client, self.manifest, self.journal, self.budget = client, manifest, journal, budget
        self.submitted = set()
        self.phase, self.faults = "setup", set()
        self.on_abort = None

    def query(self, sql, **kwargs):
        from google.cloud import bigquery

        self.budget.spend("queries")
        references = re.findall(r"`([^`]+)`", sql)
        if any(ref not in self.manifest.tables for ref in references):
            raise SafetyError("SQL references a resource outside the manifest")
        if (
            kwargs.get("location", LOCATION) != LOCATION
            or kwargs.get("project", PROJECT) != PROJECT
        ):
            raise SafetyError("Query project/location differs from the manifest")
        persist = "CREATE TEMP TABLE incoming" in sql
        checkpoint = "CREATE TEMP TABLE outcomes" in sql
        if persist and "rollback" in self.faults:
            if sql.count("COMMIT TRANSACTION;") != 1:
                raise SafetyError("Persistence SQL changed; injection requires review")
            sql = sql.replace(
                "COMMIT TRANSACTION;",
                "ASSERT FALSE AS 'Harness forced rollback';\nCOMMIT TRANSACTION;",
            )
            self.faults.remove("rollback")
        fault = "lost_persist" if persist else "lost_checkpoint" if checkpoint else ""
        lose_reply = fault in self.faults
        self.faults.discard(fault)
        config = kwargs.get("job_config") or bigquery.QueryJobConfig()
        config = bigquery.QueryJobConfig.from_api_repr(config.to_api_repr())
        config.labels = {**(config.labels or {}), **self.manifest.labels, "phase": self.phase}
        config.maximum_bytes_billed = 1024**3
        config.job_timeout_ms = 60000
        query_id = (
            kwargs.pop("job_id", None) or f"ledger_it_{self.manifest.run_id}_{uuid.uuid4().hex}"
        )
        self.submitted.add(query_id)
        kwargs.update(
            job_id=query_id,
            job_config=config,
            location=LOCATION,
            project=PROJECT,
            retry=None,
            job_retry=None,
            timeout=10,
        )
        self.journal.record("query_submitted", query_job_id=query_id, phase=self.phase)
        return QueryResult(self.client.query(sql, **kwargs), self, query_id, lose_reply)

    def get_job(self, job_id, **kwargs):
        if job_id not in self.submitted:
            raise SafetyError("Lookup is not for an original test query job")
        self.budget.check()
        self.journal.record("query_lookup", query_job_id=job_id, phase=self.phase)
        job = self.client.get_job(
            job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10
        )
        error = job.error_result or {}
        if (
            self.on_abort
            and job.state == "DONE"
            and "Transaction is aborted due to concurrent update" in error.get("message", "")
        ):
            callback, self.on_abort = self.on_abort, None
            callback()
        return job

    def rows(self, sql, **kwargs):
        return self.query(sql, **kwargs).result(timeout=60, retry=None, job_retry=None)


class CountingPublisher:
    def __init__(self, sdk, manifest, journal, budget, confirmations):
        self.sdk, self.manifest, self.journal, self.budget = sdk, manifest, journal, budget
        self.confirmations = confirmations

    def topic_path(self, project, topic):
        path = self.sdk.topic_path(project, topic)
        if path != self.manifest.resources.topic:
            raise SafetyError("Publisher topic differs from the manifest")
        return path

    def publish(self, topic, data, **kwargs):
        body = json.loads(data)
        if (
            topic != self.manifest.resources.topic
            or set(body) != {"job_id"}
            or body["job_id"] not in self.manifest.job_ids
        ):
            raise SafetyError("Publication is not an approved synthetic job")
        self.budget.spend("publications")
        future = self.sdk.publish(topic, data, **kwargs)
        job_id, journal, confirmations = body["job_id"], self.journal, self.confirmations

        class Confirmation:
            def result(self, timeout):
                message_id = future.result(timeout=timeout)
                confirmations[job_id] = message_id
                journal.record("broker_confirmed", job_id=job_id, message_id=message_id)
                return message_id

        return Confirmation()

    def stop(self):
        self.sdk.stop()


def synthetic_items(manifest: Manifest) -> list[dict]:
    return [
        {
            "jobId": job_id,
            "title": "Integration Test Engineer",
            "companyName": "Synthetic Fixture",
            "descriptionText": "Synthetic integration posting used only to verify ingestion and publication reliability.",
            ("jobUrl" if index == 0 else "applyUrl"): f"https://example.invalid/{job_id}",
            "location": None,
            "salary": None,
            "employmentType": None,
            "postedAt": None if index == 0 else "2026-11-17T12:00:00+03:00",
        }
        for index, job_id in enumerate(manifest.job_ids)
    ]


class Scenarios:
    def __init__(self, manifest, clients, journal):
        from config.settings import Settings
        from repositories.raw_jobs_repo import RawJobRepository

        self.manifest, self.clients, self.journal = manifest, clients, journal
        self.budget = Budget()
        self.bq = GuardedBigQuery(clients.bq, manifest, journal, self.budget)
        # Match the existing devtools pattern: validated resource manifest, then
        # explicit Settings construction without dotenv, validators or secret lookup.
        self.settings = Settings.model_construct(
            gcp_project_id=PROJECT,
            bq_dataset=manifest.resources.dataset,
            bq_location=LOCATION,
            pubsub_embedding_topic=manifest.resources.topic.rsplit("/", 1)[1],
            publication_ledger_enabled=True,
            environment="development",
            litellm_proxy_base_url="http://127.0.0.1:9",
            litellm_proxy_api_key="unused",
            apify_api_token="unused",
            telegram_bot_token="unused",
            e2b_api_key="unused",
        )
        self.raw = RawJobRepository(self.bq, settings=self.settings)
        self.confirmations, self.publishers = {}, []

    def publisher(self):
        sdk = self.clients.publisher_factory()
        self.publishers.append(sdk)
        return CountingPublisher(sdk, self.manifest, self.journal, self.budget, self.confirmations)

    def snapshot(self):
        raw, ledger = self.manifest.tables
        rows = self.bq.rows(f"SELECT * FROM `{raw}` ORDER BY job_id")
        entries = self.bq.rows(f"SELECT * FROM `{ledger}` ORDER BY job_id")
        raw_data, ledger_data = [dict(r.items()) for r in rows], [dict(r.items()) for r in entries]
        if any(row["job_id"] not in self.manifest.job_ids for row in raw_data + ledger_data):
            raise SafetyError("Unexpected job IDs; test reset prohibited")
        self.journal.record("snapshot", phase=self.bq.phase, raw=raw_data, ledger=ledger_data)
        return raw_data, ledger_data

    def reset(self, phase):
        self.snapshot()  # Archive evidence and verify IDs before test-only deletion.
        raw, ledger = self.manifest.tables
        self.bq.rows(
            f"BEGIN TRANSACTION; DELETE FROM `{ledger}` WHERE TRUE; DELETE FROM `{raw}` WHERE TRUE; COMMIT TRANSACTION;"
        )
        self.bq.phase = phase
        self.confirmations.clear()

    def batch(self):
        from services.normalizers.batch import normalize_batch

        result = normalize_batch(synthetic_items(self.manifest))
        require(
            (result.received, result.normalized, result.rejected, len(result.jobs)) == (2, 2, 0, 2)
        )
        return result

    async def ingest(self):
        from services.ingestion_publications import ingest_with_publications

        result = await ingest_with_publications(
            self.batch(),
            dataset_id="integration-no-apify",
            actor_run_id="integration-no-apify",
            settings=self.settings,
            repository=self.raw,
        )
        self.journal.record("ingestion", phase=self.bq.phase, counters=result)
        return result

    def published(self):
        rows, entries = self.snapshot()
        require(len(rows) == len(entries) == 2)
        require(all(r["status"] == "NEW_RAW" for r in rows))
        require(all(p["state"] == "PUBLISHED" and p["message_id"] for p in entries))
        require({p["job_id"]: p["message_id"] for p in entries} == self.confirmations)
        return rows, entries

    async def run(self):
        from fastapi import HTTPException

        from schemas.publication_models import QueryOutcomeUnconfirmed
        from services import publication_dispatcher

        async with asyncio.timeout(MAX_SECONDS):
            with patch.object(publication_dispatcher.pubsub_v1, "PublisherClient", self.publisher):
                self.reset("rollback")
                self.bq.faults = {"rollback"}
                before = self.budget.publications
                try:
                    await self.ingest()
                except HTTPException as exc:
                    require(exc.status_code == 502)
                else:
                    raise SafetyError("Injected rollback did not fail ingestion")
                require(self.snapshot() == ([], []))
                require(self.budget.publications == before)

                self.bq.phase = "happy"
                first = await self.ingest()
                require((first["inserted"], first["recovered"], first["published"]) == (2, 0, 2))
                baseline = self.published()
                before = self.budget.publications
                repeat = await self.ingest()
                require((repeat["inserted"], repeat["recovered"], repeat["published"]) == (0, 0, 0))
                require(self.budget.publications == before and self.snapshot() == baseline)

                self.reset("lost_response")
                self.bq.faults = {"lost_persist", "lost_checkpoint"}
                before = self.budget.publications
                require((await self.ingest())["published"] == 2)
                require(self.budget.publications - before == 2 and not self.bq.faults)
                self.published()

                self.reset("checkpoint_conflict")
                await self.checkpoint_conflict()
                self.published()

                self.reset("expired_claim")
                original = self.raw.publications.record_outcomes
                lost_id = self.manifest.job_ids[0]

                async def expire_one(claims, confirmed, unconfirmed_ids, **kwargs):
                    self.expire([lost_id])
                    return await original(claims, confirmed, unconfirmed_ids, **kwargs)

                with patch.object(self.raw.publications, "record_outcomes", expire_one):
                    result = await self.ingest()
                require(result["published"] == result["publish_failures"] == 1)
                recovery = await self.ingest()
                require(
                    (recovery["inserted"], recovery["recovered"], recovery["published"])
                    == (0, 1, 1)
                )
                self.published()

                self.reset("crash")

                async def crash(*args, **kwargs):
                    raise QueryOutcomeUnconfirmed("injected-before-checkpoint")

                with patch.object(self.raw.publications, "record_outcomes", crash):
                    try:
                        await self.ingest()
                    except HTTPException as exc:
                        require(exc.status_code == 502)
                    else:
                        raise SafetyError("Injected crash did not fail ingestion")
                require(all(p["state"] == "IN_FLIGHT" for p in self.snapshot()[1]))
                self.expire(list(self.manifest.job_ids))
                recovery = await self.ingest()
                require(recovery["recovered"] == recovery["published"] == 2)
                self.published()

                self.reset("concurrent")
                before = self.budget.publications
                await asyncio.gather(self.ingest(), self.ingest())
                require(self.budget.publications - before == 2)
                self.published()
                before = self.budget.publications
                require((await self.ingest())["published"] == 0)
                require(self.budget.publications == before)
        self.journal.record(
            "suite_passed", queries=self.budget.queries, publications=self.budget.publications
        )

    def expire(self, ids):
        from google.cloud import bigquery

        require(set(ids).issubset(self.manifest.job_ids))
        self.bq.rows(
            f"UPDATE `{self.manifest.tables[1]}` SET lease_expires_at=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 1 SECOND) WHERE job_id IN UNNEST(@ids) AND state='IN_FLIGHT'",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ids", "STRING", ids),
                ]
            ),
        )

    async def checkpoint_conflict(self):
        from google.cloud import bigquery

        original = self.raw.publications.record_outcomes
        active_session = None
        session_lock = threading.Lock()

        def session_query(sql, session=None):
            config = bigquery.QueryJobConfig(create_session=session is None)
            if session:
                config.connection_properties = [bigquery.ConnectionProperty("session_id", session)]
            job = self.bq.query(sql, job_config=config)
            job.result(timeout=10, retry=None, job_retry=None)
            if session:
                return session
            require(job.session_info is not None)
            return job.session_info.session_id

        def release():
            nonlocal active_session
            with session_lock:
                session = active_session
                active_session = None
            if session:
                try:
                    session_query("ROLLBACK TRANSACTION;", session)
                finally:
                    self.bq.rows(
                        "CALL BQ.ABORT_SESSION(@session)",
                        job_config=bigquery.QueryJobConfig(
                            query_parameters=[
                                bigquery.ScalarQueryParameter("session", "STRING", session)
                            ]
                        ),
                    )
                self.journal.record("session_closed", session_id=session)

        async def conflicting(claims, confirmed, unconfirmed_ids, **kwargs):
            nonlocal active_session
            active_session = session_query(
                f"BEGIN TRANSACTION; DELETE FROM `{self.manifest.tables[1]}` WHERE FALSE;",
            )
            self.journal.record("session_opened", session_id=active_session)
            self.bq.on_abort = release
            try:
                result = await asyncio.wait_for(
                    original(claims, confirmed, unconfirmed_ids, **kwargs), 60
                )
                require(
                    self.bq.on_abort is None
                )  # An actual GCP abort, not merely a successful race.
                return result
            finally:
                self.bq.on_abort = None
                release()

        before = self.budget.publications
        with patch.object(self.raw.publications, "record_outcomes", conflicting):
            require((await self.ingest())["published"] == 2)
        require(self.budget.publications - before == 2)

    def close(self):
        for publisher in self.publishers:
            try:
                publisher.transport.close()
            except Exception as exc:
                self.journal.record("client_close_failed", error_type=type(exc).__name__)


def require(condition):
    if not condition:
        raise SafetyError("Integration assertion failed; inspect the sanitized evidence")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["plan", "provision", "check", "run", "cleanup"])
    parser.add_argument("--run-id")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--iam-review-reference", default="")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args(argv)
    clients = journal = scenarios = None
    try:
        if args.phase == "plan":
            if args.execute:
                raise SafetyError("Planning never executes cloud operations")
            run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            manifest = Manifest(
                run_id=run_id,
                source_sha256=source_fingerprint(),
                resources=resources_for(run_id),
                iam_review_reference=args.iam_review_reference,
            )
            print(manifest.model_dump_json(indent=2))
            return 0
        if args.manifest is None or args.evidence is None:
            raise SafetyError("Reviewed manifest and a new evidence path are required")
        manifest = Manifest.model_validate_json(args.manifest.read_text(encoding="utf-8-sig"))
        validate_execution(manifest, args.phase, args.execute, args.confirm)
        journal = Journal(args.evidence)  # Refuses overwrite before discovering credentials.
        journal.record(
            "started",
            phase=args.phase,
            manifest=manifest.model_dump(),
            python=platform.python_version(),
        )
        clients = create_clients(manifest, operator=args.phase in {"provision", "cleanup"})
        if args.phase == "provision":
            provision(manifest, clients, journal)
        elif args.phase == "cleanup":
            cleanup(manifest, clients, journal)
        else:
            preflight(manifest, clients)
            journal.record("preflight_passed")
            if args.phase == "run":
                scenarios = Scenarios(manifest, clients, journal)
                asyncio.run(scenarios.run())
        journal.record("completed", phase=args.phase)
        print(f"{args.phase}: passed; evidence saved")
        return 0
    except Exception as exc:
        # SDK messages can contain URLs, SQL or tokens. Never echo raw exception text.
        if journal:
            journal.record("stopped", error_type=type(exc).__name__)
        print(
            f"Stopped ({type(exc).__name__}); inspect gates and sanitized evidence.",
            file=sys.stderr,
        )
        return 1
    finally:
        if scenarios:
            scenarios.close()
        if clients:
            try:
                clients.close()
            except Exception as exc:
                if journal:
                    journal.record("client_close_failed", error_type=type(exc).__name__)
        if journal:
            journal.close()


if __name__ == "__main__":
    raise SystemExit(main())
