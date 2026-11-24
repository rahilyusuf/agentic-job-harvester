"""Offline safety checks for the explicitly invoked integration CLI; never live tests."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from scripts import verify_publication_ledger as harness


@pytest.fixture
def manifest():
    run_id = "20261117123000"
    return harness.Manifest(
        run_id=run_id,
        source_sha256=harness.source_fingerprint(),
        resources=harness.resources_for(run_id),
        iam_review_reference="review/isolated-iam",
    )


@pytest.fixture
def clients(manifest):
    result = harness.Clients(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    result.bq.get_dataset.return_value = SimpleNamespace(
        labels=manifest.labels,
        full_dataset_id=manifest.dataset_id.replace(".", ":", 1),
        location="US",
        default_table_expiration_ms=harness.TTL_MS,
    )
    result.publisher.get_topic.return_value = SimpleNamespace(
        name=manifest.resources.topic,
        labels=manifest.labels,
    )
    result.subscriber.get_subscription.return_value = SimpleNamespace(
        name=manifest.resources.subscription,
        topic=manifest.resources.topic,
        labels=manifest.labels,
        ack_deadline_seconds=60,
        message_retention_duration=timedelta(days=1),
        state=SimpleNamespace(name="ACTIVE"),
        push_config=SimpleNamespace(push_endpoint=""),
        bigquery_config=SimpleNamespace(table=""),
        cloud_storage_config=SimpleNamespace(bucket=""),
        detached=False,
        filter="",
    )
    result.publisher.list_topic_subscriptions.return_value = [manifest.resources.subscription]

    def table(table_id, **kwargs):
        name = table_id.rsplit(".", 1)[1]
        created = datetime.now(timezone.utc)
        return SimpleNamespace(
            labels=manifest.labels,
            schema=[
                bigquery.SchemaField(n, t, mode=m) for n, t, m in harness.schema_contract(name)
            ],
            table_type="TABLE",
            full_table_id=table_id.replace(".", ":", 1),
            clustering_fields=["status", "company"]
            if name == "raw_job_postings"
            else ["state", "topic", "job_id"],
            streaming_buffer=None,
            created=created,
            expires=created + timedelta(days=7),
            location="US",
            require_partition_filter=False,
            time_partitioning=bigquery.TimePartitioning(
                type_="DAY",
                field="scraped_at",
                expiration_ms=365 * 86400 * 1000,
            )
            if name == "raw_job_postings"
            else None,
            range_partitioning=None,
        )

    result.bq.get_table.side_effect = table
    result.bq.list_tables.return_value = []
    return result


def test_offline_plan_never_discovers_credentials_or_constructs_clients(monkeypatch, capsys):
    create = MagicMock(side_effect=AssertionError("No credentials"))
    monkeypatch.setattr(harness, "create_clients", create)
    assert harness.main(["plan", "--run-id", "20261117123000"]) == 0
    data = json.loads(capsys.readouterr().out)
    parsed = harness.Manifest.model_validate(data)
    assert parsed.resources.dataset == "ledger_it_20261117123000"
    assert parsed.resources.topic.endswith("/ledger-it-20261117123000")
    create.assert_not_called()


@pytest.mark.parametrize("phase", ["provision", "check", "run", "cleanup"])
def test_no_execute_or_wrong_confirmation_rejects_before_clients(
    monkeypatch,
    tmp_path,
    manifest,
    phase,
):
    path = tmp_path / "manifest.json"
    path.write_text(manifest.model_dump_json(), encoding="utf-8")
    create = MagicMock()
    monkeypatch.setattr(harness, "create_clients", create)
    args = [phase, "--manifest", str(path), "--evidence", str(tmp_path / "evidence.jsonl")]
    assert harness.main(args) == 1
    assert harness.main(args + ["--execute", "--confirm", "WRONG"]) == 1
    create.assert_not_called()
    assert not (tmp_path / "evidence.jsonl").exists()


def test_manifest_cannot_select_existing_production_resources(manifest):
    data = manifest.model_dump()
    for name, value in (
        ("dataset", "job_intelligence"),
        ("topic", "projects/agentic-job-harvester-v2/topics/job-embedding-queue"),
        ("subscription", "projects/agentic-job-harvester-v2/subscriptions/job-embedding-test-sub"),
        ("project", "different-project"),
        ("service_account", "production@example.com"),
    ):
        changed = {**data, "resources": {**data["resources"], name: value}}
        with pytest.raises(ValueError):
            harness.Manifest.model_validate(changed)
    with pytest.raises(ValueError):
        harness.Manifest.model_validate({**data, "unexpected_override": True})


def test_execution_rejects_stale_source_and_missing_iam_review(manifest):
    stale = manifest.model_copy(update={"source_sha256": "0" * 64})
    with pytest.raises(harness.SafetyError, match="Source changed"):
        harness.validate_execution(stale, "check", True, f"CHECK {manifest.run_id}")
    unreviewed = manifest.model_copy(update={"iam_review_reference": ""})
    with pytest.raises(harness.SafetyError, match="IAM review"):
        harness.validate_execution(unreviewed, "check", True, f"CHECK {manifest.run_id}")


def test_run_requires_production_python_runtime(monkeypatch, manifest):
    monkeypatch.setattr(harness.sys, "version_info", (3, 14))
    with pytest.raises(harness.SafetyError, match="Python 3.11"):
        harness.validate_execution(manifest, "run", True, f"RUN {manifest.run_id}")


def test_existing_evidence_is_never_overwritten_before_client_creation(
    tmp_path,
    manifest,
    monkeypatch,
):
    path, evidence = tmp_path / "manifest.json", tmp_path / "evidence.jsonl"
    path.write_text(manifest.model_dump_json(), encoding="utf-8")
    evidence.write_text("independently owned", encoding="utf-8")
    create = MagicMock()
    monkeypatch.setattr(harness, "create_clients", create)
    assert (
        harness.main(
            [
                "check",
                "--manifest",
                str(path),
                "--evidence",
                str(evidence),
                "--execute",
                "--confirm",
                f"CHECK {manifest.run_id}",
            ]
        )
        == 1
    )
    assert evidence.read_text(encoding="utf-8") == "independently owned"
    create.assert_not_called()


def test_preflight_checks_actual_schema_and_never_consumes(manifest, clients):
    harness.preflight(manifest, clients)
    clients.bq.query.assert_not_called()
    clients.publisher.publish.assert_not_called()
    for name in ("pull", "streaming_pull", "acknowledge", "seek", "modify_ack_deadline"):
        getattr(clients.subscriber, name).assert_not_called()


@pytest.mark.parametrize(
    "problem", ["location", "labels", "push", "attachment", "schema", "buffer"]
)
def test_preflight_rejects_configuration_drift(manifest, clients, problem):
    if problem == "location":
        clients.bq.get_dataset.return_value.location = "EU"
    elif problem == "labels":
        clients.publisher.get_topic.return_value.labels = {}
    elif problem == "push":
        clients.subscriber.get_subscription.return_value.push_config.push_endpoint = (
            "https://worker"
        )
    elif problem == "attachment":
        clients.publisher.list_topic_subscriptions.return_value.append("another-subscription")
    else:
        original = clients.bq.get_table.side_effect

        def changed(*args, **kwargs):
            table = original(*args, **kwargs)
            if problem == "schema":
                table.schema[0] = bigquery.SchemaField("job_id", "INTEGER", mode="REQUIRED")
            else:
                table.streaming_buffer = object()
            return table

        clients.bq.get_table.side_effect = changed
    with pytest.raises(harness.SafetyError):
        harness.preflight(manifest, clients)
    clients.publisher.publish.assert_not_called()
    clients.bq.query.assert_not_called()


def test_provision_refuses_any_existing_name_before_creating(manifest, clients):
    clients.bq.get_dataset.side_effect = NotFound("absent")
    with pytest.raises(harness.SafetyError, match="already exists"):
        harness.provision(manifest, clients, MagicMock())
    clients.bq.create_dataset.assert_not_called()
    clients.publisher.create_topic.assert_not_called()


def test_provision_creates_only_reviewed_resources(manifest, clients):
    clients.bq.get_dataset.side_effect = NotFound("absent")
    clients.publisher.get_topic.side_effect = NotFound("absent")
    clients.subscriber.get_subscription.side_effect = NotFound("absent")
    harness.provision(manifest, clients, MagicMock())
    assert clients.bq.create_dataset.call_args.args[0].dataset_id == manifest.resources.dataset
    assert clients.bq.create_table.call_count == 2
    for call in clients.bq.create_table.call_args_list:
        table = call.args[0]
        assert table.labels == manifest.labels and table.expires is not None
        assert call.kwargs["exists_ok"] is False
    sub = clients.subscriber.create_subscription.call_args.kwargs["request"]
    assert sub["topic"] == manifest.resources.topic
    assert sub["name"] == manifest.resources.subscription
    assert not sub["push_config"].push_endpoint


def test_cleanup_checks_every_resource_before_deletion(manifest, clients):
    clients.publisher.list_topic_subscriptions.return_value = ["unexpected-worker"]
    with pytest.raises(harness.SafetyError):
        harness.cleanup(manifest, clients, MagicMock())
    clients.subscriber.delete_subscription.assert_not_called()
    clients.publisher.delete_topic.assert_not_called()
    clients.bq.delete_dataset.assert_not_called()


def test_cleanup_deletes_only_owned_resources_and_no_messages(manifest, clients):
    harness.cleanup(manifest, clients, MagicMock())
    assert clients.bq.delete_dataset.call_args.args[0] == manifest.dataset_id
    assert clients.publisher.delete_topic.call_args.kwargs["request"] == {
        "topic": manifest.resources.topic
    }
    assert clients.subscriber.delete_subscription.call_args.kwargs["request"] == {
        "subscription": manifest.resources.subscription,
    }
    clients.subscriber.pull.assert_not_called()
    clients.subscriber.acknowledge.assert_not_called()


def test_operation_budgets_and_expired_deadlines_fail_closed(monkeypatch):
    budget = harness.Budget()
    for _ in range(harness.MAX_PUBLICATIONS):
        budget.spend("publications")
    with pytest.raises(harness.SafetyError):
        budget.spend("publications")
    budget.queries = harness.MAX_QUERIES
    with pytest.raises(harness.SafetyError):
        budget.spend("queries")
    monkeypatch.setattr(harness.time, "monotonic", lambda: budget.deadline + 1)
    with pytest.raises(harness.SafetyError):
        budget.check()


def test_guarded_bigquery_rejects_production_and_injects_only_test_faults(manifest):
    client, journal = MagicMock(), MagicMock()
    guarded = harness.GuardedBigQuery(client, manifest, journal, harness.Budget())
    with pytest.raises(harness.SafetyError):
        guarded.query("SELECT * FROM `agentic-job-harvester-v2.job_intelligence.raw_job_postings`")
    client.query.assert_not_called()
    with pytest.raises(harness.SafetyError):
        guarded.get_job("another-query-id")
    guarded.faults = {"rollback"}
    guarded.query(
        f"BEGIN TRANSACTION; CREATE TEMP TABLE incoming AS SELECT * FROM `{manifest.tables[0]}`; COMMIT TRANSACTION;"
    )
    assert "ASSERT FALSE AS 'Harness forced rollback'" in client.query.call_args.args[0]
    assert client.query.call_args.kwargs["retry"] is None
    assert client.query.call_args.kwargs["job_config"].maximum_bytes_billed == 1024**3


@pytest.mark.asyncio
async def test_lost_real_job_response_resolves_original_id_without_resubmission(manifest):
    from config.settings import Settings
    from repositories.publications_repo import PublicationRepository

    client, journal = MagicMock(), MagicMock()
    guarded = harness.GuardedBigQuery(client, manifest, journal, harness.Budget())
    guarded.faults = {"lost_persist"}
    job = MagicMock(state="DONE", error_result=None)
    job.result.return_value = [{"ok": True}]
    client.query.return_value = client.get_job.return_value = job
    configured = Settings.model_construct(
        gcp_project_id=harness.PROJECT,
        bq_dataset=manifest.resources.dataset,
        bq_location="US",
    )
    repo = PublicationRepository(guarded, settings=configured)
    assert await repo.execute("CREATE TEMP TABLE incoming AS SELECT 1; SELECT TRUE AS ok", []) == [
        {"ok": True}
    ]
    assert client.query.call_count == 1
    assert client.get_job.call_args.args[0] == client.query.call_args.kwargs["job_id"]


def test_publisher_only_forwards_two_synthetic_ids_and_records_real_confirmation(manifest):
    sdk, journal = MagicMock(), MagicMock()
    sdk.publish.return_value.result.return_value = "broker-confirmed-id"
    confirmations = {}
    publisher = harness.CountingPublisher(sdk, manifest, journal, harness.Budget(), confirmations)
    with pytest.raises(harness.SafetyError):
        publisher.publish("production-topic", b'{"job_id":"bad"}')
    sdk.publish.assert_not_called()
    job_id = manifest.job_ids[0]
    result = publisher.publish(manifest.resources.topic, json.dumps({"job_id": job_id}).encode())
    assert result.result(timeout=10) == "broker-confirmed-id"
    assert confirmations == {job_id: "broker-confirmed-id"}


def test_two_synthetic_jobs_use_normalizer_without_apify_or_settings_resolution(manifest):
    from services.normalizers.batch import normalize_batch

    result = normalize_batch(harness.synthetic_items(manifest))
    assert (result.received, result.normalized, result.rejected, len(result.jobs)) == (2, 2, 0, 2)
    assert tuple(j.job_id for j in result.jobs) == manifest.job_ids
    assert result.apply_url_fallback == 1
    assert result.jobs[0].posted_at is None and result.jobs[1].posted_at.utcoffset() is not None


def test_build_context_excludes_local_secrets_and_integration_tooling():
    rules = (harness.ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    for pattern in (
        ".git/",
        "venv/",
        "**/.env*",
        "devtools/",
        "tests/",
        "Input/",
        "scripts/verify_publication_ledger.py",
    ):
        assert pattern in rules


def test_scenario_construction_does_not_resolve_settings_or_secrets(manifest, clients, monkeypatch):
    import config.settings as config

    get_settings = MagicMock(side_effect=AssertionError("No production settings"))
    fetch_secret = MagicMock(side_effect=AssertionError("No secrets"))
    monkeypatch.setattr(config, "get_settings", get_settings)
    monkeypatch.setattr(config, "_fetch_secret", fetch_secret)
    scenarios = harness.Scenarios(manifest, clients, MagicMock())
    assert scenarios.raw.publications.client is scenarios.bq
    assert scenarios.settings.bq_dataset == manifest.resources.dataset
    assert scenarios.settings.pubsub_embedding_topic == manifest.resources.topic.rsplit("/", 1)[1]
    assert len(scenarios.batch().jobs) == 2
    get_settings.assert_not_called()
    fetch_secret.assert_not_called()
    clients.bq.query.assert_not_called()


def test_main_check_uses_impersonated_scope_and_closes_clients(
    manifest,
    clients,
    monkeypatch,
    tmp_path,
):
    path = tmp_path / "manifest.json"
    path.write_text(manifest.model_dump_json(), encoding="utf-8")
    create = MagicMock(return_value=clients)
    monkeypatch.setattr(harness, "create_clients", create)
    result = harness.main(
        [
            "check",
            "--manifest",
            str(path),
            "--execute",
            "--confirm",
            f"CHECK {manifest.run_id}",
            "--evidence",
            str(tmp_path / "evidence.jsonl"),
        ]
    )
    assert result == 0 and create.call_args.kwargs == {"operator": False}
    clients.bq.close.assert_called_once()
    clients.subscriber.close.assert_called_once()
    clients.publisher.publish.assert_not_called()
    events = [json.loads(line) for line in (tmp_path / "evidence.jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == ["started", "preflight_passed", "completed"]
