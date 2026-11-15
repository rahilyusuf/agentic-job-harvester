from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from devtools.adapters import GoogleCloudOperations
from devtools.config import DATASET, LOCATION, PROJECT, SUBSCRIPTION, TABLE, TOPIC


@pytest.fixture
def metadata(monkeypatch):
    from google.cloud import bigquery, pubsub_v1

    dataset = SimpleNamespace(
        reference=SimpleNamespace(project=PROJECT),
        dataset_id=DATASET,
        location=LOCATION,
    )
    table = SimpleNamespace(
        reference=SimpleNamespace(project=PROJECT, dataset_id=DATASET, table_id=TABLE),
        table_type="TABLE",
    )
    client = MagicMock(project=PROJECT)
    client.get_dataset.return_value = dataset
    client.get_table.return_value = table
    publisher = MagicMock()
    topic = pubsub_v1.types.Topic(name=f"projects/{PROJECT}/topics/{TOPIC}")
    subscription = pubsub_v1.types.Subscription(
        name=f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}",
        topic=topic.name,
        state=pubsub_v1.types.Subscription.State.ACTIVE,
        ack_deadline_seconds=60,
        message_retention_duration={"seconds": 86400},
    )
    publisher.get_topic.return_value = topic
    publisher.list_topic_subscriptions.return_value = [subscription.name]
    subscriber = MagicMock()
    subscriber.get_subscription.return_value = subscription
    monkeypatch.setattr(bigquery, "Client", MagicMock(return_value=client))
    monkeypatch.setattr(pubsub_v1, "PublisherClient", MagicMock(return_value=publisher))
    monkeypatch.setattr(pubsub_v1, "SubscriberClient", MagicMock(return_value=subscriber))
    return client, publisher, subscriber, dataset, table, subscription


@pytest.mark.asyncio
async def test_exact_metadata_verification(metadata):
    client, publisher, subscriber, _, _, subscription = metadata
    status = await GoogleCloudOperations().gcp_status()
    assert status.verified and status.pubsub.pull_only
    client.get_dataset.assert_called_once_with(f"{PROJECT}.{DATASET}", retry=None, timeout=10)
    client.get_table.assert_called_once_with(f"{PROJECT}.{DATASET}.{TABLE}", retry=None, timeout=10)
    publisher.list_topic_subscriptions.assert_called_once_with(
        request={"topic": subscription.topic},
        retry=None,
        timeout=10,
    )
    subscriber.get_subscription.assert_called_once_with(
        request={"subscription": subscription.name},
        retry=None,
        timeout=10,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "project",
        "dataset",
        "location",
        "table",
        "view",
        "topic",
        "subscription",
        "push",
        "sink",
        "state",
        "deadline",
        "retention",
        "other_subscriber",
        "filter",
    ],
)
async def test_unsafe_metadata_fails_closed(metadata, case):
    client, publisher, _, dataset, table, subscription = metadata
    if case == "project":
        dataset.reference.project = "wrong"
    elif case == "dataset":
        dataset.dataset_id = "wrong"
    elif case == "location":
        dataset.location = "EU"
    elif case == "table":
        table.reference.table_id = "wrong"
    elif case == "view":
        table.table_type = "VIEW"
    elif case == "topic":
        subscription.topic = "projects/other/topics/other"
    elif case == "subscription":
        subscription.name = "projects/other/subscriptions/other"
    elif case == "push":
        subscription.push_config.push_endpoint = "https://worker.example.com"
    elif case == "sink":
        subscription.bigquery_config.table = "project.dataset.table"
    elif case == "state":
        subscription.state = 0
    elif case == "deadline":
        subscription.ack_deadline_seconds = 10
    elif case == "retention":
        subscription.message_retention_duration = {"seconds": 600}
    elif case == "other_subscriber":
        publisher.list_topic_subscriptions.return_value.append(
            "projects/test/subscriptions/embedding-worker"
        )
    elif case == "filter":
        subscription.filter = 'attributes.kind = "other"'
    with pytest.raises(ValueError):
        await GoogleCloudOperations().gcp_status()
    client.insert_rows_json.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", [[], [{"index": 0, "errors": [{"reason": "invalid"}]}], TimeoutError("uncertain")]
)
async def test_actual_repository_once_path_without_secret_or_retry(metadata, monkeypatch, outcome):
    import repositories.raw_jobs_repo as module
    from devtools.dependencies import load_fixture
    from services.normalizers.batch import normalize_batch

    def forbidden():
        pytest.fail("Production settings were resolved")

    monkeypatch.setattr(module, "get_settings", forbidden)
    client = metadata[0]
    client.query_and_wait.return_value = []
    if isinstance(outcome, Exception):
        client.insert_rows_json.side_effect = outcome
    else:
        client.insert_rows_json.return_value = outcome
    jobs = normalize_batch(load_fixture()).jobs[:3]
    adapter = GoogleCloudOperations()
    if outcome:
        with pytest.raises((RuntimeError, TimeoutError)):
            await adapter.insert_once(jobs)
    else:
        assert await adapter.insert_once(jobs) == [job.job_id for job in jobs]
    client.insert_rows_json.assert_called_once()
    assert client.insert_rows_json.call_args.kwargs == {"retry": None}
    assert client.insert_rows_json.call_args.args[0] == f"{PROJECT}.{DATASET}.{TABLE}"
    assert len(client.insert_rows_json.call_args.args[1]) == 3


@pytest.mark.asyncio
async def test_actual_production_publisher_is_used(metadata):
    publisher = metadata[1]
    publisher.topic_path.return_value = f"projects/{PROJECT}/topics/{TOPIC}"
    publisher.publish.return_value.result.return_value = "ack-message-id"
    assert await GoogleCloudOperations().publish(["confirmed-job"]) == (1, 0)
    publisher.publish.assert_called_once_with(
        f"projects/{PROJECT}/topics/{TOPIC}",
        b'{"job_id": "confirmed-job"}',
    )


@pytest.mark.asyncio
async def test_production_repository_retry_policy_is_preserved(metadata, monkeypatch):
    from devtools.dependencies import load_fixture
    from repositories.raw_jobs_repo import RawJobRepository
    from services.normalizers.batch import normalize_batch

    client = metadata[0]
    client.query_and_wait.return_value = []
    client.insert_rows_json.side_effect = [RuntimeError("retryable failure"), []]
    sleep = AsyncMock()
    monkeypatch.setattr("utils.retry.asyncio.sleep", sleep)
    jobs = normalize_batch(load_fixture()).jobs[:3]
    repo = RawJobRepository(bq_client=client, dataset=DATASET)
    assert await repo.batch_insert_jobs(jobs) == [job.job_id for job in jobs]
    assert client.insert_rows_json.call_count == 2
    assert all(call.kwargs == {} for call in client.insert_rows_json.call_args_list)
    sleep.assert_awaited_once()


def test_devtools_excluded_from_all_production_build_contexts():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    rules = (root / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert "devtools/" in rules and "tests/devtools/" in rules
    assert not any(line.startswith("!") for line in rules)
    for path in (root / "infra").glob("*.Dockerfile"):
        assert path.with_name(path.name + ".dockerignore").exists() is False
        assert "devtools" not in path.read_text(encoding="utf-8")
