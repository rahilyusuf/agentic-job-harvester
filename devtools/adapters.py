"""Actual adapters: metadata-only checks and explicitly requested bounded writes."""

import asyncio
import os

from devtools.config import DATASET, LOCATION, PROJECT, SUBSCRIPTION, TABLE, TOPIC
from devtools.schemas import GCPStatus, PubSubStatus
from schemas.job_models import RawJob


def _publication_settings():
    """Respect explicit dev process configuration without resolving production secrets."""
    from pydantic import TypeAdapter

    from config.settings import Settings

    return Settings.model_construct(
        gcp_project_id=PROJECT,
        bq_dataset=DATASET,
        bq_location=LOCATION,
        pubsub_embedding_topic=TOPIC,
        publication_ledger_enabled=TypeAdapter(bool).validate_python(
            os.environ.get("PUBLICATION_LEDGER_ENABLED", "false")
        ),
    )


class GoogleCloudOperations:
    """No SDK clients are constructed until an authenticated endpoint invokes a method."""

    def _pubsub_status(self) -> PubSubStatus:
        from google.cloud import pubsub_v1

        topic_name = f"projects/{PROJECT}/topics/{TOPIC}"
        subscription_name = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        try:
            topic = publisher.get_topic(request={"topic": topic_name}, retry=None, timeout=10)
            subscription = subscriber.get_subscription(
                request={"subscription": subscription_name},
                retry=None,
                timeout=10,
            )
            attached = list(
                publisher.list_topic_subscriptions(
                    request={"topic": topic_name},
                    retry=None,
                    timeout=10,
                )
            )
            active = subscription.state == pubsub_v1.types.Subscription.State.ACTIVE
            pull_only = not (
                subscription.push_config.push_endpoint
                or subscription.bigquery_config.table
                or subscription.cloud_storage_config.bucket
                or subscription.detached
                or subscription.filter
            )
            exclusive = attached == [subscription_name]
            if not (
                topic.name == topic_name
                and subscription.name == subscription_name
                and subscription.topic == topic_name
                and active
                and pull_only
                and exclusive
                and subscription.ack_deadline_seconds == 60
                and subscription.message_retention_duration.total_seconds() == 86400
            ):
                raise ValueError("Unsafe test subscription configuration")
            return PubSubStatus(
                topic=topic_name,
                subscription=subscription_name,
                active=active,
                pull_only=pull_only,
                exclusive_subscription=exclusive,
                ack_deadline_seconds=60,
                retention_seconds=86400,
            )
        finally:
            publisher.transport.close()
            subscriber.close()

    def _gcp_status(self) -> GCPStatus:
        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT, location=LOCATION)
        try:
            dataset = client.get_dataset(f"{PROJECT}.{DATASET}", retry=None, timeout=10)
            table = client.get_table(f"{PROJECT}.{DATASET}.{TABLE}", retry=None, timeout=10)
            if not (
                client.project == PROJECT
                and dataset.reference.project == PROJECT
                and dataset.dataset_id == DATASET
                and dataset.location == LOCATION
                and table.reference.project == PROJECT
                and table.reference.dataset_id == DATASET
                and table.reference.table_id == TABLE
                and table.table_type == "TABLE"
            ):
                raise ValueError("Unsafe test dataset configuration")
        finally:
            client.close()
        return GCPStatus(
            project=PROJECT,
            dataset=DATASET,
            location=LOCATION,
            table=TABLE,
            verified=True,
            pubsub=self._pubsub_status(),
        )

    async def gcp_status(self) -> GCPStatus:
        return await asyncio.to_thread(self._gcp_status)

    async def pubsub_status(self) -> PubSubStatus:
        return await asyncio.to_thread(self._pubsub_status)

    async def insert_once(self, jobs: list[RawJob]) -> list[str]:
        from google.cloud import bigquery

        from repositories.raw_jobs_repo import RawJobRepository

        if len(jobs) > 3 or len({job.job_id for job in jobs}) != len(jobs):
            raise ValueError("Invalid test batch")

        settings = _publication_settings()

        def insert() -> list[str]:
            client = bigquery.Client(project=PROJECT, location=LOCATION)
            try:
                # Constructing Settings normally resolves unrelated production secrets.
                # All resource values below are fixed, preverified allowlist constants.
                repo = RawJobRepository(bq_client=client, dataset=DATASET, settings=settings)
                return asyncio.run(repo.batch_insert_jobs_once(jobs))
            finally:
                client.close()

        return await asyncio.to_thread(insert)

    async def publish(self, job_ids: list[str]) -> tuple[int, int]:
        settings = _publication_settings()
        if settings.publication_ledger_enabled:
            from repositories.publications_repo import PublicationRepository
            from services.publication_dispatcher import dispatch_pending

            repo = PublicationRepository(settings=settings)
            try:
                result = await dispatch_pending(
                    repo, project_id=PROJECT, topic=TOPIC, job_ids=job_ids
                )
                return len(result.confirmed), len(result.unconfirmed_ids)
            finally:
                repo.client.close()
        from services.apify_receiver import _publish_job_ids

        return await _publish_job_ids(job_ids, project_id=PROJECT, topic=TOPIC)
