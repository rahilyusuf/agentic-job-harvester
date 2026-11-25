"""DML-only BigQuery publication ledger. No network operations occur at import."""

import asyncio
import json
import logging
import random
import re
import time
import uuid

from google.cloud import bigquery

from config.settings import Settings, get_settings
from observability.ingestion_logging import emit
from schemas.job_models import validate_embedding_vector
from schemas.publication_models import (
    PROCESSING_VERSION,
    CheckpointResult,
    ProcessingBusy,
    ProcessingClaim,
    PublicationClaim,
    PublicationOwnership,
    QueryFailed,
    QueryOutcomeUnconfirmed,
    QueryTransactionAborted,
    publication_id,
)


class PublicationRepository:
    def __init__(
        self, bq_client=None, *, settings: Settings | None = None, dataset: str | None = None
    ):
        self.settings = settings or get_settings()
        project = self.settings.gcp_project_id
        dataset = dataset or self.settings.bq_dataset
        if not re.fullmatch(r"[A-Za-z0-9_-]+", project) or not re.fullmatch(
            r"[A-Za-z0-9_]+", dataset
        ):
            raise ValueError("Invalid BigQuery identifier")
        self.client = (
            bq_client
            if bq_client is not None
            else bigquery.Client(project=project, location=self.settings.bq_location)
        )
        self.table = f"{project}.{dataset}.job_publications"
        self.raw_table = f"{project}.{dataset}.raw_job_postings"
        self.embeddings_table = f"{project}.{dataset}.job_embeddings"
        self.topic = f"projects/{project}/topics/{self.settings.pubsub_embedding_topic}"

    def parameters(self, *extra):
        return [
            bigquery.ScalarQueryParameter("topic", "STRING", self.topic),
            bigquery.ScalarQueryParameter("version", "STRING", PROCESSING_VERSION),
            *extra,
        ]

    async def execute(
        self, sql: str, parameters: list, *, job_id: str | None = None,
        deadline: float | None = None,
    ) -> list[dict]:
        """Retry only proven rollback; resolve ambiguity through the original job ID."""
        deadline = min(
            deadline if deadline is not None else float("inf"), time.monotonic() + 120,
        )

        def remaining(cap):
            value = min(cap, deadline - time.monotonic())
            if value <= 0:
                raise TimeoutError("BigQuery operation deadline exceeded")
            return value

        def classify(job, query_id):
            error = job.error_result
            if job.state != "DONE" or not isinstance(error, dict):
                return None
            reason = error.get("reason")
            message = error.get("message") or ""
            # invalidQuery alone is NOT proof of rollback. This terminal signature is.
            if reason in {"invalidQuery", "concurrentUpdate"} and (
                "Transaction is aborted due to concurrent update" in message
            ):
                return QueryTransactionAborted(query_id)
            if reason == "invalidQuery" and (
                message.startswith("Syntax error:")
                or re.match(
                    r"(?:Query error: )?(?:Ambiguous existing raw jobs|"
                    r"Retained identity has no raw job|Duplicate publication identities|"
                    r"Duplicate publication jobs) at \[", message,
                )
            ):
                # These owned assertions precede every write/COMMIT; syntax never executes.
                category = (
                    "raw_identity_conflict"
                    if "Ambiguous existing raw jobs" in message
                    or "Retained identity has no raw job" in message
                    else "query_failed"
                )
                return QueryFailed(query_id, category)
            # A script can fail AFTER COMMIT. Without rollback proof, hold it uncertain.
            if "COMMIT TRANSACTION" not in sql.upper() and reason in {
                "invalidQuery", "accessDenied", "notFound", "invalid",
            }:
                return QueryFailed(query_id)
            return None

        def run(query_id):
            try:
                job = self.client.query(
                    sql,
                    job_config=bigquery.QueryJobConfig(query_parameters=parameters),
                    job_id=query_id, location=self.settings.bq_location,
                    job_retry=None, retry=None, timeout=remaining(10),
                )
                return [dict(row.items()) for row in job.result(
                    timeout=remaining(60), retry=None, job_retry=None,
                )]
            except Exception:
                resolved_job = None
                try:
                    job = self.client.get_job(
                        query_id, location=self.settings.bq_location,
                        retry=None, timeout=remaining(10),
                    )
                    resolved_job = job
                    failure = classify(job, query_id)
                    if failure is not None:
                        raise failure
                    return [dict(row.items()) for row in job.result(
                        timeout=remaining(60), retry=None, job_retry=None,
                    )]
                except QueryFailed:
                    raise
                except Exception as exc:
                    if resolved_job is not None:
                        failure = classify(resolved_job, query_id)
                        if failure is not None:
                            raise failure from exc
                    emit(
                        "ingestion.bigquery.unconfirmed", logging.ERROR,
                        query_job_id=query_id, error_category="query_outcome_unconfirmed",
                    )
                    raise QueryOutcomeUnconfirmed(query_id) from exc

        for attempt in range(3):
            query_id = job_id if attempt == 0 and job_id else f"harvester_{uuid.uuid4().hex}"
            try:
                return await asyncio.to_thread(run, query_id)
            except QueryTransactionAborted:
                if attempt == 2:
                    raise
                delay = random.uniform(0, 2 ** attempt)
                if time.monotonic() + delay >= deadline:
                    raise
                await asyncio.sleep(delay)
            except QueryFailed as exc:
                emit(
                    "ingestion.bigquery.failed", logging.ERROR,
                    query_job_id=query_id, error_category=exc.category,
                )
                raise
        raise AssertionError("Unreachable")

    async def claim_pending(
        self, job_ids: list[str] | None = None, limit: int = 100, min_age_seconds: int = 0,
        *, deadline: float | None = None,
    ) -> list[PublicationClaim]:
        if not 1 <= limit <= 100 or not 0 <= min_age_seconds <= 31536000:
            raise ValueError("Invalid publication batch bounds")
        if job_ids == []:
            return []
        token = uuid.uuid4().hex
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{self.table}` WHERE FALSE;
        ASSERT NOT EXISTS (SELECT publication_id FROM `{self.table}` WHERE topic=@topic
            GROUP BY publication_id HAVING COUNT(*)>1) AS 'Duplicate publication identities';
        ASSERT NOT EXISTS (SELECT job_id, processing_version FROM `{self.table}` WHERE topic=@topic
            GROUP BY job_id, processing_version HAVING COUNT(*)>1) AS 'Duplicate publication jobs';
        UPDATE `{self.table}` SET state='UNKNOWN', last_error_code='attempts_exhausted',
            updated_at=CURRENT_TIMESTAMP(), lease_token=NULL, lease_expires_at=NULL
        WHERE topic=@topic AND processing_version=@version AND attempt_count >= 5
          AND ((state='PENDING' AND
                (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP()))
            OR (state='IN_FLIGHT' AND lease_expires_at<=CURRENT_TIMESTAMP()));
        CREATE TEMP TABLE selected AS
        SELECT publication_id, job_id FROM `{self.table}`
        WHERE topic=@topic AND processing_version=@version
          AND (@all_jobs OR job_id IN UNNEST(@ids))
          AND created_at<=TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @age SECOND)
          AND ((state='PENDING' AND
                (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP()))
            OR (state='IN_FLIGHT' AND lease_expires_at<=CURRENT_TIMESTAMP()))
        ORDER BY created_at, publication_id LIMIT @limit;
        UPDATE `{self.table}` SET state='IN_FLIGHT', lease_token=@token,
            lease_expires_at=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @lease SECOND),
            attempt_count=attempt_count+1, updated_at=CURRENT_TIMESTAMP()
        WHERE publication_id IN (SELECT publication_id FROM selected);
        COMMIT TRANSACTION;
        SELECT s.publication_id,s.job_id,@token AS lease_token,p.lease_expires_at
        FROM selected s JOIN `{self.table}` p USING(publication_id);
        """
        rows = await self.execute(
            sql,
            self.parameters(
                bigquery.ScalarQueryParameter("all_jobs", "BOOL", job_ids is None),
                bigquery.ArrayQueryParameter("ids", "STRING", job_ids or []),
                bigquery.ScalarQueryParameter("age", "INT64", min_age_seconds),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
                bigquery.ScalarQueryParameter("token", "STRING", token),
                bigquery.ScalarQueryParameter(
                    "lease", "INT64", self.settings.publication_lease_seconds
                ),
            ),
            deadline=deadline,
        )
        return [PublicationClaim.model_validate(row) for row in rows]

    async def check_ownership(
        self, claims: list[PublicationClaim], *, deadline: float | None = None,
    ) -> list[PublicationOwnership]:
        if not claims:
            return []
        rows = await self.execute(
            f"""
            WITH requested AS (
                SELECT JSON_VALUE(item,'$.publication_id') AS publication_id,
                    JSON_VALUE(item,'$.lease_token') AS token
                FROM UNNEST(JSON_QUERY_ARRAY(@claims)) item
            )
            SELECT p.publication_id,p.job_id,p.lease_token,p.lease_expires_at,
                CURRENT_TIMESTAMP() AS server_now
            FROM `{self.table}` p JOIN requested r USING(publication_id)
            WHERE p.topic=@topic AND p.processing_version=@version
                AND p.state='IN_FLIGHT' AND p.lease_token=r.token
                AND p.lease_expires_at>CURRENT_TIMESTAMP()
                AND (SELECT COUNT(*) FROM `{self.raw_table}` j WHERE j.job_id=p.job_id)=1
            """,
            self.parameters(bigquery.ScalarQueryParameter(
                "claims", "STRING", json.dumps([c.model_dump(mode="json") for c in claims]),
            )),
            deadline=deadline,
        )
        owners = [PublicationOwnership.model_validate(row) for row in rows]
        expected = {(c.publication_id, c.job_id, c.lease_token) for c in claims}
        actual = [(c.publication_id, c.job_id, c.lease_token) for c in owners]
        if len(actual) != len(set(actual)) or not set(actual).issubset(expected):
            raise ValueError("Invalid publication ownership confirmation")
        return owners

    async def record_outcomes(
        self, claims: list[PublicationClaim], confirmed: dict[str, str], unconfirmed_ids: list[str],
        *, deadline: float | None = None,
    ) -> CheckpointResult:
        if not claims:
            return CheckpointResult()
        ids = [c.job_id for c in claims]
        if (len(ids) != len(set(ids)) or len({c.publication_id for c in claims}) != len(ids)
            or set(confirmed) & set(unconfirmed_ids)
            or len(unconfirmed_ids) != len(set(unconfirmed_ids))
            or set(confirmed) | set(unconfirmed_ids) != set(ids)
            or any(not isinstance(v, str) or not v for v in confirmed.values())):
            raise ValueError("Invalid publication outcome set")
        outcomes = [
            {**claim.model_dump(mode="json"), "message_id": confirmed.get(claim.job_id)}
            for claim in claims
        ]
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{self.table}` WHERE FALSE;
        CREATE TEMP TABLE outcomes AS SELECT
            JSON_VALUE(item,'$.publication_id') AS publication_id,
            JSON_VALUE(item,'$.job_id') AS job_id,
            JSON_VALUE(item,'$.lease_token') AS token,
            JSON_VALUE(item,'$.message_id') AS message_id
        FROM UNNEST(JSON_QUERY_ARRAY(@outcomes)) item;
        ASSERT NOT EXISTS (
            SELECT p.publication_id FROM `{self.table}` p JOIN outcomes o USING(publication_id)
            GROUP BY p.publication_id HAVING COUNT(*)>1
        ) AS 'Duplicate publication identities';
        CREATE TEMP TABLE eligible AS
        SELECT o.* FROM outcomes o JOIN `{self.table}` p USING(publication_id)
        WHERE p.topic=@topic AND p.processing_version=@version AND p.job_id=o.job_id
            AND p.state='IN_FLIGHT' AND p.lease_token=o.token
            AND p.lease_expires_at>CURRENT_TIMESTAMP();
        UPDATE `{self.table}` p SET
            state=IF(o.message_id IS NULL,'PENDING','PUBLISHED'),
            message_id=o.message_id,
            published_at=IF(o.message_id IS NULL,NULL,CURRENT_TIMESTAMP()),
            next_attempt_at=IF(o.message_id IS NULL,
                GREATEST(p.lease_expires_at,
                    TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 60 SECOND)),NULL),
            last_error_code=IF(o.message_id IS NULL,'publication_unconfirmed',NULL),
            lease_token=NULL, lease_expires_at=NULL, updated_at=CURRENT_TIMESTAMP()
        FROM eligible o WHERE p.publication_id=o.publication_id;
        CREATE TEMP TABLE results AS
        SELECT o.job_id, COALESCE((e.publication_id IS NOT NULL OR
            (o.message_id IS NOT NULL AND p.state='PUBLISHED' AND p.message_id=o.message_id)),FALSE)
            AS checkpointed
        FROM outcomes o LEFT JOIN eligible e USING(publication_id)
        LEFT JOIN `{self.table}` p ON p.publication_id=o.publication_id
            AND p.topic=@topic AND p.processing_version=@version AND p.job_id=o.job_id;
        COMMIT TRANSACTION;
        SELECT * FROM results;
        """
        rows = await self.execute(
            sql, self.parameters(bigquery.ScalarQueryParameter(
                "outcomes", "STRING", json.dumps(outcomes),
            )), deadline=deadline,
        )
        if (len(rows) != len(ids) or {r["job_id"] for r in rows} != set(ids)
            or any(type(r["checkpointed"]) is not bool for r in rows)):
            raise ValueError("Invalid checkpoint confirmation")
        saved = {r["job_id"] for r in rows if r["checkpointed"]}
        return CheckpointResult(
            checkpointed_ids=[i for i in ids if i in saved],
            lease_lost_ids=[i for i in ids if i not in saved],
        )

    async def publication_counts(self, job_ids: list[str]) -> dict[str, int]:
        if not job_ids:
            return {}
        rows = await self.execute(
            f"SELECT state,COUNT(*) AS total FROM `{self.table}` WHERE topic=@topic "
            "AND processing_version=@version AND job_id IN UNNEST(@ids) GROUP BY state",
            self.parameters(bigquery.ArrayQueryParameter("ids", "STRING", job_ids)),
        )
        counts = {row["state"]: int(row["total"]) for row in rows}
        if sum(counts.values()) != len(set(job_ids)) or set(counts) - {
            "PENDING",
            "IN_FLIGHT",
            "PUBLISHED",
            "UNKNOWN",
        }:
            raise ValueError("Publication state is missing or ambiguous")
        return counts

    async def claim_processing(self, job_id: str) -> ProcessingClaim:
        key, token = publication_id(self.topic, job_id), uuid.uuid4().hex
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{self.table}` WHERE FALSE;
        ASSERT (SELECT COUNT(*) FROM `{self.raw_table}` WHERE job_id=@id)=1
            AS 'Missing or ambiguous raw job';
        ASSERT (SELECT COUNT(*) FROM `{self.table}` WHERE publication_id=@key)<=1
            AS 'Duplicate publication identities';
        INSERT `{self.table}` (publication_id,job_id,topic,processing_version,state,
            created_at,updated_at,attempt_count,last_error_code)
        SELECT @key,@id,@topic,@version,'UNKNOWN',
            CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),0,'legacy_unknown'
        WHERE NOT EXISTS (SELECT 1 FROM `{self.table}` WHERE publication_id=@key);
        UPDATE `{self.table}` SET processing_token=@token,
            processing_lease_expires_at=TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL @lease SECOND),
            updated_at=CURRENT_TIMESTAMP()
        WHERE publication_id=@key AND processing_completed_at IS NULL
          AND (processing_token IS NULL OR processing_lease_expires_at<=CURRENT_TIMESTAMP());
        IF @@row_count=1 THEN
            UPDATE `{self.raw_table}` SET status='EMBEDDING_QUEUED'
            WHERE job_id=@id AND status IN ('NEW_RAW','EMBEDDING_QUEUED');
        END IF;
        CREATE TEMP TABLE claimed AS SELECT publication_id,job_id,
            processing_token AS lease_token, processing_completed_at IS NOT NULL AS completed
        FROM `{self.table}` WHERE publication_id=@key
            AND (processing_token=@token OR processing_completed_at IS NOT NULL);
        COMMIT TRANSACTION;
        SELECT * FROM claimed;
        """
        rows = await self.execute(
            sql,
            self.parameters(
                bigquery.ScalarQueryParameter("key", "STRING", key),
                bigquery.ScalarQueryParameter("id", "STRING", job_id),
                bigquery.ScalarQueryParameter("token", "STRING", token),
                bigquery.ScalarQueryParameter(
                    "lease", "INT64", self.settings.publication_lease_seconds
                ),
            ),
        )
        if not rows:
            raise ProcessingBusy("Processing already leased")
        return ProcessingClaim.model_validate(rows[0])

    async def finish_processing(self, claim: ProcessingClaim, embedding: list[float]) -> None:
        embedding = validate_embedding_vector(embedding)
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{self.table}` WHERE FALSE;
        ASSERT (SELECT COUNT(*) FROM `{self.table}` WHERE publication_id=@key
            AND processing_token=@token AND processing_lease_expires_at>CURRENT_TIMESTAMP())=1
            AS 'Processing lease lost';
        ASSERT (SELECT COUNT(*) FROM `{self.embeddings_table}` WHERE job_id=@id)<=1
            AS 'Ambiguous embedding artifact';
        MERGE `{self.embeddings_table}` t USING (SELECT @id AS job_id) s ON t.job_id=s.job_id
        WHEN MATCHED THEN UPDATE SET embedding=@embedding, embedded_at=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT(job_id,embedding,embedded_at)
            VALUES(@id,@embedding,CURRENT_TIMESTAMP());
        UPDATE `{self.raw_table}` SET status='EMBEDDING_DONE'
            WHERE job_id=@id AND status IN ('NEW_RAW','EMBEDDING_QUEUED');
        ASSERT @@row_count=1 AS 'Raw processing state changed';
        UPDATE `{self.table}` SET processing_completed_at=CURRENT_TIMESTAMP(),
            processing_token=NULL,processing_lease_expires_at=NULL,updated_at=CURRENT_TIMESTAMP()
            WHERE publication_id=@key;
        COMMIT TRANSACTION;
        """
        await self.execute(
            sql,
            self.parameters(
                bigquery.ScalarQueryParameter("key", "STRING", claim.publication_id),
                bigquery.ScalarQueryParameter("id", "STRING", claim.job_id),
                bigquery.ScalarQueryParameter("token", "STRING", claim.lease_token),
                bigquery.ArrayQueryParameter("embedding", "FLOAT64", embedding),
            ),
        )

    async def release_processing(self, claim: ProcessingClaim) -> None:
        sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{self.table}` WHERE FALSE;
        UPDATE `{self.table}` SET processing_token=NULL,processing_lease_expires_at=NULL,
            updated_at=CURRENT_TIMESTAMP() WHERE publication_id=@key AND processing_token=@token
            AND processing_completed_at IS NULL;
        IF @@row_count=1 THEN
            UPDATE `{self.raw_table}` SET status='NEW_RAW'
                WHERE job_id=@id AND status='EMBEDDING_QUEUED';
        END IF;
        COMMIT TRANSACTION;
        """
        await self.execute(
            sql,
            self.parameters(
                bigquery.ScalarQueryParameter("key", "STRING", claim.publication_id),
                bigquery.ScalarQueryParameter("id", "STRING", claim.job_id),
                bigquery.ScalarQueryParameter("token", "STRING", claim.lease_token),
            ),
        )
