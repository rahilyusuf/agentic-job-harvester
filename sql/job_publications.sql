-- Apply separately after review; application startup never creates this table.
-- This table MUST use DML, never insertAll/streaming writes.
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_publications` (
    publication_id STRING NOT NULL,
    job_id STRING NOT NULL,
    topic STRING NOT NULL,
    processing_version STRING NOT NULL,
    state STRING NOT NULL, -- PENDING | IN_FLIGHT | PUBLISHED | UNKNOWN
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    lease_token STRING,
    lease_expires_at TIMESTAMP,
    attempt_count INT64 NOT NULL,
    next_attempt_at TIMESTAMP,
    published_at TIMESTAMP,
    message_id STRING,
    last_error_code STRING,
    evidence_ref STRING,
    processing_token STRING,
    processing_lease_expires_at TIMESTAMP,
    processing_completed_at TIMESTAMP
)
CLUSTER BY state, topic, job_id;

-- BigQuery does not enforce uniqueness. Every application transaction that
-- creates/claims rows begins with DELETE ... WHERE FALSE on this table to
-- serialize competing mutating transactions, including insert-only batches.
-- No expiration: deduplication history must outlive source/queue retention.
