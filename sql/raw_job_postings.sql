-- sql/raw_job_postings.sql
-- BigQuery DDL for raw_job_postings table.
-- Source of truth for table shape. Any schema change must be reflected here
-- AND in ARCHITECTURE.md in the same commit.
--
-- Usage: bq query --use_legacy_sql=false < sql/raw_job_postings.sql

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.raw_job_postings` (
    job_id          STRING    NOT NULL,
    source          STRING    NOT NULL,   -- Apify actor ID or n8n workflow name
    scraped_at      TIMESTAMP NOT NULL,
    job_url         STRING    NOT NULL,
    title           STRING    NOT NULL,
    company         STRING    NOT NULL,
    location        STRING,
    description     STRING    NOT NULL,
    salary_raw      STRING,
    employment_type_raw STRING,
    posted_at       TIMESTAMP,
    status          STRING    NOT NULL,   -- JobStatus enum: NEW_RAW | EMBEDDING_QUEUED | ...
    ats_platform    STRING,              -- lever | workable | greenhouse | unknown
)
PARTITION BY DATE(scraped_at)
CLUSTER BY status, company
OPTIONS (
    description = "Raw job postings as ingested from Apify/n8n scrapers. One row per unique job_id.",
    require_partition_filter = FALSE,
    partition_expiration_days = 365
);

-- Prevent duplicate job_ids within the dedup window
-- Note: BQ does not support unique constraints natively;
-- dedup logic is enforced in repositories/raw_jobs_repo.py::exists().

-- Index hint: cluster on (status, company) for efficient status transitions
-- and company-level lookups by CompanyResearchAgent.
