-- sql/job_embeddings.sql
-- BigQuery DDL for job_embeddings table.
-- Stores 768-dim text-embedding-004 vectors with a native IVF Cosine index
-- for VECTOR_SEARCH used in Tier 1 Step A retrieval.
--
-- IMPORTANT: The vector index must be created AFTER table population;
-- BQ requires at least some data before CREATE VECTOR INDEX succeeds.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_embeddings` (
    job_id       STRING    NOT NULL,
    embedding    ARRAY<FLOAT64> NOT NULL,  -- 768-dim text-embedding-004 vector
    embedded_at  TIMESTAMP NOT NULL,
)
CLUSTER BY job_id
OPTIONS (
    description = "Pre-computed 768-dim embeddings for all EMBEDDING_DONE jobs. Used by Tier 1 VECTOR_SEARCH."
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Vector Index (run separately after initial data load)
-- Implements ARCHITECTURE.md: "backed by a native BigQuery IVF Cosine vector index"
-- ─────────────────────────────────────────────────────────────────────────────
-- CREATE VECTOR INDEX IF NOT EXISTS job_embeddings_cosine_idx
--     ON `${GCP_PROJECT_ID}.${BQ_DATASET}.job_embeddings`(embedding)
--     OPTIONS (
--         index_type = 'IVF',
--         distance_type = 'COSINE',
--         ivf_options = '{"num_lists": 100}'
--     );
--
-- Note: Uncomment and run this separately once the table has data.
-- The index is maintained automatically by BQ after creation.
-- Query using: 1 - ML.DISTANCE(e_active_user, e_job, 'COSINE')
