-- sql/user_profiles.sql
-- BigQuery DDL for user_profiles table.
-- Written by user_profile_service.py; updated weekly by reflection_runner.py.
-- The e_active_user column is what VECTOR_SEARCH queries in Step A.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.user_profiles` (
    user_id                     STRING    NOT NULL,
    email                       STRING,
    telegram_chat_id            STRING,
    resume_gcs_uri              STRING    NOT NULL,   -- gs://bucket/path/resume.pdf
    resume_metadata             STRING    NOT NULL,   -- JSON-serialized ResumeMetadata
    e_resume                    ARRAY<FLOAT64> NOT NULL,   -- 768-dim base embedding
    e_active_user               ARRAY<FLOAT64> NOT NULL,   -- 768-dim active centroid
    active_vector_alpha         FLOAT64   NOT NULL DEFAULT 0.0,
    notification_threshold_score INT64   NOT NULL DEFAULT 85,
    is_active                   BOOL      NOT NULL DEFAULT TRUE,
    created_at                  TIMESTAMP NOT NULL,
    updated_at                  TIMESTAMP NOT NULL,
    last_reflection_at          TIMESTAMP,
)
CLUSTER BY user_id
OPTIONS (
    description = "User profiles. Holds resume metadata, embedding vectors, and active centroid. Read by VECTOR_SEARCH in Tier 1 Step A."
);

-- IMPORTANT: e_active_user is the live query vector for VECTOR_SEARCH.
-- Updated by reflection_runner.py using:
--   e_active_new = (1 - α) * e_resume + α * e_accepted_centroid
-- where α = active_vector_alpha, updated after each reflection cycle.

-- Vector index for user centroid lookups (if needed for user-side search):
-- CREATE VECTOR INDEX user_centroid_idx
--     ON `${GCP_PROJECT_ID}.${BQ_DATASET}.user_profiles`(e_active_user)
--     OPTIONS (index_type = 'IVF', distance_type = 'COSINE');
