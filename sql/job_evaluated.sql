-- sql/job_evaluated.sql
-- BigQuery DDL for job_evaluated table.
-- Written by ScoreDebateLoop (LoopAgent) after Tier 1 consensus.
-- Read by telegram_webhook for notification dispatch and reflection_runner for telemetry.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_evaluated` (
    job_id              STRING    NOT NULL,
    user_id             STRING    NOT NULL,
    scorer_score        INT64     NOT NULL,   -- ScorerAgent initial score (0-100)
    skeptic_score       INT64     NOT NULL,   -- SkepticAgent first-pass score (0-100)
    agreed_score        INT64     NOT NULL,   -- Final score post-consensus/veto (0-100)
    debate_rounds       INT64     NOT NULL,   -- 1 or 2
    skeptic_vetoed      BOOL      NOT NULL,   -- True = Skeptic hard veto
    routing_decision    STRING    NOT NULL,   -- SKIP | PROCESS | HIGH_PRIORITY
    scorer_rationale    STRING,
    skeptic_rationale   STRING,
    disqualifiers       ARRAY<STRING>,
    strengths           ARRAY<STRING>,
    evaluated_at        TIMESTAMP NOT NULL,
    notified_at         TIMESTAMP,            -- NULL = not yet notified via Telegram
    tier2_status        STRING,               -- NULL | PENDING | IN_PROGRESS | COMPLETE
)
PARTITION BY DATE(evaluated_at)
CLUSTER BY user_id, agreed_score
OPTIONS (
    description = "Tier 1 scoring output. One row per (job_id, user_id) pair.",
    require_partition_filter = FALSE,
    partition_expiration_days = 365
);

-- Typical access patterns:
--  1. High-score jobs for notification:
--     WHERE user_id = X AND agreed_score >= 85 AND notified_at IS NULL
--  2. Tier 2 trigger candidates:
--     WHERE tier2_status IS NULL AND agreed_score >= 85
--  3. Reflection telemetry:
--     JOIN job_user_actions ON job_id + user_id
