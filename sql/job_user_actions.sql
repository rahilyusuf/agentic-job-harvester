-- sql/job_user_actions.sql
-- BigQuery DDL for job_user_actions table.
-- Records HITL telemetry: every Apply / Pass / Analyze action the user takes.
-- Feed for ReflectionAgent active learning flywheel.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET}.job_user_actions` (
    job_id      STRING    NOT NULL,
    user_id     STRING    NOT NULL,
    action      STRING    NOT NULL,   -- apply | pass | analyze
    trace_id    STRING,               -- LangFuse trace ID for the Tier 1 scoring run
    acted_at    TIMESTAMP NOT NULL,
)
PARTITION BY DATE(acted_at)
CLUSTER BY user_id, action
OPTIONS (
    description = "HITL telemetry. Every user action (apply/pass/analyze) on a scored job. Input to ReflectionAgent.",
    require_partition_filter = FALSE,
    partition_expiration_days = 365
);

-- Typical access patterns:
--  1. Accepted jobs for centroid recalc (reflection):
--     WHERE user_id = X AND action = 'apply' AND acted_at >= TIMESTAMP_SUB(...)
--  2. Action counts for rule synthesis:
--     GROUP BY action WHERE user_id = X AND acted_at >= TIMESTAMP_SUB(...)
--  3. LangFuse score correlation:
--     WHERE trace_id IS NOT NULL
