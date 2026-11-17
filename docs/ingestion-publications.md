# Publication recovery rollout

Implemented in code, **not deployed or verified against live BigQuery**. No startup DDL
or automatic migration occurs. `PUBLICATION_LEDGER_ENABLED` defaults to false, retaining
the current ingestion/worker path until the rollout below is explicitly performed.

## Durable contract

With the flag enabled, validated raw rows and `job_publications` intentions commit in
one DML transaction. Its initial `DELETE ... WHERE FALSE` serializes all mutating
transactions on the publication table, including otherwise insert-only batches.
All application writers (single/batch raw inserts and development execution) use this
boundary. Do not write to the ledger using streaming inserts or bypass the adapter.

`PENDING -> IN_FLIGHT -> PUBLISHED` tracks broker confirmation independently of
`RawJob.status`. A new raw row creates PENDING. An existing row without ledger evidence
creates UNKNOWN and is held for review. PUBLISHED never becomes eligible solely because
the raw row remains NEW_RAW. UNKNOWN never automatically republishes.

Claims use server time, a token and a 30-minute default lease. Each dispatcher page
contains at most 100 claims and checkpoints at most ten attempts at a time. Checkpoints
save eligible outcomes independently: one lost lease cannot discard other confirmations.
The same already-PUBLISHED message ID is an idempotent checkpoint success. Published
counters mean durable confirmations, not merely successful broker RPCs.

Before each group, ownership, expiry and raw-row uniqueness are checked against BigQuery.
Server time produces a conservative monotonic deadline, rechecked inside the SDK thread
before publishing. Sends require a ten-second RPC budget plus a thirty-second lease
margin. Dispatch is bounded to 900 seconds; repository operations share the remaining
budget and are capped at 120 seconds. Leases are not renewed automatically. Skipped
claims expire normally and do not count as publication attempts. Pages advance by claim
count, so skipped sends do not starve later pages.

Unconfirmed attempts are deferred until at least the original lease expiry. Five claims
exhaust automatic recovery and hold UNKNOWN with `attempts_exhausted`. Pauses between a
final ownership check and actual broker acceptance cannot be fenced by BigQuery. A crash
after acceptance and before checkpointing can still duplicate publication: **at least once**.

BigQuery queries are submitted with a fixed job ID. On uncertain completion, the adapter
resolves that same project/location/job ID, never blindly replays an append. Only an
original terminal job proving a concurrent-update transaction abort permits a fresh
transaction: at most three attempts, with jitter bounded by one then two seconds. A
checkpoint retry reuses its tokens and broker message IDs without resending messages.
A terminal script error alone does not prove rollback after a possible COMMIT. Unknown
outcomes remain unconfirmed; permanent errors are not automatically retried. The three
transaction attempts are distinct from the five publication-claim attempts.
Reconciliation later drains due claims ordered by creation time and publication ID,
so a successful 100-job page no longer starves a remaining 13-job page.

Publication identity is SHA256 of full topic, canonical job ID, and
`initial_embedding_v1`, separated by newlines. It is independent of webhook/run IDs
and outlives raw retention. Identity checks have no 90-day cutoff; ninety days remains
a freshness/discovery window. A recrawl never replaces original content, scraped_at,
status, or publication evidence. Raw partitions still expire after 365 days, while ledger
identity remains. A retained identity with no raw row fails closed for review rather than
restoring a posting or publishing it again. This version means one initial artifact per
canonical job. Future model/content refresh needs an explicit versioned artifact
contract, not clearing PUBLISHED. Existing duplicate raw IDs are held for investigation;
do not pick arbitrary versions for worker processing. Duplicate-safe raw reads also reject
multiple matches. Strict lifetime/concurrent-insert guarantees require the enabled ledger
path for all writers. With the flag disabled, legacy streaming remains best-effort:
identity checks cover all retained rows, but streaming races, partial writes, repeated
NEW_RAW recovery publication, and forgetting IDs after raw expiry remain unresolved.
Changing topics or processing versions is not an identity reset or a content-refresh policy.

Workers acquire a separate processing token/lease. Busy delivery returns 503, completed
delivery skips, and completion atomically writes the vector, raw status and completion
marker after fencing ownership. A preexisting initial-version vector can be reused to
repair an older worker's write/status gap without calling the embedding provider again.
Provider work is bounded by the smaller of 300 seconds and half the lease; a cancelled
background SDK call might still finish remotely. This does not guarantee exactly-once
paid computation after a crash or lease loss. No providers or model settings change.
Embedding model/content provenance and artifact refresh remain a separate milestone;
this publication correction does not establish that an old vector matches recrawled content.

## Evidence-first migration of the existing 113

1. Audit raw duplicates across all retained partitions; stop on any ambiguity without
   deleting, merging or changing historical rows. Stop legacy publication/reconciliation
   writers during the cutover; keep workers
   disabled until reviewed. Do not mix legacy streaming writers with the new DML path.
2. Review/apply `sql/job_publications.sql` separately. Verify dataset location, IAM,
   raw/vector table schemas and that old raw streaming buffers have drained before
   exercising mutating worker transactions. No commands in this document are executed
   by the application.
3. Obtain an explicitly authorized local export of the exact 113 distinct raw IDs
   with status NEW_RAW. Match it against the dataset manifest and target topic.
4. Produce an evidence file containing reviewed per-job confirmations. Each entry
   requires job_id, topic, publication_confirmed=true and evidence_ref; message_id
   and published_at are optional and must never be invented. Aggregate counts or
   observing some test-subscription messages alone do not establish the exact ID set.
5. Run the offline planner below. An empty evidence list safely holds all 113 UNKNOWN.
   It never connects to GCP, applies SQL, writes raw statuses, publishes or consumes.
6. Review and import the manifest into the new table in one guarded DML transaction,
   without overwriting preexisting evidence. Supply created_at/updated_at at import
   time and attempt_count=0; preserve nullable historical publication timestamps.
7. Validate unique identities and all evidence, then enable the flag consistently on
   ingestion, development execution and worker services in a separately approved rollout.
   Establish authenticated recovery scheduling; the code does not create a Scheduler job.

```powershell
.\venv\Scripts\python.exe -m scripts.plan_publication_migration --jobs raw-export.json --evidence reviewed-evidence.json --topic projects/agentic-job-harvester-v2/topics/job-embedding-queue
```

`RECONCILIATION_SECRET` must be explicitly configured before the HTTP reconciliation
routes accept `Authorization: Bearer ...`; missing configuration returns 503 and
invalid credentials 401, in both flag modes. This requirement is independent of webhook
authentication: existing operator/Scheduler callers must provide this secret. Limits are 1..1000 with pages no larger than 100. Verify Cloud
Run IAM separately; CORS is not authentication. Secrets and payloads are never logged.

The test subscription is separate from any worker subscription. Publication evidence
does not prove current retention, delivery, processing, or successful embeddings.
Do not pull, ACK, purge or replay migration messages as an implicit deployment step.

## Validation limits

Offline tests cover contracts, SQL/parameter construction, fixed-query-ID uncertainty,
leases, repeat/concurrent delivery and checkpoint crashes with mocked adapters. They
cannot prove BigQuery's live transaction contention, IAM, location, service quotas,
message routing or streaming-buffer state. A separately approved isolated integration
test is required before turning this feature on. Preserve the raw fixture and all
independently owned tests. No automatic schema changes, commits, pushes or deployments.
