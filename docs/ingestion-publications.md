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

## Isolated integration CLI (implemented; not live-verified)

`scripts/verify_publication_ledger.py` never enables production settings. Its `plan`
phase is offline, including credential discovery. Every other phase requires a reviewed
manifest, `--execute`, an exact phase/run confirmation, and a new evidence filename.
The tool refuses existing evidence files, resource overrides and changed source hashes.
It never calls Apify, an embedding provider, Secret Manager, pull, ACK, seek or purge.
Do not use the current `/dev/ingestion/execute`: its allowlist points to the existing
`job_intelligence` dataset and `job-embedding-queue` topic, not these isolated resources.

### Prepare and review (no cloud operations)

Run from the repository root. Save the JSON output in a new, private local directory
outside the Docker context (for example `.local/ledger-it/`). Do not overwrite another
run's manifest or evidence. The following command only prints a plan:

```powershell
.\venv\Scripts\python.exe -m scripts.verify_publication_ledger plan
```

The manifest fixes project `agentic-job-harvester-v2`, location `US`, a fourteen-digit
UTC run ID, dataset `ledger_it_<run>`, topic `ledger-it-<run>`, pull subscription
`ledger-it-<run>-sub`, and service account `ledger-it-<run>@agentic-job-harvester-v2.iam.gserviceaccount.com`.
Names cannot be overridden to select existing production resources. The hash covers
the harness, production ingestion/normalizer/repository contracts, sanitizer, settings,
DDL and dependency requirements. New source requires a newly reviewed manifest.

### Separately authorize provisioning and IAM

Review the manifest before issuing any live command. IAM setup is deliberately manual:
the harness does not grant permissions, enable APIs or create service accounts.

1. An authorized operator creates the exact new test service account; stop if it already
   exists. Verify BigQuery and Pub/Sub APIs/billing without enabling anything implicitly.
2. The operator uses `provision` below with existing operator credentials. It checks all
   names are absent, creates the labeled dataset and two tables using the checked-in
   field contracts, then creates the dedicated topic and pull-only subscription. Table
   expiration is seven days; raw partitions retain the 365-day shape. The subscription
   has a 60-second ACK deadline, 24-hour retention and seven-day inactivity expiration.
3. Grant only the following reviewed scopes. The operator must check inherited roles,
   deny policies and conditions; labels alone are not an IAM isolation boundary.

| Principal | Role / scope |
|---|---|
| New test service account | `roles/bigquery.jobUser` on the project |
| New test service account | `roles/bigquery.dataEditor` on the new dataset only |
| New test service account | `roles/pubsub.publisher` on the new topic only |
| New test service account | `roles/pubsub.viewer` on the new topic and subscription for metadata |
| Approved local operator | `roles/iam.serviceAccountTokenCreator` on the new test account only |

Do not grant Subscriber, Secret Accessor, Vertex access, or production dataset/topic
access. Add the review ticket/reference to `iam_review_reference` in the manifest; `check`
and `run` refuse an empty reference. This is an operator attestation, not automatic
proof of effective IAM. The runtime impersonates the named account; it never falls back
to operator credentials. Provisioning/cleanup use operator credentials explicitly.

Below, replace `<RUN>` and paths with values from the reviewed manifest. These are
live commands for a future authorized session, not commands executed during implementation:

```powershell
.\venv\Scripts\python.exe -m scripts.verify_publication_ledger provision --manifest .local/ledger-it/manifest.json --execute --confirm "PROVISION <RUN>" --evidence .local/ledger-it/provision.jsonl
.\venv\Scripts\python.exe -m scripts.verify_publication_ledger check --manifest .local/ledger-it/manifest.json --execute --confirm "CHECK <RUN>" --evidence .local/ledger-it/check.jsonl
```

Preflight inspects actual isolated metadata: dataset ID/location/expiration, all 13 raw
and 18 ledger fields/types/modes, table ownership/type/partitioning/clustering, streaming
buffers, and the exact exclusive pull subscription. It performs no publication or data
query. Any drift stops the run. Production schema verification is a separate metadata-only
operator step: compare `job_intelligence` location, raw-table contracts/retention and
streaming-buffer conditions with the checked-in DDL. Creating an existing table with
`IF NOT EXISTS` does not verify its schema. Production schemas/IAM remain Unverified
until that evidence has been reviewed; this test identity must not access them.

### Execute in the production runtime

`run` requires Python 3.11, matching the receiver Dockerfile; a host venv on Python 3.14
is sufficient for planning/checking but cannot satisfy the live runtime gate. Use a
separately prepared Python 3.11 environment or the reviewed receiver image with only
the harness mounted read-only at `/app/scripts/verify_publication_ledger.py`. Mount the
manifest read-only, evidence separately writable, and source credentials read-only
outside the image. Do not copy credentials into a build. No dependency installation or
image build is part of running the harness. Preserve the exact source hash/image digest
alongside the evidence.

The `.dockerignore` excludes local environments, `.env` files, credential patterns,
fixtures, tests and the harness. Review the full build context for other sensitive files.
Do not invoke `infra/cloudbuild.yaml` just to build an image: it includes deployment steps.
If a deployment relied on a bundled `.env`, stop rollout until its required settings and
secret references are supplied through reviewed service configuration. Do not restore
local credentials or dotenv files to the image to make startup pass.

```text
python -m scripts.verify_publication_ledger run --manifest <manifest.json> --execute --confirm "RUN <RUN>" --evidence <new-run.jsonl>
```

The suite uses two namespaced synthetic postings and the actual production normalizer,
repository, ingestion composition and default dispatcher with real SDK clients. Only
test-client wrappers inject faults and count SDK calls; no production code or endpoints
are altered. Evidence records query IDs, broker IDs, counters and synthetic snapshots.
It never consumes a subscription or claims worker completion.

| Scenario | Required result |
|---|---|
| Forced pre-COMMIT assertion | HTTP-style 502, zero raw/ledger rows and zero publication |
| First ingestion after rollback | `inserted=2`, `recovered=0`, `published=2`, no failures; both raw rows `NEW_RAW`, ledger `PUBLISHED` |
| Identical repeat | `inserted=0`, `recovered=0`, `published=0`; zero additional SDK sends and unchanged rows/IDs |
| Lost real persistence/checkpoint response | Original query-ID lookup, real committed state recovered, two sends total |
| Real checkpoint contention | An actual transaction-abort response, holder rollback/termination, checkpoint retry with no resend |
| One lease expires before checkpoint | One confirmation remains durable; only the unresolved job republishes on recovery |
| Crash after broker ACK before checkpoint | Both remain `IN_FLIGHT`; test-only lease expiry permits at-least-once recovery, then repeats send zero |
| Concurrent ingestion | Two raw identities, two publication identities, two sends; final repeat sends zero |

Before each independent scenario, the suite archives snapshots and resets only the two
manifest-listed test tables after verifying their job IDs. This is test-fixture setup,
never a production reset mechanism. The subscription retains synthetic messages throughout.
The lock-holder test uses a real BigQuery session, bounds the checkpoint wait to 60 seconds,
and journals its session ID. If interrupted, an operator must inspect that session and
explicitly roll it back/terminate it before cleanup. A conflict that is not actually
observed fails the test; the tool does not substitute a mocked abort.

### Cost, stop conditions and cleanup

Bounds are 100 submitted query jobs, 16 SDK publication attempts and 900 seconds of active
execution. Query configurations set a 1-GiB maximum-bytes limit and a 60-second job timeout;
these service settings are not a guaranteed aggregate spend or cancellation boundary.
BigQuery scripts have child-statement/temporary-storage charges, and an RPC timeout may
leave a job running. Use query IDs to resolve those outcomes before cleanup. Initial cost
target: below US$1, typically cents for these tiny tables, subject to actual billing,
regional pricing, minimum billing units and available allowances. No Apify, embeddings,
LLMs or Cloud Run execution is included. Budget counters are local to one suite invocation;
re-running a suite requires fresh evidence and a renewed review of total spend.

The tool stops on mismatched metadata, unknown jobs/rows, assertion failures, unresolved
queries or exhausted limits. It retains evidence and does not automatically delete failed
resources. After review, resolve outstanding jobs/sessions and authorize cleanup:

```powershell
.\venv\Scripts\python.exe -m scripts.verify_publication_ledger cleanup --manifest .local/ledger-it/manifest.json --execute --confirm "CLEANUP <RUN>" --evidence .local/ledger-it/cleanup.jsonl
```

Cleanup verifies every existing resource label, table name and topic attachment before
deleting anything. It supports partial provisioning, deletes only the dedicated subscription,
topic and dataset, and refuses unexpected tables/subscriptions. Deleting that subscription
removes its synthetic backlog; it never deletes, reads or ACKs existing production messages.
The operator separately removes the temporary IAM bindings and deletes the exact test service
account after reviewing its ownership and remaining use. Keep sanitized evidence outside
the expiring dataset. Cleanup does not undo broker acceptance or prove a timed-out job ended.

### Production activation remains separately approved

Passing this suite does not authorize migration or activation. Pin the reviewed image/source,
verify production schemas/IAM and subscribers, stop legacy writers/reconciliation, and use
the evidence-first 113-job manifest above. Only the production ledger changes: preserve the
113 raw rows/statuses and existing messages; unknown per-job publication history stays UNKNOWN.
Check that none of those historical entries is automatically eligible before enabling the
flag consistently on reviewed ingestion writers. Do not deploy workers or consume the backlog.

Rollback starts by stopping ingress/reconciliation and resolving in-flight query outcomes.
Preserve all ledger evidence. Disabling the flag while accepting traffic restores legacy
NEW_RAW republication; it is not a safe publication rollback. Never clear ledger states,
delete historical rows or purge messages to conceal an incomplete cutover.
