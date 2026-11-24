# Architecture diagrams

Created with the built-in image_gen tool after read-only repository inspection. No fallback CLI, API key, project model calls, or deployment was used.

- **high-level-design.png**: overall project workflow and supporting systems.
- **low-level-design.png**: component responsibilities, module locations, data contracts and integration gaps.

Blue components indicate implemented source, not confirmed cloud deployment. Amber indicates unfinished integration or verification. These are illustrative technical diagrams; repository source and canonical architecture contracts remain authoritative.

## Reading notes

- Model gateway connections represent agent/model interaction, not an automatic business trigger.
- Matching runs separately and reads persisted embeddings; it does not receive an immediate Vertex response.
- Human Analyze authorizes the Tier 2 trigger. The dedicated consumer and durable results path are incomplete.
- The ledger is implemented but opt-in and defaults off. Publication remains at least once.
- Raw-job lifecycle state belongs to raw_job_postings, not job_embeddings.
- Direct Vertex embeddings remain an unresolved deviation from the proxy-only target.
- Cross-service trace propagation, model routing/export in deployment, schedules and effective IAM remain unverified.
- Google ADK is planned, not the current orchestration runtime.

## Evidence inspected

- ../../../ARCHITECTURE.md
- ../../../PROJECT_STRUCTURE.md
- ../../../services/apify_receiver.py
- ../../../services/embedding_worker.py
- ../../../services/user_profile_service.py
- ../../../services/tier1_matcher_runner.py
- ../../../orchestration/pipeline.py
- ../../../orchestration/hitl_controller.py
- ../../../agents/reflection_agent.py
- ../../../repositories/embeddings_repo.py
- ../../../gateway/client.py
- ../../../observability/setup.py

## Prompt specifications

### HLD generation brief

The following is the condensed generation specification; the exact final correction prompt follows.

```text
Use case: infographic-diagram.
Asset: high-level technical architecture poster for Agentic Job Harvester.
Style: readable landscape layout, white background, navy text, blue/teal implemented components, amber dashed unfinished integration.
Show six zones: Acquire, Embed, Match, Decide, Investigate, Learn.
Acquire: external Apify Actor -> authenticated FastAPI receiver -> verified dataset fetch -> normalization/deduplication -> BigQuery raw jobs and Pub/Sub embedding queue.
Embed: queue -> embedding worker -> current direct Vertex adapter -> BigQuery embeddings. Live processing remains unverified.
Match: user profile/resume vector and BigQuery exact cosine top 50 -> prefilter cap 15 -> Python Tier 1 Router, Scorer, Skeptic, at most two rounds -> evaluations.
Decide: Telegram Apply/Pass/Analyze and durable action records. Tier 1 notification wiring is incomplete and dashed.
Investigate: user Analyze -> Tier 2 queue -> missing consumer integration (dashed) -> implemented Research followed by parallel Verify/Red Flags/Skill Gap, with E2B challenges.
Learn: Reflection reads actions/accepted embeddings, updates active profile vector and Firestore preference rules. Do not claim the runner automatically loads preference rules.
Shared BigQuery store: raw jobs, embeddings, profiles, evaluations, actions. Firestore only research cache and preference rules/configuration.
Cross-cutting model gateway: Instructor + LiteLLM proxy -> Gemini Flash/Pro, associated with Tier 1, Tier 2 and Reflection, not the direct Vertex embedding path.
Observability: structured ingestion logging + privacy-safe Langfuse spans; one gateway generation = one logical invocation, live exports unverified.
Ledger is opt-in/default OFF; delivery at least once, not exactly once. Current runtime Python async; Google ADK planned.
Legend separates implemented source, incomplete wiring and unverified live behavior.
```

### HLD final correction prompt

```text
Edit this architecture diagram with a precise wiring correction only. Preserve the title, layout, colors, all boxes and labels, typography, and the implemented/planned legend.
1. Remove the solid blue model-gateway arrow that points into zone 4 "Decide". Telegram/HITL does not invoke the LLM gateway. Instead draw clear thin blue model-access connections from the Model gateway side rail to the Tier 1 runner/agent area in zone 3 "Match", the Research/parallel agents area in zone 5, and Reflection in zone 6. These are model-access dependencies, not arrows from Gemini that automatically trigger workflows. Label the small connections "Model calls" if needed. Do not connect the model gateway to the direct Vertex embedding adapter.
2. Add an explicit directional connector labelled "Analyze" from the Telegram/human feedback box to the Tier 2 queue in zone 5. Only the user Analyze action sends this trigger. Preserve the AMBER DASHED connector for the missing consumer after the Tier 2 queue.
Do not introduce any automatic score-to-Tier-2 connector. All other labels, component placement, readability and meaning must remain unchanged.
```

### LLD generation prompt

```text
Use case: infographic-diagram.
Create a precise low-level software architecture poster for "Agentic Job Harvester".
Wide landscape, very legible engineering design, clean white background, navy headings, pale blue component cards, amber dashed caveat labels. No decorative illustrations or big icons. Show a THREE-COLUMN BY THREE-ROW GRID of nine independent component cards. IMPORTANT: NO arrows between the cards; this is a component-contract map, not an execution graph. Short internal sequences may use arrows inside a card only. Generous gutters. Each card has a friendly title, a monospace source filename, and concise input/process/output lines. Render text accurately, favor large text over density.
Title: "Agentic Job Harvester ? Low-Level Design"
Subtitle: "Component responsibilities and contracts"

Card 1 title "1. Ingestion"
Filename "services/apify_receiver.py"
Lines: "Input: Apify webhook event"
"Authenticate ? verify run + dataset ? fetch"
"normalize_batch: validated RawJob + stable-ID dedupe"
"Output: ordered unique jobs + counters"

Card 2 title "2. Raw persistence"
Filenames "repositories/raw_jobs_repo.py" and "repositories/publications_repo.py"
Lines: "Default: retained-ID check + streaming insert"
"Opt-in: atomic raw row + publication intent"
"BigQuery: raw_job_postings / job_publications"
Amber badge: "Ledger defaults OFF; live SQL unverified"

Card 3 title "3. Publication"
Filename "services/publication_dispatcher.py"
Lines: "Ledger: claim ? ownership ? send ? checkpoint"
"Pub/Sub payload: { job_id }"
"Authenticated reconciliation supports recovery"
"At-least-once; broker ACK ? worker completion"

Card 4 title "4. Embedding and profiles"
Filenames "embedding_worker.py / user_profile_service.py"
Lines: "Independent callers of gateway.generate_embedding"
"Current provider: Vertex text-embedding-004"
"Output: 768-dimensional vectors"
"BigQuery: job_embeddings / user_profiles"
Small note: "Raw lifecycle belongs to raw_job_postings"

Card 5 title "5. Retrieval"
Filename "services/tier1_matcher_runner.py"
Lines: "Scheduled independently; reads stored vectors"
"vector_matcher + embeddings_repo"
"Exact cosine ML.DISTANCE: top 50"
"regex_prefilter: cap 15 candidates"

Card 6 title "6. Tier 1 intelligence"
Filename "orchestration/pipeline.py: Tier1Pipeline"
Lines: "Router ? Scorer ? Skeptic"
"Maximum 2 rounds; Skeptic final score"
"Validated output ? BigQuery job_evaluated"
Amber badge: "Notification wiring incomplete"

Card 7 title "7. Human feedback"
Filename "orchestration/hitl_controller.py"
Lines: "Telegram: Apply / Pass / Analyze"
"All actions ? BigQuery job_user_actions"
"User Analyze ? tier2-analysis-queue"
Amber badge: "Dedicated Tier 2 consumer missing"

Card 8 title "8. Tier 2 due diligence"
Filename "orchestration/pipeline.py: Tier2Pipeline"
Lines: "Research ? parallel Verify / Red Flags / Skill Gap"
"Tools: search, fetch, history, Firestore cache"
"Generated challenges execute in E2B"
Amber badge: "Callable exists; durable results incomplete"

Card 9 title "9. Feedback reflection"
Filename "agents/reflection_agent.py"
Lines: "Reads actions + accepted-job embeddings"
"Updates user_profiles active vector"
"Writes preference rules to Firestore"
Amber badge: "Schedule and end-to-end quality unverified"

Bottom shared-contract band, three cells:
"schemas/: Pydantic validation"
"repositories/interfaces.py: persistence contracts"
"config/settings.py: configuration + secrets"

Bottom model/telemetry band:
"Agent calls: GatewayClient.create ? Instructor ? LiteLLM proxy ? Gemini Flash / Pro"
"Vertex embedding adapter currently bypasses proxy"
"Observability: safe Langfuse agent spans + logical gateway generations; structured ingestion diagnostics"

Legend: "Blue = implemented source component" / "Amber dashed = incomplete or unverified integration".
Footer: "Python async orchestration today ? Google ADK planned ? No claim of live deployment or exactly-once processing".
Constraints: NO cross-card arrows, no automatic Tier 2, no invented services or new databases, no raw resume storage claim, no VECTOR_SEARCH claim. Keep text crisp and comfortably readable.
```

### LLD final correction prompt

```text
Edit ONLY the amber legend text at the bottom of this technical poster. Replace it with the exact text "Amber dashed = integration pending". Preserve every other label, all nine component cards, all colors, the layout, title, typography, and footer unchanged. Do not change any architecture content. The legend must NOT say verified; use exactly "Amber dashed = integration pending".
```
