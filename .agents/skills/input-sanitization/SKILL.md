---
name: input-sanitization
description: Use when writing or modifying any code path that takes external
  scraped/webhook content (job postings from Apify, pages from fetch_url) and passes
  it toward an LLM prompt. Covers orchestration/input_sanitizer.py and its call
  sites.
---

# Input Sanitization Patterns (this project)

## Why this exists
Two places in this pipeline pull **untrusted external text** directly toward an LLM:
1. `apify_receiver` — job postings scraped by Apify/n8n. Anyone who can get a job
   posting indexed by the sites you scrape can put arbitrary text in the title,
   company name, or description fields.
2. `CompanyResearchAgent`'s `fetch_url` tool — arbitrary web pages fetched during the
   ReAct loop.

Neither is adversarially reviewed before it reaches this system. Treat both as
**untrusted input, not trusted data** — the same posture you'd take with user-
submitted text in a normal web app, just applied to scraped content instead.

## What "sanitize" means here
This is **not** a content-moderation filter and not an attempt to perfectly detect
every injection attempt — that's not achievable and isn't the goal. The goal is
defense in depth across three layers:

1. **Structural separation.** When scraped text is placed into a prompt, it must be
   clearly delimited from instructions — e.g. wrapped in an explicit tagged block
   (`<scraped_content>...</scraped_content>`) with an explicit system-level
   instruction that content inside that block is *data to extract facts from*, never
   *instructions to follow*. This is the single highest-leverage defense — model
   providers' safety training generally respects clear structural separation between
   system instructions and user/tool-provided data.
2. **Length capping.** Cap scraped text length before it reaches a prompt (this
   project already truncates description to 2000 chars for embeddings — apply an
   equivalent cap for any scraped text going into an agent prompt, not just
   embeddings). Shorter untrusted spans are easier to structurally isolate.
3. **Pattern-level stripping, not keyword blocklisting.** Strip or neutralize
   obvious structural injection patterns (e.g. text that looks like it's trying to
   open a new instruction block, role markers, "ignore previous" style phrasing) as
   a coarse pre-filter. This is a supplement to layer 1, not a replacement — don't
   rely on pattern matching alone, it's trivially bypassable and that's fine, because
   layer 1 is the real defense.

## Where this runs
- `apify_receiver`: sanitize job title/company/description **before** they're
  written to `raw_job_postings`, so every downstream consumer (embedding worker,
  ScorerAgent, SkepticAgent) inherits already-sanitized text rather than each having
  to re-implement this.
- `CompanyResearchAgent`'s `fetch_url` tool: sanitize the fetched page content
  **before** it's added to the ReAct loop's context — this one can't be sanitized
  once at ingestion since it's fetched live during the agent run, so the tool
  function itself must call `orchestration/input_sanitizer.py` before returning
  content to the agent.

## What NOT to do
- Don't skip sanitization because "it's just a job posting site, not a user-facing
  form" — the threat model is about what text *reaches an LLM*, not who's typing it.
- Don't build a giant regex blocklist and call it done — see layer 1 above, the
  structural separation is what actually matters; the pattern stripping is a cheap
  supplement, not the core defense.
- Don't sanitize `resume_text` through this path — that's trusted candidate-provided
  content with its own handling rule (never logged, see AGENTS.md constraint #3).
  This skill is specifically about *external, scraped* content.

## Testing
- Add fixtures in `tests/orchestration/` with deliberately adversarial scraped text
  (fake instruction-injection attempts in a job description) and assert the
  sanitizer output still structurally isolates it — this is exactly the kind of
  regression that should be caught in CI, not discovered in production.
