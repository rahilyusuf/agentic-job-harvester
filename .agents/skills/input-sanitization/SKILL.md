---
name: input-sanitization
description: Modify external-content normalization or prompt construction for scraped jobs, fetched pages, search snippets, and cached research. Use when untrusted text moves toward an LLM; candidate resume privacy is a separate concern.
---

# External content trust boundaries

[Guardrails](../../rules/guardrails.md) G-10 own the sanitization requirement and G-2
own resume privacy. This is the single repository skill for this task. The former
root duplicate was removed after reference/content checks.

## Implemented helpers and missing wiring

Read [input_sanitizer.py](../../../orchestration/input_sanitizer.py) and
[the normalizer](../../../services/normalizers/apify_career_scraper.py).
`sanitize_job_fields(..., wrap=False)` caps/filters/escapes fields for clean storage;
`format_job_for_prompt(job, max_description_chars=3000)` wraps a job for a prompt;
`sanitize_scraped_text()` handles bounded external text with optional wrapping.
Use actual per-field/helper caps rather than treating the embedding description cap
as a universal prompt limit. Escaping happens before truncation/pattern stripping.

**Implemented:** ingestion uses field sanitization before `RawJob` construction.
**Planned enforcement:** agents do not yet use the formatter, and fetched pages/search
snippets lack complete prompt isolation. See S-8 in the
[backlog](../../rules/remediation-backlog.md). Do not describe required call sites as wired.

## Procedure for authorized changes

1. Trace every external field from acquisition to prompt: titles, company/location,
   descriptions, search snippets/URLs, page text, cached research and legacy stored
   records. Database/cache origin does not make external content trusted.
2. Preserve ingestion normalization without inserting XML wrappers into stored titles,
   company names or embedding inputs. Apply prompt isolation again at the LLM boundary;
   ingestion filtering alone cannot establish structural separation.
3. Use shared helpers rather than a new regex blocklist. Escape delimiter breakouts,
   cap input, and put the data-only trust instruction in the actual system message.
   A wrapper helper returning a string does not create a system-role message by itself.
4. Keep page retrieval protections distinct: destination/redirect validation and streamed
   byte/time bounds belong to S-4. Sanitized page text can still come from an unsafe fetch.
5. Do not sanitize candidate resume text with the scraped-data policy. Keep it private
   under G-2 and never expand telemetry to inspect sensitive prompts.

Structural separation and coarse pattern stripping reduce risk; neither guarantees
that a model ignores malicious instructions. Preserve source meaning where possible,
and treat aggressive stripping/truncation as a potential quality regression.

## Validation

Existing [sanitizer tests](../../../tests/orchestration/test_input_sanitizer.py) and
[normalizer tests](../../../tests/services/test_apify_career_scraper.py) exercise helpers.
For implementation wiring, add mocked prompt-construction assertions covering delimiter
breakouts, role markers, long/optional fields, search/cache/page inputs and existing
stored rows. Do not infer end-to-end coverage from helper tests alone.
