---
name: bq-vector-search
description: Guidance on BigQuery native vector search and index operations for candidate job matching. Covers ML.DISTANCE cosine similarity, 768-dimensional text-embedding-004 vectors, IVF index configuration, and retrieval pipeline integration.
---

# BigQuery Native Vector Search Patterns

This skill guides the implementation, optimization, and querying of vector search in `agentic-job-harvester-v2`.

---

## 1. Vector Pipeline Overview

Candidate job retrieval follows a two-stage filter before any agent LLM calls:
1. **Step A — Vector Search:** BigQuery Native Vector Search (`ML.DISTANCE`) retrieves top 50 roles with $<15\text{ms}$ query latency.
2. **Step B — Regex Pre-filter:** Zero-cost filter removes candidates with $>5\text{ YOE}$ or excluded executive titles (Director, VP, Head of), narrowing the pool to $\approx 15$ roles.
3. **Intelligence Layer:** LLM scoring runs *only* over the surviving $\le 15$ roles.

```
Incoming Webhook (Apify)
  └─► Pre-compute 768-dim embedding (text-embedding-004)
        └─► Persist to BigQuery job_embeddings (IVF Cosine Index)
              └─► Tier 1 Step A: ML.DISTANCE cosine search (Top 50)
                    └─► Tier 1 Step B: Regex pre-filter (~15 roles)
                          └─► Tier 1 Multi-Agent Scoring (LLM)
```

---

## 2. Embedding Specification

- **Model:** `text-embedding-004` (768 dimensions).
- **Endpoint:** Always routed through `gateway/client.py::generate_embedding()`, calling LiteLLM proxy `/embeddings`.
- **Input Text Structure:**
  ```python
  embedding_input = f"{title} {company} {location} {description[:2000]}"
  ```
- **Storage:** BigQuery table `job_embeddings` (`embedding ARRAY<FLOAT64>`).

---

## 3. BigQuery Vector Index & SQL Syntax

### Index Creation DDL
The embeddings table is indexed using an Inverted File (IVF) index optimized for cosine distance:
```sql
CREATE VECTOR INDEX IF NOT EXISTS job_embeddings_ivf_idx
ON `${PROJECT_ID}.${DATASET_ID}.job_embeddings`(embedding)
OPTIONS(
    distance_type = 'COSINE',
    index_type = 'IVF'
);
```

### Vector Search Query Pattern
Vector search computes cosine similarity using `1 - ML.DISTANCE`:
```sql
SELECT
    j.job_id,
    1 - ML.DISTANCE(u.e_active_user, e.embedding, 'COSINE') AS similarity_score
FROM
    `${PROJECT_ID}.${DATASET_ID}.job_embeddings` AS e
CROSS JOIN
    `${PROJECT_ID}.${DATASET_ID}.user_profiles` AS u
JOIN
    `${PROJECT_ID}.${DATASET_ID}.raw_job_postings` AS j
    ON e.job_id = j.job_id
WHERE
    u.user_id = @user_id
    AND j.status = 'EMBEDDING_DONE'
ORDER BY
    similarity_score DESC
LIMIT @top_k;
```

---

## 4. User Active Centroid Drift Formula

The candidate's active search vector $e_{\text{active}}$ adapts weekly via the `ReflectionAgent` using confirmed feedback:
$$e_{\text{active, new}} = (1 - \alpha) \cdot e_{\text{resume}} + \alpha \cdot e_{\text{accepted\_centroid}}$$
- $e_{\text{resume}}$: Static baseline embedding of candidate resume.
- $e_{\text{accepted\_centroid}}$: Mean embedding of roles marked `✅ Apply` or accepted by the user over the last 30 days.
- $\alpha$: Centroid drift rate (default: 0.35).

---

## 5. Architectural Guardrails

1. **Pre-computation Discipline:** Embeddings are generated once upon ingestion by `embedding_worker.py`. Never compute embeddings dynamically in agent scoring loops.
2. **Interface Isolation:** Callers depend only on `repositories.interfaces.IEmbeddingRepository.vector_search()`, never concrete BigQuery client classes directly.
3. **90-Day Deduplication Window:** Prior to inserting new raw job postings and generating embeddings, deduplicate jobs by ATS UUID or MD5 URL hash over a rolling 90-day window.

