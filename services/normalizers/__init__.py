"""services/normalizers — Source-specific normalization boundary.

Each module in this package maps a specific scraper output format
into the canonical RawJob model.  The downstream pipeline (deduplication,
BigQuery, Pub/Sub, embedding) only ever sees RawJob.

Adding a new scraper source means adding a new module here without
touching any other part of the pipeline.
"""

