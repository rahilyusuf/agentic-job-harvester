# infra/embedding_worker.Dockerfile
# Cloud Run service: embedding-worker
# Pub/Sub consumer for generating text-embedding-004 vectors.

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

ENV PORT=8080

CMD ["uvicorn", "services.embedding_worker:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
