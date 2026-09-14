# infra/apify_receiver.Dockerfile
# Cloud Run service: apify-receiver
# Accepts Apify/n8n webhook payloads and ingests to BigQuery.

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Non-root user for security
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Cloud Run listens on PORT env var (default 8080)
ENV PORT=8080

CMD ["uvicorn", "services.apify_receiver:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
