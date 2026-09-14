# infra/tier1_matcher_runner.Dockerfile
# Cloud Run job: tier1-matcher-runner
# Daily Cloud Scheduler entry point for Tier 1 scoring pipeline.
# Runs as a Cloud Run Job (not a service) — exits after completion.

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Cloud Run Job — runs to completion then exits
CMD ["python", "-m", "services.tier1_matcher_runner"]
