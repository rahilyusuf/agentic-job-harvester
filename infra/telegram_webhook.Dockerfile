# infra/telegram_webhook.Dockerfile
# Cloud Run service: telegram-webhook
# Always-warm (min-instances=1) HITL callback receiver.

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

# min-instances=1 in Cloud Run config to avoid cold start on Telegram callbacks
CMD ["uvicorn", "services.telegram_webhook:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
