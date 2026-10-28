# Async Payment Service

An asynchronous payment-processing microservice built with FastAPI, Pydantic v2, SQLAlchemy 2, PostgreSQL, RabbitMQ/FastStream and Alembic. Payment creation and its `payment.created` outbox event are committed in one database transaction. A relay publishes pending outbox rows to RabbitMQ; a single worker processes them, updates payment state and sends a webhook.

## Architecture

```text
Client -> FastAPI -> PostgreSQL (payments + outbox, one transaction)
                         ^                  |
                         |                  v
                    Worker DB <- Outbox relay -> RabbitMQ -> payment consumer
                                                        |       |
                                                        |       +-> webhook URL
                                                        +-> delayed retry -> DLQ
```

- `payments` holds the request, idempotency fingerprint, processing state and webhook delivery state.
- `outbox` is the durable bridge between the payment transaction and RabbitMQ. Publishing is at least once; the consumer is safe to repeat.
- RabbitMQ uses durable queues, publisher confirms, one in-flight payment at a time, a delayed retry queue and a retained `payments.dead` queue for messages that exhaust three processing attempts.
- The worker simulates a gateway delay of 2–5 seconds and a configurable 10% failure rate.
- Webhook POSTs include `event`, payment identifier, status, amount, currency and processing time. The same bounded exponential retry queue retries webhook failures, the attempt counter is persisted on the payment, and the payment ID is sent as the webhook `Idempotency-Key`.

## Run with Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

On PowerShell, use `Copy-Item .env.example .env`. The migration service runs before the API and consumer. API docs are available at <http://localhost:8000/docs>; RabbitMQ management is available at <http://localhost:15672> (`payments` / `payments` by default).

Set non-default `API_KEY`, PostgreSQL password and RabbitMQ credentials in `.env` outside local development. If you change the PostgreSQL credentials or database name, update `DATABASE_URL` to match; if you change the RabbitMQ credentials, update `RABBITMQ_URL` as well. Do not commit `.env`.

## API

All payment endpoints require `X-API-Key`. Payment creation also requires `Idempotency-Key`.

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: order-2026-0001' \
  -d '{
    "amount": "1250.00",
    "currency": "RUB",
    "description": "Order 2026-0001",
    "metadata": {"order_id": "2026-0001"},
    "webhook_url": "https://example.com/payment-webhook"
  }'
```

First submission returns `202 Accepted` with `payment_id`, `status` and `created_at`. A replay with the same key and same request returns the existing payment (`200 OK`); reusing a key with a different request returns `409 Conflict`.

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: local-development-key'
```

The status begins as `pending` and becomes `succeeded` or `failed`. `GET /health/live` is a liveness endpoint and also requires `X-API-Key`.

## Delivery and failure semantics

The database transaction guarantees that a payment cannot be committed without its outbox event. The relay locks unpublished rows with `SKIP LOCKED`, publishes persistent messages with confirms, then marks them published. A crash after publish but before the database commit can create a duplicate delivery; payment processing uses guarded state transitions and webhook-delivery state to make replays safe. This is at-least-once delivery, not a claim of exactly-once processing.

Payment processing and webhook delivery each get up to three attempts by default, with retry delays of 1s and 2s. After processing attempts are exhausted, the payment is marked `failed` and the worker still attempts to send that final result by webhook. Events whose processing or webhook attempts are exhausted remain in the DLQ for inspection or replay. Webhook attempts are persisted so they cannot exceed `RETRY_MAX_ATTEMPTS`. Webhook delivery is at least once: receivers should deduplicate with the supplied payment ID idempotency key.

## Configuration

See `.env.example` for the full list. `PAYMENT_FAILURE_RATE` controls the simulated gateway failure probability; `PAYMENT_MIN_DELAY_SECONDS` and `PAYMENT_MAX_DELAY_SECONDS` control the simulated processing time. `OUTBOX_POLL_INTERVAL_SECONDS` and `OUTBOX_BATCH_SIZE` tune relay throughput.

