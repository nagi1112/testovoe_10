import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage
from sqlalchemy import update

from payment_service.config import get_settings
from payment_service.db import SessionFactory
from payment_service.messaging import (
    DLQ,
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    RETRY_QUEUE,
    create_broker,
)
from payment_service.models import Payment, PaymentStatus
from payment_service.outbox import publish_outbox_forever

settings = get_settings()
logging.basicConfig(level=settings.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
broker = create_broker()
app = FastStream(broker)
outbox_task: asyncio.Task[None] | None = None


async def process_payment(payment_id: UUID) -> None:
    async with SessionFactory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise ValueError(f"Payment {payment_id} does not exist")

    await asyncio.sleep(random.uniform(settings.payment_min_delay_seconds, settings.payment_max_delay_seconds))
    if random.random() < settings.payment_failure_rate:
        raise RuntimeError("Simulated payment gateway failure")

    async with SessionFactory.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))
        )


async def deliver_webhook(payment_id: UUID) -> None:
    async with SessionFactory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None or payment.webhook_delivered_at is not None:
            return
        if payment.webhook_attempts >= settings.retry_max_attempts:
            raise RuntimeError("Webhook delivery attempt limit has been reached")
        payload: dict[str, Any] = {
            "event": "payment.processed",
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }
        webhook_url = payment.webhook_url

        current_attempt = payment.webhook_attempts

    async with SessionFactory.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
    try:
        async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
            response = await client.post(
                webhook_url,
                json=payload,
                headers={"Idempotency-Key": str(payment_id)},
            )
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise RuntimeError("Webhook delivery failed") from exc
    async with SessionFactory.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=datetime.now(UTC), webhook_attempts=current_attempt + 1)
        )


@app.after_startup
async def start_outbox_publisher() -> None:
    global outbox_task
    await broker.declare_queue(RETRY_QUEUE)
    dead_letter_exchange = await broker.declare_exchange(DLX_EXCHANGE)
    dead_letter_queue = await broker.declare_queue(DLQ)
    await dead_letter_queue.bind(dead_letter_exchange, routing_key=DLQ.name)
    outbox_task = asyncio.create_task(publish_outbox_forever(broker), name="outbox-publisher")


@app.on_shutdown
async def stop_outbox_publisher() -> None:
    if outbox_task is not None:
        outbox_task.cancel()
        try:
            await outbox_task
        except asyncio.CancelledError:
            pass


@broker.subscriber(PAYMENTS_QUEUE, exchange=PAYMENTS_EXCHANGE)
async def handle_payment_created(body: dict[str, Any], message: RabbitMessage) -> None:
    payment_id = UUID(body["payment_id"])
    async with SessionFactory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise ValueError(f"Payment {payment_id} does not exist")
        needs_processing = payment.status == PaymentStatus.PENDING

    headers = message.headers or {}
    processing_failed_permanently = bool(headers.get("x-payment-terminal-failure", False))
    if needs_processing:
        try:
            await process_payment(payment_id)
        except Exception:
            headers = message.headers or {}
            attempts = int(headers.get("x-payment-retry-count", 0))
            if attempts < settings.retry_max_attempts - 1:
                await schedule_retry(
                    body,
                    message,
                    headers={**headers, "x-payment-retry-count": attempts + 1},
                    retry_number=attempts + 1,
                    failure_kind="payment processing",
                )
                return

            logger.exception("Payment processing permanently failed after %s attempts", attempts + 1)
            async with SessionFactory.begin() as session:
                await session.execute(
                    update(Payment)
                    .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                    .values(status=PaymentStatus.FAILED, processed_at=datetime.now(UTC))
                )
            processing_failed_permanently = True
            headers = {**headers, "x-payment-terminal-failure": True}

    try:
        await deliver_webhook(payment_id)
    except Exception:
        attempts = int(headers.get("x-webhook-retry-count", 0))
        if attempts < settings.retry_max_attempts - 1:
            await schedule_retry(
                body,
                message,
                headers={**headers, "x-webhook-retry-count": attempts + 1},
                retry_number=attempts + 1,
                failure_kind="webhook delivery",
            )
            return
        logger.exception("Webhook delivery permanently failed after %s attempts", attempts + 1)
        await publish_to_dlq(body, message, headers)
        return

    if processing_failed_permanently:
        await publish_to_dlq(body, message, headers)


async def schedule_retry(
    body: dict[str, Any],
    message: RabbitMessage,
    *,
    headers: dict[str, Any],
    retry_number: int,
    failure_kind: str,
) -> None:
    delay_seconds = 2 ** (retry_number - 1)
    logger.warning(
        "%s failed; scheduling retry %s/%s in %ss",
        failure_kind,
        retry_number + 1,
        settings.retry_max_attempts,
        delay_seconds,
        exc_info=True,
    )
    await broker.publish(
        body,
        queue=RETRY_QUEUE,
        message_id=message.message_id,
        headers=headers,
        expiration=delay_seconds,
        persist=True,
    )


async def publish_to_dlq(
    body: dict[str, Any],
    message: RabbitMessage,
    headers: dict[str, Any],
) -> None:
    await broker.publish(
        body,
        exchange=DLX_EXCHANGE,
        routing_key=DLQ.name,
        message_id=message.message_id,
        headers=headers,
        persist=True,
    )


if __name__ == "__main__":
    import asyncio as _asyncio

    _asyncio.run(app.run())

