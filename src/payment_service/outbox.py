import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from payment_service.config import get_settings
from payment_service.db import SessionFactory
from payment_service.messaging import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from payment_service.models import OutboxEvent

logger = logging.getLogger(__name__)


async def publish_outbox_forever(broker: RabbitBroker) -> None:
    settings = get_settings()
    while True:
        published = 0
        async with SessionFactory() as session:
            events = list(
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(settings.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for event in events:
                try:
                    await broker.publish(
                        event.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=PAYMENTS_QUEUE.name,
                        message_id=str(event.id),
                        persist=True,
                        message_type=event.event_type,
                    )
                except Exception:
                    event.publish_attempts += 1
                    logger.exception("Failed to publish outbox event %s", event.id)
                    break
                else:
                    event.published_at = datetime.now(UTC)
                    published += 1
            if events:
                await session.commit()
        if published == 0:
            await asyncio.sleep(settings.outbox_poll_interval_seconds)

