from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel

from payment_service.config import get_settings

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
RETRY_QUEUE = RabbitQueue(
    "payments.retry",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.new",
    },
)
DLX_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
DLQ = RabbitQueue("payments.dead", durable=True)


def create_broker() -> RabbitBroker:
    return RabbitBroker(
        get_settings().rabbitmq_url,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )

