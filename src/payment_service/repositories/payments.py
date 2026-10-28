from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models import Payment


class PaymentRepository:
    """Читает и записывает модели платежей в транзакции, созданной вызывающим кодом."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def by_idempotency_key(self, key: str) -> Payment | None:
        return await self._session.scalar(select(Payment).where(Payment.idempotency_key == key))

    async def by_id(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

