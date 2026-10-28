import hashlib
import json
import logging
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models import OutboxEvent, Payment, PaymentStatus
from payment_service.repositories.payments import PaymentRepository
from payment_service.schemas import PaymentCreate

logger = logging.getLogger(__name__)


class IdempotencyConflict(Exception):
    """The key was previously used for a different payment request."""


class PaymentPersistenceUnavailable(Exception):
    """The payment and outbox row could not be persisted."""


def request_fingerprint(payload: PaymentCreate) -> str:
    canonical = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentApplicationService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)

    async def create(self, payload: PaymentCreate, idempotency_key: str) -> tuple[Payment, bool]:
        """Return (payment, created); all writes share the caller's transaction."""
        fingerprint = request_fingerprint(payload)
        existing = await self._payments.by_idempotency_key(idempotency_key)
        if existing:
            self._ensure_same_request(existing, fingerprint)
            return existing, False

        payment = Payment(
            id=uuid4(),
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            metadata_json=payload.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=str(payload.webhook_url),
        )
        event = OutboxEvent(
            aggregate_id=payment.id,
            event_type="payment.created",
            payload={"payment_id": str(payment.id)},
        )
        self._session.add_all([payment, event])
        try:
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing = await self._payments.by_idempotency_key(idempotency_key)
            if existing:
                self._ensure_same_request(existing, fingerprint)
                return existing, False
            logger.exception("Could not persist payment and outbox event")
            raise PaymentPersistenceUnavailable from None

        await self._session.refresh(payment)
        return payment, True

    @staticmethod
    def _ensure_same_request(payment: Payment, fingerprint: str) -> None:
        if payment.request_fingerprint != fingerprint:
            raise IdempotencyConflict

