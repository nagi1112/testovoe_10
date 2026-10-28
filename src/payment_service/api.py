import hmac
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import Settings, get_settings
from payment_service.db import get_session
from payment_service.models import Payment
from payment_service.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from payment_service.services.payments import (
    IdempotencyConflict,
    PaymentApplicationService,
    PaymentPersistenceUnavailable,
)

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


async def authenticate(
    x_api_key: Annotated[str, Header(alias="X-API-Key")],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    expected: SecretStr = settings.api_key
    if not expected.get_secret_value() or not hmac.compare_digest(x_api_key, expected.get_secret_value()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


def _accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(payment_id=payment.id, status=payment.status.value, created_at=payment.created_at)


@router.post("", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    payload: PaymentCreate,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
    _: Annotated[None, Depends(authenticate)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    try:
        payment, created = await PaymentApplicationService(session).create(payload, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(status_code=409, detail="Idempotency key was already used with a different request") from None
    except PaymentPersistenceUnavailable:
        raise HTTPException(status_code=503, detail="Payment service temporarily unavailable") from None
    if not created:
        response.status_code = status.HTTP_200_OK
    return _accepted(payment)


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
    _: Annotated[None, Depends(authenticate)],
) -> PaymentRead:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentRead(
        payment_id=payment.id,
        status=payment.status.value,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )

