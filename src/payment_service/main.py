import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI

from payment_service.api import authenticate, router
from payment_service.config import get_settings
from payment_service.db import engine

settings = get_settings()
logging.basicConfig(level=settings.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()


app = FastAPI(
    title="Async Payment Service",
    version="0.1.0",
    description="Accept payments and process them asynchronously with a transactional outbox.",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/health/live", tags=["health"], dependencies=[Depends(authenticate)])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}

