"""
main.py – aLiGN FastAPI application entry point.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from backend.db import init_db
from backend.routers.intelligence import router as intelligence_router
from backend.tasks import configure_scheduler, scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup: initialise DB and start scheduler. Shutdown: stop scheduler."""
    logger.info("aLiGN startup: initialising database…")
    await init_db()
    logger.info("Database initialised")

    configure_scheduler()
    scheduler.start()
    logger.info("APScheduler started with %d job(s)", len(scheduler.get_jobs()))

    yield

    logger.info("aLiGN shutdown: stopping scheduler…")
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


app = FastAPI(
    title="aLiGN – AI-native Bid & Delivery OS",
    description=(
        "Ingests daily data centre intelligence briefings, enriches signals, "
        "generates touchpoints, and creates social/TikTok drafts."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(intelligence_router)


@app.get("/health", tags=["health"])
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
