"""
intelligence.py – FastAPI router for ingesting daily data centre intelligence briefings.
POST /api/v1/intelligence/briefing
"""
from __future__ import annotations

import hashlib
import logging
from datetime import date
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, Form, HTTPException, status

from backend.models import BriefingIngestResponse
from backend.services.briefing_parser import GrokBriefingParser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence"])


async def get_parser() -> AsyncGenerator[GrokBriefingParser, None]:
    """Dependency that provides a GrokBriefingParser and ensures it is closed after use."""
    parser = GrokBriefingParser()
    try:
        yield parser
    finally:
        await parser.close()


def _make_doc_id(briefing_text: str, briefing_date: str) -> str:
    """Stable doc_id: 'brief_' + sha256(text)[:16] + '_' + date."""
    hash_hex = hashlib.sha256(briefing_text.encode()).hexdigest()[:16]
    return f"brief_{hash_hex}_{briefing_date}"


@router.post(
    "/briefing",
    response_model=BriefingIngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest a GLOBAL DATA CENTRE INTELLIGENCE BRIEFING",
    description=(
        "Accepts raw briefing text, parses it with Grok, upserts extracted data "
        "to ContractGHOST workspace, enriches signals with tender research, "
        "generates touchpoint suggestions and social drafts."
    ),
)
async def ingest_briefing(
    briefing_text: str = Form(
        ...,
        description="Raw text body of the GLOBAL DATA CENTRE INTELLIGENCE BRIEFING",
    ),
    platform: str = Form(
        default="threads",
        description="Social platform for draft generation: 'threads' or 'x'",
    ),
    parser: GrokBriefingParser = Depends(get_parser),
) -> BriefingIngestResponse:
    """
    Full pipeline:
    1. Parse briefing_text with Grok → ParsedBriefing
    2. Upsert accounts / opportunities / signals / intelligence dataset
    3. Enrich signals with tender/award research
    4. Generate touchpoint suggestions (workspace context-aware)
    5. Generate social drafts from touchpoints
    6. Return counts, doc_id, touchpoints, social_drafts
    """
    if not briefing_text or not briefing_text.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="briefing_text must not be empty",
        )

    logger.info(
        "Received briefing ingest request (text_len=%d, platform=%s)",
        len(briefing_text),
        platform,
    )

    # --- Step 1: Parse ---
    try:
        extracted = await parser.parse(briefing_text)
    except ValueError as exc:
        logger.error("Briefing parse error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse briefing: {exc}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected parse error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error during briefing parsing",
        ) from exc

    briefing_date = extracted.briefing_date or date.today().isoformat()
    doc_id = _make_doc_id(briefing_text, briefing_date)
    logger.info("doc_id=%s briefing_date=%s", doc_id, briefing_date)

    # --- Step 2: Upsert ---
    try:
        await parser.upsert_extracted_data(extracted, doc_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("Upsert error for doc_id=%s: %s", doc_id, exc)
        # Non-fatal – continue pipeline

    # --- Step 3: Enrich with tenders ---
    try:
        await parser.enrich_with_tenders(extracted, briefing_date, doc_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("Tender enrichment error for doc_id=%s: %s", doc_id, exc)
        # Non-fatal – continue pipeline

    # --- Step 4: Touchpoints ---
    touchpoints = []
    try:
        touchpoints = await parser.generate_suggested_touchpoints(extracted)
    except Exception as exc:  # noqa: BLE001
        logger.error("Touchpoint generation error for doc_id=%s: %s", doc_id, exc)

    # --- Step 5: Social drafts ---
    social_drafts = []
    try:
        social_drafts = await parser.generate_social_drafts_from_touchpoints(
            touchpoints, platform=platform
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Social draft generation error for doc_id=%s: %s", doc_id, exc)

    response = BriefingIngestResponse(
        doc_id=doc_id,
        briefing_date=briefing_date,
        accounts_count=len(extracted.accounts),
        opportunities_count=len(extracted.opportunities),
        signals_count=len(extracted.trigger_signals),
        intelligence_count=len(extracted.intelligence_dataset),
        touchpoints=touchpoints,
        social_drafts=social_drafts,
        status="success",
    )

    logger.info(
        "Briefing ingest complete: doc_id=%s accounts=%d opps=%d signals=%d "
        "intel=%d touchpoints=%d drafts=%d",
        doc_id,
        response.accounts_count,
        response.opportunities_count,
        response.signals_count,
        response.intelligence_count,
        len(touchpoints),
        len(social_drafts),
    )
    return response
