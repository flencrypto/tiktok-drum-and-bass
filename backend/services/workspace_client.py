"""
workspace_client.py – utility helpers for ContractGHOST workspace interactions,
social draft creation, Gmail draft creation, user settings, and tender research.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select

from backend.config import settings
from backend.db import AsyncSessionLocal, AccountORM, OpportunityORM, TriggerSignalORM

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Idempotency key
# ---------------------------------------------------------------------------


def make_idempotency_key(prefix: str, stable_id: str, date: str) -> str:
    """prefix + sha256(stable_id)[:16] + date, e.g. 'acct_a1b2c3d4e5f60001_2026-03-07'."""
    hash_hex = hashlib.sha256(stable_id.encode()).hexdigest()[:16]
    return f"{prefix}_{hash_hex}_{date}"


# ---------------------------------------------------------------------------
# User settings / autonomy guard
# ---------------------------------------------------------------------------


async def get_user_settings() -> dict[str, Any]:
    """Return current user preferences including autonomy_mode."""
    return {
        "autonomy_mode": settings.autonomy_mode,
        "allow_auto_post": settings.autonomy_mode,
        "allow_auto_send": settings.autonomy_mode,
        "threads_handle": settings.threads_handle,
    }


# ---------------------------------------------------------------------------
# ContractGHOST workspace upserts
# ---------------------------------------------------------------------------


async def _ghost_request(
    method: str, path: str, payload: dict[str, Any]
) -> Optional[dict[str, Any]]:
    """Internal helper – POST/PATCH to ContractGHOST API. Returns response JSON or None."""
    if not settings.ghost_api_url or not settings.ghost_api_key:
        logger.warning(
            "GHOST_API_URL or GHOST_API_KEY not configured – skipping workspace sync for %s",
            path,
        )
        return None
    url = f"{settings.ghost_api_url.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {settings.ghost_api_key}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.request(method, url, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        logger.error(
            "ContractGHOST API HTTP error %s for %s: %s",
            exc.response.status_code,
            url,
            exc.response.text,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("ContractGHOST API error for %s: %s", url, exc)
    return None


async def upsert_account(account_data: dict[str, Any], idempotency_key: str) -> None:
    """Upsert an account to ContractGHOST workspace and local DB."""
    logger.info("Upserting account: %s (key=%s)", account_data.get("name"), idempotency_key)
    await _ghost_request(
        "POST",
        "/accounts/upsert",
        {**account_data, "idempotency_key": idempotency_key},
    )
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AccountORM).where(AccountORM.idempotency_key == idempotency_key)
        )
        existing = result.scalar_one_or_none()
        if existing:
            for field, value in account_data.items():
                if hasattr(existing, field):
                    setattr(existing, field, value)
            logger.info("Updated existing account: %s", account_data.get("name"))
        else:
            session.add(AccountORM(idempotency_key=idempotency_key, **account_data))
            logger.info("Inserted new account: %s", account_data.get("name"))
        await session.commit()


async def upsert_opportunity(
    opp_data: dict[str, Any], idempotency_key: str
) -> None:
    """Upsert an opportunity to ContractGHOST workspace and local DB."""
    logger.info(
        "Upserting opportunity: %s (key=%s)", opp_data.get("name"), idempotency_key
    )
    await _ghost_request(
        "POST",
        "/opportunities/upsert",
        {**opp_data, "idempotency_key": idempotency_key},
    )
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OpportunityORM).where(
                OpportunityORM.idempotency_key == idempotency_key
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            for field, value in opp_data.items():
                if hasattr(existing, field):
                    setattr(existing, field, value)
            logger.info("Updated existing opportunity: %s", opp_data.get("name"))
        else:
            session.add(OpportunityORM(idempotency_key=idempotency_key, **opp_data))
            logger.info("Inserted new opportunity: %s", opp_data.get("name"))
        await session.commit()


async def upsert_signal(
    signal_data: dict[str, Any], idempotency_key: str, doc_id: str
) -> None:
    """Upsert a trigger signal to ContractGHOST workspace and local DB."""
    logger.info(
        "Upserting trigger signal: %s (key=%s)",
        signal_data.get("signal_type"),
        idempotency_key,
    )
    await _ghost_request(
        "POST",
        "/signals/upsert",
        {**signal_data, "idempotency_key": idempotency_key, "doc_id": doc_id},
    )
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TriggerSignalORM).where(
                TriggerSignalORM.idempotency_key == idempotency_key
            )
        )
        existing = result.scalar_one_or_none()
        if not existing:
            session.add(
                TriggerSignalORM(
                    idempotency_key=idempotency_key, doc_id=doc_id, **signal_data
                )
            )
            await session.commit()
            logger.info("Inserted new signal for doc %s", doc_id)
        else:
            logger.info("Signal already exists for key %s – skipping insert", idempotency_key)


async def upsert_intelligence_entry(
    entry_data: dict[str, Any], idempotency_key: str, doc_id: str
) -> None:
    """Upsert an intelligence dataset entry to ContractGHOST workspace and local DB."""
    from backend.db import IntelligenceEntryORM

    logger.info(
        "Upserting intelligence entry: %s (key=%s)",
        entry_data.get("title", "")[:60],
        idempotency_key,
    )
    await _ghost_request(
        "POST",
        "/intelligence/upsert",
        {**entry_data, "idempotency_key": idempotency_key, "doc_id": doc_id},
    )
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select as sa_select

        result = await session.execute(
            sa_select(IntelligenceEntryORM).where(
                IntelligenceEntryORM.idempotency_key == idempotency_key
            )
        )
        existing = result.scalar_one_or_none()
        if not existing:
            session.add(
                IntelligenceEntryORM(
                    idempotency_key=idempotency_key, doc_id=doc_id, **entry_data
                )
            )
            await session.commit()
            logger.info("Inserted new intelligence entry for doc %s", doc_id)
        else:
            logger.info(
                "Intelligence entry already exists for key %s – skipping", idempotency_key
            )


# ---------------------------------------------------------------------------
# Workspace search (for touchpoint context check)
# ---------------------------------------------------------------------------


async def search_workspace_touchpoints(
    account_name: str, days: int = 14
) -> list[dict[str, Any]]:
    """Return touchpoints for account_name within the last `days` days."""
    from sqlalchemy import and_

    from backend.db import TouchpointORM

    cutoff = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Move cutoff back by `days` days
    from datetime import timedelta

    cutoff = cutoff - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TouchpointORM).where(
                and_(
                    TouchpointORM.account_name == account_name,
                    TouchpointORM.created_at >= cutoff,
                )
            )
        )
        rows = result.scalars().all()
    return [
        {
            "account_name": r.account_name,
            "suggested_action": r.suggested_action,
            "reason": r.reason,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


async def get_recent_signals_count(account_name: str, days: int = 30) -> int:
    """Return number of trigger signals for account within the last `days` days."""
    from datetime import timedelta
    from sqlalchemy import and_

    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TriggerSignalORM).where(
                and_(
                    TriggerSignalORM.account_name == account_name,
                    TriggerSignalORM.created_at >= cutoff,
                )
            )
        )
        return len(result.scalars().all())


async def get_opportunity_stage(account_name: str) -> Optional[str]:
    """Return current stage for the most recently updated opportunity for this account."""
    from sqlalchemy import desc

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OpportunityORM)
            .where(OpportunityORM.account_name == account_name)
            .order_by(desc(OpportunityORM.updated_at))
            .limit(1)
        )
        row = result.scalar_one_or_none()
    return row.stage if row else None


async def save_touchpoint(
    touchpoint_data: dict[str, Any], idempotency_key: str
) -> None:
    """Persist a suggested touchpoint locally."""
    from backend.db import TouchpointORM

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TouchpointORM).where(
                TouchpointORM.idempotency_key == idempotency_key
            )
        )
        if result.scalar_one_or_none() is None:
            session.add(
                TouchpointORM(idempotency_key=idempotency_key, **touchpoint_data)
            )
            await session.commit()
            logger.info(
                "Saved touchpoint for account %s", touchpoint_data.get("account_name")
            )


# ---------------------------------------------------------------------------
# Social draft creation (Drafts-first – never auto-post without autonomy_mode)
# ---------------------------------------------------------------------------


async def social_create_draft(
    platform: str,
    content: str,
    image_url: Optional[str] = None,
    video_url: Optional[str] = None,
    hashtags: Optional[list[str]] = None,
) -> Optional[str]:
    """
    Create a social media draft. Returns a draft_id if the platform API is
    configured; otherwise logs the draft and returns a local placeholder ID.
    Respects drafts-first: never auto-posts unless autonomy_mode is enabled
    AND the caller has confirmed.
    """
    user_settings = await get_user_settings()
    hashtag_str = " ".join(hashtags or [])
    full_content = f"{content}\n\n{hashtag_str}".strip()

    logger.info(
        "Creating %s draft (autonomy_mode=%s): %s…",
        platform,
        user_settings["autonomy_mode"],
        full_content[:80],
    )

    if platform.lower() == "threads" and settings.threads_access_token:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                payload: dict[str, Any] = {
                    "media_type": "TEXT",
                    "text": full_content,
                }
                if image_url:
                    payload["media_type"] = "IMAGE"
                    payload["image_url"] = image_url
                resp = await client.post(
                    f"https://graph.threads.net/v1.0/{settings.threads_user_id}/threads",
                    params={"access_token": settings.threads_access_token},
                    json=payload,
                )
                resp.raise_for_status()
                draft_id = resp.json().get("id", "threads_draft_ok")
                logger.info("Threads draft created: %s", draft_id)
                return draft_id
        except Exception as exc:  # noqa: BLE001
            logger.error("Threads draft API error: %s", exc)

    if platform.lower() == "tiktok" and settings.tiktok_access_token:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://open.tiktokapis.com/v2/post/publish/video/init/",
                    headers={
                        "Authorization": f"Bearer {settings.tiktok_access_token}",
                        "Content-Type": "application/json; charset=UTF-8",
                    },
                    json={
                        "post_info": {
                            "title": full_content[:150],
                            "privacy_level": "SELF_ONLY",
                            "disable_duet": False,
                            "disable_comment": False,
                            "disable_stitch": False,
                        },
                        "source_info": {
                            "source": "PULL_FROM_URL",
                            "video_url": video_url or "",
                        },
                    },
                )
                resp.raise_for_status()
                draft_id = resp.json().get("data", {}).get("publish_id", "tiktok_draft_ok")
                logger.info("TikTok draft created: %s", draft_id)
                return draft_id
        except Exception as exc:  # noqa: BLE001
            logger.error("TikTok draft API error: %s", exc)

    # Fallback – log the draft with a synthetic ID
    synthetic_id = f"local_{platform}_{hashlib.sha256(full_content.encode()).hexdigest()[:12]}"
    logger.info("Social draft saved locally (%s): %s", platform, synthetic_id)
    return synthetic_id


# ---------------------------------------------------------------------------
# Gmail draft creation
# ---------------------------------------------------------------------------


async def gmail_draft_email(
    to: str,
    subject: str,
    body: str,
) -> Optional[str]:
    """
    Create a Gmail draft. Returns the draft ID if Gmail API is configured.
    Never auto-sends – always draft only.
    """
    user_settings = await get_user_settings()
    logger.info(
        "Creating Gmail draft to=%s subject=%s (autonomy_mode=%s)",
        to,
        subject,
        user_settings["autonomy_mode"],
    )

    if not all(
        [
            settings.google_client_id,
            settings.google_client_secret,
            settings.google_refresh_token,
        ]
    ):
        logger.warning("Gmail credentials not configured – draft logged only")
        logger.info("DRAFT to=%s | subject=%s | body=%s…", to, subject, body[:200])
        return None

    try:
        import base64
        import email as email_lib
        from email.mime.text import MIMEText

        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds = Credentials(
            token=None,
            refresh_token=settings.google_refresh_token,
            client_id=settings.google_client_id,
            client_secret=settings.google_client_secret,
            token_uri="https://oauth2.googleapis.com/token",
        )
        service = build("gmail", "v1", credentials=creds)
        mime_msg = MIMEText(body, "plain")
        mime_msg["to"] = to
        mime_msg["subject"] = subject
        raw = base64.urlsafe_b64encode(mime_msg.as_bytes()).decode()
        draft = service.users().drafts().create(  # type: ignore[attr-defined]
            userId="me", body={"message": {"raw": raw}}
        ).execute()
        draft_id = draft.get("id", "gmail_draft_ok")
        logger.info("Gmail draft created: %s", draft_id)
        return draft_id
    except Exception as exc:  # noqa: BLE001
        logger.error("Gmail draft creation error: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Tender research
# ---------------------------------------------------------------------------


async def research_tenders_and_awards(
    company_name: str,
    location: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Search for recent tenders and contract awards related to company_name.
    Scrapes Find a Tender (UK) and returns structured results.
    """
    query_parts = [company_name, "data centre", "tender", "award"]
    if location:
        query_parts.append(location)
    query = " ".join(query_parts)

    results: list[dict[str, Any]] = []

    # DuckDuckGo HTML search (no API key required)
    search_url = "https://html.duckduckgo.com/html/"
    try:
        async with httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; aLiGN-Bot/1.0)"},
            follow_redirects=True,
        ) as client:
            resp = await client.post(search_url, data={"q": query})
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for result_div in soup.select(".result")[:5]:
                title_el = result_div.select_one(".result__title")
                snippet_el = result_div.select_one(".result__snippet")
                link_el = result_div.select_one(".result__url")
                if title_el and snippet_el:
                    results.append(
                        {
                            "title": title_el.get_text(strip=True),
                            "snippet": snippet_el.get_text(strip=True),
                            "url": link_el.get_text(strip=True) if link_el else None,
                            "company": company_name,
                            "location": location,
                        }
                    )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Tender research search failed for %s: %s", company_name, exc
        )

    logger.info(
        "Tender research for %s: found %d results", company_name, len(results)
    )
    return results
