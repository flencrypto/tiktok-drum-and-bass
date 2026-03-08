"""
tasks.py – APScheduler jobs for aLiGN daily intelligence + DnB TikTok tasks.

Jobs:
  07:30 UTC – run_daily_briefing()  → fetch Gmail → parse → upsert → drafts
  08:00 UTC – run_drum_and_bass_daily() → trending DnB search → image/video → TikTok draft
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup

from backend.config import settings
from backend.services.briefing_parser import GrokBriefingParser
from backend.services.workspace_client import (
    get_user_settings,
    gmail_draft_email,
    make_idempotency_key,
    social_create_draft,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scheduler (singleton – initialised in main.py)
# ---------------------------------------------------------------------------

scheduler = AsyncIOScheduler(timezone="UTC")

# ---------------------------------------------------------------------------
# Gmail helper
# ---------------------------------------------------------------------------

_BRIEFING_SUBJECT_KEYWORD = "GLOBAL DATA CENTRE INTELLIGENCE BRIEFING"


async def fetch_latest_briefing_from_gmail() -> Optional[str]:
    """
    Search Gmail for the most recent unread GLOBAL DATA CENTRE INTELLIGENCE BRIEFING.
    Returns the plain-text body of the email, or None if not found / not configured.
    Marks the email as read after extraction.
    """
    if not all(
        [
            settings.google_client_id,
            settings.google_client_secret,
            settings.google_refresh_token,
        ]
    ):
        logger.warning("Gmail credentials not configured – cannot fetch briefing email")
        return None

    try:
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

        # Search for unread briefing emails
        query = f'subject:"{_BRIEFING_SUBJECT_KEYWORD}" is:unread'
        logger.info("Searching Gmail with query: %s", query)
        result = (
            service.users()  # type: ignore[attr-defined]
            .messages()
            .list(userId="me", q=query, maxResults=1)
            .execute()
        )
        messages = result.get("messages", [])
        if not messages:
            logger.info("No unread briefing emails found in Gmail")
            return None

        msg_id = messages[0]["id"]
        logger.info("Found briefing email id=%s", msg_id)

        # Fetch full message
        msg = (
            service.users()  # type: ignore[attr-defined]
            .messages()
            .get(userId="me", id=msg_id, format="full")
            .execute()
        )

        body = _extract_email_body(msg)
        if not body:
            logger.warning("Could not extract body from email id=%s", msg_id)
            return None

        # Mark as read
        service.users().messages().modify(  # type: ignore[attr-defined]
            userId="me",
            id=msg_id,
            body={"removeLabelIds": ["UNREAD"]},
        ).execute()
        logger.info("Marked email id=%s as read", msg_id)

        return body

    except Exception as exc:  # noqa: BLE001
        logger.error("Gmail fetch error: %s", exc)
        return None


def _extract_email_body(msg: dict[str, Any]) -> Optional[str]:
    """Recursively extract plain-text body from a Gmail message payload."""
    payload = msg.get("payload", {})
    return _parse_part(payload)


def _parse_part(part: dict[str, Any]) -> Optional[str]:
    """Recursively walk MIME parts to find text/plain content."""
    mime_type = part.get("mimeType", "")
    parts = part.get("parts", [])

    if mime_type == "text/plain":
        data = part.get("body", {}).get("data", "")
        if data:
            decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
            return decoded.strip()

    for sub_part in parts:
        result = _parse_part(sub_part)
        if result:
            return result

    return None


# ---------------------------------------------------------------------------
# Briefing ingest helper (shared by scheduler and router)
# ---------------------------------------------------------------------------


async def ingest_daily_briefing(briefing_text: str) -> dict[str, Any]:
    """
    Full ingest pipeline: parse → upsert → enrich → touchpoints → social drafts.
    Returns a summary dict.
    """
    parser = GrokBriefingParser()
    try:
        extracted = await parser.parse(briefing_text)
        briefing_date = extracted.briefing_date or date.today().isoformat()
        doc_id = f"brief_{hashlib.sha256(briefing_text.encode()).hexdigest()[:16]}_{briefing_date}"

        await parser.upsert_extracted_data(extracted, doc_id)
        await parser.enrich_with_tenders(extracted, briefing_date, doc_id)

        touchpoints = await parser.generate_suggested_touchpoints(extracted)
        social_drafts = await parser.generate_social_drafts_from_touchpoints(
            touchpoints, platform="threads"
        )

        logger.info(
            "ingest_daily_briefing complete: doc_id=%s accounts=%d opps=%d signals=%d "
            "touchpoints=%d drafts=%d",
            doc_id,
            len(extracted.accounts),
            len(extracted.opportunities),
            len(extracted.trigger_signals),
            len(touchpoints),
            len(social_drafts),
        )
        return {
            "doc_id": doc_id,
            "briefing_date": briefing_date,
            "accounts": len(extracted.accounts),
            "opportunities": len(extracted.opportunities),
            "signals": len(extracted.trigger_signals),
            "touchpoints": len(touchpoints),
            "social_drafts": len(social_drafts),
        }
    finally:
        await parser.close()


# ---------------------------------------------------------------------------
# Fallback Threads notification
# ---------------------------------------------------------------------------


async def _send_fallback_threads_notification() -> None:
    """Post an engaging Threads draft to @TheMrFlen when no briefing is found."""
    handle = settings.threads_handle
    today = date.today().strftime("%d %b %Y")
    content = (
        f"📡 {handle} – No GLOBAL DATA CENTRE INTELLIGENCE BRIEFING detected in Gmail "
        f"for {today}. "
        f"Drop the briefing text via the API or email it in to keep the intelligence pipeline running. "
        f"aLiGN is standing by 🔄"
    )
    hashtags = ["#DataCentre", "#aLiGN", "#IntelligenceBriefing", "#Infrastructure"]
    try:
        draft_id = await social_create_draft(
            platform="threads",
            content=content,
            hashtags=hashtags,
        )
        logger.info("Fallback Threads notification draft created: %s", draft_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to create fallback Threads notification: %s", exc)


# ---------------------------------------------------------------------------
# Scheduled job 1: 07:30 UTC – daily briefing
# ---------------------------------------------------------------------------


async def run_daily_briefing() -> None:
    """
    Scheduled at 07:30 UTC.
    1. Fetch latest briefing from Gmail
    2. If found → ingest_daily_briefing()
    3. If not found → trigger fallback Threads notification to @TheMrFlen
    """
    logger.info("=== run_daily_briefing starting (07:30 UTC) ===")

    briefing_text = await fetch_latest_briefing_from_gmail()

    if not briefing_text:
        logger.warning("No briefing email found – triggering fallback Threads notification")
        await _send_fallback_threads_notification()
        return

    logger.info("Briefing email fetched (%d chars) – ingesting…", len(briefing_text))
    try:
        summary = await ingest_daily_briefing(briefing_text)
        logger.info("Daily briefing ingested successfully: %s", summary)
    except Exception as exc:  # noqa: BLE001
        logger.error("Daily briefing ingest failed: %s", exc)
        await _send_fallback_threads_notification()


# ---------------------------------------------------------------------------
# DnB TikTok helpers
# ---------------------------------------------------------------------------


async def _web_search_dnb_trends() -> list[dict[str, str]]:
    """
    Search for trending drum and bass tracks / viral TikTok DnB sounds.
    Uses DuckDuckGo HTML search (no API key required) + BeautifulSoup parsing.
    """
    query = "trending drum and bass track 2026 OR viral DnB TikTok sound"
    results: list[dict[str, str]] = []

    try:
        async with httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; aLiGN-Bot/1.0)"},
            follow_redirects=True,
        ) as client:
            resp = await client.post(
                "https://html.duckduckgo.com/html/",
                data={"q": query},
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for div in soup.select(".result")[:6]:
                title_el = div.select_one(".result__title")
                snippet_el = div.select_one(".result__snippet")
                if title_el:
                    results.append(
                        {
                            "title": title_el.get_text(strip=True),
                            "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
                        }
                    )
    except Exception as exc:  # noqa: BLE001
        logger.warning("DnB web search failed: %s", exc)

    logger.info("DnB trend search returned %d results", len(results))
    return results


async def _generate_dnb_image_with_grok(trend_context: str) -> Optional[str]:
    """
    Call xAI image generation (if supported) with a high-energy rave prompt.
    Returns image URL or None.
    """
    if not settings.xai_api_key:
        logger.warning("XAI_API_KEY not set – skipping DnB image generation")
        return None

    image_prompt = (
        "High-energy drum and bass rave, massive crowd, neon laser lights, "
        "bass bins stacked floor to ceiling, DJ silhouette on stage, "
        "9:16 portrait aspect ratio, cinematic, ultra-vibrant, "
        f"2026 festival vibes. Inspired by: {trend_context[:120]}"
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{settings.xai_base_url.rstrip('/')}/images/generations",
                json={
                    "model": "grok-2-image",
                    "prompt": image_prompt,
                    "n": 1,
                    "size": "1024x1792",
                },
                headers={
                    "Authorization": f"Bearer {settings.xai_api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            image_url = resp.json()["data"][0].get("url")
            logger.info("DnB image generated: %s", image_url)
            return image_url
    except Exception as exc:  # noqa: BLE001
        logger.warning("DnB image generation failed: %s", exc)
        return None


def _build_dnb_post_text(trend_results: list[dict[str, str]], today: str) -> str:
    """Compose a punchy TikTok/Threads post text for the DnB daily."""
    if trend_results:
        top_title = trend_results[0]["title"]
        # Strip HTML tags if any
        top_title = re.sub(r"<[^>]+>", "", top_title).strip()
        top_title = top_title[:80]
    else:
        top_title = "drum and bass is running the underground"

    return (
        f"🥁💥 {today} – {top_title}\n\n"
        f"The bass don't stop. Here's what's moving the DnB scene RIGHT NOW 👇\n\n"
        f"Drop your favourite track below 🎵⬇️"
    )


def _build_dnb_video_prompt(trend_context: str, today: str) -> str:
    """Build a 10-second video prompt for beat-matched DnB content."""
    return (
        f"10-second vertical video (9:16). Beat-matched quick cuts and zoom bursts "
        f"synced to a 174bpm drum and bass drop. Neon-lit rave crowd, laser beams, "
        f"DJ hands on decks, strobe flashes. Text overlay: '{today} // DAILY DnB'. "
        f"Trending audio callout: add popular DnB sound from TikTok library. "
        f"Inspired by: {trend_context[:100]}. Ultra high energy, cinematic, loopable."
    )


# ---------------------------------------------------------------------------
# Scheduled job 2: 08:00 UTC – DnB TikTok daily
# ---------------------------------------------------------------------------


async def run_drum_and_bass_daily() -> None:
    """
    Scheduled at 08:00 UTC.
    1. Web search trending DnB tracks / viral TikTok sounds
    2. Generate rave image via Grok (xAI)
    3. Build 10s video prompt (beat-matched)
    4. Create TikTok draft (+ Threads fallback notification)
    5. Drafts-first – never auto-posts without autonomy_mode
    """
    logger.info("=== run_drum_and_bass_daily starting (08:00 UTC) ===")

    user_settings = await get_user_settings()
    today_str = date.today().strftime("%d %b %Y")

    # Step 1: Web search
    trend_results = await _web_search_dnb_trends()
    trend_context = (
        " | ".join(r["title"] for r in trend_results[:3]) if trend_results else "drum and bass 2026"
    )

    # Step 2: Generate image
    image_url = await _generate_dnb_image_with_grok(trend_context)

    # Step 3: Build post text + prompts
    post_text = _build_dnb_post_text(trend_results, today_str)
    video_prompt = _build_dnb_video_prompt(trend_context, today_str)
    hashtags = ["#DnB", "#Jungle", "#BassMusic", "#TrendingSound", "#DrumAndBass", "#Rave"]

    # Step 4: TikTok draft
    logger.info("Creating TikTok draft for DnB daily post")
    try:
        tiktok_draft_id = await social_create_draft(
            platform="tiktok",
            content=post_text,
            image_url=image_url,
            hashtags=hashtags,
        )
        logger.info("TikTok DnB draft created: %s", tiktok_draft_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("TikTok draft creation failed: %s", exc)
        tiktok_draft_id = None

    # Step 5: Threads notification draft (always created as companion)
    threads_content = (
        f"🎵 {today_str} DnB drop incoming 🥁💥\n\n"
        f"Today's vibe: {trend_context[:120]}\n\n"
        f"TikTok post is ready – check the draft! "
        f"{'Auto-post enabled ✅' if user_settings['allow_auto_post'] else 'Awaiting manual review 👀'}"
    )
    try:
        threads_draft_id = await social_create_draft(
            platform="threads",
            content=threads_content,
            image_url=image_url,
            hashtags=hashtags,
        )
        logger.info("Threads companion DnB draft created: %s", threads_draft_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("Threads companion draft failed: %s", exc)
        threads_draft_id = None

    logger.info(
        "run_drum_and_bass_daily complete: tiktok_draft=%s threads_draft=%s video_prompt_len=%d",
        tiktok_draft_id,
        threads_draft_id,
        len(video_prompt),
    )


# ---------------------------------------------------------------------------
# Scheduler registration
# ---------------------------------------------------------------------------


def configure_scheduler() -> AsyncIOScheduler:
    """Register all scheduled jobs and return the configured scheduler."""
    scheduler.add_job(
        run_daily_briefing,
        trigger="cron",
        hour=7,
        minute=30,
        id="daily_briefing",
        name="Daily Data Centre Intelligence Briefing (07:30 UTC)",
        replace_existing=True,
        misfire_grace_time=600,  # 10-minute grace window
    )
    scheduler.add_job(
        run_drum_and_bass_daily,
        trigger="cron",
        hour=8,
        minute=0,
        id="dnb_tiktok_daily",
        name="Drum & Bass TikTok Daily Post (08:00 UTC)",
        replace_existing=True,
        misfire_grace_time=600,
    )
    logger.info(
        "Scheduler configured with %d job(s): %s",
        len(scheduler.get_jobs()),
        [j.name for j in scheduler.get_jobs()],
    )
    return scheduler
