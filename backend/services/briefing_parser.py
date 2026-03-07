"""
briefing_parser.py – GrokBriefingParser: parse, upsert, enrich, touchpoints, social drafts.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date
from typing import Any, Optional

import httpx

from backend.config import settings
from backend.models import (
    AccountModel,
    IntelligenceEntryModel,
    OpportunityModel,
    ParsedBriefing,
    SocialDraft,
    TouchpointSuggestion,
    TriggerSignalModel,
)
from backend.services.workspace_client import (
    get_recent_signals_count,
    get_opportunity_stage,
    get_user_settings,
    make_idempotency_key,
    research_tenders_and_awards,
    save_touchpoint,
    search_workspace_touchpoints,
    social_create_draft,
    upsert_account,
    upsert_intelligence_entry,
    upsert_opportunity,
    upsert_signal,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grok system prompt (strong, with 4 few-shot examples)
# ---------------------------------------------------------------------------

_GROK_SYSTEM_PROMPT = """You are an AI assistant that extracts structured intelligence from global data centre industry briefings.

CRITICAL: You ONLY return valid JSON. No prose, no markdown, no code blocks, no explanation.
Return ONLY the raw JSON object — nothing before it, nothing after it.

=== OUTPUT SCHEMA ===
{
  "briefing_date": "YYYY-MM-DD",
  "overview_summary": "concise 2–4 sentence summary of today's key themes",
  "accounts": [
    {
      "name": "string (company / organisation name)",
      "domain": "string|null (e.g. google.com)",
      "industry": "string|null (e.g. Hyperscaler, Colocation, Energy, Construction)",
      "country": "string|null",
      "notes": "string|null"
    }
  ],
  "opportunities": [
    {
      "name": "string (descriptive project name)",
      "account_name": "string (must match an account name above)",
      "stage": "Target",
      "value_gbp": number|null,
      "project_type": "string|null (e.g. New Build, Expansion, Refurb, M&E Fit-Out)",
      "location": "string|null",
      "notes": "string|null"
    }
  ],
  "trigger_signals": [
    {
      "signal_type": "new_build|expansion|cancellation|acquisition|energy_deal|supply_chain_risk|other|tender_opportunity",
      "account_name": "string|null",
      "summary": "string (factual, 1–3 sentences)",
      "source_url": "string|null",
      "confidence": "high|medium|low|null",
      "project_stage": "Planning|Approved|Under Construction|Operational|Announced|Cancelled|null",
      "mw_capacity": number|null,
      "location": "string|null"
    }
  ],
  "intelligence_dataset": [
    {
      "title": "string",
      "summary": "string",
      "source_url": "string|null",
      "relevance": "string|null (why this matters to a data centre bid/delivery firm)",
      "category": "string|null (e.g. Market Trend, Regulation, Energy, Supply Chain, M&A)"
    }
  ]
}

=== RULES ===
- briefing_date: extract from content; if absent infer today's date in YYYY-MM-DD format
- All opportunities MUST have stage = "Target"
- NEVER guess — use null for any unknown or unconfirmed value
- signal_type MUST be one of the eight enum values
- project_stage MUST be one of the six enum values, or null
- Extract ALL named accounts / organisations mentioned
- Identify ALL active projects, RFPs, tenders and expansions as opportunities
- Capture ALL signals that could be sales triggers for a bid & delivery firm
- intelligence_dataset should include macro trends, regulation, energy deals, supply chain risks

=== FEW-SHOT EXAMPLES ===

[EXAMPLE 1 – CANCELLATION]
Input excerpt:
"Oracle has cancelled its planned 200MW data centre campus in Dublin following three years of planning delays. OpenAI also announced it is withdrawing from its Ireland site amid ongoing regulatory concerns around data privacy."
Corresponding trigger_signals entry:
{
  "signal_type": "cancellation",
  "account_name": "Oracle",
  "summary": "Oracle cancels planned 200MW data centre campus in Dublin after three years of planning delays; OpenAI also withdraws from Ireland site amid data privacy regulatory concerns.",
  "source_url": null,
  "confidence": "high",
  "project_stage": "Cancelled",
  "mw_capacity": 200.0,
  "location": "Dublin, Ireland"
}

[EXAMPLE 2 – ACQUISITION]
Input excerpt:
"Semtech Corporation has completed its $340M acquisition of HieFo Networks, consolidating edge AI inference capabilities. The deal expands Semtech's presence across 18 European data centre facilities."
Corresponding trigger_signals entry:
{
  "signal_type": "acquisition",
  "account_name": "Semtech",
  "summary": "Semtech acquires HieFo Networks for $340M, consolidating edge AI inference capabilities across 18 European data centre facilities.",
  "source_url": null,
  "confidence": "high",
  "project_stage": "Announced",
  "mw_capacity": null,
  "location": "Europe"
}

[EXAMPLE 3 – ENERGY DEAL]
Input excerpt:
"Google has signed a landmark 10-year geothermal power purchase agreement with Fervo Energy to supply 115MW of continuous baseload renewable power to its Nevada data centres, part of its 2030 carbon-free energy commitment."
Corresponding trigger_signals entry:
{
  "signal_type": "energy_deal",
  "account_name": "Google",
  "summary": "Google signs 10-year geothermal PPA with Fervo Energy for 115MW baseload renewable power to Nevada data centres, advancing 2030 carbon-free energy commitment.",
  "source_url": null,
  "confidence": "high",
  "project_stage": "Approved",
  "mw_capacity": 115.0,
  "location": "Nevada, USA"
}

[EXAMPLE 4 – SUPPLY CHAIN RISK]
Input excerpt:
"Industry sources report a significant helium shortage following a 15% production cut at the Ras Laffan facility in Qatar. Hard drive manufacturers are warning of 6–8 week lead time extensions for helium-sealed HDDs, with downstream impact expected on hyperscale storage build schedules through Q3 2026."
Corresponding trigger_signals entry:
{
  "signal_type": "supply_chain_risk",
  "account_name": null,
  "summary": "Global helium shortage after 15% production cut at Ras Laffan, Qatar; HDD manufacturers warn of 6–8 week lead time extension for helium-sealed drives, impacting hyperscale storage builds through Q3 2026.",
  "source_url": null,
  "confidence": "medium",
  "project_stage": null,
  "mw_capacity": null,
  "location": "Global"
}

Now process the following briefing and return the COMPLETE JSON object:
"""


# ---------------------------------------------------------------------------
# GrokBriefingParser
# ---------------------------------------------------------------------------


class GrokBriefingParser:
    """
    Parses GLOBAL DATA CENTRE INTELLIGENCE BRIEFINGs using the xAI Grok API,
    upserts extracted data to ContractGHOST workspace, enriches signals with
    tender/award data, and generates touchpoints + social drafts.
    """

    def __init__(self) -> None:
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=60)
        return self._http_client

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def parse(self, briefing_text: str) -> ParsedBriefing:
        """
        Send briefing_text to xAI Grok /chat/completions and parse the
        strict-JSON response into a ParsedBriefing model.
        """
        logger.info(
            "Sending briefing to Grok (%d chars) using model %s",
            len(briefing_text),
            settings.grok_model,
        )

        if not settings.xai_api_key:
            logger.warning("XAI_API_KEY not set – returning empty ParsedBriefing")
            return ParsedBriefing(
                briefing_date=date.today().isoformat(),
                overview_summary="[Grok not configured – no API key]",
            )

        payload: dict[str, Any] = {
            "model": settings.grok_model,
            "messages": [
                {"role": "system", "content": _GROK_SYSTEM_PROMPT},
                {"role": "user", "content": briefing_text},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        try:
            resp = await self.http_client.post(
                f"{settings.xai_base_url.rstrip('/')}/chat/completions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {settings.xai_api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            raw_content = resp.json()["choices"][0]["message"]["content"]
            logger.info("Grok response received (%d chars)", len(raw_content))
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Grok API HTTP error %s: %s",
                exc.response.status_code,
                exc.response.text[:500],
            )
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("Grok API error: %s", exc)
            raise

        try:
            data = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse Grok JSON response: %s | raw=%s", exc, raw_content[:300])
            raise ValueError(f"Grok returned invalid JSON: {exc}") from exc

        try:
            parsed = ParsedBriefing.model_validate(data)
        except Exception as exc:  # noqa: BLE001
            logger.error("ParsedBriefing validation error: %s | data=%s", exc, str(data)[:300])
            raise ValueError(f"ParsedBriefing validation failed: {exc}") from exc

        logger.info(
            "Parsed briefing: date=%s accounts=%d opps=%d signals=%d intel=%d",
            parsed.briefing_date,
            len(parsed.accounts),
            len(parsed.opportunities),
            len(parsed.trigger_signals),
            len(parsed.intelligence_dataset),
        )
        return parsed

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------

    async def upsert_extracted_data(
        self, extracted: ParsedBriefing, doc_id: str
    ) -> None:
        """
        Upsert all accounts, opportunities, trigger signals, and intelligence
        dataset entries extracted from the briefing into ContractGHOST workspace.
        """
        bd = extracted.briefing_date

        # Accounts
        for account in extracted.accounts:
            ikey = make_idempotency_key("acct", account.name, bd)
            await upsert_account(
                {
                    "name": account.name,
                    "domain": account.domain,
                    "industry": account.industry,
                    "country": account.country,
                    "notes": account.notes,
                },
                ikey,
            )

        # Opportunities
        for opp in extracted.opportunities:
            stable_id = f"{opp.account_name}::{opp.name}"
            ikey = make_idempotency_key("opp", stable_id, bd)
            await upsert_opportunity(
                {
                    "name": opp.name,
                    "account_name": opp.account_name,
                    "stage": opp.stage,
                    "value_gbp": opp.value_gbp,
                    "project_type": opp.project_type,
                    "location": opp.location,
                    "notes": opp.notes,
                },
                ikey,
            )

        # Trigger signals
        for idx, signal in enumerate(extracted.trigger_signals):
            stable_id = f"{signal.signal_type}::{signal.account_name or 'global'}::{signal.summary[:40]}::{idx}"
            ikey = make_idempotency_key("sig", stable_id, bd)
            await upsert_signal(
                {
                    "signal_type": signal.signal_type.value,
                    "account_name": signal.account_name,
                    "summary": signal.summary,
                    "source_url": signal.source_url,
                    "confidence": signal.confidence,
                    "project_stage": signal.project_stage.value if signal.project_stage else None,
                    "mw_capacity": signal.mw_capacity,
                    "location": signal.location,
                },
                ikey,
                doc_id,
            )

        # Intelligence dataset
        for idx, entry in enumerate(extracted.intelligence_dataset):
            stable_id = f"{entry.title[:40]}::{idx}"
            ikey = make_idempotency_key("intel", stable_id, bd)
            await upsert_intelligence_entry(
                {
                    "title": entry.title,
                    "summary": entry.summary,
                    "source_url": entry.source_url,
                    "relevance": entry.relevance,
                    "category": entry.category,
                },
                ikey,
                doc_id,
            )

        logger.info(
            "Upsert complete for doc_id=%s: %d accounts, %d opps, %d signals, %d intel entries",
            doc_id,
            len(extracted.accounts),
            len(extracted.opportunities),
            len(extracted.trigger_signals),
            len(extracted.intelligence_dataset),
        )

    # ------------------------------------------------------------------
    # Enrich with tenders
    # ------------------------------------------------------------------

    async def enrich_with_tenders(
        self,
        extracted: ParsedBriefing,
        briefing_date: str,
        doc_id: str,
    ) -> None:
        """
        For each trigger signal that involves a named account, research recent
        tenders and awards and upsert them as intelligence_dataset entries.
        """
        seen_accounts: set[str] = set()
        for signal in extracted.trigger_signals:
            account_name = signal.account_name
            if not account_name or account_name in seen_accounts:
                continue
            seen_accounts.add(account_name)

            logger.info("Enriching tenders for account: %s", account_name)
            try:
                tender_results = await research_tenders_and_awards(
                    company_name=account_name,
                    location=signal.location,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Tender research failed for %s: %s", account_name, exc)
                continue

            for i, tender in enumerate(tender_results):
                title = tender.get("title", f"Tender result {i+1}")
                snippet = tender.get("snippet", "")
                url = tender.get("url")
                stable_id = f"tender::{account_name}::{title[:40]}::{i}"
                ikey = make_idempotency_key("intel", stable_id, briefing_date)
                await upsert_intelligence_entry(
                    {
                        "title": f"[Tender/Award] {title}",
                        "summary": snippet,
                        "source_url": url,
                        "relevance": f"Tender/award signal for {account_name}",
                        "category": "Tender & Awards",
                    },
                    ikey,
                    doc_id,
                )

        logger.info(
            "Tender enrichment complete for doc_id=%s (checked %d accounts)",
            doc_id,
            len(seen_accounts),
        )

    # ------------------------------------------------------------------
    # Generate touchpoints
    # ------------------------------------------------------------------

    async def generate_suggested_touchpoints(
        self, extracted: ParsedBriefing
    ) -> list[TouchpointSuggestion]:
        """
        Generate smart relationship touchpoints for each account, applying
        workspace context checks:
          - Last touch < 14 days → skip (too soon)
          - Opportunity stage advanced → suggest follow-up call
          - Recent signals > 2 → elevate urgency to DM
        """
        touchpoints: list[TouchpointSuggestion] = []
        bd = extracted.briefing_date

        # Index signals per account for fast lookup
        signals_by_account: dict[str, list[TriggerSignalModel]] = {}
        for sig in extracted.trigger_signals:
            if sig.account_name:
                signals_by_account.setdefault(sig.account_name, []).append(sig)

        for account in extracted.accounts:
            name = account.name

            # --- Workspace check 1: recent touchpoint (<14 days) ---
            recent_touches = await search_workspace_touchpoints(name, days=14)
            if recent_touches:
                logger.info(
                    "Skipping touchpoint for %s – touched %d time(s) in last 14 days",
                    name,
                    len(recent_touches),
                )
                continue

            # --- Workspace check 2: opportunity stage ---
            opp_stage = await get_opportunity_stage(name)
            stage_advanced = opp_stage in ("Proposal", "Negotiation", "Won")

            # --- Workspace check 3: recent signal count (last 30 days) ---
            signal_count = await get_recent_signals_count(name, days=30)
            today_signals = signals_by_account.get(name, [])
            total_signals = signal_count + len(today_signals)

            if total_signals == 0 and not stage_advanced:
                logger.info("No signals or stage advancement for %s – skipping touchpoint", name)
                continue

            # --- Determine action type ---
            if stage_advanced:
                action = "call"
                reason = f"Opportunity for {name} is at stage '{opp_stage}' – timely follow-up call recommended."
                urgency = "high"
                platform = None
            elif total_signals > 2:
                action = "DM"
                reason = (
                    f"{total_signals} recent signals for {name} "
                    f"(latest: {today_signals[0].summary[:80] if today_signals else 'historical'}) "
                    "– warm outreach via DM to open conversation."
                )
                urgency = "high"
                platform = "LinkedIn"
            else:
                action = "coffee"
                signal_summary = today_signals[0].summary[:80] if today_signals else "recent activity"
                reason = (
                    f"New signal for {name}: {signal_summary}… "
                    "Good opening for a low-pressure coffee/virtual catch-up."
                )
                urgency = "normal"
                platform = None

            tp = TouchpointSuggestion(
                account_name=name,
                suggested_action=action,
                reason=reason,
                urgency=urgency,
                platform=platform,
            )
            touchpoints.append(tp)

            # Save to local DB for future context checks
            ikey = make_idempotency_key("touch", f"{name}::{action}", bd)
            await save_touchpoint(
                {
                    "account_name": tp.account_name,
                    "contact_name": tp.contact_name,
                    "suggested_action": tp.suggested_action,
                    "reason": tp.reason,
                    "urgency": tp.urgency,
                    "platform": tp.platform,
                },
                ikey,
            )

        logger.info(
            "Generated %d touchpoint suggestion(s) for briefing date %s",
            len(touchpoints),
            bd,
        )
        return touchpoints

    # ------------------------------------------------------------------
    # Social drafts from touchpoints
    # ------------------------------------------------------------------

    async def generate_social_drafts_from_touchpoints(
        self,
        touchpoints: list[TouchpointSuggestion],
        platform: str = "threads",
    ) -> list[SocialDraft]:
        """
        Generate social media drafts based on touchpoint context.
        Always creates drafts – never auto-posts (drafts-first policy).
        Checks get_user_settings() before any action.
        """
        user_settings = await get_user_settings()
        drafts: list[SocialDraft] = []

        if not touchpoints:
            logger.info("No touchpoints to generate social drafts from")
            return drafts

        for tp in touchpoints:
            if platform in ("threads", "x"):
                if tp.suggested_action == "call":
                    content = (
                        f"Big moves in the data centre sector 📡 – "
                        f"conversations with key players right now are critical. "
                        f"If you're in the {tp.account_name} ecosystem, let's talk."
                    )
                    hashtags = ["#DataCentre", "#TechInfrastructure", "#aLiGN", "#BidStrategy"]
                elif tp.suggested_action == "DM":
                    content = (
                        f"Multiple signals firing in the {tp.account_name} space 🔥 "
                        f"– market intelligence is pointing to major activity. "
                        f"Who else is watching this closely?"
                    )
                    hashtags = ["#DataCentre", "#MarketIntelligence", "#Infrastructure", "#aLiGN"]
                else:  # coffee
                    content = (
                        f"The data centre market never sleeps ☕ "
                        f"– catching signals across {tp.account_name} and peers. "
                        f"Would love to connect with anyone working in this space."
                    )
                    hashtags = ["#DataCentre", "#Networking", "#Infrastructure", "#aLiGN"]

                draft_id = await social_create_draft(
                    platform=platform,
                    content=content,
                    hashtags=hashtags,
                )
                drafts.append(
                    SocialDraft(
                        platform=platform,
                        content=content,
                        hashtags=hashtags,
                        draft_id=draft_id,
                    )
                )
                logger.info(
                    "Social draft created for %s (platform=%s, action=%s)",
                    tp.account_name,
                    platform,
                    tp.suggested_action,
                )

        logger.info("Generated %d social draft(s)", len(drafts))
        return drafts
