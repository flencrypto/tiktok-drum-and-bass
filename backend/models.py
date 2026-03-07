from __future__ import annotations

from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, Field


class SignalType(str, Enum):
    new_build = "new_build"
    expansion = "expansion"
    cancellation = "cancellation"
    acquisition = "acquisition"
    energy_deal = "energy_deal"
    supply_chain_risk = "supply_chain_risk"
    other = "other"
    tender_opportunity = "tender_opportunity"


class ProjectStage(str, Enum):
    planning = "Planning"
    approved = "Approved"
    under_construction = "Under Construction"
    operational = "Operational"
    announced = "Announced"
    cancelled = "Cancelled"


class AccountModel(BaseModel):
    name: str
    domain: Optional[str] = None
    industry: Optional[str] = None
    country: Optional[str] = None
    notes: Optional[str] = None


class OpportunityModel(BaseModel):
    name: str
    account_name: str
    stage: str = "Target"
    value_gbp: Optional[float] = None
    project_type: Optional[str] = None
    location: Optional[str] = None
    notes: Optional[str] = None


class TriggerSignalModel(BaseModel):
    signal_type: SignalType
    account_name: Optional[str] = None
    summary: str
    source_url: Optional[str] = None
    confidence: Optional[str] = None
    project_stage: Optional[ProjectStage] = None
    mw_capacity: Optional[float] = None
    location: Optional[str] = None


class IntelligenceEntryModel(BaseModel):
    title: str
    summary: str
    source_url: Optional[str] = None
    relevance: Optional[str] = None
    category: Optional[str] = None


class ParsedBriefing(BaseModel):
    briefing_date: str
    overview_summary: str
    accounts: List[AccountModel] = Field(default_factory=list)
    opportunities: List[OpportunityModel] = Field(default_factory=list)
    trigger_signals: List[TriggerSignalModel] = Field(default_factory=list)
    intelligence_dataset: List[IntelligenceEntryModel] = Field(default_factory=list)


class TouchpointSuggestion(BaseModel):
    account_name: str
    contact_name: Optional[str] = None
    suggested_action: str  # coffee / call / DM
    reason: str
    urgency: str = "normal"
    platform: Optional[str] = None


class SocialDraft(BaseModel):
    platform: str
    content: str
    image_prompt: Optional[str] = None
    video_prompt: Optional[str] = None
    hashtags: List[str] = Field(default_factory=list)
    draft_id: Optional[str] = None


class BriefingIngestResponse(BaseModel):
    doc_id: str
    briefing_date: Optional[str] = None
    accounts_count: int = 0
    opportunities_count: int = 0
    signals_count: int = 0
    intelligence_count: int = 0
    touchpoints: List[TouchpointSuggestion] = Field(default_factory=list)
    social_drafts: List[SocialDraft] = Field(default_factory=list)
    status: str = "success"
