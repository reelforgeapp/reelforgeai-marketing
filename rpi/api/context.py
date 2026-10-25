"""
Runtime wiring for the RPI API and workers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from rpi.administration.budget import BudgetStore
from rpi.administration.jobs import StageSizes
from rpi.content_intelligence.claude_client import ClaudeClient, ModelPrice
from rpi.discovery.engine import DiscoveryEngine
from rpi.enrichment.service import ResearchConfig, ResearchService
from rpi.profit_modeling.deals import ProgramTerms
from rpi.providers.gateway import PgCache, PgRequestLog, ProviderGateway
from rpi.providers.registry import ProviderRegistry, build_registry


@dataclass
class RPIContext:
    pool: object
    settings: object
    registry: ProviderRegistry
    gateway: ProviderGateway
    budget: object
    llm: Optional[object]
    discovery: DiscoveryEngine
    research: ResearchService
    stages: StageSizes
    program: ProgramTerms
    fast_price: Optional[ModelPrice] = None
    deep_price: Optional[ModelPrice] = None
    send_fn: Optional[object] = None
    extras: dict = field(default_factory=dict)


def model_prices(settings) -> tuple[ModelPrice, ModelPrice]:
    fast = ModelPrice(settings.rpi_claude_fast_model, Decimal(str(settings.rpi_claude_fast_input_usd_per_mtok)),
                      Decimal(str(settings.rpi_claude_fast_output_usd_per_mtok)))
    deep = ModelPrice(settings.rpi_claude_deep_model, Decimal(str(settings.rpi_claude_deep_input_usd_per_mtok)),
                      Decimal(str(settings.rpi_claude_deep_output_usd_per_mtok)))
    return fast, deep


def build_context(pool, settings, *, registry: Optional[ProviderRegistry] = None, llm=None, send_fn=None) -> RPIContext:
    registry = registry or build_registry(settings)
    budget = BudgetStore(pool, max_cost_per_creator_usd=Decimal(str(settings.rpi_max_cost_per_creator_usd)))
    gateway = ProviderGateway(budget=budget, cache=PgCache(pool), request_log=PgRequestLog(pool))
    fast, deep = model_prices(settings)
    if llm is None and settings.anthropic_api_key:
        llm = ClaudeClient(settings.anthropic_api_key, budget)
    research = ResearchService(pool, registry, gateway, llm, ResearchConfig(
        fast=fast if llm else None, deep=deep if llm else None,
        creatordb_scoring_permitted=settings.rpi_creatordb_scoring_permitted,
        fetch_audience_demographics=settings.rpi_fetch_audience_demographics,
    ))
    if send_fn is None and settings.brevo_api_key:
        from outreach.brevo_client import BrevoClient

        send_fn = BrevoClient().send_email
    return RPIContext(
        pool=pool, settings=settings, registry=registry, gateway=gateway, budget=budget, llm=llm,
        discovery=DiscoveryEngine(pool, registry, gateway), research=research,
        stages=StageSizes(settings.rpi_stage1_discover, settings.rpi_stage2_screen, settings.rpi_stage3_analyze,
                          settings.rpi_stage4_qualify, settings.rpi_stage5_recommend),
        program=ProgramTerms(Decimal(str(settings.rpi_affiliate_commission_rate)), settings.rpi_affiliate_commission_months,
                             settings.rpi_affiliate_attribution_days),
        fast_price=fast, deep_price=deep, send_fn=send_fn,
    )
