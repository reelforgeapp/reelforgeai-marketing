"""
Builds provider adapters from settings. Only configured providers are
active; the fixture provider is refused in production.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from rpi.providers.base import Capability, ProviderAdapter
from rpi.providers.creatordb import CreatorDBAdapter
from rpi.providers.fixture_provider import FixtureProvider
from rpi.providers.sparktoro import SparkToroAdapter
from rpi.providers.supadata import SupadataAdapter
from rpi.providers.youtube_data import YouTubeDataAdapter


class FixtureInProductionError(RuntimeError):
    pass


@dataclass
class ProviderRegistry:
    adapters: dict[str, ProviderAdapter] = field(default_factory=dict)

    def get(self, name: str) -> Optional[ProviderAdapter]:
        return self.adapters.get(name)

    def first_supporting(self, capability: Capability, prefer: tuple[str, ...] = ()) -> Optional[ProviderAdapter]:
        ordered = [self.adapters[n] for n in prefer if n in self.adapters] + [
            a for n, a in self.adapters.items() if n not in prefer
        ]
        return next((a for a in ordered if a.is_configured() and a.supports(capability)), None)

    def describe(self) -> list[dict]:
        return [
            {
                "provider": name,
                "configured": a.is_configured(),
                "capabilities": sorted(c.value for c in a.capabilities),
                "policy": a.policy_key,
            }
            for name, a in self.adapters.items()
        ]


def build_registry(settings) -> ProviderRegistry:
    adapters: dict[str, ProviderAdapter] = {}
    if settings.creatordb_api_key:
        verified = {e.strip() for e in (getattr(settings, "creatordb_verified_endpoints", "") or "").split(",") if e.strip()}
        adapters["creatordb"] = CreatorDBAdapter(
            settings.creatordb_api_key,
            base_url=settings.creatordb_base_url,
            usd_per_credit=settings.creatordb_usd_per_credit,
            rate_per_second=settings.creatordb_rate_per_second,
            verified_endpoints=verified,
        )
    if settings.sparktoro_api_key:
        adapters["sparktoro"] = SparkToroAdapter(
            settings.sparktoro_api_key,
            usd_per_credit=settings.sparktoro_usd_per_credit,
            rate_per_second=settings.sparktoro_rate_per_second,
        )
    if settings.supadata_api_key:
        adapters["supadata"] = SupadataAdapter(settings.supadata_api_key, usd_per_request=settings.supadata_usd_per_request)
    if settings.youtube_api_key:
        adapters["youtube_data_api"] = YouTubeDataAdapter(settings.youtube_api_key)
    if settings.rpi_enable_fixture_provider:
        if settings.environment == "production":
            raise FixtureInProductionError("RPI_ENABLE_FIXTURE_PROVIDER must not be set in production")
        adapters["fixture"] = FixtureProvider()
    return ProviderRegistry(adapters)
