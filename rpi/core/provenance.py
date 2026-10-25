"""
Provenance primitives shared by every RPI module.

Every value RPI stores or shows must say where it came from and how much it
can be trusted. The UI and reports rely on `MeasurementType` to keep measured
statistics, modeled estimates, AI judgments and planning assumptions apart.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MeasurementType(str, Enum):
    # Public value read directly from the source (e.g. a subscriber count).
    OBSERVED = "observed"
    # Creator-authorized first-party analytics (YouTube Analytics OAuth).
    FIRST_PARTY_MEASURED = "first_party_measured"
    # A third-party provider's modeled estimate (e.g. audience demographics).
    THIRD_PARTY_MODELED = "third_party_modeled"
    # Inferred by RPI from content (keyword rules or Claude classification).
    CONTENT_INFERRED = "content_inferred"
    # Numbers a creator sent us manually (screenshots, media kit). Unverified.
    MANUAL_SUBMITTED = "manual_submitted"
    # An operator-entered planning assumption.
    ASSUMPTION = "assumption"
    # Derived by deterministic RPI arithmetic from other values.
    COMPUTED = "computed"
    UNAVAILABLE = "unavailable"


class CommercialUse(str, Enum):
    INTERNAL_ONLY = "internal_only"
    INTERNAL_DISPLAY_ONLY = "internal_display_only"
    NO_STORAGE = "no_storage"
    UNKNOWN = "unknown"


class Provenance(BaseModel):
    """Record-level provenance attached to provider data."""

    provider: str
    operation: str
    source_record: Optional[str] = None
    retrieved_at: datetime = Field(default_factory=utcnow)
    observed_at: Optional[datetime] = None
    data_type: str
    measurement: MeasurementType
    confidence: Optional[float] = None
    completeness: Optional[float] = None
    retention_policy: str
    commercial_use: CommercialUse = CommercialUse.UNKNOWN
    raw_ref: Optional[str] = None
    response_version: Optional[str] = None
    # Per-field overrides when a record mixes measured and modeled values.
    field_measurements: dict[str, MeasurementType] = Field(default_factory=dict)
    # True when the value must not feed RPI-derived scores (YouTube API data).
    derived_metrics_prohibited: bool = False

    def measurement_for(self, field: str) -> MeasurementType:
        return self.field_measurements.get(field, self.measurement)


class Evidence(BaseModel):
    """A pointer to the source material that supports a claim."""

    kind: str  # e.g. "content", "provider_field", "manual_note"
    ref: str  # content id, URL or field path
    url: Optional[str] = None
    excerpt: Optional[str] = None
    provider: Optional[str] = None


def jsonable(value: Any) -> Any:
    """Convert pydantic models / enums / datetimes into JSON-safe values."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value
