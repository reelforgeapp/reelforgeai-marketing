"""Gateway (cache/budget/metering), value gate, redaction, Stripe signatures, attribution windows."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from rpi.administration.budget import Budget, BudgetPeriod, BudgetScope, InMemoryBudget, Priority
from rpi.attribution.stripe_signature import SignatureError, compute_signature, verify
from rpi.attribution.touches import Touch, attribute
from rpi.core.redaction import REDACTED, redact_mapping, redact_text, structlog_redactor
from rpi.enrichment.value_gate import CreatorState, evaluate
from rpi.providers.base import Capability, CreatorSearchCriteria, ResultStatus
from rpi.providers.fixture_provider import FixtureProvider
from rpi.providers.gateway import CallContext, MemoryCache, MemoryRequestLog, ProviderGateway
from rpi.providers.sparktoro import SparkToroAdapter

NOW = datetime(2026, 10, 25, tzinfo=timezone.utc)


def gateway(limit="10"):
    budget = InMemoryBudget([Budget(BudgetScope.GLOBAL, BudgetPeriod.MONTHLY, D(limit), D("0.8"))])
    return ProviderGateway(budget, MemoryCache(), MemoryRequestLog()), budget


async def test_gateway_caches_and_meters():
    gw, budget = gateway()
    fx = FixtureProvider()
    r1 = await gw.call(fx, Capability.CREATOR_PROFILE, "UCFIXTURE000003")
    r2 = await gw.call(fx, Capability.CREATOR_PROFILE, "UCFIXTURE000003")
    assert r1.status == ResultStatus.OK and r2.status == ResultStatus.CACHED
    assert r2.data.platform_account_id == "UCFIXTURE000003"
    assert fx.calls == ["profile"]  # second call served from cache
    assert budget.committed_total == D("0.01")
    assert [e["status"] for e in gw.request_log.entries] == ["ok", "cached"]


async def test_gateway_never_caches_contacts():
    gw, _ = gateway()
    fx = FixtureProvider()
    await gw.call(fx, Capability.CREATOR_CONTACTS, "UCFIXTURE000003")
    await gw.call(fx, Capability.CREATOR_CONTACTS, "UCFIXTURE000003")
    assert fx.calls == ["contacts", "contacts"]


async def test_gateway_budget_denial_blocks_call():
    gw, _ = gateway(limit="0.005")
    fx = FixtureProvider()
    r = await gw.call(fx, Capability.SEARCH_CREATORS, CreatorSearchCriteria(limit=5))
    assert r.status == ResultStatus.BUDGET_DENIED and fx.calls == []


async def test_gateway_unsupported_and_not_configured():
    gw, _ = gateway()
    r = await gw.call(FixtureProvider(), Capability.AUDIENCE_RESEARCH, "q")
    assert r.status == ResultStatus.UNSUPPORTED
    r = await gw.call(SparkToroAdapter(""), Capability.AUDIENCE_RESEARCH, "q")
    assert r.status == ResultStatus.NOT_CONFIGURED


def state(**kw):
    base = dict(pipeline_status="preliminary_qualified", funnel_stage=3, niche_relevance=0.8,
                classification_confidence=0.5, recommendation="investigate", last_fetched={})
    base.update(kw)
    return CreatorState(**base)


def test_value_gate_rules():
    assert not evaluate(Capability.AUDIENCE_DEMOGRAPHICS, state(niche_relevance=0.2)).allowed
    assert not evaluate(Capability.AUDIENCE_DEMOGRAPHICS, state(pipeline_status="rejected")).allowed
    assert evaluate(Capability.AUDIENCE_DEMOGRAPHICS, state()).allowed
    assert not evaluate(Capability.TRANSCRIPT, state(classification_confidence=0.9)).allowed
    assert not evaluate(Capability.TRANSCRIPT, state(transcripts_fetched=3)).allowed
    assert not evaluate(Capability.CREATOR_CONTACTS, state(recommendation="hold", funnel_stage=4)).allowed
    assert not evaluate(Capability.CREATOR_CONTACTS, state(funnel_stage=3)).allowed
    assert evaluate(Capability.CREATOR_CONTACTS, state(funnel_stage=4)).allowed
    fresh = state(last_fetched={"get_creator_content": NOW - timedelta(days=1)})
    assert not evaluate(Capability.CREATOR_CONTENT, fresh, now=NOW).allowed
    assert evaluate(Capability.CREATOR_CONTENT, fresh, now=NOW, force=True).allowed


def test_redaction_of_secrets():
    text = "failed with key sk-ant-api03-abcdefghijk and AIzaSyA1234567890123456789012 url?api_key=zzz123&x=1"
    out = redact_text(text)
    assert "sk-ant" not in out and "AIza" not in out and "zzz123" not in out
    assert redact_mapping({"Authorization": "Bearer x", "nested": {"api-key": "y"}, "ok": 1}) == {
        "Authorization": REDACTED, "nested": {"api-key": REDACTED}, "ok": 1}
    ev = structlog_redactor(None, None, {"event": "x", "token": "abc", "msg": "Bearer abcdefghijklmnop"})
    assert ev["token"] == REDACTED and "abcdefghijklmnop" not in ev["msg"]


def test_stripe_signature_valid_wrong_and_replay():
    secret, body, ts = "whsec_test_secret_123", b'{"id":"evt_1"}', 1_760_000_000
    header = f"t={ts},v1={compute_signature(secret, ts, body)}"
    assert verify(body, header, secret, now=ts + 10) == ts
    with pytest.raises(SignatureError, match="mismatch"):
        verify(body + b" ", header, secret, now=ts)
    with pytest.raises(SignatureError, match="replay"):
        verify(body, header, secret, now=ts + 3600)
    with pytest.raises(SignatureError):
        verify(body, "garbage", secret)
    with pytest.raises(SignatureError):
        verify(body, header, "")


def test_first_and_last_touch_within_window():
    signup = NOW
    touches = [
        Touch("old", NOW - timedelta(days=90), 30),   # outside its window
        Touch("a", NOW - timedelta(days=20), 30),
        Touch("b", NOW - timedelta(days=2), 30),
        Touch("after", NOW + timedelta(days=1), 30),  # after signup
    ]
    r = attribute(touches, signup)
    assert r.first_touch.campaign_id == "a" and r.last_touch.campaign_id == "b" and r.eligible_count == 2
    assert attribute([], signup).first_touch is None
