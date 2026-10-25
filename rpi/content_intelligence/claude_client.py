"""
Minimal Claude Messages API client for structured analysis.

Uses raw httpx like the existing services/ai_personalization.py. Structured
output comes from a forced tool call (`tool_choice: {type: tool}`); the model
has no other tools, so it can only return a report. Every call reserves its
estimated cost with the budget controller first and settles the actual cost
from the returned token usage.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Protocol

import httpx
import structlog

from rpi.administration.budget import BudgetBackend, Priority, ReservationRequest
from rpi.providers.http import ResilientHTTP, TokenBucket

logger = structlog.get_logger()

ANTHROPIC_URL = "https://api.anthropic.com"
MTOK = Decimal("1000000")


@dataclass(frozen=True)
class ModelPrice:
    model: str
    input_usd_per_mtok: Decimal
    output_usd_per_mtok: Decimal

    def cost(self, input_tokens: int, output_tokens: int) -> Decimal:
        return (Decimal(input_tokens) * self.input_usd_per_mtok + Decimal(output_tokens) * self.output_usd_per_mtok) / MTOK


@dataclass
class ToolCallResult:
    ok: bool
    data: Optional[dict[str, Any]]
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: Decimal = Decimal("0")
    error: Optional[str] = None
    budget_denied: bool = False


class LLM(Protocol):
    async def call_tool(
        self,
        *,
        price: ModelPrice,
        system: str,
        user: str,
        tool: dict,
        max_tokens: int,
        job_id: Optional[str] = None,
        creator_id: Optional[str] = None,
        priority: Priority = Priority.NORMAL,
    ) -> ToolCallResult: ...


def estimate_tokens(text: str) -> int:
    # Conservative ~3.5 characters per token for English prose with markup.
    return int(len(text) / 3.5) + 50


class ClaudeClient:
    def __init__(self, api_key: str, budget: BudgetBackend, *, transport: httpx.AsyncBaseTransport | None = None):
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not configured")
        self.budget = budget
        self.http = ResilientHTTP(
            "anthropic",
            ANTHROPIC_URL,
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            timeout=90.0,
            max_retries=2,
            rate_limiter=TokenBucket(2.0, burst=2),
            transport=transport,
            secrets=(api_key,),
        )

    async def call_tool(
        self,
        *,
        price: ModelPrice,
        system: str,
        user: str,
        tool: dict,
        max_tokens: int,
        job_id: Optional[str] = None,
        creator_id: Optional[str] = None,
        priority: Priority = Priority.NORMAL,
    ) -> ToolCallResult:
        est_in = estimate_tokens(system) + estimate_tokens(user) + estimate_tokens(str(tool))
        estimate = price.cost(est_in, max_tokens)
        reservation = await self.budget.reserve(
            ReservationRequest("anthropic", f"messages:{price.model}", estimate, priority, job_id, creator_id)
        )
        if not reservation.allowed:
            return ToolCallResult(False, None, price.model, error=reservation.decision.reason, budget_denied=True)

        body = {
            "model": price.model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "tools": [tool],
            "tool_choice": {"type": "tool", "name": tool["name"]},
        }
        try:
            response = await self.http.request("POST", "/v1/messages", json=body)
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 - settle reservation then report
            await self.budget.release(reservation.id)
            logger.error("rpi_claude_call_failed", model=price.model, error=str(exc))
            return ToolCallResult(False, None, price.model, error=str(exc))

        usage = payload.get("usage") or {}
        in_tok, out_tok = int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))
        cost = price.cost(in_tok, out_tok)
        await self.budget.settle(reservation.id, cost, Decimal(in_tok + out_tok))

        block = next(
            (b for b in payload.get("content", []) if b.get("type") == "tool_use" and b.get("name") == tool["name"]),
            None,
        )
        if block is None or not isinstance(block.get("input"), dict):
            return ToolCallResult(False, None, price.model, in_tok, out_tok, cost, error="no tool_use block in response")
        if payload.get("stop_reason") == "max_tokens":
            return ToolCallResult(False, None, price.model, in_tok, out_tok, cost, error="response truncated at max_tokens")
        return ToolCallResult(True, block["input"], price.model, in_tok, out_tok, cost)
