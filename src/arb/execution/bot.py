"""Phase 4 — Execution Bot.

Executes both legs of an arbitrage trade simultaneously using asyncio.
Paper trading mode is enforced by default; live trading requires explicit opt-in.

Safety gates (all enforced before any order):
  1. MIN_PROFIT_CENTS threshold
  2. MAX_POSITION_SIZE cap per leg
  3. Available capital check
  4. Paper-trading mode flag (default ON)
"""

from __future__ import annotations

import asyncio
import time
import math
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.arb.config import (
    PAPER_TRADING,
    INITIAL_CAPITAL,
    MAX_POSITION_SIZE,
    MIN_PROFIT_CENTS,
    POLY_FEE_PCT,
    KALSHI_FEE_PCT,
)
from src.arb.db.connection import db_conn
from src.arb.detection.detector import ArbOpportunity
from src.arb.tracking import ArbTracker


# ── Order result ──────────────────────────────────────────────────────────────

@dataclass
class LegResult:
    platform: str
    side: str          # 'yes' | 'no'
    direction: str     # 'buy'
    price_cents: float
    contracts: float
    notional_usd: float
    fee_usd: float
    success: bool
    error: str = ""
    order_id: str = ""


@dataclass
class ExecutionResult:
    opportunity_id: int
    poly_leg: LegResult
    kalshi_leg: LegResult
    total_cost_usd: float
    expected_pnl_cents: float
    paper: bool
    executed_at: float = field(default_factory=time.time)

    @property
    def success(self) -> bool:
        return self.poly_leg.success and self.kalshi_leg.success


# ── Paper executor ────────────────────────────────────────────────────────────

class PaperExecutor:
    """Simulates order fills without hitting any real exchange."""

    async def place_order(
        self,
        platform: str,
        market_external_id: str,
        side: str,
        price_cents: float,
        contracts: float,
    ) -> LegResult:
        # Simulate a small network delay
        await asyncio.sleep(0.01)
        notional = (price_cents / 100) * contracts
        fee_pct = POLY_FEE_PCT if platform == "polymarket" else KALSHI_FEE_PCT
        fee = notional * fee_pct
        return LegResult(
            platform=platform,
            side=side,
            direction="buy",
            price_cents=price_cents,
            contracts=contracts,
            notional_usd=notional,
            fee_usd=fee,
            success=True,
            order_id=f"paper-{platform[:4]}-{int(time.time()*1000)}",
        )


# ── Live executors (stubs — wired to real APIs in future) ─────────────────────

class PolymarketLiveExecutor:
    """Live order placement via Polymarket CLOB. Currently a stub."""

    async def place_order(
        self,
        market_external_id: str,
        side: str,
        price_cents: float,
        contracts: float,
    ) -> LegResult:
        raise NotImplementedError(
            "Live Polymarket execution not yet implemented — use paper trading."
        )


class KalshiLiveExecutor:
    """Live order placement via Kalshi REST API."""

    def __init__(self) -> None:
        from src.arb.config import KALSHI_API_KEY, KALSHI_API_SECRET, USE_KALSHI_DEMO
        from src.arb.feeds.kalshi_feed import _REST_BASE, _kalshi_auth_headers
        self._rest_base = _REST_BASE
        self._auth_headers = _kalshi_auth_headers

    async def place_order(
        self,
        market_external_id: str,
        side: str,
        price_cents: float,
        contracts: float,
    ) -> LegResult:
        import aiohttp
        path = "/portfolio/orders"
        payload = {
            "ticker": market_external_id,
            "client_order_id": f"arb-{int(time.time()*1000)}",
            "type": "limit",
            "action": "buy",
            "side": side,
            "count": int(math.floor(contracts)),
            "yes_price": int(price_cents) if side == "yes" else int(100 - price_cents),
            "no_price": int(100 - price_cents) if side == "yes" else int(price_cents),
            "time_in_force": "immediate_or_cancel",
        }
        headers = self._auth_headers("POST", path)
        headers["Content-Type"] = "application/json"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._rest_base}{path}",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    if resp.status in (200, 201):
                        order = data.get("order", data)
                        notional = (price_cents / 100) * contracts
                        return LegResult(
                            platform="kalshi",
                            side=side,
                            direction="buy",
                            price_cents=price_cents,
                            contracts=contracts,
                            notional_usd=notional,
                            fee_usd=notional * KALSHI_FEE_PCT,
                            success=True,
                            order_id=str(order.get("order_id", "")),
                        )
                    else:
                        return LegResult(
                            platform="kalshi", side=side, direction="buy",
                            price_cents=price_cents, contracts=contracts,
                            notional_usd=0, fee_usd=0, success=False,
                            error=str(data),
                        )
        except Exception as exc:
            return LegResult(
                platform="kalshi", side=side, direction="buy",
                price_cents=price_cents, contracts=contracts,
                notional_usd=0, fee_usd=0, success=False,
                error=str(exc),
            )


# ── Execution bot ─────────────────────────────────────────────────────────────

class ExecutionBot:
    """
    Consumes ArbOpportunity objects from a queue and executes both legs
    simultaneously (asyncio.gather).
    """

    def __init__(self) -> None:
        self.paper_mode: bool = PAPER_TRADING
        self._capital_available: float = INITIAL_CAPITAL
        self._paper_exec = PaperExecutor()
        self._kalshi_live = KalshiLiveExecutor() if not PAPER_TRADING else None
        self._queue: asyncio.Queue[ArbOpportunity] = asyncio.Queue()
        self._tracker = ArbTracker(paper=PAPER_TRADING)

        if self.paper_mode:
            logger.info("ExecutionBot: PAPER TRADING mode (no real orders)")
        else:
            logger.warning("ExecutionBot: LIVE TRADING mode — real orders will be placed")

    def submit(self, opp: ArbOpportunity) -> None:
        """Enqueue an opportunity for execution."""
        self._queue.put_nowait(opp)

    # ── Safety gates ──────────────────────────────────────────────────────────

    def _position_size(self, opp: ArbOpportunity) -> float:
        """
        Kelly-inspired fractional sizing capped at MAX_POSITION_SIZE.
        net_profit_cents / 100  = net edge per dollar wagered.
        """
        edge = opp.net_profit_cents / 100  # e.g. 0.03 = 3 cents per $1
        # Conservative: bet at most 50% of Kelly; Kelly ≈ edge / (1 - combined_cost)
        kelly_frac = edge / max(1.0 - opp.combined_cost, 0.01)
        target = min(
            self._capital_available * kelly_frac * 0.5,
            MAX_POSITION_SIZE,
        )
        return max(0.0, target)

    def _passes_gates(self, opp: ArbOpportunity) -> tuple[bool, str]:
        if opp.net_profit_cents < MIN_PROFIT_CENTS:
            return False, f"net profit {opp.net_profit_cents:.2f}¢ < minimum {MIN_PROFIT_CENTS}¢"
        size = self._position_size(opp)
        if size < 1.0:
            return False, f"position size ${size:.2f} too small"
        if size > self._capital_available:
            return False, f"insufficient capital (have ${self._capital_available:.2f})"
        return True, ""

    # ── Execution ─────────────────────────────────────────────────────────────

    async def _execute_legs(
        self, opp: ArbOpportunity, size_usd: float
    ) -> ExecutionResult:
        """Fire both legs simultaneously."""
        poly_price_c = opp.poly_ask * 100
        kalshi_price_c = opp.kalshi_ask * 100
        contracts = size_usd / ((poly_price_c + kalshi_price_c) / 100)

        if self.paper_mode:
            poly_fut = self._paper_exec.place_order(
                "polymarket", opp.poly_external_id,
                opp.poly_side, poly_price_c, contracts,
            )
            kalshi_fut = self._paper_exec.place_order(
                "kalshi", opp.kalshi_external_id,
                opp.kalshi_side, kalshi_price_c, contracts,
            )
        else:
            poly_fut = PolymarketLiveExecutor().place_order(
                opp.poly_external_id, opp.poly_side, poly_price_c, contracts,
            )
            kalshi_fut = self._kalshi_live.place_order(
                opp.kalshi_external_id, opp.kalshi_side, kalshi_price_c, contracts,
            )

        poly_result, kalshi_result = await asyncio.gather(
            poly_fut, kalshi_fut, return_exceptions=False
        )

        total_cost = poly_result.notional_usd + kalshi_result.notional_usd
        total_fees = poly_result.fee_usd + kalshi_result.fee_usd
        expected_pnl_cents = (1.0 - opp.combined_cost) * contracts * 100 - total_fees * 100

        return ExecutionResult(
            opportunity_id=0,  # filled by caller after DB save
            poly_leg=poly_result,
            kalshi_leg=kalshi_result,
            total_cost_usd=total_cost,
            expected_pnl_cents=expected_pnl_cents,
            paper=self.paper_mode,
        )

    async def _persist_trade(
        self, opp: ArbOpportunity, result: ExecutionResult, opp_db_id: int
    ) -> None:
        status = "executed" if result.success else "failed"
        async with db_conn() as conn:
            await conn.execute(
                "UPDATE arb_opportunities SET status=$1 WHERE id=$2",
                status, opp_db_id,
            )
            for leg in (result.poly_leg, result.kalshi_leg):
                market_id = (
                    opp.poly_market_id
                    if leg.platform == "polymarket"
                    else opp.kalshi_market_id
                )
                await conn.execute(
                    """
                    INSERT INTO paper_trades
                        (opportunity_id, platform, market_id, side, direction,
                         price_cents, contracts, notional_usd, fee_usd, status)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    """,
                    opp_db_id,
                    leg.platform,
                    market_id,
                    leg.side,
                    leg.direction,
                    leg.price_cents,
                    leg.contracts,
                    leg.notional_usd,
                    leg.fee_usd,
                    "open" if leg.success else "cancelled",
                )

    async def process_one(self, opp: ArbOpportunity, opp_db_id: int) -> ExecutionResult | None:
        ok, reason = self._passes_gates(opp)
        if not ok:
            logger.debug(f"Opp #{opp_db_id} rejected: {reason}")
            return None

        size = self._position_size(opp)
        logger.info(
            f"{'[PAPER] ' if self.paper_mode else '[LIVE] '}"
            f"Executing #{opp_db_id}: {opp.poly_title[:40]} "
            f"size=${size:.2f} expected_net={opp.net_profit_cents:.2f}¢"
        )

        result = await self._execute_legs(opp, size)
        result.opportunity_id = opp_db_id

        if result.success:
            self._capital_available -= result.total_cost_usd
            logger.info(
                f"{'[PAPER] ' if self.paper_mode else '[LIVE] '}"
                f"#{opp_db_id} filled | "
                f"cost=${result.total_cost_usd:.2f} "
                f"expected_pnl={result.expected_pnl_cents:.2f}¢ | "
                f"capital_remaining=${self._capital_available:.2f}"
            )
            contracts = result.total_cost_usd / max(
                ((opp.poly_ask * 100 + opp.kalshi_ask * 100) / 100), 0.01
            )
            self._tracker.record_trade(
                poly_title=opp.poly_title,
                kalshi_title=opp.kalshi_title,
                poly_side=opp.poly_side,
                kalshi_side=opp.kalshi_side,
                poly_ask=opp.poly_ask,
                kalshi_ask=opp.kalshi_ask,
                combined_cost=opp.combined_cost,
                contracts=contracts,
                size_usd=result.total_cost_usd,
                expected_pnl_cents=result.expected_pnl_cents,
                paper=self.paper_mode,
                opp_db_id=opp_db_id,
            )
        else:
            failed_leg = (
                result.poly_leg if not result.poly_leg.success else result.kalshi_leg
            )
            logger.warning(f"#{opp_db_id} execution failed: {failed_leg.error}")

        await self._persist_trade(opp, result, opp_db_id)
        return result

    async def run_loop(self) -> None:
        """Drain the queue and execute opportunities."""
        while True:
            try:
                opp = await self._queue.get()
                # The opportunity was already saved with an ID by the detector
                # Re-fetch latest DB id for this pair
                async with db_conn() as conn:
                    opp_db_id = await conn.fetchval(
                        """
                        SELECT id FROM arb_opportunities
                        WHERE pair_id=$1 AND status='detected'
                        ORDER BY detected_at DESC LIMIT 1
                        """,
                        opp.pair_id,
                    )
                if opp_db_id:
                    await self.process_one(opp, opp_db_id)
                self._queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"ExecutionBot loop error: {exc}")
