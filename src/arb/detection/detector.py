"""Phase 3 — Arbitrage Detector.

For every active matched pair, reads the latest cached order books from Redis
and checks whether buying YES on one platform + NO on the other costs < 100¢.
Opportunities above the minimum profit threshold are written to the DB.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from loguru import logger

from src.arb.config import (
    MIN_PROFIT_CENTS,
    POLY_FEE_PCT,
    KALSHI_FEE_PCT,
    ORDERBOOK_POLL_INTERVAL,
)
from src.arb.db.connection import db_conn, redis_get_json


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ArbOpportunity:
    pair_id: int
    poly_market_id: int
    kalshi_market_id: int
    poly_external_id: str
    kalshi_external_id: str
    poly_title: str
    kalshi_title: str
    # Which side to buy on each platform
    poly_side: str       # 'yes' | 'no'
    kalshi_side: str     # 'yes' | 'no'
    poly_ask: float      # price in 0-1 (fraction of $1)
    kalshi_ask: float
    combined_cost: float   # should be < 1.00 for arb
    gross_profit: float    # 1 - combined_cost   (fraction)
    poly_fee: float
    kalshi_fee: float
    net_profit_cents: float   # after fees, per $1 of contracts
    detected_at: float


# ── Detector ──────────────────────────────────────────────────────────────────

class ArbDetector:
    """
    Continuously scans active matched pairs for arbitrage opportunities.
    """

    def __init__(
        self,
        min_profit_cents: float = MIN_PROFIT_CENTS,
        poly_fee_pct: float = POLY_FEE_PCT,
        kalshi_fee_pct: float = KALSHI_FEE_PCT,
    ) -> None:
        self.min_profit_cents = min_profit_cents
        self.poly_fee_pct = poly_fee_pct
        self.kalshi_fee_pct = kalshi_fee_pct

    # ── DB helpers ────────────────────────────────────────────────────────────

    async def _load_active_pairs(self) -> list[dict]:
        async with db_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    mp.id          AS pair_id,
                    pm.id          AS poly_market_id,
                    pm.external_id AS poly_external_id,
                    pm.title       AS poly_title,
                    km.title       AS kalshi_title,
                    km.id          AS kalshi_market_id,
                    km.external_id AS kalshi_external_id,
                    mp.confidence
                FROM matched_pairs mp
                JOIN markets pm ON pm.id = mp.poly_market_id
                JOIN markets km ON km.id = mp.kalshi_market_id
                WHERE mp.is_active = TRUE
                  AND pm.status = 'active'
                  AND km.status = 'active'
                """
            )
        return [dict(r) for r in rows]

    async def _save_opportunity(self, opp: ArbOpportunity) -> int:
        async with db_conn() as conn:
            row_id: int = await conn.fetchval(
                """
                INSERT INTO arb_opportunities
                    (pair_id, poly_side, kalshi_side,
                     poly_ask, kalshi_ask, combined_cost,
                     gross_profit, poly_fee, kalshi_fee, net_profit,
                     status)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'detected')
                RETURNING id
                """,
                opp.pair_id,
                opp.poly_side,
                opp.kalshi_side,
                round(opp.poly_ask * 100, 4),      # store as cents
                round(opp.kalshi_ask * 100, 4),
                round(opp.combined_cost * 100, 4),
                round(opp.gross_profit * 100, 4),
                round(opp.poly_fee * 100, 4),
                round(opp.kalshi_fee * 100, 4),
                round(opp.net_profit_cents, 4),
            )
        return row_id

    # ── Order-book fetch ──────────────────────────────────────────────────────

    async def _get_best_ask(self, platform: str, external_id: str) -> float | None:
        """Return the best ask for YES contracts from Redis cache (fraction 0-1)."""
        prefix = "poly" if platform == "polymarket" else "kalshi"
        ob = await redis_get_json(f"{prefix}:ob:{external_id}")
        if ob is None:
            return None
        ask = ob.get("best_ask")
        if ask is None:
            asks = ob.get("asks", [])
            if asks:
                ask = asks[0]["price"] if isinstance(asks[0], dict) else asks[0][0]
        return float(ask) if ask is not None else None

    # ── Core check ────────────────────────────────────────────────────────────

    def _calculate_arb(
        self,
        pair: dict,
        poly_yes_ask: float | None,
        poly_no_ask: float | None,
        kalshi_yes_ask: float | None,
        kalshi_no_ask: float | None,
    ) -> ArbOpportunity | None:
        """
        Two strategies:
          A) Buy YES on Poly  + NO  on Kalshi  → pays out $1 regardless of outcome
          B) Buy NO  on Poly  + YES on Kalshi  → pays out $1 regardless of outcome
        Arb exists when combined cost < $1 (100 cents).
        """
        candidates: list[tuple[str, str, float, float]] = []

        if poly_yes_ask is not None and kalshi_no_ask is not None:
            candidates.append(("yes", "no", poly_yes_ask, kalshi_no_ask))

        if poly_no_ask is not None and kalshi_yes_ask is not None:
            candidates.append(("no", "yes", poly_no_ask, kalshi_yes_ask))

        best: ArbOpportunity | None = None

        for poly_side, kalshi_side, p_ask, k_ask in candidates:
            combined = p_ask + k_ask
            if combined >= 1.0:
                continue

            gross = 1.0 - combined
            # Fees are charged on notional ($1 payout per contract)
            p_fee = self.poly_fee_pct
            k_fee = self.kalshi_fee_pct
            net = (gross - p_fee - k_fee) * 100  # in cents

            if net < self.min_profit_cents:
                continue

            opp = ArbOpportunity(
                pair_id=pair["pair_id"],
                poly_market_id=pair["poly_market_id"],
                kalshi_market_id=pair["kalshi_market_id"],
                poly_external_id=pair["poly_external_id"],
                kalshi_external_id=pair["kalshi_external_id"],
                poly_title=pair["poly_title"],
                kalshi_title=pair["kalshi_title"],
                poly_side=poly_side,
                kalshi_side=kalshi_side,
                poly_ask=p_ask,
                kalshi_ask=k_ask,
                combined_cost=combined,
                gross_profit=gross,
                poly_fee=p_fee,
                kalshi_fee=k_fee,
                net_profit_cents=net,
                detected_at=time.time(),
            )

            if best is None or net > best.net_profit_cents:
                best = opp

        return best

    # ── Scan loop ─────────────────────────────────────────────────────────────

    async def scan_once(self) -> list[ArbOpportunity]:
        """Single pass over all active pairs. Returns detected opportunities."""
        pairs = await self._load_active_pairs()
        if not pairs:
            return []

        found: list[ArbOpportunity] = []

        for pair in pairs:
            poly_ext = pair["poly_external_id"]
            kalshi_ext = pair["kalshi_external_id"]

            # Fetch both sides; for NO we use 1 - YES_bid as a proxy when missing
            poly_yes_ask = await self._get_best_ask("polymarket", poly_ext)
            kalshi_yes_ask = await self._get_best_ask("kalshi", kalshi_ext)

            # NO ask ≈ 1 - YES_bid (complementary prices on binary markets)
            poly_no_ask = (1.0 - poly_yes_ask) if poly_yes_ask is not None else None
            kalshi_no_ask = (1.0 - kalshi_yes_ask) if kalshi_yes_ask is not None else None

            opp = self._calculate_arb(
                pair, poly_yes_ask, poly_no_ask, kalshi_yes_ask, kalshi_no_ask
            )

            if opp is not None:
                opp_id = await self._save_opportunity(opp)
                logger.info(
                    f"ARB #{opp_id}: {opp.poly_title[:50]} | "
                    f"Poly {opp.poly_side.upper()} @{opp.poly_ask*100:.1f}¢ + "
                    f"Kalshi {opp.kalshi_side.upper()} @{opp.kalshi_ask*100:.1f}¢ = "
                    f"{opp.combined_cost*100:.1f}¢ combined | "
                    f"net +{opp.net_profit_cents:.2f}¢"
                )
                found.append(opp)

        if found:
            logger.info(f"Detector scan: {len(found)} opportunities found from {len(pairs)} pairs")

        return found

    async def run_loop(self, interval: float = ORDERBOOK_POLL_INTERVAL) -> None:
        """Run the detector in a continuous loop."""
        while True:
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Detector error: {exc}")
            await asyncio.sleep(interval)
