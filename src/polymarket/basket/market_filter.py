"""
Market filters for Kakashi v2 Basket Strategy.

Two guards must both pass before any position is opened:
  1. Liquidity guard  — market open interest ≥ MIN_LIQUIDITY_USD ($10k)
  2. Slippage guard   — current best price has not moved more than
                        MAX_PRICE_DRIFT_PCT (3%) since the basket consensus
                        was first recorded.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from loguru import logger

from src.polymarket.basket.wallets import MIN_LIQUIDITY_USD

# Maximum price drift from consensus price before we skip the trade.
MAX_PRICE_DRIFT_PCT = 0.03   # 3%

# Entry price band — outside this band the risk/reward is structurally bad:
#   price 0.97 → risk $97 to win $3 (a near-resolved market)
#   price 0.02 → longshot lottery ticket; consensus here is usually stale
MIN_ENTRY_PRICE = 0.05
MAX_ENTRY_PRICE = 0.92


def check_price_band(snapshot: MarketSnapshot) -> tuple[bool, str]:
    """Reject entries whose price is too close to 0 or 1."""
    p = snapshot.current_price
    if p < MIN_ENTRY_PRICE or p > MAX_ENTRY_PRICE:
        return False, (
            f"price {p:.3f} outside entry band "
            f"[{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}]"
        )
    return True, ""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MarketSnapshot:
    """Minimal market data needed to run both filters."""
    market_id: str
    market_title: str
    outcome: str           # "Yes" / "No"
    current_price: float   # current best ask for the outcome token
    volume_24h: float      # 24h trading volume in USD (proxy for liquidity)
    open_interest: float   # total open interest in USD
    fetched_at: float      # unix timestamp of this snapshot


@dataclass
class FilterResult:
    """Result from running both market filters."""
    passed: bool
    reason: str            # human-readable explanation (empty string if passed)
    consensus_price: float
    current_price: float
    liquidity: float
    drift_pct: float


# ---------------------------------------------------------------------------
# Filter functions
# ---------------------------------------------------------------------------

def check_liquidity(snapshot: MarketSnapshot) -> tuple[bool, str]:
    """
    Return (True, "") if the market has enough liquidity, else (False, reason).
    Uses open_interest as primary metric; falls back to volume_24h if OI is 0.
    """
    liquidity = snapshot.open_interest if snapshot.open_interest > 0 else snapshot.volume_24h
    if liquidity < MIN_LIQUIDITY_USD:
        return False, (
            f"liquidity ${liquidity:,.0f} < required ${MIN_LIQUIDITY_USD:,.0f}"
        )
    return True, ""


def check_slippage(
    snapshot: MarketSnapshot,
    consensus_price: float,
) -> tuple[bool, str]:
    """
    Return (True, "") if the current price is within MAX_PRICE_DRIFT_PCT of
    the consensus price recorded when the basket signal formed.

    A drift > 3% means the market has already moved against us; skip.
    """
    if consensus_price <= 0:
        return False, "consensus_price is zero or negative"

    drift = abs(snapshot.current_price - consensus_price) / consensus_price
    if drift > MAX_PRICE_DRIFT_PCT:
        return False, (
            f"price drifted {drift*100:.1f}% "
            f"(consensus={consensus_price:.3f}, now={snapshot.current_price:.3f}, "
            f"max={MAX_PRICE_DRIFT_PCT*100:.0f}%)"
        )
    return True, ""


def run_filters(
    snapshot: MarketSnapshot,
    consensus_price: float,
) -> FilterResult:
    """
    Run both guards in order.  Returns a FilterResult with passed=True only
    when both guards pass.

    Call this immediately before placing (or recording) any paper position.
    """
    liquidity = snapshot.open_interest if snapshot.open_interest > 0 else snapshot.volume_24h
    drift = (
        abs(snapshot.current_price - consensus_price) / consensus_price
        if consensus_price > 0 else 1.0
    )

    liq_ok, liq_reason = check_liquidity(snapshot)
    if not liq_ok:
        logger.debug(
            f"[filter] SKIP {snapshot.market_title[:40]} | {liq_reason}"
        )
        return FilterResult(
            passed=False,
            reason=liq_reason,
            consensus_price=consensus_price,
            current_price=snapshot.current_price,
            liquidity=liquidity,
            drift_pct=drift,
        )

    band_ok, band_reason = check_price_band(snapshot)
    if not band_ok:
        logger.debug(
            f"[filter] SKIP {snapshot.market_title[:40]} | {band_reason}"
        )
        return FilterResult(
            passed=False,
            reason=band_reason,
            consensus_price=consensus_price,
            current_price=snapshot.current_price,
            liquidity=liquidity,
            drift_pct=drift,
        )

    slip_ok, slip_reason = check_slippage(snapshot, consensus_price)
    if not slip_ok:
        logger.debug(
            f"[filter] SKIP {snapshot.market_title[:40]} | {slip_reason}"
        )
        return FilterResult(
            passed=False,
            reason=slip_reason,
            consensus_price=consensus_price,
            current_price=snapshot.current_price,
            liquidity=liquidity,
            drift_pct=drift,
        )

    logger.debug(
        f"[filter] PASS {snapshot.market_title[:40]} | "
        f"liquidity=${liquidity:,.0f} drift={drift*100:.1f}%"
    )
    return FilterResult(
        passed=True,
        reason="",
        consensus_price=consensus_price,
        current_price=snapshot.current_price,
        liquidity=liquidity,
        drift_pct=drift,
    )
