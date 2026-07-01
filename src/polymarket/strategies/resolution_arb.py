"""
Strategy 3: Resolution Arbitrage

When a Polymarket binary market is close to expiry AND the outcome is
already de-facto decided (price has already crossed the strike or is
deeply in-the-money), market prices lag — you can still buy at 0.80-0.92
when the fair value is nearly 1.00.

Edge: Late-settling markets with already-resolved outcomes.
      Buy at 0.85, collect 1.00 at resolution → +17.6% return.
      Very high win rate when screened properly.

Entry conditions (all must pass):
  1. Market closes within EXPIRY_WINDOW_HOURS hours.
  2. Market is trading at DEEP_ITM_THRESHOLD or above (e.g. 0.82+).
  3. Current price confirms outcome: Kraken spot > strike for YES,
     or spot < strike for NO (safety check).
  4. Market has not already resolved (probability_yes != 1.0 / 0.0).
"""

import asyncio
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from loguru import logger

from src.polymarket.market_discovery import DiscoveredMarket, MarketDiscovery
from src.polymarket.strategies.kraken_poly_arb import (
    KrakenClient,
    _extract_strike,
    _days_to_expiry,
    KRAKEN_PAIRS,
)
from src.data.kraken_client import KrakenClientfrom src.notifications.alerts import BotAlert
from src.polymarket import live_rules


# ── Tunables ────────────────────────────────────────────────────────────────
EXPIRY_WINDOW_HOURS = 48    # Only look at markets expiring in <= 48 hours
DEEP_ITM_THRESHOLD = 0.80   # Market must already be priced >= 80% likely
MIN_DISCOUNT = 0.05         # Must be at least 5¢ from 1.00 (i.e. price <= 0.95)
MAX_OPEN = 5                # Higher cap — these are lower-risk bets
TRADE_SIZE_USD = 15.0       # Slightly larger size since edge is clearer
# Resolution simulation parameters
RESOLUTION_WIN_PROB = 0.85  # ~85% win rate expected (spot confirms direction)
RESOLUTION_TICKS = 40       # Average 40 × 20s = ~13 minutes to simulate resolution
# ────────────────────────────────────────────────────────────────────────────


class ResolutionArb:
    """
    Near-expiry resolution arbitrage.

    For each upcoming market:
      - If time to expiry < EXPIRY_WINDOW_HOURS and price >= DEEP_ITM_THRESHOLD,
        the market is a near-certain winner.
      - We buy YES (or NO) and hold until resolution (or stop out).
      - Kraken spot confirms whether outcome is already locked in.
    """

    def __init__(
        self,
        discovery: MarketDiscovery,
        bot_name: str = "Resolution Arb",
    ):
        self.discovery = discovery
        self.bot_name = bot_name

        self._positions: Dict[str, dict] = {}
        self._trade_counter = 0
        self._spot_cache: Dict[str, float] = {}

        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl = 0.0
        self._balance = 500.0

    async def refresh_spot(self):
        """Fetch latest Kraken spot prices for confirmation checks."""
        for asset, pair in KRAKEN_PAIRS.items():
            try:
                async with KrakenClient() as kraken:
                    df = await kraken.get_dataframe(pair=pair, interval=60, lookback_hours=4)
                if not df.empty:
                    self._spot_cache[asset] = float(df["close"].iloc[-1])
                    logger.debug(f"[ResArb] {asset} spot=${self._spot_cache[asset]:,.2f}")
            except Exception as e:
                logger.warning(f"[ResArb] Kraken fetch failed for {asset}: {e}")

    def _confirms_yes(self, market: DiscoveredMarket) -> bool:
        """
        Returns True if Kraken spot price confirms the YES outcome is
        already locked in (or confirmation unavailable, gives benefit of doubt).
        """
        asset = market.asset
        spot = self._spot_cache.get(asset)
        if spot is None:
            return True  # No Kraken data — allow but with lower confidence

        strike = _extract_strike(market.title, asset)
        if strike is None:
            return True  # No parseable strike — allow

        title_lower = market.title.lower()
        if "below" in title_lower or "under" in title_lower:
            # YES = price is below strike
            return spot < strike
        else:
            # YES = price is above strike
            return spot > strike

    # ── Entry logic ─────────────────────────────────────────────────────────

    async def scan_and_enter(self, markets: List[DiscoveredMarket]):
        """Find near-expiry, deeply-priced markets and enter."""
        if len(self._positions) >= MAX_OPEN:
            return

        for market in markets:
            if len(self._positions) >= MAX_OPEN:
                break

            days = _days_to_expiry(market.closes_at)
            hours = days * 24

            # Must be near expiry
            if hours > EXPIRY_WINDOW_HOURS:
                continue

            # Must not have already resolved
            prob = market.probability_yes
            if prob >= 0.99 or prob <= 0.01:
                continue

            # Don't re-enter markets we're already in
            already_in = any(
                p["market_id"] == market.market_id
                for p in self._positions.values()
            )
            if already_in:
                continue

            # Check deep ITM on YES side
            if prob >= DEEP_ITM_THRESHOLD and (1.0 - prob) >= MIN_DISCOUNT:
                if not self._confirms_yes(market):
                    logger.debug(
                        f"[ResArb] {market.title[:45]} YES={prob:.2f} but spot DOESN'T confirm — skipping"
                    )
                    continue

                # Require a real ask to take liquidity
                if not live_rules.has_real_orderbook(market.best_bid, market.best_ask):
                    logger.debug(f"[ResArb] SKIP no real orderbook for {market.title[:40]}")
                    continue

                entry_price = market.best_ask   # Taker pays the ask (not mid)
                side = "BUY_YES"
                confidence = min(0.99, prob + 0.05)   # High confidence

            # Check deep ITM on NO side (market says NO is >80% likely)
            elif (1.0 - prob) >= DEEP_ITM_THRESHOLD and prob >= MIN_DISCOUNT:
                # YES is unlikely, so BUY_NO
                no_prob = 1.0 - prob
                if self._confirms_yes(market):
                    # Spot confirms YES outcome — don't bet NO
                    logger.debug(
                        f"[ResArb] {market.title[:45]} NO={no_prob:.2f} but spot confirms YES — skipping"
                    )
                    continue

                # Require a real bid to sell YES (=buy NO)
                if not live_rules.has_real_orderbook(market.best_bid, market.best_ask):
                    logger.debug(f"[ResArb] SKIP no real orderbook for {market.title[:40]}")
                    continue

                entry_price = 1.0 - market.best_bid  # Taker receives bid on the YES side; NO cost = 1-bid
                side = "BUY_NO"
                confidence = min(0.99, no_prob + 0.05)
                no_prob_for_log = no_prob
            else:
                continue

            self._trade_counter += 1
            trade_id = f"RA_{self._trade_counter}_{market.market_id[:8]}"

            prob_for_position = prob if side == "BUY_YES" else (1.0 - prob)

            self._positions[trade_id] = {
                "market_id": market.market_id,
                "market_title": market.title,
                "asset": market.asset,
                "side": side,
                "entry_price": entry_price,
                "current_price": entry_price,
                "prob_at_entry": prob_for_position,
                "days_at_entry": days,
                "confidence": confidence,
                "steps": 0,
            }

            logger.info(
                f"[ResArb] ENTER {side} @ {entry_price:.3f} | "
                f"prob={prob_for_position:.2f} | expires in {hours:.1f}h | "
                f"{market.title[:45]}"
            )

    # ── Position management ──────────────────────────────────────────────────

    async def manage_positions(self, markets: List[DiscoveredMarket]):
        """
        Simulate price walk toward resolution.
        Deep ITM positions drift toward 1.00 over time (mean-reverting up).
        """
        market_map: Dict[str, DiscoveredMarket] = {m.market_id: m for m in markets}
        closed: List[str] = []

        for trade_id, pos in list(self._positions.items()):
            pos["steps"] += 1

            # Update from live market if available
            live_market = market_map.get(pos["market_id"])
            if live_market:
                prob = live_market.probability_yes
                if pos["side"] == "BUY_YES":
                    live_price = (live_market.best_bid + live_market.best_ask) / 2 if live_market.best_ask > 0 else prob
                else:
                    live_price = 1.0 - ((live_market.best_bid + live_market.best_ask) / 2 if live_market.best_ask > 0 else prob)
                pos["current_price"] = 0.7 * live_price + 0.3 * pos["current_price"]
            # else: no live data — hold price flat, do not drift synthetically toward 1.0

            entry = pos["entry_price"]
            current = pos["current_price"]
            move = current - entry

            # Resolution criteria
            # WIN: price reaches 0.95+ (near full resolution)
            # LOSS: price drops back below 0.70 (outcome in doubt — stop out)
            hit_resolution = current >= 0.95
            hit_stop = current <= 0.70
            timed_out = pos["steps"] >= RESOLUTION_TICKS

            if not (hit_resolution or hit_stop or timed_out):
                continue

            if hit_resolution:
                close_price = 0.97
                pnl_pct = (close_price - entry) / entry
                is_win = True
            elif hit_stop:
                close_price = 0.70
                pnl_pct = (close_price - entry) / entry
                is_win = False
            else:
                close_price = max(0.01, min(0.99, current))
                pnl_pct = (close_price - entry) / entry
                is_win = pnl_pct > 0.001

            # Deduct real Polymarket taker fees on both legs
            # NOTE: If held to full resolution and redeemed at $1.00 via smart contract,
            # only entry fee applies. Here we simulate a market sell at 0.97 (taker),
            # so both legs are charged.
            gross_pnl_pct = pnl_pct
            pnl_pct = live_rules.net_pnl(gross_pnl_pct, entry, close_price)
            is_win = pnl_pct > 0

            dollar_pnl = pnl_pct * TRADE_SIZE_USD
            self._balance += dollar_pnl

            if is_win:
                self._session_wins += 1
            else:
                self._session_losses += 1
            self._session_pnl += pnl_pct

            reason = "RESOLVED" if hit_resolution else ("STOP" if hit_stop else "TIMEOUT")
            logger.info(
                f"[ResArb] CLOSE {reason} | {pos['side']} {pos['asset']} | "
                f"entry={entry:.3f} close={close_price:.3f} | "
                f"gross={gross_pnl_pct*100:+.1f}% fee={live_rules.fee_drag(entry, close_price)*100:.2f}% "
                f"net={pnl_pct*100:+.1f}% (${dollar_pnl:+.2f}) | "
                f"WR={self._session_wins}/{self._session_wins+self._session_losses}"
            )

            try:
                await BotAlert.trade_result(
                    bot_name=self.bot_name,
                    asset=pos["asset"],
                    signal_type=pos["side"],
                    pnl_pct=pnl_pct,
                    is_win=is_win,
                    entry_price=entry,
                    exit_price=close_price,
                    session_wins=self._session_wins,
                    session_losses=self._session_losses,
                    session_pnl=self._session_pnl,
                    all_time_wins=self._session_wins,
                    all_time_losses=self._session_losses,
                    streak=0,
                    dollar_pnl=dollar_pnl,
                    account_balance=self._balance,
                    starting_capital=500.0,
                )
            except Exception as exc:
                logger.debug(f"[ResArb] Discord close alert skipped: {exc}")

            closed.append(trade_id)

        for tid in closed:
            self._positions.pop(tid, None)

    def get_stats(self) -> dict:
        total = self._session_wins + self._session_losses
        return {
            "strategy": "ResolutionArb",
            "session_trades": total,
            "session_wins": self._session_wins,
            "session_losses": self._session_losses,
            "win_rate": self._session_wins / max(1, total),
            "session_pnl": self._session_pnl,
            "open_positions": len(self._positions),
            "balance": self._balance,
        }
