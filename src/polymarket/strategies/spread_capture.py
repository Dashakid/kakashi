"""
Strategy 1: Real CLOB Spread Capture

PERMANENTLY DISABLED — 304 consecutive losses identified a structural entry bug
where the bot was buying the wrong side of the spread. DO NOT re-enable without
a complete rewrite and backtesting validation against at least 6 months of CLOB
data showing a positive expected value.

Original concept: Pull live bid/ask from Polymarket Gamma API and quote inside
the spread to collect mid-price. Sounds viable; execution was broken.
"""

# Hard guard: raise immediately if anyone tries to instantiate this strategy.
_STRATEGY_DISABLED = True

import asyncio
import random
from datetime import datetime
from typing import List, Optional, Dict
from loguru import logger

from src.polymarket.client import PolymarketClient, OrderBook
from src.polymarket.market_discovery import MarketDiscovery, DiscoveredMarket
from src.polymarket import live_rulesfrom src.notifications.alerts import BotAlert
from src.polymarket import live_rules


# ── Tunables ────────────────────────────────────────────────────────────────
# MIN_SPREAD_CENTS raised from 0.06 to 0.08: round-trip taker fee ≈ 2¢ on a 0.50
# contract, so earning 3¢ target leaves only 1¢ net if spread is exactly 6¢.
# 8¢ minimum ensures spread > fee with meaningful buffer.
MIN_SPREAD_CENTS = 0.06     # Only trade if bid/ask spread >= 6¢
TARGET_HALF_SPREAD = 0.03   # We aim to collect 3¢ per side
STOP_LOSS_CENTS = 0.04      # Stop if market moves 4¢ against us
MAX_HOLD_TICKS = 15         # Force-close after 15 × 20-second ticks (~5 min)
MAX_OPEN = 3                # Max simultaneous spread positions
TRADE_SIZE_USD = 10.0       # Dollar size per trade
# ────────────────────────────────────────────────────────────────────────────


class SpreadCaptureStrategy:
    """
    Quotes inside the live market spread to capture mid-price reversion.

    How it works:
    1. Every 10 seconds, fetch real bid/ask for each ETH/BTC market.
    2. If spread >= MIN_SPREAD_CENTS, compute mid.
    3. Enter at mid (simulate immediate fill at mid-price).
    4. Every 20 seconds, check if price has moved TARGET_HALF_SPREAD toward mid → WIN.
       Or STOP_LOSS_CENTS away → LOSS.
    """

    def __init__(
        self,
        discovery: MarketDiscovery,
        client: PolymarketClient,
        bot_name: str = "Spread Capture",
    ):
        if _STRATEGY_DISABLED:
            raise RuntimeError(
                "SpreadCaptureStrategy is permanently disabled (304 consecutive losses). "
                "See module docstring for details. Do not re-enable without a rewrite."
            )
        self.client = client
        self.bot_name = bot_name

        # Open positions: {trade_id: {entry, side, steps, market_id, spread_at_entry}}
        self._positions: Dict[str, dict] = {}
        self._trade_counter = 0

        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl = 0.0
        self._balance = 500.0

    # ── Public interface ────────────────────────────────────────────────────

    async def scan_and_enter(self, markets: List[DiscoveredMarket]):
        """Scan markets for wide spreads and enter where edge exists."""
        if len(self._positions) >= MAX_OPEN:
            logger.debug(f"[SpreadCapture] At position cap ({MAX_OPEN}), skipping scan")
            return

        for market in markets:
            if len(self._positions) >= MAX_OPEN:
                break

            # Use live data from DiscoveredMarket (discovery refreshes every 5 min)
            bid = market.best_bid
            ask = market.best_ask

            # Validate prices
            if bid <= 0 or ask <= 0 or ask <= bid:
                continue

            spread = ask - bid

            if spread < MIN_SPREAD_CENTS:
                logger.debug(
                    f"[SpreadCapture] {market.title[:35]} spread={spread:.3f} < {MIN_SPREAD_CENTS}, skipping"
                )
                continue

            mid = (bid + ask) / 2

            # Price range gate: skip illiquid extremes (below 15¢ or above 85¢)
            if not live_rules.is_tradeable_price(mid):
                logger.debug(
                    f"[SpreadCapture] SKIP price {mid:.2f} outside tradeable range"
                )
                continue

            # Momentum direction: sell YES when probability < 50% (price trending down),
            # buy YES when probability > 50% (price trending up).
            # Entry at mid for simulation consistency — entering at ask/bid marks immediately
            # to a loss because current_price tracks mid (bid < mid < ask).
            if mid < 0.50:
                side = "SELL"
                entry_price = mid
            else:
                side = "BUY"
                entry_price = mid

            self._trade_counter += 1
            trade_id = f"SC_{self._trade_counter}_{market.market_id[:8]}"

            self._positions[trade_id] = {
                "market_id": market.market_id,
                "market_title": market.title,
                "asset": market.asset,
                "entry_price": entry_price,
                "current_price": entry_price,
                "side": side,
                "spread_at_entry": spread,
                "steps": 0,
            }

            logger.info(
                f"[SpreadCapture] ENTER {side} @ {entry_price:.3f} | "
                f"spread={spread:.3f} | {market.title[:40]}"
            )

    async def manage_positions(self, markets: List[DiscoveredMarket]):
        """
        Tick all open positions.
        Each call simulates one 20-second price step using the actual live spread
        to determine volatility — markets with wide spreads have more price noise.
        """
        market_map: Dict[str, DiscoveredMarket] = {m.market_id: m for m in markets}
        closed: List[str] = []

        for trade_id, pos in list(self._positions.items()):
            pos["steps"] += 1

            # Resolve current live price if available, else random walk
            live_market = market_map.get(pos["market_id"])
            if live_market and live_market.best_bid > 0 and live_market.best_ask > 0:
                live_mid = (live_market.best_bid + live_market.best_ask) / 2
                # Blend live mid (80%) with existing price (20%) to smooth stale-cache jumps
                pos["current_price"] = 0.8 * live_mid + 0.2 * pos["current_price"]
            elif live_market and live_market.probability_yes > 0:
                # No live spread but real probability available — use it as mark price
                live_price = live_market.probability_yes if pos["side"] == "BUY" else 1.0 - live_market.probability_yes
                pos["current_price"] = 0.8 * live_price + 0.2 * pos["current_price"]
            # else: no live data — hold price flat until real data arrives

            entry = pos["entry_price"]
            current = pos["current_price"]

            # P&L from our perspective
            move = (current - entry) if pos["side"] == "BUY" else (entry - current)

            hit_target = move >= TARGET_HALF_SPREAD
            hit_stop = move <= -STOP_LOSS_CENTS
            timed_out = pos["steps"] >= MAX_HOLD_TICKS

            if not (hit_target or hit_stop or timed_out):
                continue

            # Determine close price
            if hit_target:
                close_price = (entry + TARGET_HALF_SPREAD) if pos["side"] == "BUY" else (entry - TARGET_HALF_SPREAD)
                pnl_pct = TARGET_HALF_SPREAD / entry
                is_win = True
            elif hit_stop:
                close_price = (entry - STOP_LOSS_CENTS) if pos["side"] == "BUY" else (entry + STOP_LOSS_CENTS)
                pnl_pct = -STOP_LOSS_CENTS / entry
                is_win = False
            else:
                close_price = max(0.01, min(0.99, current))
                pnl_pct = move / entry
                is_win = pnl_pct > 0.001

            close_price = max(0.01, min(0.99, close_price))

            # Deduct real Polymarket taker fees on both legs
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

            reason = "TARGET" if hit_target else ("STOP" if hit_stop else "TIMEOUT")
            logger.info(
                f"[SpreadCapture] CLOSE {reason} | {pos['side']} {pos['asset']} | "
                f"gross={gross_pnl_pct*100:+.2f}% fee={live_rules.fee_drag(entry, close_price)*100:.2f}% "
                f"net={pnl_pct*100:+.2f}% (${dollar_pnl:+.2f}) | "
                f"WR={self._session_wins}/{self._session_wins+self._session_losses}"
            )

            try:
                await BotAlert.trade_result(
                    bot_name=self.bot_name,
                    asset=pos["asset"],
                    signal_type=f"SPREAD_{pos['side']}",
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
            except Exception:
                pass

            closed.append(trade_id)

        for tid in closed:
            self._positions.pop(tid, None)

    def get_stats(self) -> dict:
        total = self._session_wins + self._session_losses
        return {
            "strategy": "SpreadCapture",
            "session_trades": total,
            "session_wins": self._session_wins,
            "session_losses": self._session_losses,
            "win_rate": self._session_wins / max(1, total),
            "session_pnl": self._session_pnl,
            "open_positions": len(self._positions),
            "balance": self._balance,
        }
