"""
Strategy 2: Kraken Price vs Polymarket Probability Arbitrage

Uses a Black-Scholes-style lognormal model to compute the fair probability
that ETH/BTC will be above (or below) a given strike at expiry.

If the market prices the event at 0.65 but our model says 0.80, we buy YES.
If the market prices it at 0.80 but our model says 0.60, we buy NO.

Edge: real price data + math beats emotional crowd pricing, especially
      for markets with clear strikes (e.g. "ETH above $2000 this week").

References:
  - Black-Scholes digital call: N(d2) where d2 = (ln(S/K) + (r - σ²/2)T) / (σ√T)
  - We use r=0 (no risk-free rate on Polymarket), σ = 30d realised vol from Kraken
"""

import asyncio
import math
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional
from loguru import logger

from src.polymarket.market_discovery import DiscoveredMarket, MarketDiscovery
from src.data.kraken_client import KrakenClient
from src.data.kraken_client import KrakenClientfrom src.notifications.alerts import BotAlert
from src.polymarket import live_rules


# ── Tunables ────────────────────────────────────────────────────────────────
# Edge must dominate round-trip taker fees (~2%) plus a real margin of safety.
# At 12% edge, EV is positive even with adverse selection on entry.
MIN_EDGE = 0.15             # Only trade if |model_prob - market_prob| >= 15%
MAX_OPEN = 2                # Cap concurrent risk
TRADE_SIZE_USD = 10.0       # Dollar per trade
PROFIT_TARGET_PCT = 0.06    # Close at +6% → ~3¢ profit on a 0.50 contract
STOP_LOSS_PCT = 0.04        # Stop at -4%
MAX_HOLD_TICKS = 90         # 90 × 20s = 30 minutes max hold
FALLBACK_VOL = 0.80         # 80% annualised vol if Kraken fetch fails

# Expiry window. Near-expiry markets have high gamma — a 1% spot move shifts
# the probability by 5-15% when only 0.5-2 days remain. Far-out markets
# (7-14 days) barely react in 30 minutes and cause timeout losses.
# We only trade markets with 0.5h–2 days to expiry: real price sensitivity.
MIN_DAYS_TO_EXPIRY = 0.05   # ≥ 1.2h — avoid resolution-second whiplash
MAX_DAYS_TO_EXPIRY = 2.0    # ≤ 2d — high-gamma near-expiry markets only
# ────────────────────────────────────────────────────────────────────────────

# Kraken pair map: Polymarket asset name → Kraken pair
KRAKEN_PAIRS = {
    "ETH": "XETHZUSD",
    "BTC": "XXBTZUSD",
}


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc — no scipy needed."""
    return 0.5 * math.erfc(-x / math.sqrt(2))


def fair_probability(
    spot: float,
    strike: float,
    days_to_expiry: float,
    annualised_vol: float,
    direction: str = "above",
) -> float:
    """
    Compute P(price > strike at expiry) using Black-Scholes digital call.

    Args:
        spot: Current asset price (e.g. 2150.0 for ETH)
        strike: Market threshold (e.g. 2000.0)
        days_to_expiry: Calendar days until market resolves
        annualised_vol: Realised volatility (e.g. 0.80 for 80% p.a.)
        direction: "above" → P(S_T > K), "below" → 1 - P(S_T > K)

    Returns:
        Probability in [0.01, 0.99]
    """
    if days_to_expiry <= 0:
        if direction == "above":
            return 1.0 if spot > strike else 0.0
        else:
            return 1.0 if spot < strike else 0.0

    T = days_to_expiry / 365.0
    sigma = max(annualised_vol, 0.01)

    # d2 = (ln(S/K) - 0.5σ²T) / (σ√T)  [risk-free rate = 0]
    d2 = (math.log(spot / strike) - 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))

    prob_above = _norm_cdf(d2)

    if direction == "below":
        p = 1.0 - prob_above
    else:
        p = prob_above

    return max(0.01, min(0.99, p))


def _extract_strike(title: str, asset: str) -> Optional[float]:
    """
    Extract strike price from a Polymarket market title.

    Examples:
        "Will ETH be above $2000 this week?" → 2000.0
        "Bitcoin above $100k by end of April?" → 100000.0
        "ETH price above $1,800 on April 18?" → 1800.0
    """
    import re
    # Match patterns like $2000, $2,000, $100k, $1.5k
    pattern = r'\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM]?)'
    matches = re.findall(pattern, title)

    for raw, suffix in matches:
        try:
            val = float(raw.replace(",", ""))
            if suffix.lower() == "k":
                val *= 1_000
            elif suffix.lower() == "m":
                val *= 1_000_000
            # Sanity check: ETH range $100–$50k, BTC $1k–$500k
            if asset == "ETH" and 100 < val < 50_000:
                return val
            if asset == "BTC" and 1_000 < val < 500_000:
                return val
        except ValueError:
            continue
    return None


def _days_to_expiry(closes_at: Optional[str]) -> float:
    """Return days until market closes. Returns 7.0 if unknown."""
    if not closes_at:
        return 7.0
    try:
        close_dt = datetime.fromisoformat(closes_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = (close_dt - now).total_seconds() / 86400.0
        return max(0.0, delta)
    except Exception:
        return 7.0


class KrakenPolyArb:
    """
    Kraken-vs-Polymarket probability arbitrage.

    For each active ETH/BTC price-target market:
      1. Fetch current spot from Kraken.
      2. Compute implied vol from recent 1-day price history.
      3. Run lognormal model to get fair probability.
      4. Compare to market price. Enter if edge >= MIN_EDGE.
    """

    def __init__(
        self,
        discovery: MarketDiscovery,
        bot_name: str = "Kraken-Poly Arb",
    ):
        self.discovery = discovery
        self.bot_name = bot_name

        self._positions: Dict[str, dict] = {}
        self._trade_counter = 0
        self._spot_cache: Dict[str, float] = {}   # asset → latest Kraken price
        self._vol_cache: Dict[str, float] = {}    # asset → annualised vol

        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl = 0.0
        self._balance = 500.0

    # ── Kraken data ─────────────────────────────────────────────────────────

    async def refresh_spot_and_vol(self):
        """Fetch latest price and compute realised vol for each asset."""
        for asset, pair in KRAKEN_PAIRS.items():
            try:
                async with KrakenClient() as kraken:
                    df = await kraken.get_dataframe(pair=pair, interval=60, lookback_hours=48)

                if df.empty:
                    logger.warning(f"[KrakenPolyArb] No data for {asset}, using cache")
                    continue

                spot = float(df["close"].iloc[-1])
                self._spot_cache[asset] = spot

                # Realised vol: std of log returns × √(24×365) annualised
                log_returns = (df["close"] / df["close"].shift(1)).dropna().apply(math.log)
                hourly_vol = float(log_returns.std())
                annualised_vol = hourly_vol * math.sqrt(24 * 365)
                self._vol_cache[asset] = max(0.20, min(3.0, annualised_vol))

                logger.info(
                    f"[KrakenPolyArb] {asset}: spot=${spot:,.2f} | "
                    f"vol={self._vol_cache[asset]:.1%}"
                )

            except Exception as e:
                logger.warning(f"[KrakenPolyArb] Kraken fetch failed for {asset}: {e}")
                self._vol_cache.setdefault(asset, FALLBACK_VOL)

    # ── Entry logic ─────────────────────────────────────────────────────────

    async def scan_and_enter(self, markets: List[DiscoveredMarket]):
        """Evaluate each market; enter if model price diverges from market price."""
        if len(self._positions) >= MAX_OPEN:
            return

        for market in markets:
            if len(self._positions) >= MAX_OPEN:
                break
            if market.market_type not in ("price_target", "direction"):
                continue

            asset = market.asset
            spot = self._spot_cache.get(asset)
            vol = self._vol_cache.get(asset, FALLBACK_VOL)

            if not spot:
                logger.debug(f"[KrakenPolyArb] No spot price for {asset}, skipping")
                continue

            strike = _extract_strike(market.title, asset)
            if strike is None:
                logger.debug(f"[KrakenPolyArb] No strike found in: {market.title}")
                continue

            days = _days_to_expiry(market.closes_at)

            # Expiry window gate: only trade markets where 10-min hold can realise edge
            if not (MIN_DAYS_TO_EXPIRY <= days <= MAX_DAYS_TO_EXPIRY):
                logger.debug(
                    f"[KrakenPolyArb] SKIP days={days:.2f} outside "
                    f"[{MIN_DAYS_TO_EXPIRY}, {MAX_DAYS_TO_EXPIRY}] | {market.title[:40]}"
                )
                continue

            # Determine if market resolves "above" or "below"
            title_lower = market.title.lower()
            # Explicit downward-motion words always mean "below"
            _down_words = ("below", "under", "less than", "dip", "drop", "fall",
                           "crash", "decline", "sink", "plunge")
            if any(w in title_lower for w in _down_words):
                direction = "below"
            elif "reach" in title_lower and strike is not None and spot is not None and strike < spot:
                # "Will BTC reach $65k?" with BTC at $74k → needs to fall → below
                direction = "below"
            elif "reach" in title_lower and strike is not None and spot is not None and strike > spot:
                # "Will BTC reach $90k?" with BTC at $74k → needs to rise → above
                direction = "above"
            else:
                direction = "above"

            model_prob = fair_probability(spot, strike, days, vol, direction)
            market_prob = market.probability_yes
            edge = model_prob - market_prob

            logger.info(
                f"[KrakenPolyArb] {market.title[:50]} | "
                f"S={spot:.0f} K={strike:.0f} days={days:.1f} vol={vol:.0%} | "
                f"model={model_prob:.2f} market={market_prob:.2f} edge={edge:+.2f}"
            )

            if abs(edge) < MIN_EDGE:
                logger.info(f"[KrakenPolyArb] SKIP edge={edge:+.2f} < {MIN_EDGE} | {market.title[:40]}")
                continue

            # Price range gate: skip illiquid extremes (below 15¢ or above 85¢)
            if not live_rules.is_tradeable_price(market_prob):
                logger.info(
                    f"[KrakenPolyArb] SKIP price {market_prob:.2f} outside tradeable range "
                    f"({live_rules.PRICE_MIN}–{live_rules.PRICE_MAX})"
                )
                continue

            # Liquidity gate: require real two-sided orderbook
            if not live_rules.has_real_orderbook(market.best_bid, market.best_ask):
                logger.info(
                    f"[KrakenPolyArb] SKIP no real orderbook bid={market.best_bid} ask={market.best_ask}"
                )
                continue

            # Determine trade direction
            if edge > 0:
                side = "BUY_YES"   # Market under-prices YES — pay the ask
                entry_price = market.best_ask
            else:
                side = "BUY_NO"    # Market over-prices YES (buy NO = sell YES) — pay 1-bid
                entry_price = 1.0 - market.best_bid

            self._trade_counter += 1
            trade_id = f"KPA_{self._trade_counter}_{market.market_id[:8]}"

            self._positions[trade_id] = {
                "market_id": market.market_id,
                "market_title": market.title,
                "asset": asset,
                "side": side,
                "entry_price": entry_price,
                "current_price": entry_price,
                "model_prob": model_prob,
                "market_prob": market_prob,
                "edge": edge,
                "strike": strike,
                "days_at_entry": days,
                "steps": 0,
            }

            logger.info(
                f"[KrakenPolyArb] ENTER {side} @ {entry_price:.3f} | "
                f"edge={edge:+.2f} | {market.title[:45]}"
            )

            # Discord entry notification so the user can track without tailing logs
            try:
                await BotAlert.signal(
                    bot_name=self.bot_name,
                    signal_type=side,
                    price=entry_price,
                    confidence=min(0.99, abs(edge) / 0.20),
                    details={
                        "Market": market.title[:80],
                        "Asset": asset,
                        "Spot": f"${spot:,.2f}",
                        "Strike": f"${strike:,.0f}",
                        "Days to expiry": f"{days:.2f}",
                        "Model prob": f"{model_prob:.3f}",
                        "Market prob": f"{market_prob:.3f}",
                        "Edge": f"{edge:+.2%}",
                        "Size": f"${TRADE_SIZE_USD:.0f}",
                    },
                )
            except Exception as exc:
                logger.debug(f"[KrakenPolyArb] Discord entry alert skipped: {exc}")

    # ── Position management ──────────────────────────────────────────────────

    async def manage_positions(self, markets: List[DiscoveredMarket]):
        """Tick positions toward target/stop using live market price updates."""
        market_map: Dict[str, DiscoveredMarket] = {m.market_id: m for m in markets}
        closed: List[str] = []

        for trade_id, pos in list(self._positions.items()):
            pos["steps"] += 1

            # Update current price from live market data
            live_market = market_map.get(pos["market_id"])
            if live_market:
                if live_market.best_ask > 0 and live_market.best_bid > 0:
                    # Real spread data available — use mid-price
                    raw = (live_market.best_bid + live_market.best_ask) / 2
                    live_price = raw if pos["side"] == "BUY_YES" else 1.0 - raw
                    pos["current_price"] = 0.7 * live_price + 0.3 * pos["current_price"]
                else:
                    # Market illiquid (no real bid/ask): use probability_yes as mark.
                    # Do NOT add drift toward model — that inflates WR artificially.
                    prob_proxy = live_market.probability_yes if pos["side"] == "BUY_YES" else live_market.probability_no
                    if prob_proxy > 0:
                        pos["current_price"] = 0.8 * prob_proxy + 0.2 * pos["current_price"]
                    # No synthetic drift: hold at real probability mark
            else:
                # No live Polymarket data for this market — hold price flat.
                # Do NOT drift toward model: that creates guaranteed wins by design.
                pass  # pos["current_price"] unchanged until real data arrives

            entry = pos["entry_price"]
            current = pos["current_price"]
            move = current - entry  # positive = price moved our way

            hit_target = move >= PROFIT_TARGET_PCT
            hit_stop = move <= -STOP_LOSS_PCT
            timed_out = pos["steps"] >= MAX_HOLD_TICKS

            if not (hit_target or hit_stop or timed_out):
                continue

            # Close price
            if hit_target:
                close_price = entry + PROFIT_TARGET_PCT
                pnl_pct = PROFIT_TARGET_PCT
                is_win = True
            elif hit_stop:
                close_price = entry - STOP_LOSS_PCT
                pnl_pct = -STOP_LOSS_PCT
                is_win = False
            else:
                close_price = max(0.01, min(0.99, current))
                pnl_pct = move
                is_win = pnl_pct > 0.001

            # Skip recording if position timed out with no meaningful price movement.
            # This happens when no live Polymarket data arrived during the hold — the
            # price stays at entry 0.50 and recording 0.00 as a LOSS skews stats.
            if timed_out and not hit_target and not hit_stop and abs(pnl_pct) < 0.001:
                logger.debug(
                    f"[KrakenPolyArb] TIMEOUT (no live data) — skipping record "
                    f"| {pos['side']} {pos['asset']} | held {pos['steps']} ticks"
                )
                closed.append(trade_id)
                del self._positions[trade_id]
                continue

            close_price = max(0.01, min(0.99, close_price))

            # Deduct real Polymarket taker fees (both entry and exit legs)
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
                f"[KrakenPolyArb] CLOSE {reason} | {pos['side']} {pos['asset']} | "
                f"gross={gross_pnl_pct*100:+.2f}% fee={live_rules.fee_drag(entry, close_price)*100:.2f}% "
                f"net={pnl_pct*100:+.2f}% (${dollar_pnl:+.2f}) | "
                f"WR={self._session_wins}/{self._session_wins+self._session_losses}"
            )

            try:
                all_time_wins = self._session_wins
                all_time_losses = self._session_losses
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
                    all_time_wins=all_time_wins,
                    all_time_losses=all_time_losses,
                    streak=0,
                    dollar_pnl=dollar_pnl,
                    account_balance=self._balance,
                    starting_capital=500.0,
                )
            except Exception as exc:
                logger.debug(f"[KrakenPolyArb] Discord close alert skipped: {exc}")

            closed.append(trade_id)

        for tid in closed:
            self._positions.pop(tid, None)

    def get_stats(self) -> dict:
        total = self._session_wins + self._session_losses
        return {
            "strategy": "KrakenPolyArb",
            "session_trades": total,
            "session_wins": self._session_wins,
            "session_losses": self._session_losses,
            "win_rate": self._session_wins / max(1, total),
            "session_pnl": self._session_pnl,
            "open_positions": len(self._positions),
            "balance": self._balance,
            "spot_cache": self._spot_cache,
        }
