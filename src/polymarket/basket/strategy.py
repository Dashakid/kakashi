"""
BasketStrategy — core logic for Kakashi v2.

Paper mode ONLY.  LIVE_TRADING is hardcoded False and cannot be overridden.

Loop overview
-------------
Every POLL_INTERVAL_SECONDS:
  1. For each active basket, fetch the current open positions of every
     tracked wallet via the Polymarket data API.
  2. Build a "consensus map": for each (market, outcome) pair, count how
     many wallets in the basket hold that position.
  3. If consensus_count / basket_size >= CONSENSUS_THRESHOLD (80%):
       a. Run market filters (liquidity + slippage).
       b. If we don't already have an open paper trade for this signal,
          size the trade at MAX_POSITION_PCT (5%) of account balance,
          capped at PAPER_TRADE_MAX_USD.
       c. Record via BasketTracker and fire a Discord alert.
  4. Check all open paper positions for resolution (exit_price available
     from the API).  Close and record P&L.
  5. Every RERANK_INTERVAL_SECONDS: flag stale wallets for review.

All API calls use aiohttp with a ClientTimeout.  No auth required for
read-only data-api endpoints.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

from src.notifications.alerts import BotAlert
from src.notifications.discord_webhook import get_discord_client
from src.polymarket.basket.market_filter import MarketSnapshot, run_filters
from src.polymarket.basket.tracker import BasketTracker
from src.polymarket.basket.wallets import BASKETS, all_basket_names

# ---------------------------------------------------------------------------
# Hard constants — PAPER ONLY, NOT runtime-configurable
# ---------------------------------------------------------------------------

# ⚠️  LIVE_TRADING IS PERMANENTLY DISABLED.
# This cannot be changed via env var.  Remove this guard and rebuild
# only after 30+ days of validated paper P&L.
LIVE_TRADING: bool = False

CONSENSUS_THRESHOLD  = 0.50     # 50% of basket wallets must agree
MAX_POSITION_PCT     = 0.05     # 5% of balance per trade
PAPER_TRADE_MAX_USD  = 100.0    # hard cap on paper notional per position
PAPER_BALANCE_USD    = 1_000.0  # starting paper balance for sizing

MIN_BASKET_SIZE      = 2        # require at least 2 wallets per basket
MAX_OPEN_PER_BASKET  = 3        # max concurrent open paper trades per basket

POLL_INTERVAL_SECONDS   = 60    # check positions every 60 s
RERANK_INTERVAL_SECONDS = 7 * 24 * 3600   # weekly wallet refresh flag

DATA_API   = "https://data-api.polymarket.com"
GAMMA_API  = "https://gamma-api.polymarket.com"

BOT_NAME = "Kakashi v2 (Basket)"


# ---------------------------------------------------------------------------
# Internal data classes
# ---------------------------------------------------------------------------

@dataclass
class WalletPosition:
    """One wallet's position in one market as returned by the data API."""
    wallet: str
    market_id: str
    market_title: str
    outcome: str          # "Yes" / "No"
    size_usd: float
    avg_price: float
    token_id: str = ""


@dataclass
class ConsensusSignal:
    """A basket-level signal that has passed the 80% threshold."""
    basket: str
    market_id: str
    market_title: str
    outcome: str
    consensus_price: float          # average entry price across agreeing wallets
    agreeing_wallets: List[str]
    basket_size: int
    consensus_pct: float            # agreeing / basket_size


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class BasketStrategy:
    """
    Runs the basket-consensus Polymarket copy strategy in paper mode.

    Instantiated and driven by main_kakashi_v2.py.
    """

    def __init__(self) -> None:
        self._tracker = BasketTracker()
        self._http: Optional[aiohttp.ClientSession] = None
        self._last_rerank_ts: float = 0.0
        self._session_signals: int = 0
        self._market_cache: Dict[str, MarketSnapshot] = {}

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run_forever(self) -> None:
        """Entry point called by main_kakashi_v2.run()."""
        webhook = os.getenv("KAKASHI_V2_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL")
        if webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = webhook
            logger.info("🔔 Using Discord webhook for Kakashi v2")

        timeout = aiohttp.ClientTimeout(total=30, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self._http = session

            try:
                await BotAlert.startup(
                    bot_name=BOT_NAME,
                    strategy=(
                        "Basket consensus: 80%+ wallet agreement | "
                        "paper-only | 5% max per trade"
                    ),
                    mode="PAPER",
                )
            except Exception as exc:
                logger.debug(f"Startup alert failed: {exc}")

            logger.info(f"🚀 {BOT_NAME} running — PAPER mode (LIVE_TRADING=False hardcoded)")
            self._log_basket_summary()

            while True:
                try:
                    await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(f"[strategy] Unhandled error in tick: {exc}", exc_info=True)

                await asyncio.sleep(POLL_INTERVAL_SECONDS)

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    async def _tick(self) -> None:
        now = time.time()

        # Weekly refresh flag
        if now - self._last_rerank_ts > RERANK_INTERVAL_SECONDS:
            await self._flag_stale_wallets()
            self._last_rerank_ts = now

        # Process each active basket
        for basket_name in all_basket_names():
            if not self._tracker.is_basket_active(basket_name):
                logger.debug(f"[strategy] basket '{basket_name}' is dropped — skipping")
                continue

            wallet_entries = BASKETS.get(basket_name, [])
            if len(wallet_entries) < MIN_BASKET_SIZE:
                logger.debug(
                    f"[strategy] basket '{basket_name}' has {len(wallet_entries)} wallet(s) "
                    f"(need ≥{MIN_BASKET_SIZE}) — skipping"
                )
                continue

            await self._process_basket(basket_name, wallet_entries)

        # Check open positions for resolution
        await self._check_open_positions()

    # ------------------------------------------------------------------
    # Per-basket processing
    # ------------------------------------------------------------------

    async def _process_basket(
        self,
        basket_name: str,
        wallet_entries: list,
    ) -> None:
        """Fetch positions for all wallets in a basket, find consensus."""
        all_positions: List[WalletPosition] = []
        for address, label in wallet_entries:
            try:
                positions = await self._fetch_wallet_positions(address, label)
                all_positions.extend(positions)
            except Exception as exc:
                logger.warning(
                    f"[strategy] Failed to fetch {label} ({address[:8]}…): {exc}"
                )

        if not all_positions:
            return

        signals = self._find_consensus(basket_name, all_positions, len(wallet_entries))
        for signal in signals:
            await self._handle_signal(signal)

    def _find_consensus(
        self,
        basket_name: str,
        positions: List[WalletPosition],
        basket_size: int,
    ) -> List[ConsensusSignal]:
        """
        Group positions by (market_id, outcome).
        Return ConsensusSignal for each group where agreeing_count / basket_size
        >= CONSENSUS_THRESHOLD.
        """
        # key → {wallets, prices, title}
        groups: Dict[Tuple[str, str], Dict] = {}
        for pos in positions:
            key = (pos.market_id, pos.outcome)
            if key not in groups:
                groups[key] = {
                    "wallets": [],
                    "prices": [],
                    "title": pos.market_title,
                }
            groups[key]["wallets"].append(pos.wallet)
            groups[key]["prices"].append(pos.avg_price)

        signals = []
        for (market_id, outcome), data in groups.items():
            unique_wallets = list(set(data["wallets"]))
            consensus_pct = len(unique_wallets) / basket_size
            if consensus_pct < CONSENSUS_THRESHOLD:
                continue

            avg_price = (
                sum(data["prices"]) / len(data["prices"]) if data["prices"] else 0.5
            )
            signals.append(
                ConsensusSignal(
                    basket=basket_name,
                    market_id=market_id,
                    market_title=data["title"],
                    outcome=outcome,
                    consensus_price=avg_price,
                    agreeing_wallets=unique_wallets,
                    basket_size=basket_size,
                    consensus_pct=consensus_pct,
                )
            )
            logger.info(
                f"[strategy] CONSENSUS {basket_name}/{outcome} "
                f"{data['title'][:40]} | "
                f"{len(unique_wallets)}/{basket_size} wallets "
                f"({consensus_pct*100:.0f}%)"
            )

        return signals

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    async def _handle_signal(self, signal: ConsensusSignal) -> None:
        """Validate filters, check dedup, then open a paper trade."""
        logger.info(
            f"[strategy] HANDLE {signal.basket}/{signal.outcome} "
            f"market_id={signal.market_id[:12]}... "
            f"consensus={signal.consensus_pct*100:.0f}%"
        )

        # Dedup: already open for this market+outcome in this basket?
        open_trades = self._tracker.open_positions_for_basket(signal.basket)
        for t in open_trades:
            if t.market_id == signal.market_id and t.outcome == signal.outcome:
                logger.debug(
                    f"[strategy] SKIP dup {signal.basket}/{signal.market_id}/{signal.outcome}"
                )
                return

        # Per-basket cap
        if len(open_trades) >= MAX_OPEN_PER_BASKET:
            logger.debug(
                f"[strategy] basket '{signal.basket}' at max open "
                f"({MAX_OPEN_PER_BASKET}) — skipping"
            )
            return

        # Fetch current market snapshot for filters
        snapshot = await self._fetch_market_snapshot(
            signal.market_id, signal.outcome, signal.consensus_price
        )
        if snapshot is None:
            logger.info(
                f"[strategy] SKIP snapshot-missing {signal.basket}/{signal.outcome} "
                f"market_id={signal.market_id}"
            )
            return

        filter_result = run_filters(snapshot, signal.consensus_price)
        if not filter_result.passed:
            logger.info(
                f"[strategy] FILTERED {signal.market_title[:40]} | {filter_result.reason}"
            )
            return

        # Size: 5% of paper balance, capped at PAPER_TRADE_MAX_USD
        size_usd = min(PAPER_BALANCE_USD * MAX_POSITION_PCT, PAPER_TRADE_MAX_USD)

        trade_id = self._tracker.open_trade(
            basket=signal.basket,
            market_id=signal.market_id,
            market_title=signal.market_title,
            outcome=signal.outcome,
            entry_price=snapshot.current_price,
            paper_size_usd=size_usd,
        )

        logger.info(
            f"[strategy] OPENED trade_id={trade_id} "
            f"{signal.basket}/{signal.outcome} @ {snapshot.current_price:.3f}"
        )

        self._session_signals += 1
        await self._send_signal_alert(signal, snapshot, size_usd, trade_id)

    # ------------------------------------------------------------------
    # Position resolution
    # ------------------------------------------------------------------

    async def _check_open_positions(self) -> None:
        """
        For every open paper trade, check if the market has resolved.
        If resolved, close the trade at the final price (1.0 for YES win, 0.0 for loss).
        """
        open_trades = list(self._tracker._state.open_trades.values())
        for trade in open_trades:
            try:
                resolved, final_price = await self._check_market_resolved(
                    trade.market_id, trade.outcome
                )
                if resolved:
                    closed = self._tracker.close_trade(
                        trade.trade_id,
                        exit_price=final_price,
                        reason="resolved",
                    )
                    if closed is not None:
                        from src.tracking.trade_logger import log_trade
                        log_trade(
                            bot="v2",
                            market=getattr(trade, "market_title", trade.market_id),
                            outcome=trade.outcome,
                            entry_price=closed.entry_price,
                            exit_price=closed.exit_price,
                            size_usd=closed.paper_size_usd,
                            is_win=closed.is_win,
                            resolved_via="gamma_api",
                        )
            except Exception as exc:
                logger.debug(
                    f"[strategy] resolution check failed for {trade.trade_id}: {exc}"
                )

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    async def _fetch_wallet_positions(
        self, address: str, label: str
    ) -> List[WalletPosition]:
        """
        Fetch current open positions for one wallet from the data API.
        Returns an empty list on error.
        """
        url = f"{DATA_API}/positions?user={address}&sizeThreshold=10"
        try:
            async with self._http.get(url) as resp:
                if resp.status != 200:
                    logger.debug(f"[api] positions {label}: HTTP {resp.status}")
                    return []
                data = await resp.json()
        except Exception as exc:
            logger.debug(f"[api] positions {label}: {exc}")
            return []

        results = []
        for item in data if isinstance(data, list) else data.get("data", []):
            try:
                # Normalise outcome to "Yes" / "No"
                raw_outcome = item.get("outcome") or item.get("side", "Yes")
                outcome = "Yes" if str(raw_outcome).lower() in ("yes", "1", "long") else "No"

                results.append(
                    WalletPosition(
                        wallet=address,
                        market_id=item.get("conditionId") or item.get("market_id", ""),
                        market_title=item.get("title") or item.get("market", ""),
                        outcome=outcome,
                        size_usd=float(item.get("currentValue") or item.get("size", 0)),
                        avg_price=float(item.get("avgPrice") or item.get("price", 0.5)),
                        token_id=item.get("asset") or item.get("tokenId", ""),
                    )
                )
            except Exception:
                continue
        return results

    async def _fetch_market_snapshot(
        self,
        market_id: str,
        outcome: str,
        consensus_price: float,
    ) -> Optional[MarketSnapshot]:
        """
        Fetch current price and open-interest for a market.
        Returns None on error.
        """
        logger.info(f"[strategy] snapshot lookup market_id={market_id}")

        # Try 0: local cache
        cached = self._market_cache.get(market_id)
        if cached is not None:
            return cached

        async def _get_markets(url: str) -> List[dict]:
            try:
                async with self._http.get(url) as resp:
                    if resp.status != 200:
                        logger.debug(f"[api] market snapshot {market_id}: HTTP {resp.status} url={url}")
                        return []
                    data = await resp.json()
            except Exception as exc:
                logger.debug(f"[api] market snapshot {market_id}: {exc}")
                return []

            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for key in ("data", "markets"):
                    value = data.get(key)
                    if isinstance(value, list):
                        return value
            return []

        def _market_condition_id(m: dict) -> str:
            return str(m.get("conditionId") or m.get("condition_id") or "")

        def _market_has_token_id(m: dict, token_id: str) -> bool:
            if not token_id:
                return False
            tid = token_id.lower()
            for tok in m.get("tokens", []) or []:
                if str(tok.get("tokenId") or tok.get("asset") or "").lower() == tid:
                    return True
            return False

        def _to_snapshot(m: dict) -> Optional[MarketSnapshot]:
            def _safe_float(value: object, default: float = 0.0) -> float:
                if value is None:
                    return default
                try:
                    return float(str(value).replace(",", "").strip())
                except Exception:
                    return default

            def _as_list(value: object) -> List[object]:
                if isinstance(value, list):
                    return value
                if isinstance(value, str):
                    text = value.strip()
                    if not text:
                        return []
                    try:
                        parsed = json.loads(text)
                        return parsed if isinstance(parsed, list) else []
                    except Exception:
                        return []
                return []

            try:
                current_price = consensus_price  # fallback

                # Shape A: tokens is a list of dicts with outcome/price.
                token_rows = m.get("tokens", [])
                if isinstance(token_rows, list):
                    for tok in token_rows:
                        if not isinstance(tok, dict):
                            continue
                        tok_outcome = str(tok.get("outcome") or tok.get("name") or "").lower()
                        if outcome.lower() in tok_outcome:
                            current_price = _safe_float(
                                tok.get("price") or tok.get("lastTradePrice"),
                                consensus_price,
                            )
                            break

                # Shape B: outcomes/outcomePrices are JSON-encoded strings.
                if current_price == consensus_price:
                    outcomes = _as_list(m.get("outcomes"))
                    outcome_prices = _as_list(m.get("outcomePrices"))
                    for idx, out_name in enumerate(outcomes):
                        if outcome.lower() not in str(out_name).lower():
                            continue
                        if idx < len(outcome_prices):
                            current_price = _safe_float(outcome_prices[idx], consensus_price)
                        break

                event_list = m.get("events", [])
                event_open_interest = 0.0
                if isinstance(event_list, list) and event_list and isinstance(event_list[0], dict):
                    event_open_interest = _safe_float(event_list[0].get("openInterest"), 0.0)

                open_interest = _safe_float(
                    m.get("openInterest") or m.get("liquidity") or m.get("volume") or event_open_interest,
                    0.0,
                )
                volume_24h = _safe_float(m.get("volume24hr") or m.get("volume24h"), 0.0)
            except Exception:
                return None

            return MarketSnapshot(
                market_id=market_id,
                market_title=m.get("question") or m.get("title", ""),
                outcome=outcome,
                current_price=current_price,
                volume_24h=volume_24h,
                open_interest=open_interest,
                fetched_at=time.time(),
            )

        # Try 1: direct condition id lookups
        for url in (
            f"{GAMMA_API}/markets?condition_ids={market_id}",
            f"{GAMMA_API}/markets?conditionIds={market_id}",
            f"{GAMMA_API}/markets?conditionId={market_id}",
        ):
            markets = await _get_markets(url)
            if markets:
                snap = _to_snapshot(markets[0])
                if snap is not None:
                    self._market_cache[market_id] = snap
                    return snap

        # Try 2: keyword search fallback and match conditionId/token id
        search_markets = await _get_markets(
            f"{GAMMA_API}/markets?search=Bitcoin&active=true&limit=100"
        )
        if search_markets:
            target = market_id.lower()
            match = next(
                (
                    m
                    for m in search_markets
                    if _market_condition_id(m).lower() == target or _market_has_token_id(m, target)
                ),
                None,
            )
            if match is not None:
                snap = _to_snapshot(match)
                if snap is not None:
                    self._market_cache[market_id] = snap
                    return snap

        logger.warning(f"[strategy] snapshot-missing for {market_id[:20]} — all lookups failed")
        return None

    async def _check_market_resolved(
        self, market_id: str, outcome: str
    ) -> Tuple[bool, float]:
        """
        Returns (is_resolved, final_price).
        final_price = 1.0 if our outcome won, 0.0 if lost.
        """
        markets: List[dict] = []
        for url in (
            f"{GAMMA_API}/markets?condition_ids={market_id}",
            f"{GAMMA_API}/markets?conditionIds={market_id}",
            f"{GAMMA_API}/markets?conditionId={market_id}",
        ):
            try:
                async with self._http.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
            except Exception:
                continue

            markets = data if isinstance(data, list) else data.get("data", [])
            if markets:
                break

        if not markets:
            return False, 0.0

        m = markets[0]
        closed = m.get("closed") or m.get("resolved") or False
        if not closed:
            return False, 0.0

        # Determine winning outcome
        winner = (m.get("winnerOutcome") or m.get("resolutionOutcome") or "").lower()
        if not winner:
            return False, 0.0

        final_price = 1.0 if outcome.lower() in winner else 0.0
        return True, final_price

    # ------------------------------------------------------------------
    # Weekly wallet refresh flag
    # ------------------------------------------------------------------

    async def _flag_stale_wallets(self) -> None:
        """
        Log which baskets are due for manual wallet review.
        Does NOT automatically remove wallets — that requires human research.
        """
        due = self._tracker.baskets_due_for_wallet_refresh()
        if not due:
            return

        logger.info(
            f"[strategy] Wallets due for weekly review: {due}. "
            "Check polymarket.com/leaderboard and update wallets.py if needed."
        )
        try:
            discord = get_discord_client()
            if discord.enabled:
                embed = {
                    "title": "📋 Kakashi v2 — Weekly Wallet Review Due",
                    "description": (
                        f"Baskets due: **{', '.join(due)}**\n\n"
                        "Check polymarketanalytics.com/traders and "
                        "update `src/polymarket/basket/wallets.py`."
                    ),
                    "color": 0xFFAA00,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await discord.send_message(embed=embed)
        except Exception as exc:
            logger.debug(f"Wallet review alert failed: {exc}")

        for basket in due:
            self._tracker.mark_wallet_refresh_done(basket)

    # ------------------------------------------------------------------
    # Discord alerts
    # ------------------------------------------------------------------

    async def _send_signal_alert(
        self,
        signal: ConsensusSignal,
        snapshot: MarketSnapshot,
        size_usd: float,
        trade_id: str,
    ) -> None:
        try:
            await BotAlert.signal(
                bot_name=BOT_NAME,
                signal_type=f"COPY_{signal.outcome.upper()}",
                price=snapshot.current_price,
                confidence=signal.consensus_pct,
                details={
                    "Market":    signal.market_title[:80],
                    "Basket":    signal.basket,
                    "Outcome":   signal.outcome,
                    "Consensus": f"{signal.consensus_pct*100:.0f}% ({len(signal.agreeing_wallets)}/{signal.basket_size} wallets)",
                    "Liquidity": f"${snapshot.open_interest:,.0f}",
                    "Price":     f"{snapshot.current_price:.3f} (consensus {signal.consensus_price:.3f})",
                    "Size":      f"${size_usd:.2f} paper",
                    "Trade ID":  trade_id,
                    "Mode":      "📄 PAPER ONLY",
                },
            )
        except Exception as exc:
            logger.debug(f"Signal alert failed: {exc}")

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def _log_basket_summary(self) -> None:
        for name, entries in BASKETS.items():
            active = self._tracker.is_basket_active(name)
            tag = "✅" if active else "🚫 DROPPED"
            logger.info(
                f"[basket] {name:10s} {tag}  wallets={len(entries)}"
            )
        if all(len(v) == 0 for v in BASKETS.values()):
            logger.warning(
                "[basket] All baskets are empty — populate wallets.py before expecting trades."
            )
