"""
Top Trader Follower Bot — copies the hottest weekly Polymarket wallets.

Replaces the consensus-based copycat with a simpler, faster-validating thesis:

    "Find the top 10 wallets by 7-day realized PnL.
     Copy every new position they open above the size threshold."

Why this works on Polymarket:
- Wallets concentrate by niche (sports / politics / crypto). Consensus is rare.
- A wallet on a hot streak this week is the best proxy for next-week skill.
- Bigger trades = higher conviction. Filtering small bets removes noise.

API endpoints (no auth):
- GET https://data-api.polymarket.com/trades?limit=1000&minAmount=500
       → recent large trades (used for discovery + 7d volume aggregation)
- GET https://data-api.polymarket.com/positions?user=ADDRESS
       → current positions (used to detect new entries by tracked wallets)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from loguru import logger

from src.notifications.alerts import BotAlert
from src.polymarket.client import PolymarketClient


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Ranking
TOP_N = 5                            # how many hot wallets we follow at once
LOOKBACK_DAYS = 7                    # rolling window for "this week"
MIN_7D_VOLUME_USD = 500.0            # meaningful volume — filters out one-off trades
MIN_7D_REALIZED_PNL_USD = 200.0      # real profit, not just $1 noise
MIN_WIN_RATE = 0.58                  # must win more than they lose across all positions
MIN_RESOLVED_TRADES = 20             # minimum resolved positions — rules out hot streaks

# Hardcoded high-quality wallets — always followed regardless of filters.
# Respectful-Clan: 64% win rate, +$67k cashPnL, $1.1M portfolio
# Blind-Leaf: 87% win rate, 60W/9L
PRIORITY_WALLETS: List[Tuple[str, str]] = [
    ("0x6e1d5040d0ac73709b0621f620d2a60b80d2d0fa", "Respectful-Clan"),
    ("0x2b5920274740ca61745eeb077587bf5bb4ff9db6", "Blind-Leaf"),
]

# Discovery
TRADE_SCAN_LIMIT = 1000              # how many recent large trades to scan
DISCOVERY_MIN_TRADE_USD = 50.0       # ignore trades under $50 when discovering

# Copy filter (per their incoming position)
MIN_COPY_POSITION_USD = 1.0          # minimum their position size (USD) to copy
MIN_PCT_OF_7D_VOLUME = 0.05          # AND ≥5% of their weekly volume
MIN_CONVICTION = 0.10                # skip if position < 10% of wallet's weekly volume (noise filter)
SKIP_PRICE_LOW = 0.15                # skip near-resolved markets (≤15¢)
SKIP_PRICE_HIGH = 0.85               #  or (≥85¢) — no edge there
MAX_POSITION_AGE_MINUTES = 1440      # copy positions opened within the last 24h

# Our paper sizing
PAPER_TRADE_SIZE_USD = 100.0
STARTING_CAPITAL_USD = 450.0  # actual wallet balance — synced from chain on startup

# Live order config
# Set LIVE_TRADING=True to place real $5 orders alongside paper tracking.
# Keep size small until 50+ real trades confirm edge holds with real fills.
LIVE_TRADING = False
# Trade size scales with account balance: 7% per trade, min $1, max $50.
# e.g. $14 balance → $1, $100 → $7, $500 → $35
LIVE_TRADE_PCT = 0.07
LIVE_TRADE_MIN_USD = 1.0
LIVE_TRADE_MAX_USD = 1.0
DAILY_LIVE_SPEND_CAP_USD = 100.0

# Cadence
POLL_INTERVAL_SECONDS = 5            # faster reaction time on short-lived market moves
RERANK_INTERVAL_SECONDS = 6 * 3600   # 6 h — refresh top-10 list
BALANCE_SYNC_INTERVAL_SECONDS = 3600  # 1 h — resync real USDC balance from chain
RERANK_EMPTY_RETRY_SECONDS = 1800    # 30 min — retry rerank if no wallets passed last time
POSITIONS_FALLBACK_REFRESH_SECONDS = 60  # periodic safety refresh even if no new trade signal
WALLET_TRADE_POLL_LIMIT = 50

# Risk: cap concurrent open paper positions
MAX_OPEN_POSITIONS = 5

# Only copy markets that resolve within this many days
MAX_RESOLUTION_DAYS = 30

# Persistence
DATA_DIR = Path("data")
STATE_FILE = DATA_DIR / "kakashi_state.json"
TOP_WALLETS_SNAPSHOT_FILE = Path("top_wallets.json")

BOT_NAME = "Kakashi Bot"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TopWallet:
    address: str
    label: str
    volume_7d: float
    realized_pnl_total: float       # all-time realized (proxy for skill)
    trades_7d: int
    last_seen_ts: float
    win_rate: float = 0.0           # fraction of positions with positive cashPnL
    resolved_trades: int = 0        # total positions seen (open + closed) — proxy for sample size


@dataclass
class CopiedPosition:
    market_id: str
    market_title: str
    outcome: str                    # "Yes" / "No" / etc.
    wallet_address: str
    wallet_label: str
    entry_price: float              # their avg fill we copied at
    paper_size_usd: float           # our $100 paper notional
    paper_shares: float             # paper_size_usd / entry_price
    opened_ts: float
    last_seen_size: float           # their position size when we opened
    live_order_id: Optional[str] = None   # real Polymarket order ID (None = paper only)
    live_shares: float = 0.0              # real shares bought (live_size / entry_price)
    token_id: str = ""                    # CLOB ERC-1155 token id (asset field) — needed to sell
    outcome_idx: int = 0                  # 0=Yes, 1=No (cached for the SELL leg)


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class KakashiBot:
    """Follows the hottest weekly traders on Polymarket."""

    def __init__(self) -> None:
        self.top_wallets: Dict[str, TopWallet] = {}
        self.open_positions: Dict[str, CopiedPosition] = {}   # key = market_id+outcome+wallet
        self._last_rerank_ts: float = 0.0
        self._last_balance_sync_ts: float = 0.0
        self._http: Optional[aiohttp.ClientSession] = None
        self._poly_client: Optional[PolymarketClient] = None  # real order client
        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl_dollars = 0.0
        self._account_balance = STARTING_CAPITAL_USD
        self._daily_live_spend_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._daily_live_spend_usd = 0.0
        self._wallet_last_trade_marker: Dict[str, Tuple[float, str]] = {}
        self._wallet_last_positions_scan_ts: Dict[str, float] = {}
        self._load_state()

    # ----- helpers --------------------------------------------------------

    def _live_trade_size(self) -> float:
        """Return trade size in USD: 7% of current balance, clamped to [$1, $50]."""
        size = self._account_balance * LIVE_TRADE_PCT
        return round(max(LIVE_TRADE_MIN_USD, min(LIVE_TRADE_MAX_USD, size)), 2)

    def _today_utc(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _roll_daily_budget_if_needed(self) -> None:
        today = self._today_utc()
        if self._daily_live_spend_day != today:
            logger.info(
                f"🔄 Daily live budget reset: {self._daily_live_spend_day} -> {today} "
                f"(spent was ${self._daily_live_spend_usd:.2f})"
            )
            self._daily_live_spend_day = today
            self._daily_live_spend_usd = 0.0
            self._save_state()

    def _remaining_daily_live_budget(self) -> float:
        self._roll_daily_budget_if_needed()
        return max(0.0, round(DAILY_LIVE_SPEND_CAP_USD - self._daily_live_spend_usd, 2))

    async def _sync_real_balance(self) -> None:
        """Fetch actual USDC balance from chain and update _account_balance."""
        if not self._poly_client:
            return
        try:
            bal = await self._poly_client.get_usdc_balance()
            if bal is not None and bal > 0:
                old = self._account_balance
                self._account_balance = round(bal, 2)
                logger.info(
                    f"💰 Real balance synced: ${self._account_balance:.2f} "
                    f"(was ${old:.2f}) | next trade size: ${self._live_trade_size():.2f}"
                )
                self._save_state()
            else:
                logger.debug("Balance sync returned 0 or None — keeping current estimate")
        except Exception as e:
            logger.debug(f"Balance sync failed: {e}")

    # ----- persistence ----------------------------------------------------

    def _load_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            raw = json.loads(STATE_FILE.read_text())
            self.top_wallets = {
                a: TopWallet(**d) for a, d in raw.get("top_wallets", {}).items()
            }
            self.open_positions = {
                k: CopiedPosition(**d) for k, d in raw.get("open_positions", {}).items()
            }
            # session_wins/losses/pnl reset to 0 on every restart intentionally —
            # they represent THIS run's stats only, not all-time totals.
            self._account_balance = raw.get("account_balance", STARTING_CAPITAL_USD)
            self._daily_live_spend_day = raw.get("daily_live_spend_day", self._today_utc())
            self._daily_live_spend_usd = float(raw.get("daily_live_spend_usd", 0.0) or 0.0)
            self._roll_daily_budget_if_needed()
            logger.info(
                f"📂 Loaded state: {len(self.top_wallets)} top wallets, "
                f"{len(self.open_positions)} open positions, "
                f"session {self._session_wins}W/{self._session_losses}L "
                f"${self._session_pnl_dollars:+.2f}, "
                f"daily_live_spend ${self._daily_live_spend_usd:.2f}/${DAILY_LIVE_SPEND_CAP_USD:.2f}"
            )
        except Exception as e:
            logger.warning(f"Could not load state: {e}")

    def _save_state(self) -> None:
        try:
            DATA_DIR.mkdir(exist_ok=True)
            payload = json.dumps({
                "top_wallets": {a: asdict(w) for a, w in self.top_wallets.items()},
                "open_positions": {k: asdict(p) for k, p in self.open_positions.items()},
                "session_wins": self._session_wins,
                "session_losses": self._session_losses,
                "session_pnl_dollars": self._session_pnl_dollars,
                "account_balance": self._account_balance,
                "daily_live_spend_day": self._daily_live_spend_day,
                "daily_live_spend_usd": self._daily_live_spend_usd,
            }, indent=2)
            # Atomic write: temp file → rename so a crash mid-write never corrupts state.
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(payload)
            tmp.replace(STATE_FILE)
        except Exception as e:
            logger.warning(f"Could not save state: {e}")

    # ----- API ------------------------------------------------------------

    async def _fetch_recent_trades(self, limit: int = TRADE_SCAN_LIMIT) -> List[dict]:
        """Pull recent large trades from Polymarket data API."""
        try:
            url = "https://data-api.polymarket.com/trades"
            params = {
                "limit": str(limit),
                "minAmount": str(int(DISCOVERY_MIN_TRADE_USD)),
            }
            async with self._http.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"trades API status {resp.status}")
                    return []
                data = await resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for key in ("data", "trades", "items", "results"):
                        val = data.get(key)
                        if isinstance(val, list):
                            return val
                logger.warning(
                    f"trades API payload shape not recognized: {type(data).__name__}"
                )
                return []
        except Exception as e:
            logger.warning(f"trades fetch failed: {e}")
            return []

    async def _fetch_positions(self, address: str) -> List[dict]:
        """Fetch a wallet's current open positions."""
        try:
            url = "https://data-api.polymarket.com/positions"
            params = {"user": address, "sizeThreshold": "1"}
            async with self._http.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception:
            return []

    async def _fetch_wallet_trades(self, address: str, limit: int = WALLET_TRADE_POLL_LIMIT) -> List[dict]:
        """Fetch most recent trades for a specific wallet.

        We use this as the low-latency trigger: if a new trade appears, scan
        positions immediately for copyable entries.
        """
        try:
            url = "https://data-api.polymarket.com/trades"
            params = {
                "user": address,
                "limit": str(limit),
            }
            async with self._http.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception:
            return []

    @staticmethod
    def _trade_timestamp(tr: dict) -> float:
        """Best-effort parse of trade event timestamp to unix seconds."""
        raw = tr.get("timestamp", tr.get("ts", tr.get("createdAt", tr.get("created_at", 0))))
        if raw is None:
            return 0.0
        if isinstance(raw, (int, float)):
            val = float(raw)
            # Guard for ms timestamps
            if val > 1e12:
                val /= 1000.0
            return val
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _trade_marker(tr: dict) -> Tuple[float, str]:
        """Return sortable marker (timestamp, unique-id-like) for dedupe."""
        ts = KakashiBot._trade_timestamp(tr)
        tid = str(
            tr.get("id")
            or tr.get("tradeID")
            or tr.get("tradeId")
            or tr.get("transactionHash")
            or tr.get("txHash")
            or tr.get("orderID")
            or ""
        )
        return (ts, tid)

    async def _wallet_has_new_trade(self, address: str) -> bool:
        """Whether wallet has a newer trade event than what we've already seen."""
        trades = await self._fetch_wallet_trades(address)
        if not trades:
            return False

        addr = address.lower()
        filtered: List[dict] = []
        for tr in trades:
            # Endpoint may already be filtered by user=address, but keep this
            # defensive in case payload shape changes.
            owners = [
                str(tr.get("proxyWallet") or "").lower(),
                str(tr.get("user") or "").lower(),
                str(tr.get("maker") or "").lower(),
                str(tr.get("taker") or "").lower(),
                str(tr.get("owner") or "").lower(),
            ]
            if any(o == addr for o in owners if o):
                filtered.append(tr)

        if not filtered:
            filtered = trades

        newest = max(filtered, key=self._trade_marker)
        marker = self._trade_marker(newest)
        prev = self._wallet_last_trade_marker.get(address)
        self._wallet_last_trade_marker[address] = marker
        return prev is None or marker > prev

    async def _fetch_market_price(self, condition_id: str) -> Optional[float]:
        """Get mid-price of a market for our paper exit valuation."""
        try:
            url = "https://gamma-api.polymarket.com/markets"
            params = {"conditionId": condition_id}
            async with self._http.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("markets", [])
                if not markets:
                    return None
                m = markets[0]
                bid = float(m.get("bestBid", m.get("best_bid", 0)) or 0)
                ask = float(m.get("bestAsk", m.get("best_ask", 0)) or 0)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
                return float(m.get("lastTradePrice", m.get("price", 0.5)) or 0.5)
        except Exception:
            return None

    @staticmethod
    def _trade_wallet_address(tr: dict) -> str:
        """Extract wallet address from best-known trade payload fields."""
        for field in ("proxyWallet", "user", "maker", "owner", "taker"):
            raw = tr.get(field)
            if raw is None:
                continue
            addr = str(raw).strip().lower()
            if addr.startswith("0x") and len(addr) >= 10:
                return addr
        return ""

    @staticmethod
    def _trade_usd_notional(tr: dict) -> float:
        """Best-effort USD notional extraction from trade payload."""
        # Primary: shares * price
        shares_raw = tr.get("size", tr.get("amount", tr.get("shares", 0)))
        price_raw = tr.get("price", tr.get("avgPrice", tr.get("avg_price", 0)))
        try:
            shares = float(shares_raw or 0)
            price = float(price_raw or 0)
            if shares > 0 and price > 0:
                return shares * price
        except Exception:
            pass

        # Fallback: direct USD fields when present
        for field in ("usdcSize", "usdc_size", "notional", "volume", "amountUsd", "amount_usd"):
            try:
                val = float(tr.get(field) or 0)
                if val > 0:
                    return val
            except Exception:
                continue
        return 0.0

    def _save_top_wallets_snapshot(self) -> None:
        """Write a human-readable wallet snapshot used by ranking scripts/tooling."""
        try:
            wallets = sorted(
                self.top_wallets.values(),
                key=lambda w: (w.volume_7d, w.trades_7d),
                reverse=True,
            )
            payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "method": "kakashi_bot._rerank_top_wallets (7d volume from data-api trades)",
                "total_wallets": len(wallets),
                "wallets": [
                    {
                        "address": w.address,
                        "label": w.label,
                        "volume_7d": round(w.volume_7d, 2),
                        "trades_7d": w.trades_7d,
                        "last_seen_ts": w.last_seen_ts,
                    }
                    for w in wallets
                ],
            }
            TOP_WALLETS_SNAPSHOT_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            logger.debug(f"Could not write top wallets snapshot: {e}")

    # ----- ranking --------------------------------------------------------

    async def _rerank_top_wallets(self) -> None:
        """
        Rebuild the top-N wallet list:
          1. Discover wallets active in last 7d from large-trade feed
          2. Aggregate per-wallet 7d volume + trade count
          3. For top-30 by volume, fetch /positions, get realized PnL
          4. Filter to wallets meeting volume + PnL thresholds
          5. Take top N by realized PnL
        """
        logger.info("🔄 Re-ranking top weekly wallets...")
        trades = await self._fetch_recent_trades()
        if not trades:
            logger.warning("No trades returned from API — falling back to priority wallets only")
            self.top_wallets = {
                addr: TopWallet(
                    address=addr,
                    label=label,
                    volume_7d=0.0,
                    realized_pnl_total=0.0,
                    trades_7d=0,
                    last_seen_ts=time.time(),
                )
                for addr, label in PRIORITY_WALLETS[:TOP_N]
            }
            self._last_rerank_ts = time.time()
            self._save_state()
            self._save_top_wallets_snapshot()
            return

        cutoff_ts = time.time() - LOOKBACK_DAYS * 86400
        per_wallet_vol: Dict[str, float] = {}
        per_wallet_count: Dict[str, int] = {}
        per_wallet_last_seen: Dict[str, float] = {}
        parsed_count = 0
        skipped_no_wallet = 0
        skipped_no_notional = 0
        skipped_old = 0

        for tr in trades:
            try:
                ts = self._trade_timestamp(tr)
            except Exception:
                ts = 0.0

            if ts and ts < cutoff_ts:
                skipped_old += 1
                continue

            addr = self._trade_wallet_address(tr)
            if not addr:
                skipped_no_wallet += 1
                continue

            usd = self._trade_usd_notional(tr)
            if usd <= 0:
                skipped_no_notional += 1
                continue

            parsed_count += 1
            per_wallet_vol[addr] = per_wallet_vol.get(addr, 0.0) + usd
            per_wallet_count[addr] = per_wallet_count.get(addr, 0) + 1
            per_wallet_last_seen[addr] = max(per_wallet_last_seen.get(addr, 0.0), ts)

        logger.info(
            "🧪 Rerank parse stats | "
            f"raw={len(trades)} parsed={parsed_count} "
            f"old={skipped_old} no_wallet={skipped_no_wallet} no_notional={skipped_no_notional}"
        )

        # Top 30 candidates by volume
        candidates = sorted(per_wallet_vol.items(), key=lambda x: x[1], reverse=True)[:30]
        logger.info(f"🔍 {len(per_wallet_vol)} wallets active in 7d; scoring top {len(candidates)} by volume")

        # The positions API only exposes current open-position snapshots. Using
        # cashPnl/win rate from that payload as a hard filter eliminates good
        # wallets based on unrealized noise, then leaves us copying from stale
        # persisted state. Rank from fresh 7d activity instead.
        logger.info("🔬 Top-10 candidates (addr, vol_7d, trades_7d, last_seen_age_min):")
        now_ts = time.time()
        for addr, vol in candidates[:10]:
            age_min = max(0.0, (now_ts - per_wallet_last_seen.get(addr, now_ts)) / 60)
            trades_7d = per_wallet_count.get(addr, 0)
            logger.info(
                f"    {addr[:10]}.. vol=${vol:>10,.0f}  trades={trades_7d:>3}  age={age_min:>6.0f}m"
            )

        ranked: List[TopWallet] = []
        for addr, vol in candidates:
            if vol < MIN_7D_VOLUME_USD:
                continue
            ranked.append(TopWallet(
                address=addr,
                label=f"Trader_{addr[2:8].upper()}",
                volume_7d=vol,
                realized_pnl_total=0.0,
                trades_7d=per_wallet_count.get(addr, 0),
                last_seen_ts=per_wallet_last_seen.get(addr, time.time()),
                win_rate=0.0,
                resolved_trades=0,
            ))

        # Always include priority wallets — inject BEFORE sorting so they rank
        # by their actual 7d activity alongside discovered wallets, not forced to #1.
        ranked_addrs = {w.address.lower() for w in ranked}
        for pw_addr, pw_label in PRIORITY_WALLETS:
            if pw_addr.lower() not in ranked_addrs:
                vol = per_wallet_vol.get(pw_addr.lower(), 0.0)
                ranked.append(TopWallet(
                    address=pw_addr,
                    label=pw_label,
                    volume_7d=vol,
                    realized_pnl_total=0.0,
                    trades_7d=per_wallet_count.get(pw_addr.lower(), 0),
                    last_seen_ts=time.time(),
                    win_rate=0.0,
                    resolved_trades=0,
                ))

        ranked.sort(key=lambda w: (w.volume_7d, w.trades_7d), reverse=True)
        ranked = ranked[:TOP_N]
        self.top_wallets = {w.address: w for w in ranked}
        self._last_rerank_ts = time.time()
        self._save_state()
        self._save_top_wallets_snapshot()

        if not ranked:
            logger.warning(
                f"⚠️ No discovered wallets met the 7d activity floor (vol≥${MIN_7D_VOLUME_USD:,.0f}). "
                "Using priority wallets until the next rerank."
            )
        else:
            logger.info(f"✅ Top {len(ranked)} weekly traders:")
            for i, w in enumerate(ranked, 1):
                logger.info(
                    f"  #{i} {w.label} vol7d=${w.volume_7d:,.0f} trades={w.trades_7d}"
                )
            try:
                lines = [
                    f"**#{i}** `{w.label}` · vol ${w.volume_7d/1000:.0f}k · {w.trades_7d} trades"
                    for i, w in enumerate(ranked, 1)
                ]
                from src.notifications.discord_webhook import get_discord_client
                d = get_discord_client()
                if d.enabled:
                    await d.send_message(embed={
                        "title": f"🏆 Top {len(ranked)} Weekly Traders Refreshed",
                        "description": "\n".join(lines),
                        "color": 0x4FC3F7,
                        "footer": {"text": f"Top Trader Follower | next refresh in {RERANK_INTERVAL_SECONDS//3600}h"},
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                logger.debug(f"Discord rank post failed: {e}")

    # ----- copy logic -----------------------------------------------------

    @staticmethod
    def _pos_key(market_id: str, outcome: str, wallet: str) -> str:
        return f"{market_id}|{outcome}|{wallet}"

    async def _check_for_new_copies(self) -> None:
        """Walk every top wallet, find new positions worth copying."""
        if not self.top_wallets:
            return

        self._roll_daily_budget_if_needed()

        for addr, wallet in list(self.top_wallets.items()):
            if len(self.open_positions) >= MAX_OPEN_POSITIONS:
                logger.info(f"⛔ Max open positions ({MAX_OPEN_POSITIONS}) reached — skipping new entries")
                break

            # Level 2 trigger: only hit /positions immediately when we see a new
            # trade for this wallet. Also force periodic refresh as safety net.
            has_new_trade = await self._wallet_has_new_trade(addr)
            now_ts = time.time()
            last_scan = self._wallet_last_positions_scan_ts.get(addr, 0.0)
            force_refresh = (now_ts - last_scan) >= POSITIONS_FALLBACK_REFRESH_SECONDS
            if not has_new_trade and not force_refresh:
                continue

            await asyncio.sleep(0.1)  # lighter throttle to reduce copy latency
            positions = await self._fetch_positions(addr)
            self._wallet_last_positions_scan_ts[addr] = now_ts
            if not positions:
                continue

            for raw in positions:
                try:
                    size_usd = float(raw.get("size", 0)) * float(raw.get("avgPrice", 0) or 0)
                    min_size = max(MIN_COPY_POSITION_USD, wallet.volume_7d * MIN_PCT_OF_7D_VOLUME)
                    if size_usd < min_size:
                        continue

                    # Conviction filter: skip if position is <10% of wallet's 7d volume
                    # (low conviction = noise, not a real bet)
                    if wallet.volume_7d > 0:
                        conviction = size_usd / wallet.volume_7d
                        if conviction < MIN_CONVICTION:
                            logger.debug(
                                f"⏭️  Low conviction ({conviction:.1%} of 7d vol) — "
                                f"{wallet.label} {raw.get('title','?')[:40]!r} skipped"
                            )
                            continue
                    else:
                        conviction = 0.0

                    avg_price = float(raw.get("avgPrice") or 0)
                    if avg_price <= SKIP_PRICE_LOW or avg_price >= SKIP_PRICE_HIGH:
                        continue

                    # Only copy fresh positions (opened within MAX_POSITION_AGE_MINUTES)
                    opened_ts_raw = raw.get("openedAt") or raw.get("createdAt") or raw.get("timestamp")
                    if opened_ts_raw:
                        try:
                            if isinstance(opened_ts_raw, (int, float)):
                                opened_dt = datetime.fromtimestamp(opened_ts_raw, tz=timezone.utc)
                            else:
                                opened_dt = datetime.fromisoformat(str(opened_ts_raw).replace("Z", "+00:00"))
                                if opened_dt.tzinfo is None:
                                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
                            age_minutes = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60
                            if age_minutes > MAX_POSITION_AGE_MINUTES:
                                logger.debug(
                                    f"⏭️  Skipping stale position {raw.get('title','?')!r} "
                                    f"— opened {age_minutes:.0f}min ago (>{MAX_POSITION_AGE_MINUTES}min)"
                                )
                                continue
                        except Exception:
                            pass  # if we can't parse the timestamp, allow it through

                    # Skip markets that don't resolve within MAX_RESOLUTION_DAYS
                    end_date_raw = raw.get("endDate") or raw.get("endDateIso") or raw.get("end_date_iso")
                    if end_date_raw:
                        try:
                            if isinstance(end_date_raw, (int, float)):
                                end_dt = datetime.fromtimestamp(end_date_raw, tz=timezone.utc).date()
                            else:
                                end_dt = datetime.fromisoformat(str(end_date_raw).replace("Z", "+00:00")).date()
                            today = datetime.now(timezone.utc).date()
                            if (end_dt - today).days > MAX_RESOLUTION_DAYS:
                                logger.debug(f"⏭️  Skipping {raw.get('title','?')!r} — resolves {end_dt} (>{MAX_RESOLUTION_DAYS}d away)")
                                continue
                        except Exception:
                            pass  # if we can't parse the date, allow it through

                    cond_id = raw.get("conditionId", "")
                    outcome = raw.get("outcome", "")
                    title = raw.get("title", "")
                    if not cond_id or not outcome or not title:
                        continue

                    key = self._pos_key(cond_id, outcome, addr)
                    if key in self.open_positions:
                        continue  # already copying this one

                    # Multi-wallet agreement: count how many tracked wallets hold
                    # the same market+outcome — 2+ is a materiality signal grounded
                    # in real money behaviour, no LLM needed.
                    agreement_count = sum(
                        1 for p in self.open_positions.values()
                        if p.market_id == cond_id and p.outcome == outcome
                    )
                    conviction_bonus = agreement_count >= 2

                    # Cross-wallet dedup: don't open a second position on the same market+outcome
                    already_copying = agreement_count > 0
                    if already_copying:
                        logger.debug(f"⏭️  Already copying {outcome} on {title[:50]!r} from another wallet — skipping")
                        continue

                    # Open paper-tracker shell (sized to mirror live order)
                    paper_shares = PAPER_TRADE_SIZE_USD / max(avg_price, 0.01)
                    pos = CopiedPosition(
                        market_id=cond_id,
                        market_title=title,
                        outcome=outcome,
                        wallet_address=addr,
                        wallet_label=wallet.label,
                        entry_price=avg_price,
                        paper_size_usd=PAPER_TRADE_SIZE_USD,
                        paper_shares=paper_shares,
                        opened_ts=time.time(),
                        last_seen_size=float(raw.get("size", 0)),
                    )

                    # ── Real order (live trading) ──────────────────────────────
                    if LIVE_TRADING and self._poly_client:
                        try:
                            remaining_budget = self._remaining_daily_live_budget()
                            if remaining_budget < LIVE_TRADE_MIN_USD:
                                logger.info(
                                    f"⛔ Daily live spend cap reached "
                                    f"(${self._daily_live_spend_usd:.2f}/${DAILY_LIVE_SPEND_CAP_USD:.2f}) — "
                                    "skipping new live entries"
                                )
                                continue

                            # outcome 0 = Yes, 1 = No (Polymarket convention)
                            outcome_idx = 1 if outcome.lower() in ("no", "down", "false") else 0
                            # The positions API gives us the token id directly in `asset`
                            token_id = str(raw.get("asset") or "")
                            live_size_usd = round(min(self._live_trade_size(), remaining_budget), 2)
                            if live_size_usd < LIVE_TRADE_MIN_USD:
                                logger.info(
                                    f"⏭️  Remaining daily budget ${remaining_budget:.2f} below "
                                    f"min trade ${LIVE_TRADE_MIN_USD:.2f} — skipping"
                                )
                                continue
                            live_shares = live_size_usd / max(avg_price, 0.01)
                            order_bucket = int(time.time() // POLL_INTERVAL_SECONDS)
                            client_order_key = (
                                f"buy|{cond_id}|{outcome_idx}|{addr.lower()}|{order_bucket}"
                            )
                            resp = await self._poly_client.place_order(
                                market_id=cond_id,
                                outcome=outcome_idx,
                                size=round(live_shares, 4),
                                price=round(min(avg_price + 0.02, 0.97), 4),  # slight ask-cross to get filled
                                side="BUY",
                                token_id=token_id or None,
                                client_order_key=client_order_key,
                            )
                            order_id = resp.get("orderId") or resp.get("id") or resp.get("order_id")
                            if order_id:
                                pos.live_order_id = str(order_id)
                                pos.live_shares = live_shares
                                pos.token_id = token_id
                                pos.outcome_idx = outcome_idx
                                self._daily_live_spend_usd = round(self._daily_live_spend_usd + live_size_usd, 2)
                                logger.info(
                                    f"💸 LIVE ORDER placed | {outcome} @ {avg_price:.3f} | "
                                    f"${live_size_usd} (bal=${self._account_balance:.0f}) | order_id={order_id} | "
                                    f"daily_live_spend=${self._daily_live_spend_usd:.2f}/${DAILY_LIVE_SPEND_CAP_USD:.2f}"
                                )
                            else:
                                logger.warning(f"Live order returned no ID: {resp}")
                        except Exception as live_err:
                            logger.warning(f"Live order failed: {live_err}")

                    # LIVE-ONLY MODE: only track positions that actually filled.
                    # If the live order failed, skip — no phantom paper trade.
                    if LIVE_TRADING and not pos.live_order_id:
                        logger.info(
                            f"⏭️  Skipping {wallet.label} {outcome} @ {avg_price:.3f} "
                            f"(live order failed — not opening paper-only)"
                        )
                        continue

                    self.open_positions[key] = pos
                    logger.info(
                        f"🟢 COPY OPEN | {wallet.label} | {title[:60]} | "
                        f"{outcome} @ {avg_price:.3f} | their size ${size_usd:,.0f} | "
                        f"conviction={conviction:.1%} | "
                        f"{'🔥 multi-wallet agreement | ' if conviction_bonus else ''}"
                        f"live={'✅' if pos.live_order_id else '📄 paper'}"
                    )
                    if not pos.live_order_id:
                        logger.info(
                            f"📝 PAPER TRADE: {outcome} on {title[:50]} | "
                            f"size=${pos.paper_size_usd:.2f} | price={avg_price:.3f}"
                        )
                    try:
                        await BotAlert.signal(
                            bot_name=BOT_NAME,
                            signal_type=f"COPY_{outcome.upper()}",
                            price=avg_price,
                            confidence=0.85 if conviction_bonus else 0.7,
                            details={
                                "Following": wallet.label,
                                "Their bet": f"${size_usd:,.0f}",
                                "Conviction": f"{conviction:.1%} of 7d vol",
                                "Multi-wallet": f"🔥 {agreement_count + 1} wallets agree" if conviction_bonus else "single wallet",
                                "Market": title[:200],
                                "Live size": f"${self._live_trade_size():.2f}" if LIVE_TRADING else "disabled",
                                "Daily budget left": f"${self._remaining_daily_live_budget():.2f}",
                                "Order ID": pos.live_order_id or "—",
                            },
                        )
                    except Exception as e:
                        logger.debug(f"Discord signal post failed: {e}")
                except Exception as e:
                    logger.debug(f"Skipping malformed position: {e}")

        self._save_state()

    async def _check_for_closes(self) -> None:
        """For every open paper copy, see if the followed wallet closed it
        OR if the market resolved. Close + record."""
        if not self.open_positions:
            return

        # Refresh positions per wallet (only for wallets we still have copies of)
        wallets_with_copies = {p.wallet_address for p in self.open_positions.values()}
        live_positions: Dict[str, Dict[str, dict]] = {}  # addr -> key(cond+outcome) -> raw
        for addr in wallets_with_copies:
            await asyncio.sleep(0.1)  # lighter throttle to reduce close-detection latency
            raws = await self._fetch_positions(addr)
            live_positions[addr] = {
                f"{r.get('conditionId','')}|{r.get('outcome','')}": r
                for r in raws
                if r.get("conditionId") and r.get("outcome")
            }

        to_close: List[Tuple[str, str, float, str]] = []  # (key, reason, exit_price, signal_type)

        for key, pos in list(self.open_positions.items()):
            wallet_pos_map = live_positions.get(pos.wallet_address, {})
            their_key = f"{pos.market_id}|{pos.outcome}"
            their_raw = wallet_pos_map.get(their_key)

            exit_price: Optional[float] = None
            reason = ""

            if their_raw is None:
                # They closed (no longer holding). Use market mid-price for our exit.
                exit_price = await self._fetch_market_price(pos.market_id)
                reason = "wallet_closed"
            else:
                redeemable = bool(their_raw.get("redeemable", False))
                their_size = float(their_raw.get("size", 0))
                if redeemable:
                    # Market resolved — settlement is 1.0 (WIN) or 0.0 (LOSS).
                    # curPrice propagates to 0 or 1 after resolution, but there is a
                    # lag: until fully settled it can sit at mid-value (e.g. 0.5).
                    # Only close if curPrice has actually reached a definitive value;
                    # otherwise skip this cycle and check again next loop.
                    cur = float(their_raw.get("curPrice") or their_raw.get("currentPrice") or 0)
                    if cur >= 0.9:
                        exit_price = 1.0
                        reason = "resolved_win"
                    elif cur <= 0.1:
                        exit_price = 0.0
                        reason = "resolved_loss"
                    else:
                        # Resolution still propagating — skip and retry next cycle
                        logger.debug(
                            f"⏳ Resolution pending ({pos.market_id[:20]} curPrice={cur:.3f}) — will retry"
                        )
                        continue
                elif their_size < pos.last_seen_size * 0.5:
                    # they sold ≥50% of their position → mirror exit
                    exit_price = float(their_raw.get("curPrice") or 0) or await self._fetch_market_price(pos.market_id)
                    reason = "wallet_partial_exit"

            if exit_price is None:
                continue

            signal_type = f"COPY_{pos.outcome.upper()}"
            to_close.append((key, reason, exit_price, signal_type))

        for key, reason, exit_price, signal_type in to_close:
            await self._close_position(key, exit_price, reason, signal_type)

    async def _close_position(self, key: str, exit_price: float, reason: str, signal_type: str) -> None:
        pos = self.open_positions.pop(key, None)
        if pos is None:
            return

        # ── Real exit order ─────────────────────────────────────────────────
        live_dollar_pnl = 0.0
        sell_confirmed = False
        if LIVE_TRADING and self._poly_client and pos.live_shares > 0:
            try:
                outcome_idx = pos.outcome_idx if pos.token_id else (
                    1 if pos.outcome.lower() in ("no", "down", "false") else 0
                )
                # For resolved markets (exit_price 0 or 1), CLOB rejects limit orders;
                # the position must be redeemed, not sold.  Only send a sell order when
                # the market is still active (exit price strictly between 0 and 1).
                if exit_price >= 0.99 or exit_price <= 0.01:
                    logger.info(
                        f"⏭️  Skipping SELL order — market resolved (exit_price={exit_price:.2f}); "
                        "position will auto-redeem"
                    )
                    sell_confirmed = True  # treat as confirmed so P&L is counted
                    live_dollar_pnl = pos.live_shares * (exit_price - pos.entry_price)
                else:
                    sell_price = round(max(exit_price - 0.02, 0.02), 4)  # slight bid-cross to get filled
                    close_bucket = int(time.time() // POLL_INTERVAL_SECONDS)
                    client_order_key = (
                        f"sell|{pos.market_id}|{outcome_idx}|{pos.wallet_address.lower()}|{close_bucket}"
                    )
                    resp = await self._poly_client.place_order(
                        market_id=pos.market_id,
                        outcome=outcome_idx,
                        size=round(pos.live_shares, 4),
                        price=sell_price,
                        side="SELL",
                        token_id=pos.token_id or None,
                        client_order_key=client_order_key,
                    )
                    sell_id = resp.get("orderId") or resp.get("id") or resp.get("order_id")
                    if sell_id:
                        sell_confirmed = True
                        live_dollar_pnl = pos.live_shares * (exit_price - pos.entry_price)
                        logger.info(
                            f"💸 LIVE SELL placed | {pos.outcome} @ {exit_price:.3f} | "
                            f"live_pnl=${live_dollar_pnl:+.2f} | order_id={sell_id}"
                        )
                    else:
                        logger.warning(f"Live sell returned no order_id — not counting as filled: {resp}")
            except Exception as live_err:
                logger.warning(f"Live sell failed: {live_err}")

        pnl_pct = (exit_price - pos.entry_price) / max(pos.entry_price, 0.01)
        # Real account balance / wins / losses only count when the sell order
        # was confirmed with an order_id. Unconfirmed sells must NOT inflate balance.
        live_filled = sell_confirmed
        if live_filled:
            dollar_pnl = live_dollar_pnl
        else:
            # paper P&L is for tracking the strategy only — does NOT touch account
            dollar_pnl = pos.paper_shares * (exit_price - pos.entry_price)
        # For definitive market resolutions, trust the resolution reason over the
        # mid-price P&L calc (which uses a 0.5 fallback when bid/ask unavailable).
        if reason == "resolved_win":
            is_win = True
        elif reason == "resolved_loss":
            is_win = False
        else:
            # wallet_closed / wallet_partial_exit: use actual P&L direction
            is_win = dollar_pnl > 0
        # Always track session W/L — these stats show how the strategy is doing in paper
        # mode and are shown in the Discord embed.  Only _account_balance is restricted
        # to confirmed live fills so real-money P&L doesn't get inflated by paper trades.
        if is_win:
            self._session_wins += 1
        else:
            self._session_losses += 1
        self._session_pnl_dollars += dollar_pnl
        if live_filled:
            self._account_balance += dollar_pnl
        else:
            logger.info(
                f"📄 Paper-only close — paper_pnl=${dollar_pnl:+.2f} "
                f"NOT added to real account balance (still ${self._account_balance:.2f})"
            )

        duration_min = int((time.time() - pos.opened_ts) / 60)

        # Map internal reason → resolved_via for unified trade log
        _via_map = {
            "resolved_win":     "gamma_api",
            "resolved_loss":    "gamma_api",
            "wallet_closed":    "wallet_exit",
            "wallet_partial_exit": "wallet_exit",
        }
        from src.tracking.trade_logger import log_trade
        from datetime import datetime, timezone
        log_trade(
            bot="kakashi",
            market=pos.market_title,
            outcome=pos.outcome,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            size_usd=abs(dollar_pnl / max(abs(pnl_pct), 1e-9)),
            is_win=is_win,
            resolved_via=_via_map.get(reason, reason),
            entered_at=datetime.fromtimestamp(pos.opened_ts, tz=timezone.utc),
        )

        logger.info(
            f"{'✅ WIN' if is_win else '❌ LOSS'} | {pos.wallet_label} | "
            f"{pos.market_title[:50]} | {pos.outcome} {pos.entry_price:.3f}→{exit_price:.3f} | "
            f"live_pnl=${dollar_pnl:+.2f} (live_shares={pos.live_shares:.2f}) | reason={reason}"
        )

        all_time_w, all_time_l = self._session_wins, self._session_losses

        try:
            await BotAlert.trade_result(
                bot_name=BOT_NAME,
                asset=pos.market_title[:60],
                signal_type=signal_type,
                pnl_pct=pnl_pct,
                is_win=is_win,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                session_wins=self._session_wins,
                session_losses=self._session_losses,
                session_pnl=self._session_pnl_dollars / max(STARTING_CAPITAL_USD, 0.01),
                all_time_wins=all_time_w,
                all_time_losses=all_time_l,
                dollar_pnl=dollar_pnl,
                account_balance=self._account_balance,
                starting_capital=STARTING_CAPITAL_USD,
                is_live=LIVE_TRADING,
            )
        except Exception as e:
            logger.debug(f"Discord trade_result failed: {e}")

        self._save_state()

    # ----- main loop ------------------------------------------------------

    async def run_forever(self) -> None:
        # Use a dedicated Discord webhook for this bot if configured.
        follower_webhook = os.getenv("KAKASHI_WEBHOOK_URL") or os.getenv("TOP_FOLLOWER_WEBHOOK_URL")
        if follower_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = follower_webhook
            logger.info("🔔 Using dedicated Discord webhook for Kakashi Bot")

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30, connect=5)
        ) as session:
            self._http = session

            # Initialise real order client if live trading enabled
            if LIVE_TRADING:
                try:
                    self._poly_client = PolymarketClient()
                    await self._poly_client.initialize()
                    logger.info(
                        f"💰 Live trading ENABLED — {LIVE_TRADE_PCT*100:.0f}% of balance/copy "
                        f"(${self._live_trade_size():.2f} now, min=${LIVE_TRADE_MIN_USD}, max=${LIVE_TRADE_MAX_USD}), "
                        f"daily cap=${DAILY_LIVE_SPEND_CAP_USD:.2f}"
                    )
                except Exception as e:
                    logger.warning(f"Could not init live order client: {e} — running paper-only")
                    self._poly_client = None
            else:
                logger.info("📄 Paper-only mode (LIVE_TRADING=False)")

            # Sync real balance immediately on startup
            await self._sync_real_balance()
            self._last_balance_sync_ts = time.time()

            try:
                await BotAlert.startup(
                    bot_name=BOT_NAME,
                    strategy=(
                        f"Follow top {TOP_N} Polymarket wallets by 7d realized PnL. "
                        f"Copy any new position ≥${MIN_COPY_POSITION_USD:.0f} "
                        f"AND ≥{int(MIN_PCT_OF_7D_VOLUME*100)}% of weekly volume. "
                        f"Paper size ${PAPER_TRADE_SIZE_USD:.0f}/copy."
                    ),
                    mode="live" if LIVE_TRADING else "paper",
                )
            except Exception:
                pass

            try:
                while True:
                    try:
                        elapsed = time.time() - self._last_rerank_ts
                        cooldown = RERANK_EMPTY_RETRY_SECONDS if not self.top_wallets else RERANK_INTERVAL_SECONDS
                        if elapsed > cooldown:
                            await self._rerank_top_wallets()

                        # Sync real USDC balance every hour
                        if time.time() - self._last_balance_sync_ts > BALANCE_SYNC_INTERVAL_SECONDS:
                            await self._sync_real_balance()
                            self._last_balance_sync_ts = time.time()

                        await self._check_for_closes()
                        await self._check_for_new_copies()

                    except asyncio.CancelledError:
                        logger.info("Top trader follower cancelled")
                        raise
                    except Exception:
                        logger.exception("Loop error")

                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
            finally:
                if self._poly_client:
                    await self._poly_client.close()
