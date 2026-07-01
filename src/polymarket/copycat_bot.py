"""
Kakashi Bot — follows profitable "sharp" wallets on Polymarket.

Strategy (as described by profitable retail traders):
  1. Maintain a list of known profitable/sharp wallet addresses
  2. Poll each wallet's current positions every 5 minutes
  3. When 60%+ of tracked wallets are on the same side of a market → COPY
  4. Track each copied position until market resolves
  5. Score performance per wallet and drop under-performers over time

All Polymarket trades are public on-chain. This is 100% legal & transparent.

APIs used (no auth required):
  - GET https://data-api.polymarket.com/positions?user=ADDRESS  → current positions
  - GET https://data-api.polymarket.com/portfolio?user=ADDRESS  → total portfolio value
  - GET https://gamma-api.polymarket.com/markets/{market_id}   → market current price
"""

import asyncio
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

from src.tracking.win_loss_tracker import get_tracker
from src.notifications.alerts import BotAlert
from src.polymarket.leaderboard import get_leaderboard_wallets
from src.notifications.discord_webhook import get_discord_client


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

class _CircuitBreaker:
    """
    Per-host circuit breaker.  After OPEN_THRESHOLD consecutive failures the
    circuit opens and all requests to that host are rejected for RESET_SECONDS.
    This prevents runaway retries when an API is fully down.
    """
    OPEN_THRESHOLD = 5      # failures before opening
    RESET_SECONDS  = 300    # 5 minutes in open state before half-open retry

    def __init__(self) -> None:
        self._failures: Dict[str, int] = {}
        self._open_since: Dict[str, datetime] = {}

    def _host(self, url: str) -> str:
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except Exception:
            return url

    def is_open(self, url: str) -> bool:
        host = self._host(url)
        open_since = self._open_since.get(host)
        if open_since is None:
            return False
        age = (datetime.now(timezone.utc) - open_since).total_seconds()
        if age >= self.RESET_SECONDS:
            # Half-open: allow one probe through, reset failure count
            self._failures[host] = 0
            del self._open_since[host]
            logger.info(f"⚡ Circuit breaker HALF-OPEN for {host}")
            return False
        return True

    def record_success(self, url: str) -> None:
        host = self._host(url)
        self._failures[host] = 0
        self._open_since.pop(host, None)

    def record_failure(self, url: str) -> None:
        host = self._host(url)
        self._failures[host] = self._failures.get(host, 0) + 1
        if self._failures[host] >= self.OPEN_THRESHOLD and host not in self._open_since:
            self._open_since[host] = datetime.now(timezone.utc)
            logger.warning(
                f"⚡ Circuit breaker OPEN for {host} after {self._failures[host]} failures "
                f"— pausing for {self.RESET_SECONDS}s"
            )

_circuit_breaker = _CircuitBreaker()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Wallet list is loaded from top_wallets.json (produced by rank_wallets.py).
# If that file doesn't exist the bot starts with an empty seed and discovers
# wallets dynamically via the leaderboard.
_TOP_WALLETS_FILE = Path("top_wallets.json")
_TOP_WALLETS_LIMIT = 100  # cap how many ranked wallets to seed


def _load_sharp_wallets() -> list:
    """Return wallet dicts from top_wallets.json, sorted by net_pnl_usdc descending."""
    if _TOP_WALLETS_FILE.exists():
        try:
            data = json.loads(_TOP_WALLETS_FILE.read_text())
            # Sort by score (consistency metric from rank_wallets.py) so the
            # highest-quality wallets are seeded first before the first 2h leaderboard refresh.
            raw = sorted(
                data.get("wallets", []),
                key=lambda w: w.get("score", 0),
                reverse=True,
            )
            wallets = [
                {"address": w["address"], "label": w["label"]}
                for w in raw[:_TOP_WALLETS_LIMIT]
            ]
            if wallets:
                logger.info(
                    f"CopycatBot: seeded {len(wallets)} wallets from {_TOP_WALLETS_FILE} "
                    f"(generated {data.get('generated_at', 'unknown')})"
                )
                return wallets
        except Exception as e:
            logger.warning(f"CopycatBot: could not load {_TOP_WALLETS_FILE}: {e} — starting empty")
    logger.info("CopycatBot: top_wallets.json not found — starting with empty wallet list (dynamic discovery only)")
    return []


def _parse_dt(s: str) -> datetime:
    """Parse ISO datetime string and ensure timezone-aware (UTC)."""
    if not s:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(s)
        # If no timezone, assume UTC
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


SHARP_WALLETS: list = _load_sharp_wallets()

# No pre-confirmed wallets. Every wallet must earn sharp status via live scoring.
PRECONFIRMED_SHARP: set = set()

# In PAPER mode, bypass the sharp gate so signals fire immediately and we can
# observe which wallets are actually profitable before going live.
# In LIVE mode this MUST be False — only confirmed sharp wallets trigger orders.
# Override with env var BYPASS_SHARP_GATE=true/false.
import os as _os
_bypass_env = _os.getenv("BYPASS_SHARP_GATE", "").strip().lower()
_paper_mode = _os.getenv("PAPER_MODE", "true").strip().lower() == "true"
BYPASS_SHARP_GATE: bool = (
    True if _bypass_env == "true"
    else False if _bypass_env == "false"
    else _paper_mode  # default: bypass in paper, enforce in live
)

# Sharp-wallet qualification thresholds (production values — tighter than validation phase).
# Raise these if signal quality degrades; lower only for paper-mode experimentation.
SHARP_MIN_RESOLVED  = 5     # minimum resolved positions to be considered sharp
SHARP_MIN_WIN_RATE  = 0.60  # minimum win rate
SHARP_MIN_PNL       = 50.0  # minimum net cash PnL (USD)

# Daily loss cap: pause new orders if the bot's realized P&L for the day drops below this.
MAX_DAILY_LOSS_USD  = 50.0

# Consensus threshold: require at least 2 wallets to agree before copying.
# Reduces noise from single-wallet "signals" that may be idiosyncratic.
MIN_CONSENSUS_WALLETS = 3  # require 3+ top wallets agreeing before copying

# Minimum position size (USDC) to count a wallet's position as a "signal"
MIN_POSITION_SIZE = 25.0  # raised to $25 — filters dust and tiny test positions

# How often to poll wallets (seconds)
# 120s during validation phase — catches short-lived signals before they decay
POLL_INTERVAL_SECONDS = 30  # 30 seconds

# Paper trade size per copied position (USD)
PAPER_TRADE_SIZE = 10.0

# Path to persist wallet scores & copied positions
DATA_DIR = Path("data")
COPYCAT_STATE_FILE = DATA_DIR / "copycat_state.json"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class WalletPosition:
    """A position held by a tracked wallet."""
    wallet_address: str
    wallet_label: str
    market_id: str      # Polymarket conditionId
    market_title: str
    outcome: str        # "Yes" / "No" / "Up" / "Down"
    size: float         # Number of shares
    avg_price: float    # Average entry price (0–1)
    current_price: float
    cash_pnl: float     # Realised + unrealised PnL in USDC
    end_date: Optional[str]
    redeemable: bool    # True if market already resolved
    seen_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class ConsensusSignal:
    """A trade signal generated from wallet consensus."""
    market_id: str
    market_title: str
    outcome: str        # Side with consensus
    consensus_pct: float
    supporting_wallets: List[str]  # Labels of wallets on this side
    avg_entry_price: float
    signal_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    copied: bool = False
    close_price: Optional[float] = None
    close_time: Optional[str] = None
    pnl_pct: Optional[float] = None


@dataclass
class WalletScore:
    """Rolling performance score for a tracked wallet."""
    address: str
    label: str
    total_positions_seen: int = 0
    winning_positions: int = 0
    losing_positions: int = 0
    total_cash_pnl: float = 0.0
    portfolio_value_usdc: float = 0.0
    last_seen: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def win_rate(self) -> float:
        total = self.winning_positions + self.losing_positions
        return self.winning_positions / total if total > 0 else 0.0

    @property
    def is_sharp(self) -> bool:
        """Production bar: SHARP_MIN_RESOLVED resolved, SHARP_MIN_WIN_RATE WR, SHARP_MIN_PNL net PnL."""
        resolved = self.winning_positions + self.losing_positions
        return (
            resolved >= SHARP_MIN_RESOLVED
            and self.win_rate >= SHARP_MIN_WIN_RATE
            and self.total_cash_pnl >= SHARP_MIN_PNL
        )


# ---------------------------------------------------------------------------
# Core bot
# ---------------------------------------------------------------------------

class CopycatBot:
    """
    Monitors profitable wallets and copies their consensus trades.
    """

    def __init__(
        self,
        wallets: List[Dict] = None,
        paper_trade_size: float = PAPER_TRADE_SIZE,
        live_mode: bool = False,
        trade_size_usdc: float = 2.0,
        client=None,
        paper_exec=None,
    ):
        self.wallets = wallets or SHARP_WALLETS
        self.paper_trade_size = paper_trade_size
        self.live_mode = live_mode
        self.trade_size_usdc = trade_size_usdc
        self.client = client
        self.paper_exec = paper_exec  # PaperExecutor | None
        self.tracker = get_tracker()
        # Report scheduling
        self._last_daily_summary: Optional[datetime] = None
        self._last_weekly_card:   Optional[datetime] = None

        # State
        self.wallet_scores: Dict[str, WalletScore] = {}
        self.last_positions: Dict[str, List[WalletPosition]] = {}  # wallet_addr → positions
        self.active_signals: Dict[str, ConsensusSignal] = {}       # market_id → signal
        self.session_wins = 0
        self.session_losses = 0
        self.session_pnl = 0.0
        self.STARTING_CAPITAL = 500.0  # Paper account
        self._balance = self.STARTING_CAPITAL

        # Daily P&L tracking for the drawdown circuit breaker.
        # Resets each UTC day; new orders are paused when _daily_pnl <= -MAX_DAILY_LOSS_USD.
        self._daily_pnl: float = 0.0
        self._daily_reset_date = datetime.now(timezone.utc).date()

        # HTTP session (created in run_forever)
        self._http: Optional[aiohttp.ClientSession] = None
        # Leaderboard refresh tracking
        self._last_leaderboard_refresh: Optional[datetime] = None
        self._LEADERBOARD_REFRESH_HOURS = 2  # refresh top traders every 2 hours
        # Resolution cache: skip re-querying markets we know aren't closed yet
        # Maps condition_id → earliest time to recheck (5-min backoff)
        self._resolution_skip_until: Dict[str, datetime] = {}
        # Permanently resolved market IDs (confirmed via CLOB winner field).
        # Persisted in copycat_state.json — survives restarts so the bot never
        # re-queries CLOB for a market it already knows is dead.
        self._resolved_markets: set = set()
        # On first poll, last_positions is empty so ALL redeemable positions look "new".
        # Skip scoring on the first poll — only score positions that become redeemable
        # after we've observed them as open at least once.
        self._first_poll: bool = True

        self._load_state()

        # Init wallet scores for any new wallets
        for w in self.wallets:
            addr = w["address"].lower()
            if addr not in self.wallet_scores:
                self.wallet_scores[addr] = WalletScore(address=addr, label=w["label"])

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_state(self):
        if COPYCAT_STATE_FILE.exists():
            try:
                data = json.loads(COPYCAT_STATE_FILE.read_text())
                for addr, s in data.get("wallet_scores", {}).items():
                    self.wallet_scores[addr] = WalletScore(**s)
                for mid, sig in data.get("active_signals", {}).items():
                    loaded = ConsensusSignal(**sig)
                    # Only restore open (not yet closed) signals.
                    # Closed signals are already recorded in win_loss_history — no
                    # need to keep them in memory, and re-loading them causes
                    # _close_resolved_signals to re-record a phantom -100% loss on
                    # restart if the market has since resolved in the position API.
                    if loaded.close_time:
                        continue
                    self.active_signals[mid] = loaded
                # Restore permanently-resolved market IDs — these never need re-querying.
                self._resolved_markets = set(data.get("resolved_markets", []))
                # Pre-populate skip cache so first cycle skips all known-dead markets.
                _far_future = datetime.now(timezone.utc) + timedelta(days=365)
                for mid in self._resolved_markets:
                    self._resolution_skip_until[mid] = _far_future
                logger.info(f"📂 Loaded copycat state: {len(self.wallet_scores)} wallets, "
                            f"{len(self.active_signals)} active signals, "
                            f"{len(self._resolved_markets)} resolved markets cached")
            except Exception as e:
                logger.warning(f"Could not load copycat state: {e}")

    def _prune_memory(self):
        """Remove stale entries that would grow dicts unbounded over weeks of trading.

        Called from _save_state so it runs on every poll cycle without extra overhead.
        """
        now = datetime.now(timezone.utc)
        # _resolution_skip_until: remove entries whose backoff window has passed.
        expired = [k for k, t in self._resolution_skip_until.items() if now >= t]
        for k in expired:
            del self._resolution_skip_until[k]

        # active_signals: remove signals that have been closed for >48 h.
        # The resolution loop already removes them after 24 h, but this catches
        # any that slipped through (e.g. bot was restarted before the cleanup ran).
        stale = [
            k for k, s in self.active_signals.items()
            if s.close_time and (now - _parse_dt(s.close_time)) > timedelta(hours=48)
        ]
        for k in stale:
            del self.active_signals[k]

        if expired or stale:
            logger.debug(
                f"🧹 Memory pruned: {len(expired)} skip-cache entries, "
                f"{len(stale)} stale signals removed"
            )

    def _save_state(self):
        self._prune_memory()
        try:
            DATA_DIR.mkdir(exist_ok=True)
            payload = json.dumps({
                "wallet_scores": {k: asdict(v) for k, v in self.wallet_scores.items()},
                "active_signals": {k: asdict(v) for k, v in self.active_signals.items()},
                "resolved_markets": sorted(self._resolved_markets),
            }, indent=2)
            # Atomic write: temp file → rename so a crash mid-write never corrupts state.
            tmp = COPYCAT_STATE_FILE.with_suffix(".tmp")
            tmp.write_text(payload)
            tmp.replace(COPYCAT_STATE_FILE)
            # Heartbeat for dashboard health monitoring
            open_signals = sum(1 for s in self.active_signals.values() if not s.close_time)
            hb_tmp = (DATA_DIR / "heartbeat.json.tmp")
            hb_tmp.write_text(json.dumps({
                "last_poll": datetime.now(timezone.utc).isoformat(),
                "mode": "live" if self.live_mode else "paper",
                "pid": os.getpid(),
                "wallets_tracked": len(self.wallets),
                "active_signals": open_signals,
            }))
            hb_tmp.replace(DATA_DIR / "heartbeat.json")
        except Exception as e:
            logger.warning(f"Could not save copycat state: {e}")

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_json(
        self,
        url: str,
        params: dict = None,
        timeout_sec: int = 10,
        retries: int = 3,
    ) -> Any:
        """GET with exponential backoff and circuit-breaker protection. Returns parsed JSON or None."""
        if _circuit_breaker.is_open(url):
            logger.debug(f"Circuit breaker OPEN — skipping {url}")
            return None
        for attempt in range(retries):
            if attempt > 0:
                await asyncio.sleep(2 ** (attempt - 1))  # 1 s, 2 s
            try:
                async with self._http.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout_sec),
                ) as resp:
                    if resp.status == 200:
                        _circuit_breaker.record_success(url)
                        return await resp.json()
                    if resp.status == 429:
                        wait = 2 ** attempt
                        logger.debug(f"Rate-limited {url} — waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    logger.debug(f"HTTP {resp.status} from {url}")
                    _circuit_breaker.record_failure(url)
                    return None
            except asyncio.TimeoutError:
                logger.debug(f"Timeout on {url} (attempt {attempt + 1}/{retries})")
                _circuit_breaker.record_failure(url)
            except Exception as exc:
                logger.debug(f"Fetch error {url}: {exc} (attempt {attempt + 1}/{retries})")
                _circuit_breaker.record_failure(url)
        return None

    async def _recover_orphaned_positions(self):
        """
        Recover positions from paper_executor that were orphaned during restart.
        
        On restart, active_signals may be lost or discarded. This method:
        1. Retrieves all open positions from paper_executor's persistent store
        2. Reconstructs ConsensusSignal objects for any not in active_signals
        3. Immediately checks for resolution (markets from yesterday may resolve today)
        """
        try:
            # Get open positions from paper_executor's persistent store
            # Reload to ensure fresh data (simpler than executor thread, avoids CWD issues)
            self.paper_exec._load_positions()  # Loads into self.paper_exec.positions
            positions = self.paper_exec.positions
            
            logger.info(f"🔄 RECOVERY SCAN: Loading {len(positions) if positions else 0} positions from paper_executor.positions")
            
            if not positions:
                logger.debug("No orphaned positions to recover")
                return
            
            recovered_count = 0
            for pos in positions:
                condition_id = pos.get("condition_id", "")
                outcome = pos.get("outcome", "").lower()
                sig_key = f"{condition_id}:{outcome}"
                
                # Skip if already in active_signals
                if sig_key in self.active_signals:
                    continue
                
                # Reconstruct signal from position data
                signal = ConsensusSignal(
                    market_id=condition_id,
                    market_title=pos.get("market_slug", condition_id[:20]),
                    outcome=outcome,
                    consensus_pct=1.0,  # Solo recovery, treat as 100% consensus
                    supporting_wallets=["paper-recovery"],
                    avg_entry_price=pos.get("avg_price", 0.5),
                    signal_time=pos.get("opened_at", datetime.now(timezone.utc).isoformat()),
                    copied=True,
                )
                
                self.active_signals[sig_key] = signal
                recovered_count += 1
                logger.info(
                    f"🔄 RECOVERED position: {sig_key} | "
                    f"entry=${pos.get('amount_usd', 0):.2f} | "
                    f"from {pos.get('opened_at', 'unknown')}"
                )
            
            if recovered_count > 0:
                logger.info(f"📂 Recovered {recovered_count} orphaned position(s) from paper_executor — will check on next poll")
        except Exception as e:
            logger.warning(f"Could not recover orphaned positions: {e}")

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    async def _fetch_positions(self, address: str) -> List[dict]:
        """Fetch current open positions for a wallet from Polymarket data API."""
        data = await self._get_json(
            "https://data-api.polymarket.com/positions",
            params={"user": address, "sizeThreshold": str(MIN_POSITION_SIZE)},
        )
        return data if isinstance(data, list) else []

    async def _fetch_portfolio_value(self, address: str) -> float:
        """Fetch total portfolio value (USDC) for a wallet."""
        data = await self._get_json(
            "https://data-api.polymarket.com/portfolio",
            params={"user": address},
            timeout_sec=8,
        )
        if isinstance(data, list) and data:
            return float(data[0].get("value", 0))
        if isinstance(data, dict):
            return float(data.get("value", 0))
        return 0.0

    async def _fetch_market_price(self, condition_id: str) -> Optional[float]:
        """Get current mid-price for a market."""
        data = await self._get_json(
            "https://gamma-api.polymarket.com/markets",
            params={"conditionId": condition_id},
            timeout_sec=8,
        )
        if data is None:
            return None
        markets = data if isinstance(data, list) else data.get("markets", [])
        if not markets:
            return None
        m = markets[0]
        bid = float(m.get("bestBid", m.get("best_bid", 0)) or 0)
        ask = float(m.get("bestAsk", m.get("best_ask", 0)) or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return float(m.get("lastTradePrice", m.get("price", 0.5)) or 0.5)

    async def _check_market_resolution(
        self, condition_id: str, outcome: str
    ) -> tuple[bool, float]:
        """
        Query CLOB API and return (is_resolved, price).

        Uses clob.polymarket.com/markets/{conditionId} which returns
        tokens[].winner (True/False/None) — the only reliable resolution signal.
        Caches "not yet closed" markets for 5 minutes to avoid 300+ API calls/cycle.
        """
        now = datetime.now(timezone.utc)
        skip_until = self._resolution_skip_until.get(condition_id)
        if skip_until and now < skip_until:
            return False, 0.5

        try:
            async with self._http.get(
                f"https://clob.polymarket.com/markets/{condition_id}",
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"CLOB API error {resp.status} for {condition_id}")
                    self._resolution_skip_until[condition_id] = now + timedelta(minutes=5)
                    return False, 0.5
                data = await resp.json()

            tokens = data.get("tokens", [])
            if not tokens:
                self._resolution_skip_until[condition_id] = now + timedelta(minutes=5)
                return False, 0.5

            # Find token matching our outcome; fall back to first token
            target = next(
                (t for t in tokens if (t.get("outcome") or "").lower() == outcome.lower()),
                tokens[0],
            )
            winner = target.get("winner")

            if winner is None:
                # Market not resolved yet — backoff 5 minutes
                self._resolution_skip_until[condition_id] = now + timedelta(minutes=5)
                price = float(target.get("price") or 0.5)
                return False, price if 0 < price < 1 else 0.5

            # winner=True → this outcome won (price=1), winner=False → lost (price=0)
            res_price = 1.0 if winner else 0.0
            logger.info(f"✅ Market resolved via CLOB: {condition_id[:20]}... | outcome={outcome} | winner={winner}")
            # Permanently cache: this market is resolved and will never need re-querying.
            self._resolved_markets.add(condition_id)
            self._resolution_skip_until[condition_id] = now + timedelta(days=365)
            return True, res_price

        except Exception as e:
            logger.warning(f"Exception in _check_market_resolution for {condition_id}: {e}")
            self._resolution_skip_until[condition_id] = now + timedelta(minutes=5)
            return False, 0.5

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def _parse_position(self, raw: dict, wallet_address: str, wallet_label: str) -> Optional[WalletPosition]:
        """Parse a raw position dict from the API."""
        try:
            size = float(raw.get("size", 0))
            if size < MIN_POSITION_SIZE:
                return None

            avg_price = float(raw.get("avgPrice", 0) or 0)
            cur_price = float(raw.get("curPrice", 0) or 0)
            cash_pnl = float(raw.get("cashPnl", 0) or 0)
            condition_id = raw.get("conditionId", "")
            title = raw.get("title", "")
            outcome = raw.get("outcome", "")
            end_date = raw.get("endDate")
            redeemable = bool(raw.get("redeemable", False))

            if not condition_id or not title or not outcome:
                return None

            return WalletPosition(
                wallet_address=wallet_address,
                wallet_label=wallet_label,
                market_id=condition_id,
                market_title=title,
                outcome=outcome,
                size=size,
                avg_price=avg_price,
                current_price=cur_price,
                cash_pnl=cash_pnl,
                end_date=end_date,
                redeemable=redeemable,
            )
        except Exception as e:
            logger.debug(f"Error parsing position: {e}")
            return None

    def _build_consensus(self, all_positions: List[WalletPosition]) -> List[ConsensusSignal]:
        """
        Group positions by market, check if majority of wallets agree on a side.
        Returns list of new ConsensusSignal objects that pass the threshold.
        """
        # Group by market_id → outcome → [wallet_labels]
        market_votes: Dict[str, Dict[str, List[Tuple[str, float]]]] = defaultdict(lambda: defaultdict(list))
        market_titles: Dict[str, str] = {}

        for pos in all_positions:
            # Normalize outcome to lowercase so "Yes"/"yes"/"YES" all map to the same key,
            # preventing duplicate signals for the same market+side.
            market_votes[pos.market_id][pos.outcome.lower()].append((pos.wallet_label, pos.avg_price))
            market_titles[pos.market_id] = pos.market_title

        signals = []
        label_to_addr = {w["label"]: w["address"].lower() for w in self.wallets}

        # Wallets that have been observed losing money live are disqualified from
        # contributing to consensus, even in paper/bypass mode.  Threshold: 2+
        # resolved positions AND net PnL below -$50 — they've proven they're losers.
        _LIVE_PNL_FLOOR          = -50.0
        _MIN_RESOLVED_TO_DISQUAL = 2

        def _is_disqualified(lbl: str) -> bool:
            addr = label_to_addr.get(lbl, "")
            if not addr:
                return False
            sc = self.wallet_scores.get(addr)
            if sc is None:
                return False
            resolved = sc.winning_positions + sc.losing_positions
            return resolved >= _MIN_RESOLVED_TO_DISQUAL and sc.total_cash_pnl < _LIVE_PNL_FLOOR

        for market_id, outcome_votes in market_votes.items():
            for outcome, wallet_entries in outcome_votes.items():
                # Drop wallets that have demonstrated live losses before applying consensus check
                qualifying = [(lbl, p) for lbl, p in wallet_entries if not _is_disqualified(lbl)]
                if len(qualifying) < len(wallet_entries):
                    dropped = [lbl for lbl, _ in wallet_entries if _is_disqualified(lbl)]
                    logger.debug(
                        f"DISQUALIFIED {len(dropped)} loser wallet(s) from {market_id[:20]}: "
                        + ", ".join(dropped)
                    )
                wallet_entries = qualifying

                pct = len(wallet_entries) / max(len(self.wallets), 1)
                if len(wallet_entries) >= MIN_CONSENSUS_WALLETS:
                    # Require at least one supporting wallet to be sharp.
                    # This prevents confirmed-loser wallets from triggering alerts.
                    has_sharp = any(
                        label_to_addr.get(lbl, "").lower() in PRECONFIRMED_SHARP
                        or self.wallet_scores.get(
                            label_to_addr.get(lbl, ""),
                            WalletScore(address="", label=lbl),
                        ).is_sharp
                        for lbl, _ in wallet_entries
                    )
                    if not BYPASS_SHARP_GATE and not has_sharp:
                        continue

                    # Skip positions that are already near-resolved (price at extremes).
                    # These are old resolved markets CERTova hasn't redeemed yet — not fresh signals.
                    # Check the avg_price of the supporting entries: if wallet bought at 0.85+ it's already decided.
                    prices = [p for _, p in wallet_entries if p > 0]
                    avg_price = sum(prices) / len(prices) if prices else 0.5
                    if avg_price >= 0.85 or avg_price <= 0.15:
                        logger.debug(
                            f"SKIP already-resolved signal: {market_titles.get(market_id, market_id)[:50]} "
                            f"| {outcome} @ {avg_price:.3f} — market already decided"
                        )
                        continue

                    # Also check current_price from the positions data — skip if already resolved
                    current_prices = []
                    for pos in all_positions:
                        if pos.market_id == market_id and pos.outcome == outcome:
                            if pos.current_price > 0:
                                current_prices.append(pos.current_price)
                    if current_prices:
                        cur = sum(current_prices) / len(current_prices)
                        if cur >= 0.90 or cur <= 0.10:
                            logger.debug(
                                f"SKIP resolved market: {market_titles.get(market_id, market_id)[:50]} "
                                f"| cur_price={cur:.3f}"
                            )
                            continue

                    # Skip markets that expire within 48 hours — too close to
                    # resolution to enter; also catches already-expired markets.
                    end_dates = [
                        pos.end_date for pos in all_positions
                        if pos.market_id == market_id and pos.end_date
                    ]
                    if end_dates:
                        try:
                            earliest = min(_parse_dt(d) for d in end_dates)
                            hours_left = (earliest - datetime.now(timezone.utc)).total_seconds() / 3600
                            if hours_left < 48:
                                logger.debug(
                                    f"SKIP near-expiry market: {market_titles.get(market_id, market_id)[:50]} "
                                    f"| {hours_left:.1f}h remaining"
                                )
                                continue
                        except Exception:
                            pass

                    # Already have a signal for this market+outcome? Skip.
                    sig_key = f"{market_id}:{outcome}"
                    if sig_key in self.active_signals:
                        continue

                    signal = ConsensusSignal(
                        market_id=market_id,
                        market_title=market_titles[market_id],
                        outcome=outcome,
                        consensus_pct=pct,
                        supporting_wallets=[label for label, _ in wallet_entries],
                        avg_entry_price=avg_price,
                    )
                    signals.append(signal)

        return signals

    async def _close_resolved_signals(self):
        """Check active signals — close any whose market has resolved."""
        logger.info(f"🔍 _close_resolved_signals: Checking {len(self.active_signals)} signals for resolution...")
        
        to_remove = []
        for sig_key, signal in list(self.active_signals.items()):
            if signal.close_time:
                # Already closed, just clean up after 24h
                close_dt = _parse_dt(signal.close_time)
                if datetime.now(timezone.utc) - close_dt > timedelta(hours=24):
                    to_remove.append(sig_key)
                continue

            # Check Gamma API for resolution (uses resolved+resolutionPrice, not
            # price thresholds, so stale lastTradePrice can't block detection).
            gamma_resolved, price = await self._check_market_resolution(
                signal.market_id, signal.outcome
            )
            logger.debug(f"  📌 {sig_key[:30]}... | gamma_resolved={gamma_resolved} price={price:.3f}")
            timed_out = False
            if signal.signal_time:
                age = datetime.now(timezone.utc) - _parse_dt(signal.signal_time)
                timed_out = age > timedelta(days=14)

            # Only trust Gamma API for resolution — price-based fallback caused 515 fake losses
            # (stale lastTradePrice near 0/1 is not a reliable resolution signal).
            resolved = gamma_resolved

            if resolved or timed_out:
                close_price = price
                # Win: the correct outcome token's resolutionPrice == 1.
                # _check_market_resolution already returns price=1.0 if our outcome won,
                # price=0.0 if it lost — so we just check price >= 0.99 on gamma_resolved.
                # For definitive gamma resolution, trust the resolution price.
                # For timeouts/wallet-exits, fall back to actual P&L direction.
                pnl_pct = (close_price - signal.avg_entry_price) / signal.avg_entry_price if signal.avg_entry_price > 0 else 0
                if gamma_resolved:
                    is_win = price >= 0.99
                else:
                    is_win = pnl_pct > 0

                logger.info(f"📌 RESOLVING: {sig_key[:40]}... | resolved={resolved} timed_out={timed_out} is_win={is_win} pnl={pnl_pct:+.1%}")

                # Only record P&L if a paper trade was actually executed for this signal.
                # Signals loaded from state on restart may never have had a paper position opened
                # (e.g. old state from before paper_positions.json was cleared) — recording
                # a loss for those would inflate the loss count with phantom trades.
                record_trade = signal.copied

                signal.close_price = close_price
                signal.close_time = datetime.now(timezone.utc).isoformat()
                signal.pnl_pct = pnl_pct

                if not record_trade:
                    logger.info(
                        f"⏭️  SKIP recording trade for un-copied signal: {sig_key[:40]} "
                        f"(copied=False — no paper position was opened)"
                    )
                    continue

                dollar_pnl = pnl_pct * self.paper_trade_size
                self._balance += dollar_pnl
                self._daily_pnl += dollar_pnl  # feed the circuit breaker
                if is_win:
                    self.session_wins += 1
                else:
                    self.session_losses += 1
                # Accumulate as fraction of starting capital so BotAlert's
                # {:+.2%} format shows a meaningful percentage (e.g. -10%, not -5483%).
                self.session_pnl += dollar_pnl / max(self.STARTING_CAPITAL, 0.01)

                # Close the paper position in pm_trader (sell at current price,
                # or let resolve_all() pay out $1/share if market is officially closed)
                if self.paper_exec:
                    sold = await self.paper_exec.sell(signal.market_id, signal.outcome)
                    if not sold.get("ok"):
                        # Market may be officially closed — try resolution payout
                        await self.paper_exec.resolve_all()

                self.tracker.record_trade(
                    bot_name="Kakashi Bot",
                    asset="POLY",
                    signal_type=f"COPY_{signal.outcome.upper()}",
                    entry_price=signal.avg_entry_price,
                    exit_price=close_price,
                    pnl_pct=pnl_pct,
                    confidence=signal.consensus_pct,
                    is_win=is_win,
                    duration_minutes=int((datetime.now(timezone.utc) - _parse_dt(signal.signal_time)).total_seconds() / 60),
                    notes=f"Consensus {signal.consensus_pct:.0%} | wallets: {','.join(signal.supporting_wallets)}",
                    dollar_pnl=dollar_pnl,
                )

                outcome_str = "✅ WIN" if is_win else "❌ LOSS"
                logger.info(
                    f"{outcome_str} | Copycat: {signal.market_title[:50]} | "
                    f"{signal.outcome} | entry={signal.avg_entry_price:.3f} close={close_price:.3f} "
                    f"pnl={pnl_pct:+.1%}"
                )

                # Get real all-time stats from tracker
                bot_stats = self.tracker.get_bot_stats("Kakashi Bot")
                all_time_wins = bot_stats.wins if bot_stats else self.session_wins
                all_time_losses = bot_stats.losses if bot_stats else self.session_losses

                # Notify Discord
                try:
                    await BotAlert.trade_result(
                        bot_name="Kakashi Bot",
                        asset="POLY",
                        signal_type=f"COPY_{signal.outcome.upper()}",
                        pnl_pct=pnl_pct,
                        is_win=is_win,
                        entry_price=signal.avg_entry_price,  # probability (0-1)
                        exit_price=close_price,              # probability (0-1)
                        session_wins=self.session_wins,
                        session_losses=self.session_losses,
                        session_pnl=self.session_pnl,
                        all_time_wins=all_time_wins,
                        all_time_losses=all_time_losses,
                        streak=0,
                        dollar_pnl=dollar_pnl,
                        account_balance=self._balance,
                        starting_capital=self.STARTING_CAPITAL,
                    )
                except Exception as exc:
                    logger.debug(f"Discord trade-result alert skipped: {exc}")

        for key in to_remove:
            self.active_signals.pop(key, None)

    # ------------------------------------------------------------------
    # Scheduled Discord reports (paper mode only)
    # ------------------------------------------------------------------

    async def _maybe_daily_summary(self):
        """Send daily P&L summary to Discord once every 24 hours."""
        if not self.paper_exec:
            return
        now = datetime.now(timezone.utc)
        if self._last_daily_summary and (now - self._last_daily_summary).total_seconds() < 86_400:
            return
        try:
            data = await self.paper_exec.daily_summary()
            if not data:
                return

            pnl    = data["total_pnl"]
            roi    = data["roi_pct"]
            color  = 0x00C853 if pnl >= 0 else 0xFF1744
            pnl_sign = "+" if pnl >= 0 else ""

            discord = get_discord_client()
            if discord.enabled:
                embed = {
                    "title": f"📊 Daily P&L Summary — {now.strftime('%Y-%m-%d')}",
                    "color": color,
                    "fields": [
                        {"name": "Account",         "value": data["account"],                       "inline": True},
                        {"name": "Cash",            "value": f"${data['cash']:,.2f}",               "inline": True},
                        {"name": "Open Positions",  "value": f"{data['open_positions']}  (${data['positions_value']:,.2f})", "inline": True},
                        {"name": "Total Value",     "value": f"${data['total_value']:,.2f}",         "inline": True},
                        {"name": "Total P&L",       "value": f"{pnl_sign}${pnl:,.2f}  ({pnl_sign}{roi:.2f}%)", "inline": True},
                        {"name": "Win Rate",        "value": f"{data['win_rate']:.1f}%",            "inline": True},
                        {"name": "Total Trades",    "value": str(data["total_trades"]),             "inline": True},
                        {"name": "Trades Today",    "value": str(data["today_trades"]),             "inline": True},
                        {"name": "Max Drawdown",    "value": f"{data['max_drawdown']:.2f}%",        "inline": True},
                    ],
                    "footer": {"text": "Kakashi Bot | pm-trader paper account | verifiable audit trail"},
                    "timestamp": now.isoformat(),
                }
                await discord.send_message(embed=embed)
                logger.info("📊 Daily P&L summary sent to Discord")
            self._last_daily_summary = now
        except Exception as e:
            logger.warning(f"Daily summary failed: {e}")

    async def _maybe_weekly_card(self):
        """Send the pm-trader stats card to Discord every Sunday."""
        if not self.paper_exec:
            return
        now = datetime.now(timezone.utc)
        # Only on Sundays (weekday 6) and not more than once per week
        if now.weekday() != 6:
            return
        if self._last_weekly_card and (now - self._last_weekly_card).total_seconds() < 7 * 86_400:
            return
        try:
            card = await self.paper_exec.weekly_card()
            discord = get_discord_client()
            if discord.enabled and card:
                await discord.send_message(
                    content=f"**📈 Weekly Stats Card — {now.strftime('%Y-%m-%d')}**\n"
                            f"```\n{card[:1800]}\n```"
                )
                logger.info("📈 Weekly stats card sent to Discord")
            self._last_weekly_card = now
        except Exception as e:
            logger.warning(f"Weekly card failed: {e}")

    async def _refresh_leaderboard_wallets(self):
        """
        Discover top 25 traders from live trade activity, score by on-chain PnL,
        and replace the tracked wallet list. Runs every 6 hours.
        """
        try:
            # Seed addresses to always include in the scoring pool
            seed_addrs = [w["address"].lower() for w in SHARP_WALLETS]

            lb_wallets = await get_leaderboard_wallets(
                self._http,
                top_n=50,
                min_profit=50.0,  # only track wallets with ≥$50 realized PnL
                seed_wallets=seed_addrs,
            )

            if not lb_wallets:
                logger.info("Wallet discovery returned nothing — keeping existing list")
                return

            # Require at least MIN_CONSENSUS_WALLETS+1 wallets so consensus is reachable.
            # A tiny list (e.g. 1 wallet) makes MIN_CONSENSUS_WALLETS=2 impossible and
            # silently stops all trading.
            if len(lb_wallets) < MIN_CONSENSUS_WALLETS + 1:
                logger.warning(
                    f"⚠️ Leaderboard returned only {len(lb_wallets)} wallet(s) — "
                    f"need ≥{MIN_CONSENSUS_WALLETS + 1} for consensus; keeping existing list"
                )
                return

            # Replace wallet list entirely with freshly scored top 25
            prev_count = len(self.wallets)
            self.wallets = [{"address": w["address"], "label": w["label"]} for w in lb_wallets]

            # Init scores for any new wallets
            for w in self.wallets:
                addr = w["address"].lower()
                if addr not in self.wallet_scores:
                    self.wallet_scores[addr] = WalletScore(address=addr, label=w["label"])

            self._last_leaderboard_refresh = datetime.now(timezone.utc)
            logger.info(
                f"🏆 Wallet refresh: {len(self.wallets)} top traders now tracked "
                f"(was {prev_count})"
            )
            for w in lb_wallets[:5]:
                logger.info(
                    f"  • {w['label']} — PnL=${w['_pnl']:+.0f}  WR={w['_win_rate']:.0%}"
                )

            try:
                from src.notifications.discord_webhook import get_discord_client
                discord = get_discord_client()
                if discord.enabled:
                    top_lines = "\n".join(
                        f"{w['label']}: PnL=${w['_pnl']:+.0f} WR={w['_win_rate']:.0%}"
                        for w in lb_wallets[:10]
                    )
                    embed = {
                        "title": "🏆 Top 25 Traders Refreshed",
                        "color": 0xFFD700,
                        "fields": [
                            {"name": "Top 10 by PnL", "value": top_lines or "none", "inline": False},
                            {"name": "Total Tracked", "value": str(len(self.wallets)), "inline": True},
                        ],
                        "footer": {"text": f"Kakashi Bot | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await discord.send_message(embed=embed)
            except Exception as exc:
                logger.debug(f"Discord leaderboard-refresh alert skipped: {exc}")

            # Sync the chain watcher's wallet list if it's running
            chain_watcher = getattr(self, "_chain_watcher", None)
            if chain_watcher is not None:
                chain_watcher.update_wallets(self.wallets)

        except Exception as e:
            logger.warning(f"Wallet refresh error: {e}")

    async def _place_live_order(self, signal: ConsensusSignal) -> bool:
        """Place a real order on Polymarket for a consensus signal (2 attempts, 2s backoff)."""
        if not self.client:
            logger.warning("Live mode enabled but no client — skipping order")
            return False

        # Guard: ensure sufficient USDC balance before committing capital
        MIN_USDC_BALANCE = float(os.getenv("MIN_USDC_BALANCE", "10.0"))
        try:
            balance = await self.client.get_usdc_balance()
            if balance is not None and balance < MIN_USDC_BALANCE:
                logger.warning(
                    f"💸 Skipping live order — USDC balance ${balance:.2f} below minimum ${MIN_USDC_BALANCE:.2f}"
                )
                return False
        except Exception as exc:
            logger.warning(f"Balance check failed (proceeding with caution): {exc}")

        outcome_idx = 0 if signal.outcome.lower() in ("yes", "up") else 1
        shares = self.trade_size_usdc / signal.avg_entry_price if signal.avg_entry_price > 0 else 0
        if shares <= 0:
            logger.warning(f"Could not calculate shares for {signal.market_title[:40]}")
            return False

        logger.info(
            f"🚀 LIVE ORDER | {signal.market_title[:50]} | {signal.outcome} | "
            f"${self.trade_size_usdc:.2f} → {shares:.4f} shares @ {signal.avg_entry_price:.3f}"
        )

        last_exc: Exception = Exception("unknown")
        for attempt in range(1, 3):
            try:
                resp = await self.client.place_order(
                    market_id=signal.market_id,
                    outcome=outcome_idx,
                    size=round(shares, 4),
                    price=round(signal.avg_entry_price, 3),
                    side="BUY",
                )
                order_id = resp.get("orderId") or resp.get("orderID") or resp.get("id")
                if order_id:
                    logger.info(f"✅ Order accepted: {order_id}")
                    return True
                logger.warning(f"Order response had no ID (attempt {attempt}): {resp}")
                return False
            except Exception as e:
                last_exc = e
                if attempt < 2:
                    logger.warning(f"⚠️ Live order attempt {attempt} failed: {e} — retrying in 2s")
                    await asyncio.sleep(2)

        logger.error(f"❌ Live order FINAL FAILURE after 2 attempts: {last_exc}")
        try:
            discord = get_discord_client()
            if discord.enabled:
                await discord.send_message(
                    content=(
                        f"❌ **Live order failed** after 2 attempts\n"
                        f"Market: {signal.market_title[:80]}\n"
                        f"Outcome: {signal.outcome} @ {signal.avg_entry_price:.3f}\n"
                        f"Error: `{last_exc}`"
                    )
                )
        except Exception as exc:
            logger.debug(f"Discord order-failure alert skipped: {exc}")
        return False

    # ------------------------------------------------------------------
    # On-chain trade handler (called by ChainWatcher at ~2s latency)
    # ------------------------------------------------------------------

    async def on_chain_trade(
        self,
        condition_id: str,
        title: str,
        outcome: str,
        side: str,
        usdc_amount: float,
        wallet_label: str,
        avg_price: float,
    ) -> None:
        """
        Called by ChainWatcher when a tracked wallet's on-chain trade is confirmed.

        Opens a paper trade immediately — bypasses the positions API entirely so we
        act at ~2s latency rather than waiting up to 30 min for the polling cycle.

        Only acts on BUY signals (we don't copy exits; our own close logic handles that).
        Deduplication: we skip if we already have an active signal for this market+outcome.
        """
        if side != "BUY":
            # Don't copy sells — our exit strategy is time/resolution based, not mirroring
            logger.debug(f"⛓️  on_chain_trade: ignoring SELL from {wallet_label}")
            return

        if usdc_amount < 2.0:
            logger.debug(f"⛓️  on_chain_trade: dust trade ${usdc_amount:.2f} from {wallet_label} — skipping")
            return

        if not condition_id:
            logger.debug(f"⛓️  on_chain_trade: no condition_id for {title[:50]} — skipping")
            return

        sig_key = f"{condition_id}:{outcome.lower()}"
        if sig_key in self.active_signals:
            logger.debug(f"⛓️  on_chain_trade: already have signal for {sig_key[:40]} — skipping dupe")
            return

        # Daily loss circuit breaker
        if self._daily_pnl <= -MAX_DAILY_LOSS_USD:
            logger.warning(f"⛓️  on_chain_trade: daily loss cap active — not opening new trade")
            return

        # Pre-check CLOB: don't enter a market that's already resolved
        already_resolved, res_price = await self._check_market_resolution(condition_id, outcome)
        if already_resolved:
            logger.info(
                f"⛓️  on_chain_trade: market already resolved ({res_price:.2f}) — skipping "
                f"{title[:50]}"
            )
            return

        signal = ConsensusSignal(
            market_id=condition_id,
            market_title=title,
            outcome=outcome,
            consensus_pct=1.0 / max(len(self.wallets), 1),  # single wallet
            supporting_wallets=[wallet_label],
            avg_entry_price=avg_price,
        )
        self.active_signals[sig_key] = signal

        logger.info(
            f"⚡ CHAIN SIGNAL | {title[:60]} | {outcome} | ${usdc_amount:,.0f} from {wallet_label} "
            f"| est_price={avg_price:.3f}"
        )

        if self.paper_exec:
            result = await self.paper_exec.buy(condition_id, outcome, self.paper_trade_size)
            if result.get("ok"):
                signal.copied = True
                logger.info(f"✅ CHAIN PAPER TRADE | {outcome} | {title[:50]} | copied=True")
            else:
                logger.warning(f"⚠️  chain paper_exec.buy failed: {result}")

        if self.live_mode:
            await self._place_live_order(signal)

        # Discord alert
        try:
            await BotAlert.signal(
                bot_name="Kakashi Bot",
                signal_type=f"CHAIN_{outcome.upper()}",
                price=avg_price,
                confidence=signal.consensus_pct,
                details={
                    "Source": f"⚡ On-chain (Polygon, ~2s latency)",
                    "Wallet": wallet_label,
                    "Market": title[:60],
                    "Size": f"${usdc_amount:,.0f} USDC",
                    "Est. Price": f"{avg_price:.3f}",
                    "Paper Trade": f"${self.paper_trade_size:.0f}",
                },
            )
        except Exception as exc:
            logger.debug(f"Discord chain-signal alert skipped: {exc}")

    async def poll_once(self):
        """One full polling cycle: fetch all wallet positions, detect consensus, open/close signals."""
        # Refresh leaderboard wallets every 6 hours
        needs_refresh = (
            self._last_leaderboard_refresh is None
            or (datetime.now(timezone.utc) - self._last_leaderboard_refresh).total_seconds()
            > self._LEADERBOARD_REFRESH_HOURS * 3600
        )
        if needs_refresh:
            await self._refresh_leaderboard_wallets()

        # Daily drawdown circuit breaker — reset at UTC midnight, then check cap.
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset_date:
            self._daily_pnl = 0.0
            self._daily_reset_date = today
            logger.info("🔄 Daily P&L reset (UTC midnight)")
        if self._daily_pnl <= -MAX_DAILY_LOSS_USD:
            logger.warning(
                f"🛑 Daily loss cap hit (${self._daily_pnl:.2f} ≤ -${MAX_DAILY_LOSS_USD:.0f}) "
                f"— pausing new orders for today"
            )
            # Still run resolution checks so existing positions can be closed.
            await self._close_resolved_signals()
            self._save_state()
            return

        logger.info(f"🔍 Copycat poll: checking {len(self.wallets)} wallets...")

        all_current_positions: List[WalletPosition] = []

        for wallet in self.wallets:
            addr = wallet["address"]
            label = wallet["label"]
            addr_lower = addr.lower()

            await asyncio.sleep(0.5)  # avoid 429 rate limiting
            raw_positions = await self._fetch_positions(addr)
            parsed = [self._parse_position(p, addr_lower, label) for p in raw_positions]
            positions = [p for p in parsed if p is not None]

            # Track live portfolio value from raw API data
            score = self.wallet_scores.get(addr_lower)
            if score and raw_positions:
                score.portfolio_value_usdc = sum(
                    float(p.get("currentValue") or 0) for p in raw_positions
                )

            # Update wallet score based on resolved positions (redeemable=True).
            # Skip on first poll — last_positions is empty so every redeemable position
            # would look "new", bulk-recording all historical losses on restart.
            score = self.wallet_scores.get(addr_lower)
            if score and not self._first_poll:
                prev_positions = {p.market_id: p for p in self.last_positions.get(addr_lower, [])}
                for pos in positions:
                    if pos.redeemable and pos.market_id not in prev_positions:
                        # Newly resolved position
                        score.total_positions_seen += 1
                        score.last_seen = datetime.now(timezone.utc).isoformat()
                        if pos.cash_pnl > 0:
                            score.winning_positions += 1
                            score.total_cash_pnl += pos.cash_pnl
                        else:
                            score.losing_positions += 1
                            score.total_cash_pnl += pos.cash_pnl

            # Only include non-resolved positions for consensus
            active_positions = [p for p in positions if not p.redeemable]
            self.last_positions[addr_lower] = positions

            logger.debug(f"  {label}: {len(active_positions)} active positions")
            all_current_positions.extend(active_positions)

        # After first poll, last_positions is seeded — future polls can score normally
        self._first_poll = False

        # Detect consensus signals
        new_signals = self._build_consensus(all_current_positions)
        for signal in new_signals:
            sig_key = f"{signal.market_id}:{signal.outcome.lower()}"

            # Pre-check CLOB before recording the signal.
            #
            # The positions API has a multi-hour lag before setting redeemable=True
            # on already-resolved markets.  Without this check, we fire a consensus
            # signal → immediately call _close_resolved_signals in the same poll →
            # CLOB returns winner=False → record a phantom -100% loss with
            # duration_minutes=0.  All 41 initial trades were this exact pattern.
            already_resolved, res_price = await self._check_market_resolution(
                signal.market_id, signal.outcome
            )
            if already_resolved:
                logger.info(
                    f"⏭️  Skipping already-resolved market: {signal.market_title[:50]} "
                    f"| outcome={signal.outcome} res_price={res_price:.2f} "
                    "(positions API lagging behind CLOB)"
                )
                # Cache the resolution so we don't re-query for 5 minutes
                self._resolution_skip_until[signal.market_id] = (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                )
                continue

            self.active_signals[sig_key] = signal
            logger.info(
                f"🎯 CONSENSUS SIGNAL | {signal.market_title[:60]} | "
                f"{signal.outcome} | {signal.consensus_pct:.0%} of wallets agree | "
                f"avg_entry={signal.avg_entry_price:.3f} | "
                f"wallets: {', '.join(signal.supporting_wallets)}"
            )
            if self.live_mode:
                await self._place_live_order(signal)
            if self.paper_exec:
                result = await self.paper_exec.buy(
                    signal.market_id,
                    signal.outcome,
                    self.trade_size_usdc,
                )
                if result.get("ok"):
                    signal.copied = True
                    logger.info(f"✅ PAPER TRADE EXECUTED: {signal.outcome} | copied=True")
            # Notify Discord
            try:
                await BotAlert.signal(
                    bot_name="Kakashi Bot",
                    signal_type=f"COPY_{signal.outcome.upper()}",
                    price=signal.avg_entry_price,  # contract probability (0-1), not dollar price
                    confidence=signal.consensus_pct,
                    details={
                        "Market": signal.market_title[:60],
                        "Wallets Agree": f"{len(signal.supporting_wallets)}/{len(self.wallets)} ({signal.consensus_pct:.0%})",
                        "Wallets": ", ".join(signal.supporting_wallets),
                        "Contract Price": f"{signal.avg_entry_price:.3f} (implied {signal.avg_entry_price*100:.0f}% probability)",
                        "Paper Trade Size": f"${self.paper_trade_size:.0f}",
                    },
                )
            except Exception as exc:
                logger.debug(f"Discord signal alert skipped: {exc}")

        # Check and close resolved signals
        await self._close_resolved_signals()

        # Resolve any completed markets via Gamma API (catches orphaned positions
        # not in active_signals, e.g. after a bot restart)
        if self.paper_exec:
            gamma_resolved = await self.paper_exec.resolve_all(self._http)
            for r in gamma_resolved:
                opened_at = r.get("opened_at", "")
                duration = 0
                if opened_at:
                    try:
                        dur = datetime.now(timezone.utc) - _parse_dt(opened_at)
                        duration = int(dur.total_seconds() / 60)
                    except Exception:
                        pass
                self.tracker.record_trade(
                    bot_name="Kakashi Bot",
                    asset="POLY",
                    signal_type=f"COPY_{r['outcome'].upper()}",
                    entry_price=r["avg_price"],
                    exit_price=r["resolution_price"],
                    pnl_pct=r["pnl_pct"],
                    confidence=0.0,
                    is_win=r["is_win"],
                    duration_minutes=duration,
                    notes=f"Gamma resolution | {r.get('market_slug', '')}",
                )

        # Scheduled Discord reports (paper mode only)
        await self._maybe_daily_summary()
        await self._maybe_weekly_card()

        # Log wallet leaderboard every cycle
        scored = sorted(self.wallet_scores.values(), key=lambda s: s.total_cash_pnl, reverse=True)
        logger.info("📊 Wallet scores:")
        for s in scored:
            resolved = s.winning_positions + s.losing_positions
            logger.info(
                f"   {s.label}: {resolved} resolved | WR={s.win_rate:.0%} | "
                f"PnL=${s.total_cash_pnl:+.2f} | sharp={s.is_sharp}"
            )

        # Log active signals
        active = [s for s in self.active_signals.values() if not s.close_time]
        if active:
            logger.info(f"📌 Active copied positions: {len(active)}")
            for s in active:
                logger.info(f"   {s.market_title[:50]} | {s.outcome} @ {s.avg_entry_price:.3f} | "
                            f"{s.consensus_pct:.0%} consensus")

        self._save_state()

    async def run_forever(self):
        """Main loop: poll wallets every POLL_INTERVAL_SECONDS."""
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        self.headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession(timeout=timeout, headers=self.headers) as session:
            self._http = session

            logger.info(
                f"🤖 Kakashi Bot started | tracking {len(self.wallets)} wallets | "
                f"consensus threshold: {MIN_CONSENSUS_WALLETS} wallets | "
                f"poll interval: {POLL_INTERVAL_SECONDS}s"
            )

            # On startup, recover any positions from paper_executor that may have been orphaned
            # during a restart (e.g., April 26 positions still resolvable on April 27)
            if self.paper_exec:
                await self._recover_orphaned_positions()

            # In live mode, reconcile any open CLOB orders from before the restart.
            # If an order was placed but the bot crashed before recording it, this prevents
            # placing a duplicate order on the same market.
            if self.live_mode and self.client:
                try:
                    open_orders = self.client.get_open_orders()
                    if open_orders:
                        logger.info(
                            f"⚡ Live reconciliation: {len(open_orders)} open CLOB orders found on restart — "
                            "will not duplicate-place for markets already in order book"
                        )
                        for o in open_orders:
                            mkt_id = o.get("asset_id") or o.get("market_id", "")
                            if mkt_id and mkt_id not in self.active_signals:
                                logger.warning(
                                    f"   Orphaned live order {o.get('id','')} on market {mkt_id} — "
                                    "added to active_signals as pre-existing position"
                                )
                                # Record as active so _place_live_order skips it
                                self.active_signals[mkt_id] = ConsensusSignal(
                                    market_id=mkt_id,
                                    market_title=o.get("market", mkt_id),
                                    outcome=o.get("side", "YES"),
                                    supporting_wallets=[],
                                    consensus_pct=0.0,
                                    avg_entry_price=float(o.get("price", 0.5)),
                                    copied=True,
                                )
                except Exception as exc:
                    logger.warning(f"Live order reconciliation failed (non-critical): {exc}")

            while True:
                try:
                    await self.poll_once()
                except Exception as e:
                    logger.error(f"❌ Copycat poll error: {e}", exc_info=True)

                logger.info(f"💤 Sleeping {POLL_INTERVAL_SECONDS}s until next poll...")
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
