"""
Per-basket win/loss tracker for Kakashi v2.

Persists state to data/kakashi_v2_state.json.

Responsibilities
----------------
- Record paper trade opens and closes per basket.
- Compute per-basket ROI over a rolling 30-day window.
- Weekly wallet refresh: flag baskets whose wallets have gone stale
  (last-30d P&L negative); the strategy layer decides whether to pause them.
- Drop basket from active rotation after 30 days of negative ROI.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_FILE = Path("data/kakashi_v2_state.json")
ROI_WINDOW_DAYS      = 30   # rolling window for basket ROI evaluation
DROP_BASKET_ROI      = 0.0  # drop basket if ROI < 0 over the last 30 days
WALLET_REFRESH_DAYS  = 7    # how often to flag wallets for re-evaluation


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PaperTrade:
    """A single paper trade belonging to a basket."""
    trade_id: str           # unique — "{basket}_{market_id}_{outcome}_{ts}"
    basket: str
    market_id: str
    market_title: str
    outcome: str            # "Yes" / "No"
    entry_price: float
    paper_size_usd: float   # notional dollars we "risked"
    opened_ts: float        # unix timestamp
    # Populated on close:
    exit_price: float = 0.0
    closed_ts: float = 0.0
    pnl_usd: float = 0.0
    is_win: Optional[bool] = None   # None = still open
    close_reason: str = ""          # "resolved", "timeout", "manual"


@dataclass
class BasketStats:
    """Cumulative stats for one basket."""
    name: str
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_usd: float = 0.0
    pnl_last_30d_usd: float = 0.0
    win_rate: float = 0.0           # wins / total_trades
    roi_last_30d: float = 0.0       # pnl_last_30d / total_notional_30d
    is_active: bool = True          # False = dropped due to negative ROI
    last_refresh_ts: float = 0.0    # last time wallets were flagged for review


@dataclass
class TrackerState:
    """Full state persisted to disk."""
    open_trades: Dict[str, PaperTrade] = field(default_factory=dict)    # trade_id → trade
    closed_trades: List[PaperTrade] = field(default_factory=list)
    basket_stats: Dict[str, BasketStats] = field(default_factory=dict)  # basket name → stats
    last_saved_ts: float = 0.0


# ---------------------------------------------------------------------------
# Tracker
# ---------------------------------------------------------------------------

class BasketTracker:
    """
    Manages per-basket paper trade lifecycle and rolling ROI.

    Usage
    -----
        tracker = BasketTracker()
        trade_id = tracker.open_trade(basket="sports", ...)
        ...
        tracker.close_trade(trade_id, exit_price=0.72, reason="resolved")
        roi = tracker.basket_roi("sports")
    """

    def __init__(self) -> None:
        self._state = TrackerState()
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not STATE_FILE.exists():
            logger.info("[tracker] No existing state — starting fresh")
            self._save()
            return
        try:
            raw = json.loads(STATE_FILE.read_text())

            # Rebuild open trades
            for tid, td in raw.get("open_trades", {}).items():
                self._state.open_trades[tid] = PaperTrade(**td)

            # Rebuild closed trades
            for td in raw.get("closed_trades", []):
                self._state.closed_trades.append(PaperTrade(**td))

            # Rebuild basket stats
            for bname, bs in raw.get("basket_stats", {}).items():
                self._state.basket_stats[bname] = BasketStats(**bs)

            self._state.last_saved_ts = raw.get("last_saved_ts", 0.0)
            logger.info(
                f"[tracker] Loaded state: "
                f"{len(self._state.open_trades)} open, "
                f"{len(self._state.closed_trades)} closed trades"
            )
        except Exception as exc:
            logger.warning(f"[tracker] Failed to load state ({exc}) — starting fresh")

    def _save(self) -> None:
        self._state.last_saved_ts = time.time()
        try:
            payload = {
                "open_trades": {
                    tid: asdict(t) for tid, t in self._state.open_trades.items()
                },
                "closed_trades": [asdict(t) for t in self._state.closed_trades],
                "basket_stats": {
                    bn: asdict(bs) for bn, bs in self._state.basket_stats.items()
                },
                "last_saved_ts": self._state.last_saved_ts,
            }
            STATE_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as exc:
            logger.error(f"[tracker] Failed to save state: {exc}")

    # ------------------------------------------------------------------
    # Trade lifecycle
    # ------------------------------------------------------------------

    def open_trade(
        self,
        basket: str,
        market_id: str,
        market_title: str,
        outcome: str,
        entry_price: float,
        paper_size_usd: float,
    ) -> str:
        """Record a new paper trade. Returns the generated trade_id."""
        ts = time.time()
        trade_id = f"{basket}_{market_id}_{outcome}_{int(ts)}"
        trade = PaperTrade(
            trade_id=trade_id,
            basket=basket,
            market_id=market_id,
            market_title=market_title,
            outcome=outcome,
            entry_price=entry_price,
            paper_size_usd=paper_size_usd,
            opened_ts=ts,
        )
        self._state.open_trades[trade_id] = trade
        self._ensure_basket_stats(basket)
        logger.info(
            f"[tracker] OPEN {trade_id} | {basket} | {market_title[:40]} "
            f"| {outcome} @ {entry_price:.3f} | ${paper_size_usd:.2f}"
        )
        self._save()
        return trade_id

    def close_trade(
        self,
        trade_id: str,
        exit_price: float,
        reason: str = "resolved",
    ) -> Optional[PaperTrade]:
        """
        Close an open paper trade, compute P&L, update basket stats.
        Returns the closed PaperTrade or None if trade_id not found.
        """
        trade = self._state.open_trades.pop(trade_id, None)
        if trade is None:
            logger.warning(f"[tracker] close_trade: unknown trade_id {trade_id}")
            return None

        trade.exit_price  = exit_price
        trade.closed_ts   = time.time()
        trade.close_reason = reason

        # P&L: for a YES token, pnl = (exit - entry) * shares
        # shares = paper_size_usd / entry_price
        if trade.entry_price > 0:
            shares = trade.paper_size_usd / trade.entry_price
            trade.pnl_usd = (exit_price - trade.entry_price) * shares
        else:
            trade.pnl_usd = 0.0

        trade.is_win = trade.pnl_usd > 0

        self._state.closed_trades.append(trade)
        self._update_basket_stats(trade.basket)

        outcome_emoji = "✅" if trade.is_win else "❌"
        logger.info(
            f"[tracker] CLOSE {trade_id} {outcome_emoji} | "
            f"exit={exit_price:.3f} pnl=${trade.pnl_usd:+.2f} reason={reason}"
        )
        self._save()
        return trade

    # ------------------------------------------------------------------
    # Stats helpers
    # ------------------------------------------------------------------

    def _ensure_basket_stats(self, basket: str) -> None:
        if basket not in self._state.basket_stats:
            self._state.basket_stats[basket] = BasketStats(name=basket)

    def _update_basket_stats(self, basket: str) -> None:
        self._ensure_basket_stats(basket)
        stats = self._state.basket_stats[basket]
        cutoff = time.time() - ROI_WINDOW_DAYS * 86400

        closed_for_basket = [
            t for t in self._state.closed_trades if t.basket == basket
        ]

        stats.total_trades = len(closed_for_basket)
        stats.wins         = sum(1 for t in closed_for_basket if t.is_win)
        stats.losses       = sum(1 for t in closed_for_basket if t.is_win is False)
        stats.total_pnl_usd = sum(t.pnl_usd for t in closed_for_basket)
        stats.win_rate = stats.wins / stats.total_trades if stats.total_trades else 0.0

        recent = [t for t in closed_for_basket if t.closed_ts >= cutoff]
        stats.pnl_last_30d_usd = sum(t.pnl_usd for t in recent)
        notional_30d = sum(t.paper_size_usd for t in recent)
        stats.roi_last_30d = (
            stats.pnl_last_30d_usd / notional_30d if notional_30d > 0 else 0.0
        )

        # Auto-drop basket if 30-day ROI is negative (after ≥5 closed trades)
        if stats.total_trades >= 5 and stats.roi_last_30d < DROP_BASKET_ROI:
            if stats.is_active:
                logger.warning(
                    f"[tracker] DROPPING basket '{basket}' — "
                    f"30d ROI={stats.roi_last_30d*100:.1f}% after {stats.total_trades} trades"
                )
                stats.is_active = False

    def basket_roi(self, basket: str) -> float:
        """Return rolling 30-day ROI for a basket (0.0 if no data)."""
        stats = self._state.basket_stats.get(basket)
        return stats.roi_last_30d if stats else 0.0

    def is_basket_active(self, basket: str) -> bool:
        """Return True if the basket is still in the active rotation."""
        stats = self._state.basket_stats.get(basket)
        return stats.is_active if stats else True   # new baskets start active

    def open_positions_for_basket(self, basket: str) -> List[PaperTrade]:
        """Return all currently open trades for a given basket."""
        return [t for t in self._state.open_trades.values() if t.basket == basket]

    def open_position_count(self) -> int:
        return len(self._state.open_trades)

    # ------------------------------------------------------------------
    # Weekly wallet-refresh flag
    # ------------------------------------------------------------------

    def baskets_due_for_wallet_refresh(self) -> List[str]:
        """
        Return basket names whose wallet list is due for a refresh
        (i.e. last refresh was > WALLET_REFRESH_DAYS ago).

        The caller (strategy) is responsible for actually checking wallet
        P&L via the API and calling mark_wallet_refresh_done().
        """
        now = time.time()
        cutoff = now - WALLET_REFRESH_DAYS * 86400
        due = []
        for name, stats in self._state.basket_stats.items():
            if stats.last_refresh_ts < cutoff:
                due.append(name)
        # Also include baskets that have never been tracked yet
        return due

    def mark_wallet_refresh_done(self, basket: str) -> None:
        self._ensure_basket_stats(basket)
        self._state.basket_stats[basket].last_refresh_ts = time.time()
        self._save()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """Return a one-line human-readable summary of all basket stats."""
        lines = ["[BasketTracker summary]"]
        for name, stats in self._state.basket_stats.items():
            active_tag = "✅" if stats.is_active else "🚫 DROPPED"
            lines.append(
                f"  {name:10s} {active_tag}  "
                f"trades={stats.total_trades}  "
                f"WR={stats.win_rate*100:.0f}%  "
                f"30d_roi={stats.roi_last_30d*100:+.1f}%  "
                f"total_pnl=${stats.total_pnl_usd:+.2f}"
            )
        open_count = len(self._state.open_trades)
        lines.append(f"  open positions: {open_count}")
        return "\n".join(lines)
