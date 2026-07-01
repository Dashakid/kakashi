"""Arb Bot P&L tracker — mirrors the kakashi/win_loss tracking pattern.

Each executed arb trade is logged to:
  - data/win_loss_history.jsonl  (shared with all bots — same format)
  - data/arb_state.json          (arb-specific running state)

An arb trade is a WIN whenever net_profit_cents > 0 at entry, because the
position is simultaneously long YES on one side and long NO on the other —
meaning exactly one leg pays out $1 per contract regardless of resolution.
Expected P&L is therefore locked in at trade entry.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

_STATE_FILE = Path("data/arb_state.json")
_HISTORY_FILE = Path("data/win_loss_history.jsonl")

# Threshold (cents) for counting a trade as a win
WIN_THRESHOLD_CENTS = 0.5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception as exc:
        logger.warning(f"ArbTracker: could not load state: {exc}")
    return {
        "timestamp": None,
        "mode": "paper",
        "capital_initial": 1000.0,
        "capital_current": 1000.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "total_pnl_cents": 0.0,
        "total_pnl_usd": 0.0,
        "roi_pct": 0.0,
        "avg_spread_pct": 0.0,
        "best_trade_pnl_cents": 0.0,
        "worst_trade_pnl_cents": 0.0,
        "open_positions": {},
        "closed_trades": [],
        "notes": "",
    }


def _save_state(state: dict) -> None:
    try:
        state["timestamp"] = _now_iso()
        _STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as exc:
        logger.error(f"ArbTracker: failed to save state: {exc}")


def _append_history(entry: dict) -> None:
    try:
        _HISTORY_FILE.parent.mkdir(exist_ok=True)
        with _HISTORY_FILE.open("a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        logger.error(f"ArbTracker: failed to append history: {exc}")


class ArbTracker:
    """Thread-safe(ish) arb P&L tracker.  One instance lives on ExecutionBot."""

    def __init__(self, paper: bool = True) -> None:
        self._state = _load_state()
        self._state["mode"] = "paper" if paper else "live"
        _save_state(self._state)
        logger.info(
            f"ArbTracker loaded: {self._state['total_trades']} trades, "
            f"P&L={self._state['total_pnl_usd']:.2f} USD, "
            f"win_rate={self._state['win_rate']:.1%}"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def record_trade(
        self,
        *,
        poly_title: str,
        kalshi_title: str,
        poly_side: str,
        kalshi_side: str,
        poly_ask: float,          # 0-1 price
        kalshi_ask: float,        # 0-1 price
        combined_cost: float,     # poly_ask + kalshi_ask
        contracts: float,
        size_usd: float,
        expected_pnl_cents: float,
        paper: bool,
        opp_db_id: int,
    ) -> None:
        """Record one executed arb trade (both legs filled)."""
        s = self._state

        # Spread as percentage edge
        spread_pct = (1.0 - combined_cost) * 100  # e.g. 8.0 for 8% edge

        is_win = expected_pnl_cents > WIN_THRESHOLD_CENTS
        pnl_usd = expected_pnl_cents / 100.0

        # Update running totals
        s["total_trades"] += 1
        if is_win:
            s["wins"] += 1
        else:
            s["losses"] += 1
        s["win_rate"] = s["wins"] / s["total_trades"] if s["total_trades"] else 0.0
        s["total_pnl_cents"] = round(s["total_pnl_cents"] + expected_pnl_cents, 4)
        s["total_pnl_usd"] = round(s["total_pnl_usd"] + pnl_usd, 4)
        s["capital_current"] = round(s["capital_current"] + pnl_usd, 4)
        s["roi_pct"] = round(
            (s["capital_current"] - s["capital_initial"]) / s["capital_initial"] * 100, 4
        )

        # Rolling average spread
        prev_avg = s.get("avg_spread_pct", 0.0)
        n = s["total_trades"]
        s["avg_spread_pct"] = round(prev_avg + (spread_pct - prev_avg) / n, 4)

        # Best / worst
        if expected_pnl_cents > s["best_trade_pnl_cents"]:
            s["best_trade_pnl_cents"] = round(expected_pnl_cents, 4)
        if expected_pnl_cents < s["worst_trade_pnl_cents"]:
            s["worst_trade_pnl_cents"] = round(expected_pnl_cents, 4)

        # Closed trade record (keep last 200 for state file)
        trade_record = {
            "id": opp_db_id,
            "timestamp": _now_iso(),
            "poly": poly_title[:60],
            "kalshi": kalshi_title[:60],
            "poly_side": poly_side,
            "kalshi_side": kalshi_side,
            "poly_ask": round(poly_ask, 4),
            "kalshi_ask": round(kalshi_ask, 4),
            "combined_cost": round(combined_cost, 4),
            "spread_pct": round(spread_pct, 2),
            "size_usd": round(size_usd, 2),
            "contracts": round(contracts, 4),
            "expected_pnl_cents": round(expected_pnl_cents, 4),
            "pnl_usd": round(pnl_usd, 4),
            "is_win": is_win,
            "paper": paper,
        }
        s["closed_trades"] = (s["closed_trades"] + [trade_record])[-200:]
        _save_state(s)

        # Append to shared win_loss_history.jsonl (same schema as kakashi)
        history_entry = {
            "timestamp": _now_iso(),
            "bot_name": "Arb Bot",
            "asset": f"{poly_title[:30]}/{kalshi_title[:30]}",
            "signal_type": "ARB_ENTER",
            "entry_price": round(combined_cost, 4),
            "exit_price": 1.0,
            "pnl_pct": round(spread_pct / 100, 4),
            "confidence": round(1.0 - combined_cost, 4),
            "is_win": is_win,
            "duration_minutes": 0,
            "notes": (
                f"poly_ask={poly_ask:.3f} kalshi_ask={kalshi_ask:.3f} "
                f"spread={spread_pct:.2f}% size=${size_usd:.2f} "
                f"opp_id={opp_db_id} paper={paper}"
            ),
            "market_features": None,
        }
        _append_history(history_entry)

        logger.info(
            f"ArbTracker #{opp_db_id}: {'WIN' if is_win else 'LOSS'} | "
            f"spread={spread_pct:.2f}% | pnl={pnl_usd:+.4f} USD | "
            f"total_pnl={s['total_pnl_usd']:+.2f} USD | "
            f"roi={s['roi_pct']:+.2f}% | "
            f"trades={s['total_trades']} win_rate={s['win_rate']:.1%}"
        )

    @property
    def summary(self) -> dict[str, Any]:
        s = self._state
        return {
            "total_trades": s["total_trades"],
            "wins": s["wins"],
            "losses": s["losses"],
            "win_rate": s["win_rate"],
            "total_pnl_usd": s["total_pnl_usd"],
            "roi_pct": s["roi_pct"],
            "capital_current": s["capital_current"],
        }
