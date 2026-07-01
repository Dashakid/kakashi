"""
Paper trade executor backed by pm_trader.Engine.

Provides an async interface so it fits into the bot's asyncio event loop
without blocking. pm_trader uses synchronous httpx internally, so all
Engine calls are dispatched via run_in_executor.

Lifecycle:
  - Created in main_kakashi.py when PAPER_MODE=true
  - Passed into KakashiBot.__init__
  - bot calls await paper_exec.buy()  on consensus signal open
  - bot calls await paper_exec.sell() on consensus signal close
  - bot calls await paper_exec.daily_summary() / weekly_card() for Discord reports
  - bot calls paper_exec.close() on shutdown

P&L is stored in a SQLite DB at PM_TRADER_DATA_DIR/PM_TRADER_ACCOUNT/paper.db,
which is the verifiable audit trail for investors.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from loguru import logger

from src.notifications.discord_webhook import get_discord_client

try:
    from pm_trader.engine import Engine
    from pm_trader.analytics import compute_stats
    from pm_trader.card import generate_card
    from pm_trader.models import SimError
    _PM_AVAILABLE = True
except ImportError:
    _PM_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration (from env or defaults)
# ---------------------------------------------------------------------------

_DATA_DIR        = Path(os.getenv("PM_TRADER_DATA_DIR", str(Path.home() / ".pm-trader")))
_ACCOUNT         = os.getenv("PM_TRADER_ACCOUNT", "copycat-bot")
_START_BAL       = float(os.getenv("PAPER_BALANCE", "10000"))
_POSITIONS_FILE  = Path(os.getenv("PAPER_POSITIONS_FILE", "data/paper_positions.json"))
_GAMMA_API       = "https://gamma-api.polymarket.com/markets"


class PaperExecutor:
    """Async wrapper around pm_trader.Engine for verified paper trading."""

    def __init__(self) -> None:
        if not _PM_AVAILABLE:
            raise RuntimeError(
                "pm_trader not installed. "
                "Run: pip install polymarket-paper-trader"
            )
        self._account_dir = _DATA_DIR / _ACCOUNT
        self._account_dir.mkdir(parents=True, exist_ok=True)
        # Single-worker executor: SQLite connections cannot be shared across
        # threads, so all Engine calls must run on the same dedicated thread.
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._engine: Optional[Engine] = None
        # In-memory position cache (synced to JSON after each operation)
        self.positions: list = []
        self._load_positions()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_engine(self) -> Engine:
        """Return (or lazily create) the Engine — always called inside _executor."""
        if self._engine is None:
            self._engine = Engine(self._account_dir)
            try:
                self._engine.get_account()
            except Exception:
                self._engine.init_account(_START_BAL)
                logger.info(
                    f"📒 Paper account '{_ACCOUNT}' initialized | "
                    f"starting balance: ${_START_BAL:,.0f}"
                )
        return self._engine

    async def _run(self, fn):
        """Dispatch a blocking Engine call to the single dedicated thread."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, fn)

    # ------------------------------------------------------------------
    # Position file helpers (all called inside the executor thread)
    # ------------------------------------------------------------------

    def _load_positions(self) -> None:
        """Load positions from JSON file into self.positions."""
        try:
            if _POSITIONS_FILE.exists():
                data = json.loads(_POSITIONS_FILE.read_text())
                self.positions = data.get("positions", [])
                logger.info(f"📂 Loaded {len(self.positions)} open positions from {_POSITIONS_FILE}")
            else:
                self.positions = []
                logger.debug(f"📂 Position file {_POSITIONS_FILE} not found, starting fresh")
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️  Corrupted positions JSON {_POSITIONS_FILE}: {e} — starting fresh")
            self.positions = []
        except Exception as e:
            logger.warning(f"⚠️  Failed to load positions: {e} — starting fresh")
            self.positions = []

    def _save_positions(self) -> None:
        """Save self.positions to JSON file with error handling."""
        try:
            _POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(_POSITIONS_FILE, "w") as f:
                json.dump({"positions": self.positions}, f, indent=2, default=str)
            logger.debug(f"💾 Saved {len(self.positions)} positions to {_POSITIONS_FILE}")
        except Exception as e:
            logger.warning(f"⚠️  Failed to save positions to {_POSITIONS_FILE}: {e}")

    def _load_positions_sync(self) -> list:
        """Sync version for use inside executor thread (returns list, doesn't modify self.positions)."""
        try:
            if _POSITIONS_FILE.exists():
                return json.loads(_POSITIONS_FILE.read_text()).get("positions", [])
        except Exception as e:
            logger.debug(f"Could not load positions sync: {e}")
        return []

    def _save_positions_sync(self, positions: list) -> None:
        """Sync version for use inside executor thread (atomic write)."""
        try:
            _POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = _POSITIONS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps({"positions": positions}, indent=2))
            tmp.replace(_POSITIONS_FILE)
        except Exception as e:
            logger.warning(f"Could not save paper_positions.json: {e}")

    def _add_position_sync(self, pos: dict) -> None:
        positions = self._load_positions_sync()
        key = (pos["condition_id"], pos["outcome"])
        positions = [p for p in positions if (p["condition_id"], p["outcome"]) != key]
        positions.append(pos)
        self._save_positions_sync(positions)

    def _remove_position_sync(self, condition_id: str, outcome: str) -> None:
        positions = self._load_positions_sync()
        out_low = outcome.lower()
        positions = [
            p for p in positions
            if not (p["condition_id"] == condition_id and p["outcome"].lower() == out_low)
        ]
        self._save_positions_sync(positions)

    # ------------------------------------------------------------------
    # Trade execution
    # ------------------------------------------------------------------

    async def buy(
        self,
        condition_id: str,
        outcome: str,
        amount_usd: float,
    ) -> dict:
        """
        Paper-buy on a Polymarket market.

        Args:
            condition_id: Polymarket conditionId (0x…) — passed directly to
                          the CLOB API which accepts it as a market identifier.
            outcome:      "Yes" / "No" / "Up" / "Down" (case-insensitive).
            amount_usd:   USD to spend — same as self.trade_size_usdc in bot.
        """
        outcome_norm = outcome.lower()

        def _do() -> dict:
            engine = self._get_engine()
            try:
                result = engine.buy(condition_id, outcome_norm, amount_usd)
                ret = {
                    "ok":            True,
                    "market_slug":   result.trade.market_slug,
                    "question":      result.trade.market_question,
                    "outcome":       result.trade.outcome,
                    "shares":        result.trade.shares,
                    "avg_price":     result.trade.avg_price,
                    "amount_usd":    result.trade.amount_usd,
                    "fee":           result.trade.fee,
                    "cash":          result.account.cash,
                    "created_at":    result.trade.created_at,
                }
                self._add_position_sync({
                    "condition_id": condition_id,
                    "outcome":      outcome_norm,
                    "shares":       ret["shares"],
                    "avg_price":    ret["avg_price"],
                    "amount_usd":   amount_usd,
                    "market_slug":  ret["market_slug"],
                    "question":     ret.get("question", ""),
                    "opened_at":    datetime.now(timezone.utc).isoformat(),
                })
                # Reload self.positions from file to keep in sync
                self._load_positions()
                return ret
            except SimError as e:
                return {"ok": False, "error": e.message, "code": e.code}
            except Exception as e:
                return {"ok": False, "error": str(e), "code": "UNKNOWN"}

        result = await self._run(_do)
        if result["ok"]:
            logger.info(
                f"📒 PAPER BUY | {result['market_slug']} | "
                f"{result['outcome'].upper()} | "
                f"${amount_usd:.2f} → {result['shares']:.4f} shares "
                f"@ {result['avg_price']:.3f} | "
                f"fee=${result['fee']:.4f} | "
                f"cash=${result['cash']:.2f}"
            )
        else:
            logger.warning(
                f"📒 PAPER BUY failed | {condition_id[:20]}… | "
                f"{result['error']} [{result.get('code','')}]"
            )
            try:
                discord = get_discord_client()
                if discord.enabled:
                    await discord.send_message(
                        content=(
                            f"⚠️ **Paper buy failed** | `{condition_id[:20]}…`\n"
                            f"Error: `{result['error']}` [{result.get('code', '')}]"
                        )
                    )
            except Exception:
                pass
        return result

    async def sell(
        self,
        condition_id: str,
        outcome: str,
    ) -> dict:
        """
        Paper-sell the entire position for (condition_id, outcome).

        Looks up current share count directly from the pm_trader DB so the
        caller doesn't need to track it.
        """
        outcome_norm = outcome.lower()

        def _do() -> dict:
            engine = self._get_engine()
            try:
                pos = engine.db.get_position(condition_id, outcome_norm)
                if pos is None or pos.shares <= 0:
                    return {
                        "ok":    False,
                        "error": "No open position to sell",
                        "code":  "NO_POSITION",
                    }
                result = engine.sell(condition_id, outcome_norm, pos.shares)
                self._remove_position_sync(condition_id, outcome_norm)
                # Reload self.positions from file to keep in sync
                self._load_positions()
                return {
                    "ok":          True,
                    "market_slug": result.trade.market_slug,
                    "outcome":     result.trade.outcome,
                    "shares":      result.trade.shares,
                    "avg_price":   result.trade.avg_price,
                    "fee":         result.trade.fee,
                    "cash":        result.account.cash,
                }
            except SimError as e:
                return {"ok": False, "error": e.message, "code": e.code}
            except Exception as e:
                return {"ok": False, "error": str(e), "code": "UNKNOWN"}

        result = await self._run(_do)
        if result["ok"]:
            logger.info(
                f"📒 PAPER SELL | {result['market_slug']} | "
                f"{result['outcome'].upper()} | "
                f"{result['shares']:.4f} shares @ {result['avg_price']:.3f} | "
                f"fee=${result['fee']:.4f} | cash=${result['cash']:.2f}"
            )
        else:
            # NO_POSITION is expected if the market was already resolved via resolve_all()
            if result.get("code") == "NO_POSITION":
                logger.debug(f"📒 PAPER SELL skipped | {condition_id[:20]}… | {result['error']}")
            else:
                logger.warning(
                    f"📒 PAPER SELL failed | {condition_id[:20]}… | "
                    f"{result['error']} [{result.get('code', '')}]"
                )
                try:
                    discord = get_discord_client()
                    if discord.enabled:
                        await discord.send_message(
                            content=(
                                f"⚠️ **Paper sell failed** | `{condition_id[:20]}…`\n"
                                f"Error: `{result['error']}` [{result.get('code', '')}]"
                            )
                        )
                except Exception:
                    pass
        return result

    async def resolve_all(self, http: Optional[aiohttp.ClientSession] = None) -> list:
        """
        Settle P&L for all officially closed markets.

        When http is provided, queries the Gamma API for each open position:
          - if resolved=true + resolutionPrice is present, close the position
            and return a list of result dicts (caller records the trade).
        Without http, falls back to pm_trader engine.resolve_all() which
        settles any markets that already have resolution data locally.
        """
        if http is None:
            def _do():
                try:
                    return self._get_engine().resolve_all()
                except Exception as e:
                    logger.debug(f"Paper resolve_all: {e}")
                    return []
            results = await self._run(_do)
            for r in results:
                logger.info(
                    f"📒 PAPER RESOLVE | {r.position.market_slug} | "
                    f"{r.position.outcome.upper()} | payout=${r.payout:.2f}"
                )
            return results

        # --- CLOB-API-aware resolution ---
        # Uses clob.polymarket.com/markets/{conditionId} → tokens[].winner
        # The Gamma API does not reliably return outcome/resolved fields.
        positions = await self._run(self._load_positions_sync)
        resolved = []

        for pos in positions:
            condition_id = pos["condition_id"]
            outcome      = pos["outcome"]
            try:
                async with http.get(
                    f"https://clob.polymarket.com/markets/{condition_id}",
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
            except Exception:
                continue

            tokens = data.get("tokens", [])
            if not tokens:
                continue

            target = next(
                (t for t in tokens if (t.get("outcome") or "").lower() == outcome.lower()),
                tokens[0],
            )
            winner = target.get("winner")
            if winner is None:
                continue

            resolution_price = 1.0 if winner else 0.0
            is_win = resolution_price >= 0.99

            # Close in pm_trader; sell() also removes from positions JSON on success.
            sell_result = await self.sell(condition_id, outcome)
            if not sell_result.get("ok"):
                # pm_trader can't sell a closed market — force-remove from JSON
                # so we don't keep retrying a dead position.
                await self._run(lambda: self._remove_position_sync(condition_id, outcome))

            avg_price  = pos["avg_price"]
            shares     = pos["shares"]
            dollar_pnl = (resolution_price - avg_price) * shares
            pnl_pct    = (resolution_price - avg_price) / avg_price if avg_price > 0 else 0

            outcome_icon = "✅ WIN" if is_win else "❌ LOSS"
            logger.info(
                f"📒 {outcome_icon} | {pos.get('market_slug', condition_id[:20])} | "
                f"{outcome.upper()} | entry={avg_price:.3f} → "
                f"resolution={resolution_price:.0f} | "
                f"pnl=${dollar_pnl:+.2f} ({pnl_pct:+.1%})"
            )

            resolved.append({
                "condition_id":    condition_id,
                "outcome":         outcome,
                "market_slug":     pos.get("market_slug", ""),
                "question":        pos.get("question", ""),
                "avg_price":       avg_price,
                "shares":          shares,
                "amount_usd":      pos.get("amount_usd", 0.0),
                "resolution_price": resolution_price,
                "is_win":          is_win,
                "pnl_pct":         pnl_pct,
                "dollar_pnl":      dollar_pnl,
                "opened_at":       pos.get("opened_at", ""),
            })

        return resolved

    async def _check_market_resolution(self, condition_id: str, outcome: str):
        """Check Gamma API if market has resolved. Returns (resolved, price)."""
        try:
            url = f"https://gamma-api.polymarket.com/markets?conditionId={condition_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    markets = await r.json()
            for m in markets:
                token_outcome = m.get("outcome", "").strip().lower()
                if token_outcome == outcome.strip().lower():
                    if m.get("resolved"):
                        return True, float(m.get("resolutionPrice", 0))
            return False, None
        except Exception as e:
            logger.warning(f"Resolution check failed: {e}")
            return False, None

    # ------------------------------------------------------------------
    # Balance query (safe to call any time)
    # ------------------------------------------------------------------

    async def get_balance(self) -> dict:
        def _do():
            try:
                return self._get_engine().get_balance()
            except Exception:
                return {}
        return await self._run(_do)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    async def daily_summary(self) -> dict:
        """
        Compute today's trading summary.
        Returns a plain dict — callers format it for Discord themselves.
        """
        def _do() -> dict:
            engine    = self._get_engine()
            account   = engine.get_account()
            all_trades = engine.get_history(10_000)
            portfolio  = engine.get_portfolio()
            pos_value  = sum(p["current_value"] for p in portfolio)
            stats      = compute_stats(all_trades, account, pos_value)

            # Count today's trades (UTC)
            day_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today_count = 0
            for t in all_trades:
                ts = t.created_at
                if ts is None:
                    continue
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= day_start:
                    today_count += 1

            return {
                "account":         _ACCOUNT,
                "cash":            account.cash,
                "starting_balance": account.starting_balance,
                "total_value":     account.cash + pos_value,
                "positions_value": pos_value,
                "total_pnl":       stats.get("pnl", 0.0),
                "roi_pct":         stats.get("roi_pct", 0.0),
                "win_rate":        stats.get("win_rate", 0.0),
                "total_trades":    stats.get("total_trades", 0),
                "open_positions":  len(portfolio),
                "today_trades":    today_count,
                "max_drawdown":    stats.get("max_drawdown", 0.0),
                "sharpe":          stats.get("sharpe_ratio", 0.0),
            }

        return await self._run(_do)

    async def weekly_card(self) -> str:
        """
        Generate the pm-trader shareable stats card text.
        Sent to Discord as a code block for the weekly report.
        """
        def _do() -> str:
            engine    = self._get_engine()
            account   = engine.get_account()
            trades    = engine.get_history(10_000)
            portfolio = engine.get_portfolio()
            pos_value = sum(p["current_value"] for p in portfolio)
            stats     = compute_stats(trades, account, pos_value)
            return generate_card(stats, _ACCOUNT, portfolio)

        return await self._run(_do)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Shut down the engine and the dedicated executor thread."""
        def _do():
            if self._engine is not None:
                self._engine.close()
                self._engine = None
        # Run close() on the same thread that owns the SQLite connection
        future = self._executor.submit(_do)
        future.result(timeout=5)
        self._executor.shutdown(wait=True)
        logger.debug("PaperExecutor: engine closed")
