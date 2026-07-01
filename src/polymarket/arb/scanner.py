"""
Polymarket Arbitrage Scanner

Scans all active binary markets for YES + NO price gaps.
In an efficient market: yes_price + no_price == 1.0
When the sum < 1.0, the gap represents a risk-free profit opportunity:
  - Buy YES at yes_price
  - Buy NO at no_price
  - One leg resolves at $1 → total payout = position_size
  - Total cost = position_size * (yes_price + no_price)
  - Profit = position_size * gap (always positive regardless of outcome)
"""

from __future__ import annotations

import json
import asyncio
from dataclasses import dataclass, field, asdict


def _parse_json_field(value) -> list:
    """Gamma returns some fields as JSON strings; parse them into lists."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, ValueError):
            pass
    return []
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

MIN_GAP = 0.04           # 4% minimum gap to flag as opportunity
MIN_LIQUIDITY = 5_000    # $5k liquidity required on each side
MIN_VOLUME_24H = 10_000  # $10k 24h volume minimum
MAX_POSITION_PCT = 0.05  # 5% of bankroll per arb
BANKROLL = 20_000        # paper bankroll in USD
TAKER_FEE = 0.02         # 2% taker fee per leg (4% total round-trip cost)

# Data storage
DATA_DIR = Path("data")
ARB_TRADES_FILE = DATA_DIR / "arb_trades.jsonl"

# Gamma API
GAMMA_URL = "https://gamma-api.polymarket.com/markets"


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class ArbOpportunity:
    """A detected arbitrage gap in a Polymarket binary market."""
    market_id: str
    market_title: str
    yes_price: float
    no_price: float
    gap: float                  # 1.0 - (yes_price + no_price)
    yes_liquidity: float        # USD liquidity on YES side
    no_liquidity: float         # USD liquidity on NO side
    volume_24h: float
    ends_at: str                # ISO timestamp
    expected_profit_pct: float  # gap after fees (gap - 2*TAKER_FEE)

    @property
    def is_profitable_after_fees(self) -> bool:
        return self.expected_profit_pct > 0

    @property
    def min_liquidity(self) -> float:
        return min(self.yes_liquidity, self.no_liquidity)


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

async def scan_markets(session: aiohttp.ClientSession) -> list[ArbOpportunity]:
    """
    Fetch all active Polymarket markets and return arb opportunities.

    Filters:
    - Binary markets only (exactly 2 tokens: YES + NO)
    - gap > MIN_GAP (4%)
    - Both sides have > MIN_LIQUIDITY ($5k)
    - 24h volume > MIN_VOLUME_24H ($10k)
    - Market does not expire within 24 hours
    """
    opportunities: list[ArbOpportunity] = []
    offset = 0
    batch = 100
    total_scanned = 0

    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc + timedelta(hours=24)  # must not expire in <24h

    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": batch,
            "offset": offset,
        }
        try:
            async with session.get(
                GAMMA_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Gamma API returned {resp.status} at offset={offset}")
                    break
                markets = await resp.json()
        except Exception as e:
            logger.error(f"Gamma API fetch error at offset={offset}: {e}")
            break

        if not markets:
            break

        for m in markets:
            total_scanned += 1
            opp = _evaluate_market(m, cutoff_dt)
            if opp is not None:
                opportunities.append(opp)

        if len(markets) < batch:
            break  # last page
        offset += batch

    # Sort by gap descending
    opportunities.sort(key=lambda o: o.gap, reverse=True)
    logger.info(
        f"Arb scan complete: {total_scanned} markets scanned | "
        f"{len(opportunities)} opportunities found (gap > {MIN_GAP:.0%})"
    )
    return opportunities


def _evaluate_market(m: dict, cutoff_dt: datetime) -> Optional[ArbOpportunity]:
    """
    Evaluate a single market dict from Gamma API.
    Returns ArbOpportunity if it passes all filters, else None.
    """
    try:
        yes_price = 0.0
        no_price = 0.0
        yes_liq = 0.0
        no_liq = 0.0

        # Gamma previously exposed token objects; newer responses use
        # parallel outcomes/outcomePrices arrays.
        tokens = m.get("tokens") or []
        if len(tokens) == 2:
            yes_tok = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), None)
            no_tok = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)
            if yes_tok is None or no_tok is None:
                return None

            yes_price = float(yes_tok.get("price", 0) or 0)
            no_price = float(no_tok.get("price", 0) or 0)
            yes_liq = float(yes_tok.get("liquidity", 0) or 0)
            no_liq = float(no_tok.get("liquidity", 0) or 0)
        else:
            outcomes = _parse_json_field(m.get("outcomes"))
            prices   = _parse_json_field(m.get("outcomePrices"))
            if len(outcomes) != 2 or len(prices) != 2:
                return None

            normalized = [str(outcome).strip().upper() for outcome in outcomes]
            try:
                yes_idx = normalized.index("YES")
                no_idx = normalized.index("NO")
            except ValueError:
                return None

            yes_price = float(prices[yes_idx] or 0)
            no_price = float(prices[no_idx] or 0)

        if yes_price <= 0 or no_price <= 0:
            return None

        gap = 1.0 - (yes_price + no_price)
        if gap <= MIN_GAP:
            return None

        # Liquidity check
        if yes_liq == 0 and no_liq == 0:
            market_liquidity = float(m.get("liquidityClob", m.get("liquidity", 0)) or 0)
            yes_liq = market_liquidity / 2
            no_liq = market_liquidity / 2

        if yes_liq < MIN_LIQUIDITY or no_liq < MIN_LIQUIDITY:
            return None

        # Volume check
        vol = float(m.get("volume24hrClob", m.get("volume24hr", 0)) or 0)
        if vol < MIN_VOLUME_24H:
            return None

        # Expiry check: must not expire within 24h
        ends_at_str = m.get("endDate") or m.get("endDateIso") or ""
        if ends_at_str:
            try:
                ends_at_dt = datetime.fromisoformat(
                    ends_at_str.replace("Z", "+00:00")
                )
                if ends_at_dt.tzinfo is None:
                    ends_at_dt = ends_at_dt.replace(tzinfo=timezone.utc)
                if ends_at_dt < cutoff_dt:
                    return None
            except ValueError:
                pass  # if we can't parse, don't filter

        # Profit after fees: each leg costs TAKER_FEE
        expected_profit_pct = gap - 2 * TAKER_FEE

        return ArbOpportunity(
            market_id=m.get("conditionId") or m.get("id") or "",
            market_title=m.get("question") or m.get("slug") or "Unknown",
            yes_price=round(yes_price, 4),
            no_price=round(no_price, 4),
            gap=round(gap, 4),
            yes_liquidity=round(yes_liq, 2),
            no_liquidity=round(no_liq, 2),
            volume_24h=round(vol, 2),
            ends_at=ends_at_str,
            expected_profit_pct=round(expected_profit_pct, 4),
        )

    except Exception as e:
        logger.debug(f"Market eval error: {e} | market={m.get('slug','?')}")
        return None


# ---------------------------------------------------------------------------
# Paper executor
# ---------------------------------------------------------------------------

def paper_execute_arb(opp: ArbOpportunity, bankroll: float = BANKROLL) -> dict:
    """
    Paper trade both legs simultaneously.

    Position size = min(5% of bankroll, yes_liquidity/2, no_liquidity/2)
    Logs to data/arb_trades.jsonl
    Returns the trade record dict.
    """
    position_size = min(
        bankroll * MAX_POSITION_PCT,
        opp.yes_liquidity / 2,
        opp.no_liquidity / 2,
    )
    position_size = round(position_size, 2)

    expected_profit = round(position_size * opp.gap, 4)
    expected_profit_after_fees = round(position_size * opp.expected_profit_pct, 4)

    record = {
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "market_id": opp.market_id,
        "market_title": opp.market_title,
        "yes_price": opp.yes_price,
        "no_price": opp.no_price,
        "gap": opp.gap,
        "position_size": position_size,
        "yes_leg_cost": round(position_size * opp.yes_price, 4),
        "no_leg_cost": round(position_size * opp.no_price, 4),
        "total_cost": round(position_size * (opp.yes_price + opp.no_price), 4),
        "expected_profit_gross": expected_profit,
        "expected_profit_net": expected_profit_after_fees,
        "expected_profit_pct": opp.expected_profit_pct,
        "status": "open",
        "resolved_at": None,
        "winner": None,
        "actual_pnl": None,
    }

    _log_arb_trade(record)
    logger.info(
        f"📝 PAPER ARB OPENED | {opp.market_title[:60]} | "
        f"YES={opp.yes_price:.3f} NO={opp.no_price:.3f} | "
        f"gap={opp.gap:.2%} | size=${position_size:,.0f} | "
        f"expected net profit=${expected_profit_after_fees:,.2f}"
    )
    return record


def _log_arb_trade(record: dict) -> None:
    """Append a trade record to the JSONL file."""
    DATA_DIR.mkdir(exist_ok=True)
    with ARB_TRADES_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Resolution tracking
# ---------------------------------------------------------------------------

def resolve_open_arbs(open_trades: list[dict], current_prices: dict[str, dict]) -> list[dict]:
    """
    Check each open arb trade against current market prices.
    A market is considered resolved when YES or NO price hits >= 0.99.

    Resolution math:
      YES wins → YES leg pays $1/share, NO leg = $0
      NO wins  → NO leg pays $1/share, YES leg = $0
      Either way: payout = position_size * 1.0
      Cost       = position_size * (yes_price + no_price)
      Profit     = position_size * gap  (always positive)

    Returns list of newly resolved trades with actual_pnl filled in.
    """
    resolved = []
    for trade in open_trades:
        if trade.get("status") != "open":
            continue
        market_id = trade["market_id"]
        prices = current_prices.get(market_id, {})
        yes_now = prices.get("yes_price", 0)
        no_now  = prices.get("no_price",  0)

        winner = None
        if yes_now >= 0.99:
            winner = "YES"
        elif no_now >= 0.99:
            winner = "NO"

        if winner:
            position_size = trade["position_size"]
            total_cost = trade["total_cost"]
            actual_pnl = round(position_size - total_cost, 4)

            trade.update({
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "winner": winner,
                "actual_pnl": actual_pnl,
                "status": "closed",
            })
            resolved.append(trade)
            logger.info(
                f"✅ ARB RESOLVED | {trade['market_title'][:50]} | "
                f"winner={winner} | pnl=${actual_pnl:+.2f}"
            )
    return resolved


def load_arb_trades() -> list[dict]:
    """Load all arb trades from the JSONL file."""
    if not ARB_TRADES_FILE.exists():
        return []
    trades = []
    for line in ARB_TRADES_FILE.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return trades


def save_arb_trades(trades: list[dict]) -> None:
    """Overwrite the JSONL file with updated trades (after resolution)."""
    DATA_DIR.mkdir(exist_ok=True)
    with ARB_TRADES_FILE.open("w") as f:
        for t in trades:
            f.write(json.dumps(t) + "\n")
