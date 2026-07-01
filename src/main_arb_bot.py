"""
Polymarket Paper Arbitrage Bot

Scans all active Polymarket binary markets every 5 minutes for YES + NO
price gaps. When a gap > 4% is found with sufficient liquidity, both legs
are paper-traded simultaneously.

The expected profit is always positive regardless of outcome:
  Payout = position_size × 1.0  (one leg always pays $1/share)
  Cost   = position_size × (yes_price + no_price)
  Profit = position_size × gap

Usage:
    python3 -m src.main_arb_bot
    python3 -m src.main_arb_bot --scan-once   # single scan then exit
"""

from __future__ import annotations

import asyncio
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from loguru import logger

from src.polymarket.arb.scanner import (
    ArbOpportunity,
    scan_markets,
    paper_execute_arb,
    load_arb_trades,
    save_arb_trades,
    resolve_open_arbs,
    BANKROLL,
    TAKER_FEE,
    MIN_GAP,
    MIN_LIQUIDITY,
    MIN_VOLUME_24H,
)
from src.notifications.discord_webhook import get_discord_client

POLL_INTERVAL = 300    # seconds between scans (5 minutes)
LIVE_TRADING  = False  # paper only — never flip to True without adding CLOB execution

LOG_FILE = Path("logs/arb_bot.log")


# ---------------------------------------------------------------------------
# Discord helpers
# ---------------------------------------------------------------------------

async def _alert_opportunity(opp: ArbOpportunity, size: float, net_profit: float) -> None:
    """Send Discord embed for a newly found arb opportunity."""
    try:
        discord = get_discord_client()
        if not discord.enabled:
            return
        embed = {
            "title": "⚡ ARB OPPORTUNITY",
            "color": 0x00FF88,
            "fields": [
                {"name": "Market",      "value": opp.market_title[:80],          "inline": False},
                {"name": "YES price",   "value": f"{opp.yes_price:.3f}",          "inline": True},
                {"name": "NO price",    "value": f"{opp.no_price:.3f}",           "inline": True},
                {"name": "Gap",         "value": f"**{opp.gap:.2%}**",            "inline": True},
                {"name": "After fees",  "value": f"{opp.expected_profit_pct:.2%}", "inline": True},
                {"name": "Position",    "value": f"${size:,.0f}",                 "inline": True},
                {"name": "Net profit",  "value": f"**${net_profit:,.2f}**",       "inline": True},
                {"name": "YES liq",     "value": f"${opp.yes_liquidity:,.0f}",    "inline": True},
                {"name": "NO liq",      "value": f"${opp.no_liquidity:,.0f}",     "inline": True},
                {"name": "Volume 24h",  "value": f"${opp.volume_24h:,.0f}",       "inline": True},
            ],
            "footer": {"text": f"Paper arb bot | bankroll ${BANKROLL:,.0f}"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await discord.send_message(embed=embed)
    except Exception as e:
        logger.debug(f"Discord alert failed: {e}")


async def _alert_scan_summary(
    n_scanned: int,
    opportunities: list[ArbOpportunity],
    n_executed: int,
) -> None:
    """Send Discord summary of scan results (only when opportunities are found)."""
    if not opportunities:
        return
    try:
        discord = get_discord_client()
        if not discord.enabled:
            return
        top5 = opportunities[:5]
        lines = "\n".join(
            f"`{o.gap:.2%}` gap | ${min(o.yes_liquidity, o.no_liquidity):,.0f} min-liq "
            f"| {o.market_title[:50]}"
            for o in top5
        )
        embed = {
            "title": f"🔍 Arb Scan: {len(opportunities)} gaps found / {n_scanned} scanned",
            "description": f"**Top {min(5, len(opportunities))} opportunities:**\n{lines}",
            "color": 0xFFD700,
            "fields": [
                {"name": "Executed (paper)", "value": str(n_executed), "inline": True},
                {"name": "Min gap",          "value": f"{MIN_GAP:.0%}",  "inline": True},
                {"name": "Bankroll",         "value": f"${BANKROLL:,.0f}", "inline": True},
            ],
            "footer": {"text": "Polymarket paper arb bot"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await discord.send_message(embed=embed)
    except Exception as e:
        logger.debug(f"Discord summary failed: {e}")


# ---------------------------------------------------------------------------
# Resolution check
# ---------------------------------------------------------------------------

async def check_resolutions(
    session: aiohttp.ClientSession,
    open_trades: list[dict],
) -> int:
    """
    Fetch current prices for all open arb trades and resolve any closed markets.
    Returns count of newly resolved trades.
    """
    if not open_trades:
        return 0

    current_prices: dict[str, dict] = {}
    for trade in open_trades:
        market_id = trade["market_id"]
        try:
            url = "https://gamma-api.polymarket.com/markets"
            params = {"conditionId": market_id}
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    markets = data if isinstance(data, list) else [data]
                    if markets:
                        m = markets[0]
                        tokens = m.get("tokens") or []
                        yes_tok = next((t for t in tokens if t.get("outcome","").upper()=="YES"), None)
                        no_tok  = next((t for t in tokens if t.get("outcome","").upper()=="NO"),  None)
                        current_prices[market_id] = {
                            "yes_price": float(yes_tok.get("price", 0) if yes_tok else 0),
                            "no_price":  float(no_tok.get("price",  0) if no_tok  else 0),
                        }
        except Exception as e:
            logger.debug(f"Price check error for {market_id[:20]}: {e}")
        await asyncio.sleep(0.3)  # rate limit

    resolved = resolve_open_arbs(open_trades, current_prices)
    return len(resolved)


# ---------------------------------------------------------------------------
# Main poll cycle
# ---------------------------------------------------------------------------

async def run_cycle(session: aiohttp.ClientSession, scan_only: bool = False) -> list[ArbOpportunity]:
    """
    One full arb cycle:
      1. Load existing open trades
      2. Check for resolutions
      3. Scan all markets for new gaps
      4. Paper-execute new opportunities (if not already open)
      5. Save updated trade state
    Returns the list of found opportunities (for reporting).
    """
    # Load state
    all_trades = load_arb_trades()
    open_trades = [t for t in all_trades if t.get("status") == "open"]
    already_open_ids = {t["market_id"] for t in open_trades}

    # Check resolutions on existing open positions
    if open_trades:
        n_resolved = await check_resolutions(session, open_trades)
        if n_resolved:
            # Merge resolved trades back into all_trades and save
            resolved_ids = {t["market_id"] for t in open_trades if t.get("status") == "closed"}
            closed_updated = [t for t in open_trades if t["market_id"] in resolved_ids]
            unchanged = [t for t in all_trades if t["market_id"] not in resolved_ids or t.get("status") == "closed"]
            save_arb_trades(unchanged + closed_updated)
            logger.info(f"Resolved {n_resolved} arb trades")

    # Scan for new opportunities
    opportunities = await scan_markets(session)

    if scan_only:
        return opportunities

    # Paper-execute new opportunities
    n_executed = 0
    for opp in opportunities:
        if opp.market_id in already_open_ids:
            logger.debug(f"Already open: {opp.market_title[:50]}")
            continue
        if not opp.is_profitable_after_fees:
            logger.debug(
                f"Gap {opp.gap:.2%} is below fee threshold "
                f"({opp.expected_profit_pct:.2%} after fees) — skip"
            )
            continue

        record = paper_execute_arb(opp, BANKROLL)
        position_size = record["position_size"]
        net_profit = record["expected_profit_net"]

        already_open_ids.add(opp.market_id)
        n_executed += 1

        await _alert_opportunity(opp, position_size, net_profit)

    await _alert_scan_summary(0, opportunities, n_executed)  # 0 scanned count available from scan_markets log
    return opportunities


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(scan_only: bool = False) -> None:
    LOG_FILE.parent.mkdir(exist_ok=True)
    logger.add(
        str(LOG_FILE),
        rotation="20 MB",
        retention="14 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )

    logger.info(
        f"🤖 Arb Bot starting | mode={'scan-once' if scan_only else 'continuous'} | "
        f"bankroll=${BANKROLL:,.0f} | min_gap={MIN_GAP:.0%} | "
        f"min_liq=${MIN_LIQUIDITY:,.0f} | poll={POLL_INTERVAL}s"
    )

    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    headers = {"User-Agent": "Mozilla/5.0"}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        if scan_only:
            opportunities = await run_cycle(session, scan_only=True)
            # Print results to stdout for the user
            _print_scan_report(opportunities)
            return

        # Continuous loop
        cycle = 0
        while True:
            cycle += 1
            logger.info(f"=== Arb scan cycle #{cycle} ===")
            try:
                await run_cycle(session)
            except Exception as e:
                logger.error(f"Cycle error: {e}", exc_info=True)

            logger.info(f"💤 Sleeping {POLL_INTERVAL}s until next scan...")
            await asyncio.sleep(POLL_INTERVAL)


def _print_scan_report(opportunities: list[ArbOpportunity]) -> None:
    """Print a concise scan report to stdout."""
    print()
    print("=" * 72)
    print("  POLYMARKET ARB SCAN RESULTS")
    print("=" * 72)

    open_trades = [t for t in load_arb_trades() if t.get("status") == "open"]
    closed_trades = [t for t in load_arb_trades() if t.get("status") == "closed"]
    confirmed_pnl = sum(t.get("actual_pnl", 0) or 0 for t in closed_trades)
    expected_open_pnl = sum(t.get("expected_profit_net", 0) or 0 for t in open_trades)

    print(f"  Open arbs:          {len(open_trades)}")
    print(f"  Closed arbs:        {len(closed_trades)}")
    print(f"  Confirmed P&L:      ${confirmed_pnl:+,.2f}")
    print(f"  Expected open P&L:  ${expected_open_pnl:+,.2f}")
    print()

    if not opportunities:
        print("  No gaps > {:.0%} found in this scan.".format(MIN_GAP))
        print("=" * 72)
        return

    print(f"  {len(opportunities)} gap(s) found above {MIN_GAP:.0%}:")
    print()
    print(f"  {'#':<4} {'Gap':>6} {'After fees':>10} {'YES':>6} {'NO':>6} "
          f"{'Min Liq':>10} {'Vol24h':>10}  Market")
    print("  " + "-" * 100)

    for i, o in enumerate(opportunities[:5], 1):
        profit_flag = "✅" if o.is_profitable_after_fees else "❌"
        print(
            f"  {i:<4} {o.gap:>5.1%} {profit_flag} {o.expected_profit_pct:>8.1%} "
            f"  {o.yes_price:>5.3f}  {o.no_price:>5.3f} "
            f"  ${o.min_liquidity:>8,.0f}  ${o.volume_24h:>8,.0f}"
            f"  {o.market_title[:50]}"
        )

    if len(opportunities) > 5:
        print(f"\n  ... and {len(opportunities) - 5} more opportunities")

    print()
    print("=" * 72)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket paper arb bot")
    parser.add_argument(
        "--scan-once", action="store_true",
        help="Run one scan, print results, then exit"
    )
    args = parser.parse_args()
    asyncio.run(main(scan_only=args.scan_once))
