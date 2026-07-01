"""
Consolidated hourly heartbeat → Discord.

One lightweight process. Every HEARTBEAT_INTERVAL seconds it:
  1. Counts live bot processes via pgrep
  2. Reads data/win_loss_stats.json for trade tallies
  3. Posts a single Discord embed listing every bot's status

Run with: python3 -m src.main_heartbeat
"""

import asyncio
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.notifications.discord_webhook import get_discord_client


HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "3600"))
STATS_FILE = Path("data/win_loss_stats.json")
HISTORY_FILE = Path("data/win_loss_history.jsonl")

# Module name → display label
# KrakenPolyArb and Copycat (legacy) replaced by Top Trader Follower.
# ETH MM disabled (no in-range markets). Weather Bot retired (7.6% win rate).
BOTS = [
    ("src.main",                "BTC Bot"),
    ("src.main_polymarket",     "Polymarket Fast"),
    ("src.main_kakashi",        "Kakashi Bot"),
    ("src.main_webhook",        "TradingView Webhook"),
]


def _is_running(module: str) -> bool:
    try:
        out = subprocess.run(
            ["pgrep", "-f", f"python3 -m {module}\\b"],
            capture_output=True, text=True, timeout=5,
        )
        return bool(out.stdout.strip())
    except Exception as exc:
        logger.debug(f"_is_running check failed for {module}: {exc}")
        return False


# Map bot module → tracker bot_name field (as recorded in win_loss_history.jsonl)
BOT_TRACKER_NAMES = {
    "BTC Bot": ["BTC", "BTC Bot", "btc"],
    "Polymarket Fast": ["Polymarket", "PolymarketFast", "poly"],
    "Kakashi Bot": ["KakashiBot", "Kakashi Bot"],
    "TradingView Webhook": ["Webhook", "TradingView"],
}


def _load_history() -> list:
    """Read raw trade history (jsonl). Source of truth for session tallies."""
    if not HISTORY_FILE.exists():
        return []
    try:
        out = []
        with HISTORY_FILE.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception as exc:
                    logger.debug(f"Skipping malformed trade history line: {exc}")
                    pass
        return out
    except Exception as e:
        logger.warning(f"Could not read {HISTORY_FILE}: {e}")
        return []


def _aggregate(trades: list, name_aliases: list) -> tuple:
    """Return (wins, losses, dollar_pnl) for trades whose bot_name matches any alias."""
    wins = losses = 0
    total_pnl = 0.0
    aliases_lower = [a.lower() for a in name_aliases]
    for tr in trades:
        bn = str(tr.get("bot_name", "")).lower()
        if not any(a in bn or bn in a for a in aliases_lower):
            continue
        if tr.get("is_win"):
            wins += 1
        else:
            losses += 1
        # tracker stores pnl_pct only; approximate $ from $100 paper notional fallback
        total_pnl += float(tr.get("pnl_pct", 0)) * 100.0
    return wins, losses, total_pnl


def _bot_stats_line(label: str, trades: list) -> str:
    aliases = BOT_TRACKER_NAMES.get(label, [label])
    wins, losses, pnl = _aggregate(trades, aliases)
    total = wins + losses
    if total == 0:
        return "no trades yet"
    wr = wins / total * 100
    sign = "+" if pnl >= 0 else ""
    return f"{total} trades · {wins}W/{losses}L · {wr:.0f}% WR · {sign}${pnl:.2f}"


async def send_heartbeat():
    discord = get_discord_client()
    if not discord.enabled:
        logger.warning("Discord webhook not configured — heartbeat suppressed")
        return

    trades = _load_history()
    fields = []
    alive_count = 0
    for module, label in BOTS:
        running = _is_running(module)
        if running:
            alive_count += 1
        status_emoji = "🟢" if running else "🔴"
        line = _bot_stats_line(label, trades)
        fields.append({
            "name": f"{status_emoji} {label}",
            "value": line,
            "inline": False,
        })

    grand_wins = sum(1 for t in trades if t.get("is_win"))
    grand_losses = sum(1 for t in trades if not t.get("is_win"))
    grand_pnl = sum(float(t.get("pnl_pct", 0)) * 100.0 for t in trades)
    grand_total = grand_wins + grand_losses
    grand_wr = (grand_wins / grand_total * 100) if grand_total else 0.0

    color = 0x00C853 if grand_pnl >= 0 and alive_count == len(BOTS) else (
        0xFFA000 if alive_count < len(BOTS) else 0xFF1744
    )

    embed = {
        "title": f"📡 Hourly Status — {alive_count}/{len(BOTS)} bots alive",
        "description": (
            f"All-time: **{grand_total} trades · {grand_wins}W/{grand_losses}L · "
            f"{grand_wr:.1f}% WR · {'+' if grand_pnl >= 0 else ''}${grand_pnl:.2f}**"
        ),
        "color": color,
        "fields": fields,
        "footer": {"text": f"Heartbeat | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        await discord.send_message(embed=embed)
        logger.info(f"📨 Heartbeat sent ({alive_count}/{len(BOTS)} alive, ${grand_pnl:+.2f} all-time)")
    except Exception as e:
        logger.warning(f"Heartbeat send failed: {e}")


async def main():
    logger.info(f"🚀 Heartbeat bot started — interval {HEARTBEAT_INTERVAL}s")

    # Send immediate startup heartbeat so user sees it now
    await send_heartbeat()

    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await send_heartbeat()
        except asyncio.CancelledError:
            logger.info("Heartbeat cancelled")
            break
        except Exception as e:
            logger.warning(f"Heartbeat loop error: {e}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
