"""
Oil trader orchestrator.

Runs every 6 hours:
  1. Collect oil market context (data_collector)
  2. Get Claude probability estimate (signal_engine)
  3. Get current Polymarket oil market price
  4. Calculate edge: claude_prob - polymarket_price
  5. If edge > 8% AND confidence != "low" → paper trade (LIVE_TRADING = False)
  6. Log everything + Discord alert every run

Usage:
    python -m src.main_oil_trader
    python src/main_oil_trader.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# ── path so relative imports work when run directly
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

load_dotenv()

from src.polymarket.oil.data_collector import collect_oil_context
from src.polymarket.oil.signal_engine import get_oil_probability, kelly_position_size
from src.notifications.discord_webhook import DiscordWebhook

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LIVE_TRADING = False          # NEVER change to True without full audit
MIN_EDGE = 0.08               # Minimum 8% edge to place a trade
RUN_INTERVAL_HOURS = 6

# ---------------------------------------------------------------------------
# Live Polymarket markets  (3 simultaneous)
# ---------------------------------------------------------------------------

MARKETS = [
    {
        "name": "CL June $115",
        # $110 already resolved (price=1); $115 at 70.4% is the best active target
        "slug": "will-crude-oil-cl-hit-high-115-by-end-of-june-217-913-468-473",
        "type": "price_target",
        "outcome": "\u2191 $115",
        "url": "https://polymarket.com/event/cl-hit-jun-2026",
    },
    {
        "name": "WTI Daily Direction",
        # Slug is date-specific; resolved at runtime via _resolve_market_slugs()
        "slug": None,
        "_slug_template": "wti-up-or-down-on-{month}-{day}-{year}",
        "type": "daily_direction",
        "outcome": "Up",
        "url": "https://polymarket.com/event/wti-up-or-down",
    },
    {
        "name": "Strait of Hormuz May",
        "slug": "strait-of-hormuz-traffic-returns-to-normal-by-end-of-may",
        "type": "geopolitical",
        "outcome": "Yes",
        "url": "https://polymarket.com/event/strait-of-hormuz-normal-by-end-of-may",
    },
]


def _resolve_market_slugs() -> list[dict]:
    """Resolve any dynamic slugs (e.g. today's WTI daily market)."""
    now_utc = datetime.now(tz=timezone.utc)
    markets = []
    for m in MARKETS:
        if m.get("slug") is None and m.get("_slug_template"):
            slug = m["_slug_template"].format(
                month=now_utc.strftime("%b").lower(),  # e.g. "may"
                day=str(now_utc.day),                  # e.g. "5" (no zero-pad)
                year=now_utc.year,
            )
            m = {**m, "slug": slug}
        markets.append(m)
    return markets

# Hormuz keyword sets used for the geopolitical signal
_HORMUZ_KEYS    = {"hormuz", "strait", "blockade", "naval", "iran"}
_NEGATIVE_WORDS = {"block", "clos", "attack", "strike", "threat", "sanction", "halt", "seiz", "conflict"}

STATE_FILE = ROOT / "data" / "oil_trader_state.json"
LOG_FILE   = ROOT / "logs" / "oil_trader.log"

# Configure loguru to also write to file
logger.add(str(LOG_FILE), rotation="10 MB", retention="30 days", level="INFO")

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

DEFAULT_STATE = {
    "open_positions": [],
    "session_pnl": 0.0,
    "total_trades": 0,
    "bankroll": 500.0,
    "last_run": None,
}


def load_state() -> dict:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return DEFAULT_STATE.copy()


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Polymarket price fetchers — Gamma API
# ---------------------------------------------------------------------------

import socket as _socket

import aiohttp as _aiohttp
import aiohttp.abc as _aiohttp_abc


# Cloudflare IP 172.64.153.51 reliably serves gamma-api.polymarket.com.
# 104.18.34.205 is the DNS-resolved IP but drops TLS connections locally.
_GAMMA_HOST  = "gamma-api.polymarket.com"
_GAMMA_IP    = "172.64.153.51"


class _PolymarketResolver(_aiohttp_abc.AbstractResolver):
    """Force gamma-api.polymarket.com to the known-working Cloudflare IP."""

    async def resolve(self, hostname: str, port: int = 0, family: int = _socket.AF_INET) -> list[dict]:
        if hostname == _GAMMA_HOST:
            return [{"hostname": hostname, "host": _GAMMA_IP, "port": port,
                     "family": family, "proto": 0, "flags": 0}]
        # Fall back to system DNS for all other hosts
        return await _aiohttp.DefaultResolver().resolve(hostname, port, family)

    async def close(self) -> None:
        pass


_GAMMA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://polymarket.com/",
}


async def _fetch_market_via_session(session: "_aiohttp.ClientSession", slug: str) -> tuple[float | None, str | None, str | None]:
    """Fetch a single market using an existing session (connection reuse).
    Returns (yes_price, question, end_date_iso).
    """
    import json as _json
    url = f"https://{_GAMMA_HOST}/markets?slug={slug}"
    try:
        async with session.get(url, timeout=_aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.warning(f"[Gamma] HTTP {r.status} for slug={slug}")
                return None, None, None
            data = await r.json(content_type=None)

        if not data:
            logger.warning(f"[Gamma] No results for slug={slug}")
            return None, None, None

        market = data[0]
        prices   = market.get("outcomePrices", [])
        question = market.get("question", slug)
        end_date = market.get("endDate") or market.get("endDateIso")
        if isinstance(prices, str):
            prices = _json.loads(prices)
        if not prices:
            logger.warning(f"[Gamma] No outcomePrices for slug={slug}")
            return None, None, end_date

        yes_price = float(prices[0])
        logger.info(f"[Gamma] {slug}: YES={yes_price:.3f} | '{question}'")
        return yes_price, question, end_date

    except Exception as e:
        logger.warning(f"[Gamma] Error fetching slug={slug}: {e}")
        return None, None, None


async def get_market_price(slug: str) -> tuple[float | None, str | None, str | None]:
    """
    Fetch the current YES price for a single Polymarket market.
    Routes gamma-api.polymarket.com to the known-working Cloudflare IP.
    Returns (yes_price, question, end_date_iso).
    """
    connector = _aiohttp.TCPConnector(resolver=_PolymarketResolver())
    async with _aiohttp.ClientSession(connector=connector, headers=_GAMMA_HEADERS) as s:
        return await _fetch_market_via_session(s, slug)


async def get_all_market_prices() -> dict[str, dict]:
    """
    Fetch all markets sequentially through a SINGLE shared session.
    Sequential (not concurrent) to avoid Cloudflare connection-reset on burst.
    Returns {slug: {"price": float|None, "question": str|None, "end_date": str|None}}
    """
    resolved = _resolve_market_slugs()
    connector = _aiohttp.TCPConnector(resolver=_PolymarketResolver())
    results: dict[str, dict] = {}
    async with _aiohttp.ClientSession(connector=connector, headers=_GAMMA_HEADERS) as session:
        for m in resolved:
            slug = m["slug"]
            price, question, end_date = await _fetch_market_via_session(session, slug)
            results[slug] = {"price": price, "question": question, "end_date": end_date}
    return results


def _is_near_expiry(end_date_str: str | None, hours_threshold: float = 2.0) -> bool:
    """Return True if the market expires within `hours_threshold` hours from now."""
    if not end_date_str:
        return False
    try:
        from dateutil import parser as _dp
        end_dt = _dp.parse(end_date_str)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        remaining = (end_dt - datetime.now(tz=timezone.utc)).total_seconds() / 3600
        return remaining <= hours_threshold
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Hormuz negative headline counter (geopolitical signal)
# ---------------------------------------------------------------------------

def _hormuz_negative_count(context: dict) -> int:
    """Count Hormuz headlines with negative/conflict language across GDELT + RSS."""
    count = 0
    all_titles = (
        [h.get("title", "") for h in context.get("gdelt_headlines", [])]
        + [h.get("title", "") for h in context.get("rss_headlines", [])]
    )
    for title in all_titles:
        t = title.lower()
        if any(kw in t for kw in _HORMUZ_KEYS) and any(neg in t for neg in _NEGATIVE_WORDS):
            count += 1
    return count


# ---------------------------------------------------------------------------
# Per-type signal evaluator
# ---------------------------------------------------------------------------

def evaluate_signal(
    market: dict,
    yes_price: float,
    question: str,
    signal: dict,
    context: dict,
    bankroll: float,
    end_date: str | None = None,
) -> dict:
    """
    Evaluate a single market and return a decision dict.

    Market types:
      price_target    — compare Groq oil direction to implied price-target probability
      daily_direction — direct Groq prob_up vs market Up price
      geopolitical    — use Hormuz headline count as the signal
    """
    mtype      = market["type"]
    groq_up    = signal["prob_up"]
    groq_down  = signal["prob_down"]
    confidence = signal["confidence"]

    traded, bet, direction, edge, note = False, 0.0, "\u2014", None, ""

    # ── Guard 1: skip near-resolved markets (price < 2% or > 98%)
    # A near-zero YES price means the market has already resolved NO, not that
    # there is edge in buying YES. Do not treat resolution as mispricing.
    if yes_price < 0.02 or yes_price > 0.98:
        note = f"market_near_resolved (YES={yes_price:.4f}) — skipped"
        logger.info(f"[{market['name']}] SKIP {note}")
        return {
            "label": market["name"], "slug": market["slug"], "type": mtype,
            "question": question, "market_price": yes_price,
            "edge": None, "direction": "\u2014", "bet_size": 0,
            "traded": False, "note": note,
        }

    # ── Guard 2: skip daily direction markets within 2 hours of expiry
    if mtype == "daily_direction" and _is_near_expiry(end_date, hours_threshold=2.0):
        note = f"market_near_expiry (end={end_date}) — skipped"
        logger.info(f"[{market['name']}] SKIP {note}")
        return {
            "label": market["name"], "slug": market["slug"], "type": mtype,
            "question": question, "market_price": yes_price,
            "edge": None, "direction": "\u2014", "bet_size": 0,
            "traded": False, "note": note,
        }

    if mtype == "daily_direction":
        # Compute edge in both directions independently.
        # up_edge > 0 → we think YES is underpriced
        # down_edge > 0 → we think NO is underpriced
        up_edge   = groq_up - yes_price
        down_edge = (1.0 - groq_up) - (1.0 - yes_price)   # == yes_price - groq_up
        note = (
            f"P(up)={groq_up:.0%} | market={yes_price:.3f} "
            f"| up_edge={up_edge:+.3f} | down_edge={down_edge:+.3f}"
        )
        if confidence != "low":
            if up_edge >= MIN_EDGE:
                direction = "YES"
                edge = up_edge
                bet = kelly_position_size(groq_up, yes_price, bankroll)
                note += " | bet=YES"
                if bet > 0:
                    traded = True
            elif down_edge >= MIN_EDGE:
                direction = "NO"
                edge = down_edge
                bet = kelly_position_size(groq_down, 1.0 - yes_price, bankroll)
                note += " | bet=NO"
                if bet > 0:
                    traded = True
            else:
                note += " | no_edge"
        else:
            note += " | low_confidence"

    elif mtype == "price_target":
        # Groq bullish + market underpriced → YES; Groq bearish + market overpriced → NO
        up_edge  = groq_up - yes_price
        no_edge  = (1.0 - groq_up) - (1.0 - yes_price)
        note = (
            f"P(up)={groq_up:.0%} | market={yes_price:.3f} "
            f"| up_edge={up_edge:+.3f} | down_edge={no_edge:+.3f}"
        )
        if confidence != "low":
            if groq_up >= 0.50 and up_edge >= MIN_EDGE:
                direction = "YES"
                edge = up_edge
                bet = kelly_position_size(groq_up, yes_price, bankroll)
                note += " | bet=YES"
                if bet > 0:
                    traded = True
            elif groq_up < 0.50 and no_edge >= MIN_EDGE:
                direction = "NO"
                edge = no_edge
                bet = kelly_position_size(1.0 - groq_up, 1.0 - yes_price, bankroll)
                note += " | bet=NO"
                if bet > 0:
                    traded = True
            else:
                note += " | no_edge"
        else:
            note += " | low_confidence"

    elif mtype == "geopolitical":
        # >3 negative Hormuz headlines → tensions high → Hormuz won't normalise → buy NO
        # 0 negative headlines → potential calm → buy YES (conservative 35% floor)
        hormuz_neg = _hormuz_negative_count(context)
        no_price   = 1.0 - yes_price
        note = f"Hormuz negative headlines={hormuz_neg}"
        if hormuz_neg >= 3:
            our_no_prob = min(0.92, 0.55 + hormuz_neg * 0.06)
            edge = our_no_prob - no_price
            note += f" | our_NO_prob={our_no_prob:.0%} | market_NO={no_price:.3f} | edge={edge:+.3f}"
            if edge >= MIN_EDGE and confidence != "low":
                direction = "NO"
                bet = kelly_position_size(our_no_prob, no_price, bankroll)
                note += " | bet=NO"
                if bet > 0:
                    traded = True
            else:
                note += " | no_edge"
        elif hormuz_neg == 0:
            our_yes_prob = 0.35
            edge = our_yes_prob - yes_price
            note += f" → potential YES (our prob={our_yes_prob:.0%})"
            if edge >= MIN_EDGE and confidence != "low":
                direction = "YES"
                bet = kelly_position_size(our_yes_prob, yes_price, bankroll)
                note += " | bet=YES"
                if bet > 0:
                    traded = True
            else:
                note += " | no_edge"
        else:
            edge = 0.0
            note += " → no edge"

    return {
        "label":        market["name"],
        "slug":         market["slug"],
        "type":         mtype,
        "question":     question,
        "market_price": yes_price,
        "edge":         round(edge, 4) if edge is not None else None,
        "direction":    direction,
        "bet_size":     bet,
        "traded":       traded,
        "note":         note,
    }


# ---------------------------------------------------------------------------
# Discord alert
# ---------------------------------------------------------------------------

async def send_discord_alert(
    discord: DiscordWebhook,
    context: dict,
    signal: dict,
    raw_prices: dict,          # {slug: {"price", "question"}} — not used directly here
    decisions: list[dict],
) -> None:
    wti = context.get("wti_price", "n/a")
    chg = context.get("wti_5d_change_pct", "n/a")
    prob_up     = signal.get("prob_up", 0)
    confidence  = signal.get("confidence", "?")
    reasoning   = signal.get("reasoning", "")
    key_factors = signal.get("key_factors", [])

    any_traded = any(d.get("traded") for d in decisions)
    color = 0x00FF00 if any_traded else 0xFF9900

    fields = [
        {"name": "WTI Price",  "value": f"${wti} ({chg:+.1f}% 5d)" if isinstance(chg, float) else f"${wti}", "inline": True},
        {"name": "Groq P(up)", "value": f"{prob_up:.0%} [{confidence}]", "inline": True},
        {"name": "\u200b",     "value": "\u200b",                                                              "inline": True},
    ]

    for d in decisions:
        label     = d.get("label", "")
        mkt_price = d.get("market_price")
        edge      = d.get("edge")
        direction = d.get("direction", "\u2014")
        bet_size  = d.get("bet_size", 0)
        traded    = d.get("traded", False)
        note      = d.get("note", "")
        price_str = f"{mkt_price:.3f}" if mkt_price is not None else "N/A"
        edge_str  = f"{edge*100:.1f}%" if edge is not None else "N/A"
        action    = f"PAPER {direction} ${bet_size:.2f}" if traded else "no trade"
        fields += [
            {"name": f"{label} — Price",  "value": price_str, "inline": True},
            {"name": f"{label} — Edge",   "value": edge_str,  "inline": True},
            {"name": f"{label} — Action", "value": action,    "inline": True},
        ]

    fields += [
        {"name": "Key Factors", "value": "\n".join(f"• {f}" for f in key_factors) or "—", "inline": False},
        {"name": "Reasoning",   "value": reasoning[:1000] or "—",                          "inline": False},
    ]

    embed = {
        "title": "\U0001f6e2\ufe0f Oil Trader Signal",
        "color": color,
        "fields": fields,
        "footer": {"text": f"LIVE_TRADING={LIVE_TRADING} | {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
    }
    await discord.send_message(embed=embed)


# ---------------------------------------------------------------------------
# Single run cycle
# ---------------------------------------------------------------------------

async def _resolve_open_positions(state: dict, price_map: dict) -> None:
    """Close any open positions whose market has reached resolution (price ~0 or ~1)."""
    from src.tracking.trade_logger import close_trade
    still_open = []
    for pos in state.get("open_positions", []):
        market_q = pos.get("market", "")
        direction = pos.get("direction", "")
        entry_price = pos.get("market_price", 0.5)
        trade_id = pos.get("_trade_id")

        # Find current price by matching question text in the price map
        current_price = None
        for slug_data in price_map.values():
            if slug_data.get("question") == market_q:
                current_price = slug_data.get("price")
                break

        if current_price is None:
            # Market not found in this cycle's price fetch — keep open
            still_open.append(pos)
            continue

        resolved_win = current_price >= 0.99
        resolved_loss = current_price <= 0.01
        if resolved_win or resolved_loss:
            is_win = resolved_win
            exit_price = 1.0 if is_win else 0.0
            close_trade(
                trade_id=trade_id,
                exit_price=exit_price,
                is_win=is_win,
                resolved_via="gamma_api",
                entry_price=entry_price,
                size_usd=pos.get("bet_size", 0.0),
            )
            logger.info(
                f"[Oil] Closed resolved position: {market_q[:60]} "
                f"{'WIN' if is_win else 'LOSS'} @ {exit_price}"
            )
        else:
            still_open.append(pos)

    state["open_positions"] = still_open


async def run_cycle(state: dict, discord: DiscordWebhook) -> dict:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"=== Oil Trader cycle starting at {now} ===")

    # 1 — Collect data
    logger.info("Step 1: Collecting market data…")
    context = await collect_oil_context()
    logger.info(f"Context: WTI=${context.get('wti_price')} | 5d={context.get('wti_5d_change_pct')}%")

    # 2 — Claude probability
    logger.info("Step 2: Requesting Claude probability estimate…")
    signal = await get_oil_probability(context)
    logger.info(f"Signal: prob_up={signal['prob_up']:.0%} confidence={signal['confidence']}")
    logger.info(f"Reasoning: {signal.get('reasoning', '')}")

    # 3 — Fetch all 3 Polymarket prices concurrently
    logger.info("Step 3: Fetching Polymarket prices for 3 markets…")
    raw_prices = await get_all_market_prices()
    resolved_markets = _resolve_market_slugs()

    # Resolve any open positions that have settled (price ~0 or ~1)
    await _resolve_open_positions(state, raw_prices)

    # 4 — Evaluate each market with its specific signal logic
    decisions: list[dict] = []
    for market in resolved_markets:
        slug   = market["slug"]
        entry  = raw_prices.get(slug, {"price": None, "question": None})
        price  = entry["price"]
        question = entry["question"] or slug

        if price is None:
            logger.warning(f"[{market['name']}] Market not found — skipping")
            decisions.append({
                "label": market["name"], "slug": slug, "type": market["type"],
                "question": slug, "market_price": None,
                "edge": None, "direction": "\u2014", "bet_size": 0,
                "traded": False, "note": "market not found",
            })
            continue

        end_date = entry.get("end_date")
        dec = evaluate_signal(market, price, question, signal, context, state["bankroll"], end_date)
        logger.info(
            f"[{market['name']}] price={price:.3f} edge={dec['edge']} "
            f"dir={dec['direction']} bet=${dec['bet_size']:.2f} | {dec['note']}"
        )

        # ── Dedup guard: skip if we already have an open position for this market+direction
        market_key = f"{question}::{dec['direction']}"
        already_open = any(
            p.get("market") == question and p.get("direction") == dec["direction"]
            for p in state.get("open_positions", [])
        )
        if dec["traded"] and already_open:
            logger.info(f"[{market['name']}] SKIP dedup — already open: {market_key}")
            dec["traded"] = False
            dec["note"] += " | SKIP_dedup"

        if dec["traded"]:
            state["open_positions"].append({
                "timestamp": now,
                "market": question,
                "type": market["type"],
                "direction": dec["direction"],
                "market_price": price,
                "groq_prob": signal["prob_up"],
                "edge": dec["edge"],
                "bet_size": dec["bet_size"],
                "paper": not LIVE_TRADING,
            })
            state["total_trades"] += 1
            logger.info(
                f"[{market['name']}] PAPER {dec['direction']} "
                f"${dec['bet_size']:.2f} (edge={dec['edge']})"
            )
            # Log to unified trade log (entry recorded; exit resolved next cycle)
            from src.tracking.trade_logger import open_trade
            trade_id = open_trade(
                bot="oil",
                market=question,
                outcome=dec["direction"],
                entry_price=price,
                size_usd=dec["bet_size"],
            )
            if trade_id:
                state["open_positions"][-1]["_trade_id"] = trade_id

        decisions.append(dec)

    state["last_run"] = now
    save_state(state)

    # 6 — Discord alert every run
    await send_discord_alert(discord, context, signal, raw_prices, decisions)

    return {
        "context": context,
        "signal": signal,
        "raw_prices": raw_prices,
        "decisions": decisions,
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main() -> None:
    state = load_state()
    discord = DiscordWebhook()

    logger.info(f"Oil Trader started. LIVE_TRADING={LIVE_TRADING} | Bankroll=${state['bankroll']}")

    while True:
        try:
            result = await run_cycle(state, discord)

            # Pretty-print summary for operator visibility
            print("\n" + "="*60)
            print("OIL TRADER — CYCLE SUMMARY")
            print("="*60)
            ctx = result["context"]
            sig = result["signal"]
            chg = ctx.get('wti_5d_change_pct')
            chg_str = f"{chg:+.1f}% 5d" if isinstance(chg, float) else ""
            print(f"WTI Price:  ${ctx.get('wti_price')} {chg_str}")
            print(f"Groq P(up): {sig['prob_up']:.0%} [{sig['confidence']}]")
            for d in result["decisions"]:
                mp    = f"{d['market_price']:.3f}" if d["market_price"] is not None else "N/A"
                edg   = f"{d['edge']*100:.1f}%" if d["edge"] is not None else "N/A"
                trade = f"{d['direction']} ${d['bet_size']:.2f}" if d["traded"] else "no trade"
                print(f"  [{d['label'][:20]:<20s}] type={d.get('type','?'):<17s} price={mp}  edge={edg:>7s}  → {trade}")
                if d.get('note'):
                    print(f"    note: {d['note']}")
            print(f"Reasoning:  {sig.get('reasoning', '')}"[:160])
            print("="*60 + "\n")

        except Exception as e:
            logger.error(f"Cycle error: {e}", exc_info=True)

        logger.info(f"Sleeping {RUN_INTERVAL_HOURS}h until next cycle…")
        await asyncio.sleep(RUN_INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-once", action="store_true", help="Run one cycle and exit")
    args, _ = ap.parse_known_args()
    if args.run_once:
        async def _run_once() -> None:
            state = load_state()
            discord = DiscordWebhook()
            result = await run_cycle(state, discord)
            ctx = result["context"]
            sig = result["signal"]
            chg = ctx.get("wti_5d_change_pct")
            chg_str = f"{chg:+.1f}% 5d" if isinstance(chg, float) else ""
            print("\n" + "="*60)
            print("OIL TRADER — SINGLE CYCLE RESULT")
            print("="*60)
            print(f"WTI Price:  ${ctx.get('wti_price')} {chg_str}")
            print(f"Groq P(up): {sig['prob_up']:.0%} [{sig['confidence']}]")
            print(f"Reasoning:  {sig.get('reasoning', '')[:200]}")
            print()
            for d in result["decisions"]:
                mp    = f"{d['market_price']:.3f}" if d["market_price"] is not None else "N/A"
                edg   = f"{d['edge']*100:+.1f}%" if d["edge"] is not None else "N/A"
                trade = f"→ PAPER {d['direction']} ${d['bet_size']:.2f}" if d["traded"] else "→ no trade"
                print(f"  [{d['label'][:22]:<22s}]  price={mp}  edge={edg:>7s}  {trade}")
                if d.get("note"):
                    print(f"    {d['note']}")
            print("="*60)
        asyncio.run(_run_once())
    else:
        asyncio.run(main())
