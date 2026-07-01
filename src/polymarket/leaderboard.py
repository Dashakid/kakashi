"""
Polymarket Wallet Discovery + Scoring.

Polymarket has no weekly leaderboard API. The best available signal is the
positions endpoint's cumulative cashPnL — wallets with sustained positive PnL
across many positions are genuine edge, not lucky one-offs.

Discovery (two nets, cast in parallel):
  Net 1 — Breadth scan: all available pages of the trades feed, filtering out
           HFT crypto-price markets that flood the feed with noise.
  Net 2 — Whale scan: same feed, min trade size $500. Big money = sharper.

Scoring pipeline:
  - Fetch each wallet's positions from data-api (cashPnL, currentValue)
  - Filter: ≥ 5 positions, net cashPnL ≥ $100
  - Score:  sqrt(cashPnL) × 2× boost if active in last 7 days
  - Return top N sorted by score

Known sharp wallets (confirmed from live observation) are always seeded in
so they are never dropped even during low-traffic discovery windows.
"""

import asyncio
import socket as _socket
import time
from typing import Dict, List, Optional, Tuple

import aiohttp
import aiohttp.abc as _aiohttp_abc
from loguru import logger

# ---------------------------------------------------------------------------
# DNS bypass — Polymarket APIs resolve to 127.0.0.1 locally due to split-DNS.
# Hardcode known Cloudflare IPs so connections work without VPN.
# ---------------------------------------------------------------------------
_DNS_OVERRIDES = {
    "data-api.polymarket.com": "172.64.153.51",
    "gamma-api.polymarket.com": "172.64.153.51",
}

class _PolymarketResolver(_aiohttp_abc.AbstractResolver):
    async def resolve(self, hostname: str, port: int = 0, family: int = _socket.AF_INET):
        ip = _DNS_OVERRIDES.get(hostname)
        if ip:
            return [{"hostname": hostname, "host": ip, "port": port,
                     "family": family, "proto": 0, "flags": 0}]
        return await aiohttp.DefaultResolver().resolve(hostname, port, family)
    async def close(self) -> None:
        pass


_POLY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://polymarket.com/",
}


def make_session() -> aiohttp.ClientSession:
    """Create an aiohttp session with the Polymarket DNS bypass applied."""
    connector = aiohttp.TCPConnector(resolver=_PolymarketResolver())
    return aiohttp.ClientSession(connector=connector, headers=_POLY_HEADERS)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOP_N           = 50
PAGE_SIZE       = 1_000       # Polymarket API returns max 1000 per page
MAX_PAGES       = 5           # API currently has ~5 pages of trade history
WHALE_MIN_USD   = 500
WHALE_PAGES     = 3

MIN_PROFIT_USD  = 100.0       # net cashPnL across visible positions
MIN_POSITIONS   = 5

# Activity window for the score boost — 7 days, not 24h
_7D             = 7 * 86_400
MIN_WEEKLY_TRADES  = 2
MIN_WEEKLY_VOLUME  = 30.0

# HFT slug keywords — these markets flood the feed and attract noise traders
_HFT_KEYWORDS = (
    "up-or-down", "updown", "btc-up", "eth-up",
    "5-minute", "1-minute", "hourly-", "crypto-price",
    "will-btc", "will-eth", "bitcoin-up", "bitcoin-down",
)

# Confirmed sharp wallets — always seeded regardless of trade-feed discovery.
# Add addresses here as the bot identifies them through live WalletScore tracking.
SEED_WALLETS: List[str] = [
    "0x6e1d5040d0f81d91a59f41a03e39cd3cd5d6346",  # $30k cashPnL, 80% WR, weekly active
    "0x492442eab586c73954f4c659e0b35c54da2b2b2a",  # $121k cashPnL, 33% WR, whale
    "0xec981ed70ae63a5b81a3a9f3b33c1e59e6e4b28d",  # $19k cashPnL, 75% WR
    "0x230287e270f04a4e0ef97d75d8cd12db3f64bdf3",  # $21k cashPnL, 40% WR
]


# ---------------------------------------------------------------------------
# Step 1: discover wallets from trade feed
# ---------------------------------------------------------------------------

async def _fetch_page(
    session: aiohttp.ClientSession,
    offset: int,
    min_amount: int = 10,
) -> List[dict]:
    try:
        async with session.get(
            "https://data-api.polymarket.com/trades",
            params={"limit": PAGE_SIZE, "offset": offset, "minAmount": min_amount},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            return await r.json() if r.status == 200 else []
    except Exception as exc:
        logger.debug(f"Trade page offset={offset}: {exc}")
        return []


async def _discover_wallets(
    session: aiohttp.ClientSession,
) -> Tuple[List[str], Dict[str, Dict]]:
    now    = time.time()
    cutoff = now - _7D

    breadth = [_fetch_page(session, i * PAGE_SIZE)              for i in range(MAX_PAGES)]
    whales  = [_fetch_page(session, i * PAGE_SIZE, WHALE_MIN_USD) for i in range(WHALE_PAGES)]

    all_pages = await asyncio.gather(*breadth, *whales)

    seen_hashes: set = set()
    trades: List[dict] = []
    for page in all_pages:
        for t in page:
            key = t.get("transactionHash") or t.get("id") or id(t)
            if key in seen_hashes:
                continue
            seen_hashes.add(key)
            slug  = (t.get("slug")  or t.get("eventSlug") or "").lower()
            title = (t.get("title") or "").lower()
            if any(kw in slug or kw in title for kw in _HFT_KEYWORDS):
                continue
            trades.append(t)

    logger.info(f"🔍 Trade scan: {len(trades)} real-market trades (HFT filtered)")

    volume:   Dict[str, float] = {}
    activity: Dict[str, Dict]  = {}

    for t in trades:
        addr = (t.get("proxyWallet") or "").lower()
        if not addr or not addr.startswith("0x"):
            continue

        size = float(t.get("usdcSize") or 0) or float(t.get("size") or 0) * float(t.get("price") or 1)
        ts   = float(t.get("timestamp") or 0)

        volume[addr] = volume.get(addr, 0.0) + size

        if addr not in activity:
            activity[addr] = {"weekly_trades": 0, "weekly_volume": 0.0, "last_trade_ts": 0}
        if ts > activity[addr]["last_trade_ts"]:
            activity[addr]["last_trade_ts"] = ts
        if ts >= cutoff:
            activity[addr]["weekly_trades"]  += 1
            activity[addr]["weekly_volume"]  += size

    # Merge confirmed seed wallets into the candidate pool
    for addr in SEED_WALLETS:
        addr = addr.lower()
        if addr not in volume:
            volume[addr] = 0.0
        if addr not in activity:
            activity[addr] = {"weekly_trades": 0, "weekly_volume": 0.0, "last_trade_ts": 0}

    sorted_wallets = sorted(volume, key=lambda a: volume[a], reverse=True)
    weekly_active  = sum(1 for a in activity.values() if a["weekly_trades"] >= MIN_WEEKLY_TRADES)
    logger.info(f"   {len(sorted_wallets)} unique wallets | {weekly_active} active in last 7 days")
    return sorted_wallets, activity


# ---------------------------------------------------------------------------
# Step 2: score each wallet from positions API
# ---------------------------------------------------------------------------

async def _score_wallet(session: aiohttp.ClientSession, address: str) -> Optional[Dict]:
    try:
        async with session.get(
            "https://data-api.polymarket.com/positions",
            params={"user": address},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None
            positions = await r.json()
    except Exception:
        return None

    if not positions:
        return None

    cash_pnl = portfolio_value = 0.0
    win_positions = 0

    for pos in positions:
        cash   = float(pos.get("cashPnl")      or 0)
        curval = float(pos.get("currentValue") or 0)
        cash_pnl        += cash
        portfolio_value += curval
        if cash > 0:
            win_positions += 1

    n = len(positions)
    return {
        "address":         address,
        "total_pnl":       cash_pnl,
        "portfolio_value": portfolio_value,
        "win_positions":   win_positions,
        "total_positions": n,
        "win_rate":        win_positions / n if n else 0.0,
    }


# ---------------------------------------------------------------------------
# Step 3: filter, score, rank
# ---------------------------------------------------------------------------

async def get_top_wallets(
    session: aiohttp.ClientSession,
    top_n: int = TOP_N,
    min_profit: float = MIN_PROFIT_USD,
    seed_wallets: Optional[List[str]] = None,
    concurrency: int = 20,
) -> List[Dict]:
    discovered, activity = await _discover_wallets(session)

    # Merge any extra caller-provided seeds
    extra = [a.lower() for a in (seed_wallets or [])]
    all_wallets = list(dict.fromkeys(extra + discovered))

    logger.info(f"📊 Scoring {len(all_wallets)} wallets…")

    sem = asyncio.Semaphore(concurrency)

    async def score(addr: str) -> Optional[Dict]:
        async with sem:
            return await _score_wallet(session, addr)

    results = await asyncio.gather(*[score(a) for a in all_wallets])

    scored = []
    for r in results:
        if r is None:
            continue
        if r["total_positions"] < MIN_POSITIONS:
            continue
        if r["total_pnl"] < min_profit:
            continue

        addr = r["address"]
        act  = activity.get(addr, {"weekly_trades": 0, "weekly_volume": 0.0, "last_trade_ts": 0})
        r["weekly_trades"]   = act["weekly_trades"]
        r["weekly_volume"]   = act["weekly_volume"]
        r["last_trade_ts"]   = act["last_trade_ts"]
        r["is_weekly_active"] = (
            act["weekly_trades"]  >= MIN_WEEKLY_TRADES
            and act["weekly_volume"] >= MIN_WEEKLY_VOLUME
        )

        boost      = 2.0 if r["is_weekly_active"] else 1.0
        r["score"] = (max(r["total_pnl"], 0) ** 0.5) * boost
        scored.append(r)

    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[:top_n]

    logger.info(
        f"✅ {len(top)} wallets qualified "
        f"(from {len(scored)} passing filters | ≥{MIN_POSITIONS} positions, cashPnL ≥${min_profit:,.0f})"
    )
    for rank, w in enumerate(top[:10], 1):
        logger.info(
            f"  #{rank:>2} {w['address'][:12]}…  "
            f"WR={w['win_rate']:.0%} ({w['win_positions']}/{w['total_positions']})  "
            f"cashPnL=${w['total_pnl']:+,.0f}  "
            f"7d: {w['weekly_trades']} trades / ${w['weekly_volume']:.0f}"
            f"{'  ★ active' if w['is_weekly_active'] else ''}"
        )

    return top


async def get_leaderboard_wallets(
    session: aiohttp.ClientSession,
    top_n: int = TOP_N,
    min_profit: float = MIN_PROFIT_USD,
    seed_wallets: Optional[List[str]] = None,
) -> List[Dict]:
    """Returns wallet dicts in the format used by CopycatBot."""
    top = await get_top_wallets(
        session, top_n=top_n, min_profit=min_profit, seed_wallets=seed_wallets
    )
    return [
        {
            "address":         w["address"],
            "label":           f"Trader_{w['address'][2:8].upper()}",
            "_pnl":            w["total_pnl"],
            "_win_rate":       w["win_rate"],
            "_weekly_trades":  w["weekly_trades"],
            "_weekly_volume":  w["weekly_volume"],
            "_score":          w["score"],
        }
        for w in top
    ]


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

async def _test():
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30, connect=5)
    ) as session:
        wallets = await get_leaderboard_wallets(session, top_n=20)
        print(f"\nTop {len(wallets)} wallets:")
        for w in wallets:
            active = "★" if w["_weekly_trades"] >= MIN_WEEKLY_TRADES else " "
            print(
                f"  {active} {w['label']} ({w['address'][:14]}…)  "
                f"WR={w['_win_rate']:.0%}  cashPnL=${w['_pnl']:+,.0f}  "
                f"7d: {w['_weekly_trades']} trades/${w['_weekly_volume']:.0f}"
            )


if __name__ == "__main__":
    asyncio.run(_test())
