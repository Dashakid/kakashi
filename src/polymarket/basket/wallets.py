"""
Wallet basket definitions and quality-filter constants for Kakashi v2.

HOW TO POPULATE WALLETS
------------------------
For each basket below, add tuples of ("0x_address", "display_label"):

1. Go to https://polymarket.com/leaderboard — filter by the relevant category
   (Sports / Politics / Crypto / Finance).
2. Cross-check top wallets at https://polymarketanalytics.com/traders
   — sort by Win Rate first, then by PnL.
3. A wallet must appear in the top-100 of BOTH tools to qualify.
4. Paste here as:
       ("0xabc...def", "Descriptive-Label"),

Leave lists empty until paper-testing on Kakashi v1 validates the approach.
Wallets will be refreshed weekly by BasketTracker (any wallet whose last-30d
P&L goes negative is dropped and must be re-researched manually).
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Quality gates — a tracked wallet must pass ALL of these at refresh time
# ---------------------------------------------------------------------------

MIN_RESOLVED_TRADES   = 100      # at least 100 resolved positions all-time
MIN_WIN_RATE          = 0.60     # 60%+ win rate across resolved trades
MIN_TRACK_RECORD_DAYS = 120      # wallet must be at least 4 months old
MIN_RECENT_PNL        = 0.0      # last-30d realized P&L must be ≥ $0 (positive)
MIN_LIQUIDITY_USD     = 10_000   # market must have ≥ $10k open interest to trade

# ---------------------------------------------------------------------------
# Basket definitions
# Each entry: ("0x_wallet_address", "human_readable_label")
# ---------------------------------------------------------------------------

# Tuple type alias for readability
WalletEntry = Tuple[str, str]

BASKETS: Dict[str, List[WalletEntry]] = {
    # -----------------------------------------------------------------------
    # SPORTS — wallets specialising in sports outcomes
    # (NFL, NBA, soccer, tennis, combat sports, etc.)
    # Research: polymarket.com/leaderboard → filter "Sports"
    # -----------------------------------------------------------------------
    "sports": [
        ("0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a", "surfandturf"),
        ("0x9f2fe025f84839ca81dd8e0338892605702d2ca8", "labradfordsmith22"),
        ("0x9495425feeb0c250accb89275c97587011b19a27", "bossoskil1"),
        ("0x6ac5bb06a9eb05641fd5e82640268b92f3ab4b6e", "wallet-4"),
        ("0x6e1d5040d0ac73709b0621f620d2a60b80d2d0fa", "Respectful-Clan"),
        ("0x2b5920274740ca61745eeb077587bf5bb4ff9db6", "Blind-Leaf"),
    ],

    # -----------------------------------------------------------------------
    # POLITICS — wallets specialising in political / election markets
    # (US elections, international elections, policy outcomes, etc.)
    # Research: polymarket.com/leaderboard → filter "Politics"
    # -----------------------------------------------------------------------
    "politics": [
        ("0x7ac83882979ccb5665cea83cb269e558b55077cd", "Poly-Politics-1"),
        ("0x355e5ae20cc1a3a2164f818e4bac8d22dd72a038", "Poly-Politics-2"),
    ],

    # -----------------------------------------------------------------------
    # CRYPTO — wallets specialising in crypto price / adoption markets
    # (BTC/ETH price targets, protocol launches, listings, etc.)
    # Research: polymarket.com/leaderboard → filter "Crypto"
    # -----------------------------------------------------------------------
    "crypto": [
        ("0x5e3dbb56a5c301330d7513b4d6a536acf6f2bd04", "Poly-Crypto-1"),
    ],

    # -----------------------------------------------------------------------
    # FINANCE — wallets specialising in macro / economics markets
    # (Fed rates, CPI, GDP, commodities, ETF approvals, etc.)
    # Research: polymarket.com/leaderboard → filter "Business & Finance"
    # -----------------------------------------------------------------------
    "finance": [
        # PASTE FINANCE WALLETS HERE
        # ("0x0000000000000000000000000000000000000004", "Example-Finance-Wallet"),
    ],
}

# ---------------------------------------------------------------------------
# Auto-load from basket_wallets.json if it exists (written by refresh_baskets.py)
# Wallets from the cache MERGE with (and extend) the hardcoded lists above.
# Duplicate addresses are deduplicated; hardcoded entries take priority.
# ---------------------------------------------------------------------------

_CACHE_PATH = Path(__file__).resolve().parent.parent.parent.parent / "data" / "basket_wallets.json"


def _load_cache() -> None:
    if not _CACHE_PATH.exists():
        return
    try:
        data = json.loads(_CACHE_PATH.read_text())
        cached_baskets: Dict[str, List[dict]] = data.get("baskets", {})
        for cat, entries in cached_baskets.items():
            if cat not in BASKETS:
                continue
            existing_addrs = {addr.lower() for addr, _ in BASKETS[cat]}
            for w in entries:
                addr = w.get("address", "").lower()
                label = w.get("label", addr[-8:])
                if addr and addr not in existing_addrs:
                    BASKETS[cat].append((addr, label))
                    existing_addrs.add(addr)
    except Exception:
        pass  # never crash on cache load failure


_load_cache()


# ---------------------------------------------------------------------------
# Helper — validate a wallet entry before adding at runtime
# ---------------------------------------------------------------------------

@dataclass
class WalletStats:
    """Live stats fetched from the Polymarket data API for one wallet."""
    address: str
    label: str
    resolved_trades: int
    win_rate: float              # 0.0 – 1.0
    track_record_days: int       # days since first trade
    pnl_last_30d: float          # realized USD P&L in last 30 days


def is_wallet_qualified(stats: WalletStats) -> bool:
    """Return True if a wallet passes all quality gates."""
    return (
        stats.resolved_trades   >= MIN_RESOLVED_TRADES
        and stats.win_rate      >= MIN_WIN_RATE
        and stats.track_record_days >= MIN_TRACK_RECORD_DAYS
        and stats.pnl_last_30d  >= MIN_RECENT_PNL
    )


def basket_size(name: str) -> int:
    """Number of wallets currently loaded in a basket."""
    return len(BASKETS.get(name, []))


def all_basket_names() -> List[str]:
    return list(BASKETS.keys())
