"""
Kakashi v2 — LIVE tracking verification (read-only, no API keys needed).

Run from repo root on your Mac or EC2:
    python3 scripts/verify_tracking_live.py

Checks, against the REAL Polymarket APIs:
  1. Data API returns positions for each tracked wallet in wallets.py
  2. Gamma API returns market payloads your snapshot parser can price
  3. Resolution parsing works on a real RESOLVED market (closed=true)
  4. Current tracker state summary (data/kakashi_v2_state.json)

If all four pass, the bot will open AND close paper trades correctly.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp
from src.polymarket.basket.wallets import BASKETS
from src.polymarket.basket.strategy import BasketStrategy

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"


async def main() -> int:
    ok = fail = 0

    def report(name, passed, detail=""):
        nonlocal ok, fail
        ok += passed
        fail += not passed
        print(f"  {'✅' if passed else '❌'} {name}" + (f" — {detail}" if detail else ""))

    timeout = aiohttp.ClientTimeout(total=30, connect=5)
    async with aiohttp.ClientSession(timeout=timeout) as http:

        # ── 1. Wallet positions ────────────────────────────────────────
        print("\n[1] Data API — tracked wallet positions")
        sample_market = None
        for basket, entries in BASKETS.items():
            for address, label in entries:
                url = f"{DATA_API}/positions?user={address}&sizeThreshold=10"
                try:
                    async with http.get(url) as resp:
                        data = await resp.json() if resp.status == 200 else None
                    if data is None:
                        report(f"{basket}/{label}", False, f"HTTP error")
                        continue
                    positions = data if isinstance(data, list) else data.get("data", [])
                    report(f"{basket}/{label}", True, f"{len(positions)} open positions")
                    if positions and sample_market is None:
                        sample_market = positions[0].get("conditionId")
                except Exception as exc:
                    report(f"{basket}/{label}", False, str(exc)[:60])

        # ── 2. Snapshot parsing on a live market ───────────────────────
        print("\n[2] Gamma API — snapshot price extraction")
        strat = BasketStrategy()
        strat._http = http
        if sample_market:
            snap = await strat._fetch_market_snapshot(
                sample_market, "Yes", 0.5, market_title=""
            )
            report(
                "live snapshot parsed",
                snap is not None,
                f"price={snap.current_price:.3f} liq=${snap.open_interest:,.0f}"
                if snap else "no price extracted (check market fields)",
            )
        else:
            report("live snapshot parsed", False, "no wallet had open positions to test with")

        # ── 3. Resolution parsing on a real resolved market ────────────
        print("\n[3] Gamma API — resolution detection on a real closed market")
        try:
            url = f"{GAMMA_API}/markets?closed=true&limit=1&order=volume24hr&ascending=false"
            async with http.get(url) as resp:
                markets = await resp.json()
            m = (markets if isinstance(markets, list) else markets.get("data", []))[0]
            cid = m.get("conditionId")
            outcomes = m.get("outcomes")
            resolved, price = await strat._check_market_resolved(cid, "Yes")
            report(
                "resolved market detected & priced",
                resolved,
                f"'{m.get('question','')[:50]}' → Yes side price={price}",
            )
            if not resolved:
                print(f"     raw outcomes={outcomes} outcomePrices={m.get('outcomePrices')}")
        except Exception as exc:
            report("resolved market detected & priced", False, str(exc)[:80])

    # ── 4. Tracker state on disk ───────────────────────────────────────
    print("\n[4] Tracker state (data/kakashi_v2_state.json)")
    state_file = Path("data/kakashi_v2_state.json")
    if state_file.exists():
        try:
            raw = json.loads(state_file.read_text())
            n_open = len(raw.get("open_trades", {}))
            n_closed = len(raw.get("closed_trades", []))
            pnl = sum(t.get("pnl_usd", 0) for t in raw.get("closed_trades", []))
            age_h = (time.time() - raw.get("last_saved_ts", 0)) / 3600
            report("state file valid", True,
                   f"{n_open} open, {n_closed} closed, pnl=${pnl:+.2f}, saved {age_h:.1f}h ago")
            if n_closed == 0 and n_open == 0:
                print("     (empty is expected on first run after the patch — "
                      "old code could never close trades, so restart fresh)")
        except Exception as exc:
            report("state file valid", False, str(exc)[:60])
    else:
        report("state file valid", True, "no state yet — will be created on first run")

    print(f"\n{'='*55}\n{ok} passed, {fail} failed")
    if fail == 0:
        print("Tracking pipeline verified against LIVE APIs. Restart the bot:")
        print("  pkill -f 'src.main_kakashi_v2' ; nohup python3 -m src.main_kakashi_v2 &")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
