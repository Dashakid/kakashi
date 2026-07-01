"""
Kakashi v2 — end-to-end paper trading lifecycle simulation.

Drives the REAL BasketStrategy code through a scripted 4-tick scenario with
mocked Polymarket API responses. No network. Verifies the full pipeline:

  Tick 1: 3/4 wallets hold "No" on Market A (75% consensus) → filters pass
          → paper trade OPENS. 2/4 hold "No" on Market B (50%) → OPENS.
  Tick 2: Consensus persists → dedup works, no duplicate trades.
  Tick 3: Market A resolves, "No" WINS → trade closes at 1.0 (profit).
          Market B wallets EXIT → consensus_exit close at market price.
  Tick 4: Nothing open, state file intact and valid.

Run:  python3 sim_lifecycle.py
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

import src.polymarket.basket.strategy as strat_mod
import src.polymarket.basket.tracker as tracker_mod
from src.polymarket.basket.strategy import BasketStrategy

# ---------------------------------------------------------------------------
# Scenario data
# ---------------------------------------------------------------------------

WALLETS = [
    ("0xw1", "whale-1"),
    ("0xw2", "whale-2"),
    ("0xw3", "whale-3"),
    ("0xw4", "whale-4"),
]

MARKET_A = "0xaaaa"   # will resolve, "No" wins
MARKET_B = "0xbbbb"   # whales exit early → consensus_exit

def pos(market_id, title, outcome, price):
    return {
        "conditionId": market_id,
        "title": title,
        "outcome": outcome,
        "currentValue": 500.0,
        "avgPrice": price,
        "asset": f"tok_{market_id}_{outcome}",
    }

# Per-tick wallet positions (data-api)
TICK_POSITIONS = {
    1: {
        "0xw1": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.40),
                 pos(MARKET_B, "Mets vs Angels", "No", 0.55)],
        "0xw2": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.41),
                 pos(MARKET_B, "Mets vs Angels", "No", 0.54)],
        "0xw3": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.39)],
        "0xw4": [],
    },
}
TICK_POSITIONS[2] = TICK_POSITIONS[1]                       # consensus persists
TICK_POSITIONS[3] = {
    "0xw1": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.40)], # B: exited
    "0xw2": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.41)], # B: exited
    "0xw3": [pos(MARKET_A, "Knicks vs Hawks", "No", 0.39)],
    "0xw4": [],
}
TICK_POSITIONS[4] = {w: [] for w, _ in WALLETS}

def market_payload(market_id, closed, no_price, liquidity=50_000):
    return [{
        "conditionId": market_id,
        "question": "Knicks vs Hawks" if market_id == MARKET_A else "Mets vs Angels",
        "closed": closed,
        "outcomes": '["Yes", "No"]',
        "outcomePrices": json.dumps([str(round(1 - no_price, 4)), str(no_price)]),
        "liquidity": str(liquidity),
        "volume24hr": "25000",
    }]

# Per-tick gamma market state
TICK_MARKETS = {
    1: {MARKET_A: market_payload(MARKET_A, False, 0.40),
        MARKET_B: market_payload(MARKET_B, False, 0.55)},
    2: {MARKET_A: market_payload(MARKET_A, False, 0.41),
        MARKET_B: market_payload(MARKET_B, False, 0.56)},
    3: {MARKET_A: market_payload(MARKET_A, True, 1.0),     # resolved: No wins
        MARKET_B: market_payload(MARKET_B, False, 0.62)},  # open, price moved up
    4: {MARKET_A: market_payload(MARKET_A, True, 1.0),
        MARKET_B: market_payload(MARKET_B, False, 0.62)},
}

# ---------------------------------------------------------------------------
# Fake aiohttp session routing by URL, tick-aware
# ---------------------------------------------------------------------------

class FakeResp:
    def __init__(self, payload, status=200):
        self._payload, self.status = payload, status
    async def json(self): return self._payload
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False

class FakeHttp:
    def __init__(self):
        self.tick = 1
    def get(self, url):
        if "data-api" in url and "/positions" in url:
            wallet = url.split("user=")[1].split("&")[0]
            return FakeResp(TICK_POSITIONS[self.tick].get(wallet, []))
        if "gamma-api" in url and "/markets" in url:
            for mid, payload in TICK_MARKETS[self.tick].items():
                if mid in url:
                    return FakeResp(payload)
            return FakeResp([])
        return FakeResp([], 404)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

async def main():
    state_file = Path("data/sim_state.json")
    state_file.parent.mkdir(exist_ok=True)
    if state_file.exists():
        state_file.unlink()
    tracker_mod.STATE_FILE = state_file

    # Inject scenario baskets + fast consensus-exit for the sim
    strat_mod.BASKETS = {"sports": WALLETS}
    strat_mod.all_basket_names = lambda: ["sports"]
    strat_mod.CONSENSUS_EXIT_GRACE_SECS = 0
    strat_mod.SNAPSHOT_CACHE_TTL_SECS = 0   # always fetch fresh in sim

    strat = BasketStrategy()
    http = FakeHttp()
    strat._http = http

    checks = []
    def check(name, cond):
        checks.append((name, cond))
        print(f"  {'✅' if cond else '❌'} {name}")

    with patch("src.notifications.alerts.BotAlert.signal", return_value=None), \
         patch("src.tracking.trade_logger.log_trade", return_value=None):

        for tick in (1, 2, 3, 4):
            http.tick = tick
            print(f"\n───── TICK {tick} ─────")
            await strat._tick()

            open_n = len(strat._tracker._state.open_trades)
            closed_n = len(strat._tracker._state.closed_trades)
            print(f"  open={open_n} closed={closed_n}")

            if tick == 1:
                check("2 paper trades opened (A @75%, B @50% consensus)", open_n == 2)
            if tick == 2:
                check("dedup: still exactly 2 open, no duplicates", open_n == 2 and closed_n == 0)
            if tick == 3:
                a_closed = [t for t in strat._tracker._state.closed_trades
                            if t.market_id == MARKET_A]
                b_closed = [t for t in strat._tracker._state.closed_trades
                            if t.market_id == MARKET_B]
                check("Market A closed via resolution", len(a_closed) == 1
                      and a_closed[0].close_reason == "resolved")
                check("Market A closed as WIN at exit=1.0", a_closed
                      and a_closed[0].exit_price == 1.0 and a_closed[0].is_win)
                check("Market B closed via consensus_exit (whales bailed)",
                      len(b_closed) == 1 and b_closed[0].close_reason == "consensus_exit")
                if b_closed:
                    print(f"     B exit price={b_closed[0].exit_price:.2f} "
                          f"pnl=${b_closed[0].pnl_usd:+.2f}")
            if tick == 4:
                check("all positions closed, nothing stuck open", open_n == 0)

    # ---- Final state verification ----
    print("\n───── FINAL STATE (data/sim_state.json) ─────")
    raw = json.loads(state_file.read_text())
    check("state file is valid JSON on disk", True)
    check("2 closed trades persisted", len(raw["closed_trades"]) == 2)

    stats = raw["basket_stats"]["sports"]
    total_pnl = sum(t["pnl_usd"] for t in raw["closed_trades"])
    print(f"\n  sports basket: trades={stats['total_trades']} "
          f"wins={stats['wins']} losses={stats['losses']} "
          f"win_rate={stats['win_rate']*100:.0f}% "
          f"total_pnl=${stats['total_pnl_usd']:+.2f}")
    check("basket stats computed (2 trades tracked)", stats["total_trades"] == 2)
    check("P&L math consistent", abs(stats["total_pnl_usd"] - total_pnl) < 1e-6)

    for t in raw["closed_trades"]:
        print(f"  • {t['market_title'][:20]:20s} {t['outcome']} "
              f"in={t['entry_price']:.2f} out={t['exit_price']:.2f} "
              f"${t['pnl_usd']:+7.2f} [{t['close_reason']}]")

    failed = [n for n, ok in checks if not ok]
    print(f"\n{'='*50}\n{'🎉 ALL ' + str(len(checks)) + ' LIFECYCLE CHECKS PASSED' if not failed else '❌ FAILED: ' + ', '.join(failed)}")
    return 0 if not failed else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
