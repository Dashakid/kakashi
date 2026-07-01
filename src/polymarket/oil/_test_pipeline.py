"""Integration test for the oil trading pipeline — 3 live Polymarket markets."""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent  # oil/ → polymarket/ → src/ → Contract-Trading/
sys.path.insert(0, str(ROOT))

from src.polymarket.oil.data_collector import collect_oil_context
from src.polymarket.oil.signal_engine import get_oil_probability, kelly_position_size
from src.main_oil_trader import (
    MIN_EDGE,
    get_all_market_prices,
    evaluate_signal,
    _hormuz_negative_count,
    _resolve_market_slugs,
)

SEP = "=" * 60


async def test():
    # ── Step 1: Data ────────────────────────────────────────────
    print(f"\n{SEP}\nStep 1: Collecting market data\n{SEP}")
    ctx = await collect_oil_context()
    print(json.dumps({k: v for k, v in ctx.items() if k != "gdelt_headlines"}, indent=2))
    print(f"  GDELT headlines: {len(ctx.get('gdelt_headlines', []))} items")
    print(f"  RSS headlines:   {len(ctx.get('rss_headlines', []))} items")

    hormuz_neg = _hormuz_negative_count(ctx)
    print(f"  Hormuz negative: {hormuz_neg} headlines")

    # ── Step 2: Groq signal ─────────────────────────────────────
    print(f"\n{SEP}\nStep 2: Groq probability estimate\n{SEP}")
    signal = await get_oil_probability(ctx)
    print(json.dumps(signal, indent=2))

    # ── Step 3: Polymarket prices ────────────────────────────────
    print(f"\n{SEP}\nStep 3: Fetching live Polymarket prices (3 markets)\n{SEP}")
    raw_prices = await get_all_market_prices()
    for slug, entry in raw_prices.items():
        p = entry["price"]
        q = entry["question"] or "NOT FOUND"
        print(f"  {slug}")
        print(f"    question:  {q}")
        print(f"    YES price: {p if p is not None else 'UNAVAILABLE'}")

    # ── Step 4: Edge calculations ────────────────────────────────
    print(f"\n{SEP}\nStep 4: Edge calculation per market\n{SEP}")
    BANKROLL = 500.0
    resolved_markets = _resolve_market_slugs()
    for market in resolved_markets:
        slug  = market["slug"]
        entry = raw_prices.get(slug, {})
        price = entry.get("price")
        question = entry.get("question") or slug

        print(f"\n[{market['name']}]  type={market['type']}")
        print(f"  slug:     {slug}")
        print(f"  question: {question}")

        if price is None:
            print("  ** Market price unavailable — skipping edge calc **")
            continue

        print(f"  price:    {price:.4f}")
        dec = evaluate_signal(market, price, question, signal, ctx, BANKROLL)
        edge_str  = f"{dec['edge']*100:+.1f}%" if dec['edge'] is not None else "N/A"
        trade_str = (
            f"PAPER {dec['direction']} ${dec['bet_size']:.2f}"
            if dec["traded"] else
            f"no trade  (need >={MIN_EDGE:.0%} edge + non-low confidence)"
        )
        print(f"  edge:     {edge_str}")
        print(f"  action:   {trade_str}")
        print(f"  note:     {dec['note']}")

    print(f"\n{SEP}\nDone.\n{SEP}\n")


if __name__ == "__main__":
    asyncio.run(test())
