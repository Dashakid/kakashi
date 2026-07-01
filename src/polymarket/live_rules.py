"""
Real Polymarket trading rules — single source of truth for all strategies.

Based on official Polymarket documentation (docs.polymarket.com):

FEES
----
- Taker fee: 0–1.80% per leg depending on category
  * Crypto direction markets (v2):  0–1.80%  → we use 1.0% as conservative estimate
  * Finance/price markets:          0–1.00%  → we use 1.0%
  * Politics, culture, general:     0–1.25%  → we use 1.0%
- Maker fee: ALWAYS 0%. Posting limit orders that rest on the book costs nothing.
- Resolution redemption: NO fee. Redeeming at $1 after resolution is a smart
  contract call, not a trade, so the taker fee does not apply.

HOW TO APPLY FEES (round-trip taker trade)
-------------------------------------------
  fee_drag = TAKER_FEE + TAKER_FEE * (close_price / entry_price)
  net_pnl_pct = gross_pnl_pct - fee_drag

  For a near-ATM trade (entry ≈ exit ≈ 0.50):  fee_drag ≈ 2 × TAKER_FEE = 2.0%

HOW TO APPLY FEES (entry taker + resolution redemption — no exit fee)
----------------------------------------------------------------------
  net_pnl_pct = gross_pnl_pct - TAKER_FEE

ORDER BOOK RULES
----------------
- Tick size: 0.01 for most crypto markets (can be 0.1 / 0.001 / 0.0001 on others)
- Entry price in live trading = ASK (you pay to take liquidity)
- Exit price in live trading  = BID (you receive when selling)
- A market with best_bid=0 or best_ask=0 has NO real liquidity and cannot be
  filled in live trading.

PRACTICAL TRADING CONSTRAINTS
------------------------------
- Minimum practical trade: $5 (below that slippage and overhead dominate)
- Price range for meaningful trading: 0.15–0.85
  * Below 0.15: near-zero probability — extreme noise, wide spreads, illiquid
  * Above 0.85: near-certain resolution — wide spreads, almost no sellers
  * Both extremes have poor fills and disproportionate fee impact
- No gas costs for users: Polymarket uses a gasless Polygon relayer
- Settlement currency: USDC.e on Polygon

WHAT THIS MEANS PER STRATEGY
------------------------------
KrakenPolyArb   — taker entry (pay ask) + taker exit (receive bid).
                  Fee drag both legs. Price range gate required to filter illiquid extremes.
                  MIN_EDGE must exceed fee_drag to generate positive EV.
                  At 1.0% fee/leg and 7% edge threshold: net edge after fees ≈ 5%.

SpreadCapture   — if we post AT the mid as a MAKER and wait for fill: 0% fee.
                  if we cross the spread as a TAKER at mid: 1.0% fee applies.
                  Current sim assumes immediate fill → taker model → apply TAKER_FEE.
                  Spread must exceed 2 × TAKER_FEE + TARGET to be profitable:
                  MIN_SPREAD = 0.06 + 2×0.01 = 0.08 minimum to be safe.

ResolutionArb   — taker entry (pay ask). Exit via market sell at ~0.97 (taker).
                  Fee drag both legs applies.

FastStrategy    — taker entry at ask, taker exit at bid.
                  pnl_pct tracked in Kraken spot % terms (approximation),
                  apply fixed 2×TAKER_FEE drag as flat adjustment to net pnl.

ETHMarketMaker  — same as spread capture: aiming for maker (0% fee) but
                  current sim uses taker model. Fee applied in eth_market_maker.py.
"""

# ── Fee rates ────────────────────────────────────────────────────────────────
TAKER_FEE = 0.01        # 1.0% per trade leg (conservative crypto rate)
MAKER_FEE = 0.00        # Makers always free
ROUND_TRIP_FEE = TAKER_FEE * 2  # Approximate fee for both in + out legs at mid

# ── Price range gate ─────────────────────────────────────────────────────────
PRICE_MIN = 0.15        # Don't enter contracts priced below 15¢
PRICE_MAX = 0.85        # Don't enter contracts priced above 85¢

# ── Liquidity gate ───────────────────────────────────────────────────────────
MIN_BID_REQUIRED = 0.01  # Market must have a real bid (>0) to be tradeable

# ── Order sizing ─────────────────────────────────────────────────────────────
MIN_TRADE_USD = 5.0     # Minimum practical trade size in USDC
TICK_SIZE = 0.01        # Standard tick size (1 cent) for most crypto markets


def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price to nearest valid tick increment."""
    return round(round(price / tick) * tick, 10)


def fee_drag(entry_price: float, exit_price: float) -> float:
    """
    Compute total fee drag as a fraction of entry notional for a round-trip taker trade.

    fee_drag = TAKER_FEE_entry + TAKER_FEE_exit * (exit_price / entry_price)
    """
    return TAKER_FEE + TAKER_FEE * (exit_price / entry_price)


def net_pnl(gross_pnl_pct: float, entry_price: float, exit_price: float) -> float:
    """Net P&L after real Polymarket taker fees (round-trip)."""
    return gross_pnl_pct - fee_drag(entry_price, exit_price)


def net_pnl_entry_only(gross_pnl_pct: float) -> float:
    """Net P&L after only entry-side fee (used for resolution redemption)."""
    return gross_pnl_pct - TAKER_FEE


def is_tradeable_price(prob: float) -> bool:
    """Return True only if market probability is in the tradeable range."""
    return PRICE_MIN <= prob <= PRICE_MAX


def has_real_orderbook(best_bid: float, best_ask: float) -> bool:
    """Return True only if the market has a real two-sided orderbook."""
    return best_bid > MIN_BID_REQUIRED and best_ask > best_bid
