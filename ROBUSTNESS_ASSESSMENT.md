# Kakashi Bot Robustness Assessment

## Executive Summary

**Status**: Basket-consensus copy-trading strategy for Polymarket (paper mode only). V2 SDK live trading blocker unresolved.

**Architecture**: 
- Signal: Multi-wallet consensus detection (50%+ threshold) on Polymarket markets
- Execution: Paper trading with $1,000 balance, $100 max per trade (5% position sizing)
- Data: Real-time polling of Polymarket positions API + Gamma market data
- Infrastructure: Runs on macOS or EC2 (ubuntu@57.180.191.49)

---

## Key Artifacts

### 1. Dependencies & Versions

```
py-clob-client-v2>=1.0.1          # CLOB order submission (V2 SDK)
aiohttp>=3.9.1                    # Async HTTP with timeouts
pandas>=2.1.3, numpy>=1.26.2      # Data processing
pydantic>=2.11.0                  # Config validation
loguru>=0.7.2                      # Structured logging
APScheduler>=3.10.4               # Task scheduling
eth-account>=0.11.0               # Ethereum signing
websockets>=12.0                  # WebSocket streams
fastapi>=0.115.0, uvicorn>=0.30.6 # Dashboard (optional)
pytest>=8.2.0                      # Test suite
```

**Critical Note**: Legacy `py-clob-client` migrated to `py-clob-client-v2` as of 2026-05-10. This introduced `order_version_mismatch` → `maker address not allowed` blockers.

---

### 2. Recent Execution Log (kakashi_v2.log)

```
2026-05-04 22:01:30 | INFO     | 🚀 Kakashi v2 (Basket) running — PAPER mode (LIVE_TRADING=False hardcoded)
2026-05-04 22:01:30 | INFO     | [basket] sports     ✅  wallets=4
2026-05-04 22:01:32 | INFO     | [strategy] CONSENSUS sports/No Counter-Strike ... | 2/4 wallets (50%)
2026-05-04 22:01:32 | INFO     | [strategy] CONSENSUS sports/No New York Mets ... | 2/4 wallets (50%)
2026-05-04 22:02:37 | INFO     | [strategy] CONSENSUS sports/No Knicks vs. Hawks | 2/4 wallets (50%)
[... repeating every 60-65 seconds across sports markets ...]
2026-05-04 22:20:45 | INFO     | [strategy] CONSENSUS sports/No Spread: Spurs (-4.5) | 2/4 wallets (50%)
```

**Observations**:
- ✅ Paper mode polling healthy (~60s intervals)
- ✅ Consensus detection working (50%+ threshold triggers)
- ⚠️ No live order attempts logged (hardcoded paper-only)
- ⚠️ No fill/resolution events visible in this window

---

## Signal & Copy-Trading Logic

### Entry Rules (BasketStrategy)

Located in `src/polymarket/basket/strategy.py`:

```python
CONSENSUS_THRESHOLD  = 0.50      # 50% of basket wallets must agree
MAX_POSITION_PCT     = 0.05      # 5% of balance per trade
PAPER_TRADE_MAX_USD  = 100.0     # hard cap on paper notional per position
PAPER_BALANCE_USD    = 1_000.0   # starting paper balance for sizing
MIN_BASKET_SIZE      = 2         # require at least 2 wallets per basket
MAX_OPEN_PER_BASKET  = 3         # max concurrent open paper trades per basket
```

**Signal Generation**:
1. **Every 60 seconds**: Fetch open positions for all tracked wallets via Polymarket Data API
2. **Consensus Map**: For each (market, outcome) pair, count how many wallets hold it
3. **Trigger**: If `count / basket_size >= 50%`, AND all market filters pass, OPEN a paper trade
4. **Sizing**: `trade_size = min(balance * 0.05, $100)` — no dynamic adjustment
5. **Tracking**: Record via `BasketTracker`, fire Discord alert

### Data Sources

| Source | Endpoint | Auth Required | Purpose |
|--------|----------|----------------|---------|
| Polymarket Data API | `https://data-api.polymarket.com` | No | Fetch open wallet positions |
| Polymarket Gamma API | `https://gamma-api.polymarket.com` | No | Market metadata, outcomes, prices |
| Polymarket CLOB | `wss://clob.polymarket.com` (WebSocket) | Yes (signer) | Order submission (live only) |

### Basket Configuration

Located in `src/polymarket/basket/wallets.py`. Example:

```python
BASKETS = {
    "sports": {
        "wallets": [
            "0x...",  # Top wallet 1
            "0x...",  # Top wallet 2
            "0x...",  # Top wallet 3
            "0x...",  # Top wallet 4
        ]
    },
    "politics": {"wallets": [...]},
    "crypto": {"wallets": [...]},
    "finance": {"wallets": [...]},
}
```

---

## Order Execution (CLOB)

### Current Blocker: `order_version_mismatch`

**Timeline**:
- **2026-05-10**: Migrated from `py-clob-client` (legacy) to `py-clob-client-v2`
- **2026-05-10**: Updated `src/polymarket/client.py` to use V2 SDK
- **Issue**: Live order attempts fail with `PolyApiException 400 order_version_mismatch`
- **Latest Error**: After trying different sig types (0, 1, 2), now seeing `maker address not allowed, please use the deposit wallet flow`

### Order Construction Code

Located in `src/polymarket/kakashi_bot.py` / `main_kakashi_v2.py`:

```python
from py_clob_client.client import PolymarketClient
from py_clob_client.order_builder.order_builder import OrderBuilder

# Order creation
builder = OrderBuilder(...)
order = builder.build_order(
    price=...,
    size=...,
    side="BUY" | "SELL",
)

# Submit to CLOB
response = client.create_and_post_order(order)
```

**Known Issues**:
1. **Signer mismatch**: The order is signed with one key but submitted from another
2. **API version mismatch**: V2 SDK order format may not match Polymarket's current CLOB spec
3. **Deposit wallet requirement**: Orders must originate from a deposit wallet, not a trading wallet

### Paper Mode (Bypass)

In paper mode, orders skip the CLOB entirely:

```python
if not LIVE_TRADING:  # Hardcoded True in strategy.py
    # Simulate fill at Gamma market price
    fill_price = get_market_snapshot().best_bid  # or best_ask
    record_paper_trade(...)
    return
```

---

## Risk Management

### Position Sizing

- **Initial Balance**: $1,000 (paper)
- **Max Per Trade**: `min(balance * 5%, $100)` = **$50** (on paper balance)
- **Max Open Per Basket**: 3 concurrent positions
- **No Dynamic Scaling**: If balance grows, position size remains $50

**Robustness Gap**: 
- If live trading enabled without removing the $100 cap, real capital positions will be severely undersized
- No circuit breaker for consecutive losses
- No rebalancing if consensus breaks mid-trade

### Exit Criteria

Located in `strategy.py`:

```python
# Check all open positions for resolution (exit_price available from API)
# Close and record P&L when outcome_price available
for position in open_positions:
    if position.exit_price is not None:
        pnl = (exit_price - entry_price) * size
        record_trade(pnl)
        close_position()
```

**Issue**: Relies on Gamma API `exit_price` becoming available. If market remains open or API delays, trades don't close.

---

## Infrastructure Reliability

### Deployment Targets

#### macOS (Local)
- Run: `python3 -m src.main_kakashi_v2`
- Log: `logs/kakashi_v2.log`
- State: `data/kakashi_v2_state.json`
- Restart: Manual or via bot_monitor watchdog

#### EC2 (Production)
- **Host**: ubuntu@57.180.191.49
- **Working Dir**: ~/Contract-Trading
- **Venv**: ~/.venv/bin/python3
- **Log**: logs/kakashi_v2.log (or kakashi.log for classic)
- **Service**: kakashi-bot.service (systemd, runs classic kakashi only)

### Bot Monitor Watchdog

Located in `src/bot_monitor.py`:

```python
# Monitors all active bots via pgrep
# If any dies, restarts it within seconds
pgrep -f 'python3 -m src\.main_kakashi'
# If no match, spawn: python3 -m src.main_kakashi_v2
```

**Reliability**: ✅ Handles crashes, but **not** network outages or API hangs.

### Network Resilience

| Component | Timeout | Retry Logic | Behavior |
|-----------|---------|-------------|----------|
| `aiohttp` Session | `ClientTimeout(total=30, connect=5)` | ❌ None in core loop | Raises on timeout; logs error; continues polling |
| Data API (positions) | 30s | ❌ No retry | Skips that polling cycle; logs `API_ERROR` |
| Gamma API (market data) | 30s | ❌ No retry | Market snapshot missing; signals not triggered |
| CLOB WebSocket | N/A | ❌ None | No WebSocket streams used in v2; polling only |

**Robustness Gap**: 
- Single API timeout causes one polling cycle to be dropped (no catch-up)
- No exponential backoff or circuit breaker
- No fallback to cached data if API fails

---

## Test Coverage

### Automated Tests

Run with:
```bash
pytest tests/test_robustness.py -v
```

**Coverage**:
- ✅ API timeout presence verification
- ✅ Sharpe ratio & max drawdown calculations
- ✅ Multi-wallet consensus detection
- ✅ Paper-trade sizing bounds
- ❌ Live CLOB order submission (mocked)
- ❌ Gamma API payload shape variations (coverage unclear)
- ❌ Wallet rank stability under market volatility
- ❌ Concurrent trade race conditions

**File**: `tests/test_robustness.py` (~200 lines)

### Manual Testing Script

```bash
python3 -m scripts.explicit_smoke  # Smoke test of paper signals
python3 -m tools.trade_report      # Historical P&L audit
```

---

## Paper Trading Results (as of 2026-05-04)

**Duration**: ~24 hours of consensus polling on sports markets

**Signals**: 50+ CONSENSUS events detected (2/4 wallets agreeing on market outcomes)

**Trades Opened**: Unknown (logs don't show `OPENED` events; may indicate parser issue or genuine 0 fills)

**P&L**: No completed trades visible in logs or `win_loss_history.jsonl`

**Gaps**:
- No fill-rate metrics logged
- No missed-trade or slippage tracking
- State file (`kakashi_v2_state.json`) doesn't expose open positions in logs
- No dashboard endpoint to query live state

---

## Configuration (Redacted)

### .env (DO NOT COMMIT)

```bash
# Polymarket CLOB credentials (NOT INCLUDED)
POLY_API_KEY=<redacted>
POLY_API_SECRET=<redacted>
POLY_API_PASSPHRASE=<redacted>

# Discord webhooks (optional, redacted)
DISCORD_WEBHOOK_URL=<redacted>
KAKASHI_V2_WEBHOOK_URL=<redacted>

# Kalshi API (optional, redacted)
KALSHI_USERNAME=<redacted>
KALSHI_PASSWORD=<redacted>

# AWS (optional, redacted)
AWS_ACCESS_KEY_ID=<redacted>
AWS_SECRET_ACCESS_KEY=<redacted>
```

### Key Config Files

- `src/config.py` — Global settings (balance, thresholds)
- `src/polymarket/basket/wallets.py` — Tracked wallets per basket
- `src/polymarket/basket/market_filter.py` — Liquidity & slippage gates
- `kakashi-bot.service` — systemd service for EC2

---

## Known Issues & Recent Fixes

### Issue #1: Live Order Blocker (UNRESOLVED)

**Problem**: CLOB order submission returns `order_version_mismatch` → `maker address not allowed`

**Root Cause**: 
- V2 SDK order format doesn't match current Polymarket CLOB spec
- Possible signer/funder key mismatch
- Possible missing deposit wallet registration

**Attempts**:
- Tried sig types 0, 1, 2
- Tried explicit API key derivation vs env vars
- Tried alternate keyset

**Status**: 🔴 BLOCKED — Live trading cannot proceed

---

### Issue #2: Gamma API Payload Inconsistency (FIXED 2026-05-06)

**Problem**: Gamma `/markets` endpoint sometimes returns `outcomes`/`outcomePrices` as JSON strings instead of structured `tokens` array

**Impact**: Parser crashed; snapshot was marked missing; no signals triggered

**Fix**: 
- Added robust snapshot parser in `src/polymarket/basket/strategy.py` 
- Handles both payload shapes
- Validates float conversions with fallbacks

**Status**: ✅ Deployed; needs real market validation

---

### Issue #3: Wallet Ranking Used Unrealized PnL (FIXED 2026-05-07)

**Problem**: Basket wallets ranked by `cashPnl` from open positions snapshot, which is unrealized and noisy

**Impact**: Ranked wallets changed every cycle; consensus unstable

**Fix**: 
- Removed PnL-based filtering
- Now ranks by 7-day activity volume instead
- Falls back to `PRIORITY_WALLETS` if no discovery

**Status**: ✅ Deployed

---

## Recommendations for Robustness

### Critical (Blocks Live Trading)

1. **Resolve CLOB `order_version_mismatch`**
   - Verify order construction matches Polymarket CLOB spec v1.1+
   - Test with Polymarket's reference client or official examples
   - Confirm signer/funder wallet registration and deposit flow

2. **Add Retry Logic & Circuit Breakers**
   - Implement exponential backoff for API timeouts (3 retries, 2s → 4s → 8s)
   - Add circuit breaker: if 5 consecutive API failures, pause polling for 5 min
   - Log all failures and alert on threshold crossing

3. **Add Live Position Cancellation Safety**
   - Before enabling live trading, add graceful shutdown hook
   - Automatically cancel all open CLOB orders on process termination
   - Track order IDs and verify cancellations

### High (Operational)

4. **Add Consensus Stability Check**
   - Don't open trade if consensus drops below threshold within 10 seconds
   - Lock in direction once entered; re-entry only after cooldown

5. **Instrument Fill Rates & Slippage**
   - Log entry vs exit prices (expected vs actual)
   - Track time-to-fill per trade
   - Alert if slippage exceeds 2%

6. **Enhance State Persistence**
   - Save open trades to `kakashi_state.json` with timestamps
   - Resume trades across restarts (in paper mode at least)
   - Validate state on startup; alert if corrupted

### Medium (Data Quality)

7. **Validate Gamma API Payloads**
   - Schema validation on snapshot parsing
   - Log both shapes when encountered
   - Fallback to Polymarket Data API if Gamma missing

8. **Add Wallet Health Checks**
   - Track if tracked wallets go silent (no positions for 24h)
   - Alert on wallet deactivation
   - Rerank weekly, not continuously

---

## How to Use This Repo

### Local Setup

```bash
# Clone
git clone https://github.com/Dashakid/Contract-Trading.git
cd Contract-Trading

# Create .env (copy from your secure vault; DO NOT COMMIT)
cp .env.example .env  # if exists, or create manually

# Install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run paper-mode bot
python3 -m src.main_kakashi_v2

# Run tests
pytest tests/test_robustness.py -v
```

### EC2 Deployment

```bash
# SSH to EC2
ssh -i /path/to/key.pem ubuntu@57.180.191.49

# Deploy (rsync code)
rsync -avz --exclude=.git --exclude=logs --exclude=data ~/Contract-Trading/ ~/Contract-Trading/

# Restart via bot_monitor (watchdog will pick up changes)
pkill -f 'python3 -m src\.main_kakashi_v2'

# Or use systemd for classic Kakashi
sudo systemctl restart kakashi-bot

# Tail logs
tail -f logs/kakashi_v2.log
```

---

## Questions for Review

1. **CLOB Order Bug**: Can you spot the `order_version_mismatch` root cause? Check `src/polymarket/client.py` order builder + Polymarket API docs.
2. **Consensus Threshold**: Is 50% too low? Current wallets agree 50%, real consensus is 2/4 (0 in some baskets).
3. **Timeout Handling**: Should we add retry logic, or switch to WebSocket-based subscriptions to avoid polling?
4. **Live Transition**: Once CLOB fixed, what's the manual verification process? (e.g., 7 days of paper P&L + $X min balance)

---

## File Structure

```
Contract-Trading/
├── src/
│   ├── polymarket/
│   │   ├── basket/
│   │   │   ├── strategy.py         ← Main v2 logic
│   │   │   ├── tracker.py          ← State & P&L tracking
│   │   │   ├── wallets.py          ← Basket definitions
│   │   │   └── market_filter.py    ← Liquidity gates
│   │   ├── client.py               ← CLOB client wrapper
│   │   ├── kakashi_bot.py          ← Classic Kakashi (legacy)
│   │   └── leaderboard.py          ← Wallet ranking
│   ├── main_kakashi_v2.py          ← Entry point
│   ├── config.py                   ← Global config
│   └── ...other bots...
├── tests/
│   └── test_robustness.py          ← Robustness checks
├── logs/
│   └── kakashi_v2.log              ← Runtime logs
├── data/
│   ├── kakashi_v2_state.json       ← Open trades & P&L
│   └── ...
├── requirements.txt                ← Dependencies
├── .env                            ← Secrets (DO NOT COMMIT)
├── .gitignore                      ← Already excludes .env
└── README.md
```

---

## License

Proprietary — for authorized use only. Trading strategies and research are confidential.

---

**Last Updated**: 2026-07-01  
**Assessment By**: GitHub Copilot (Claude Haiku 4.5)  
**Status**: Ready for external review (all secrets excluded)
