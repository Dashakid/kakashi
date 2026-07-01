# Kakashi Bot — Polymarket Copy-Trading Strategy

A basket-consensus copy-trading bot for Polymarket that follows top-performing wallets across multiple market categories (sports, politics, crypto, finance).

## Overview

**Status**: Paper mode only (live trading blockers being resolved)

**Strategy**: Multi-wallet consensus detection
- Monitors tracked wallets across basket categories
- Detects when 50%+ of basket wallets agree on an outcome
- Opens paper trades at detected entry prices
- Tracks P&L and wallets over time

**Infrastructure**: 
- Runs on macOS (local) or EC2 Ubuntu (production)
- Async aiohttp-based polling of Polymarket APIs
- Paper balance: $1,000 USD (paper trading only)
- No live order submission yet (see ROBUSTNESS_ASSESSMENT.md)

---

## Quick Start

### Prerequisites

- Python 3.10+
- Virtual environment (venv, conda, etc.)
- Git

### Installation

```bash
git clone https://github.com/Dashakid/kakashi-bot.git
cd kakashi-bot

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required variables (for live trading only):
```
POLY_API_KEY=<your_api_key>
POLY_API_SECRET=<your_api_secret>
POLY_API_PASSPHRASE=<your_passphrase>
```

Optional:
```
DISCORD_WEBHOOK_URL=<your_discord_webhook>
KAKASHI_V2_WEBHOOK_URL=<dedicated_kakashi_webhook>
```

### Run Paper-Mode Bot

```bash
python3 -m src.main_kakashi_v2

# Logs output to: logs/kakashi_v2.log
# State persisted to: data/kakashi_v2_state.json
```

### Run Tests

```bash
pytest tests/test_robustness.py -v
```

---

## Architecture

### Core Files

| File | Purpose |
|------|---------|
| `src/main_kakashi_v2.py` | Entry point; async event loop setup |
| `src/polymarket/basket/strategy.py` | Main consensus detection & paper trading logic |
| `src/polymarket/basket/tracker.py` | State persistence & P&L tracking |
| `src/polymarket/basket/wallets.py` | Basket definitions (tracked wallets per category) |
| `src/polymarket/basket/market_filter.py` | Liquidity & slippage gates before entry |
| `src/polymarket/client.py` | CLOB client wrapper (V2 SDK) |

### Signal Flow

```
1. Every 60 seconds:
   ├─ Fetch open positions for all tracked wallets (Data API)
   ├─ Build consensus map: (market, outcome) → count of agreeing wallets
   ├─ For each market with 50%+ consensus:
   │  ├─ Run market filters (liquidity, slippage)
   │  ├─ Size paper trade: min(balance * 5%, $100)
   │  └─ Record trade & send Discord alert
   └─ Check all open positions for resolution
      └─ Close filled trades, record P&L

2. Weekly: Re-rank tracked wallets by activity volume
```

### API Endpoints

| Endpoint | Purpose | Auth |
|----------|---------|------|
| `https://data-api.polymarket.com` | Wallet positions, market data | None (read-only) |
| `https://gamma-api.polymarket.com` | Market metadata, outcomes | None (read-only) |
| `wss://clob.polymarket.com` | Order submission | Yes (signer) |

---

## Key Parameters

Located in `src/polymarket/basket/strategy.py`:

```python
CONSENSUS_THRESHOLD  = 0.50      # 50% of basket must agree
MAX_POSITION_PCT     = 0.05      # 5% of balance per trade
PAPER_TRADE_MAX_USD  = 100.0     # Hard cap on paper notional
PAPER_BALANCE_USD    = 1_000.0   # Starting paper balance
MIN_BASKET_SIZE      = 2         # Require ≥2 wallets per basket
MAX_OPEN_PER_BASKET  = 3         # Max concurrent open trades per basket
POLL_INTERVAL_SECONDS = 60       # Check positions every 60s
```

---

## Known Issues & Blockers

### 🔴 CLOB Live Order Submission Blocked

**Status**: Unresolved  
**Error**: `PolyApiException 400 order_version_mismatch` → `maker address not allowed`

**Root Cause**: V2 SDK order format doesn't match current Polymarket CLOB spec or signer/funder wallet config.

**Workaround**: Paper mode works perfectly (bypasses CLOB entirely).

**To Fix**: 
1. Verify order construction in `src/polymarket/client.py` against Polymarket API docs
2. Confirm signer/funder wallet registration and deposit wallet flow
3. Test with Polymarket's reference client or official examples

See **ROBUSTNESS_ASSESSMENT.md** for full details and recommendations.

### ⚠️ API Retry Logic Missing

No exponential backoff or circuit breaker for timeouts. Single API failure drops one polling cycle.

**Workaround**: Bot monitor watchdog restarts the process if it crashes.

---

## Deployment

### Local (macOS)

```bash
python3 -m src.main_kakashi_v2
```

Log: `logs/kakashi_v2.log`  
State: `data/kakashi_v2_state.json`

### EC2 Production

```bash
ssh -i /path/to/key.pem ubuntu@57.180.191.49

# Deploy code
rsync -avz --exclude=.git --exclude=logs --exclude=data . ~/Contract-Trading/

# Restart via watchdog
pkill -f 'python3 -m src\.main_kakashi_v2'

# Monitor
tail -f logs/kakashi_v2.log
```

### systemd Service (Optional)

See `kakashi-bot.service` in parent repo for systemd setup.

---

## Monitoring & Debugging

### Check Logs

```bash
tail -f logs/kakashi_v2.log

# Filter for consensus signals
grep CONSENSUS logs/kakashi_v2.log

# Filter for errors
grep ERROR logs/kakashi_v2.log
```

### Inspect State

```python
import json
with open("data/kakashi_v2_state.json") as f:
    state = json.load(f)
    
print(f"Open trades: {len(state['open_trades'])}")
print(f"Realized P&L: ${state['cumulative_pnl']:.2f}")
```

### Manual Smoke Test

```bash
python3 -m scripts.explicit_smoke
```

---

## Testing

```bash
pytest tests/test_robustness.py -v

# Test coverage:
# - API timeout presence
# - Sharpe ratio & max drawdown
# - Consensus detection
# - Paper-trade sizing bounds
```

---

## Paper Trading Results

**Duration**: 24+ hours on sports markets  
**Signals Detected**: 50+ consensus events  
**Trades Opened**: [See LATEST_KAKASHI_V2_LOG.txt for details]  
**P&L**: To be updated after sufficient data

---

## Roadmap

- [ ] Resolve CLOB `order_version_mismatch` blocker
- [ ] Add exponential backoff + circuit breaker for API timeouts
- [ ] Implement graceful position cancellation on shutdown (live mode)
- [ ] Add fill-rate and slippage tracking
- [ ] Build dashboard endpoint to query live state
- [ ] Validate Gamma API payload shape consistency
- [ ] Add wallet health checks (detect silent wallets)
- [ ] Implement consensus stability check (prevent noise entry)

---

## Contributing

For bug reports or feature requests, open an issue on GitHub.

For security issues, email privately (do not open public issues).

---

## References

- **Robustness Assessment**: `ROBUSTNESS_ASSESSMENT.md` — comprehensive analysis with recommendations
- **Latest Logs**: `LATEST_KAKASHI_V2_LOG.txt` — current execution trace
- **Polymarket API Docs**: https://docs.polymarket.com
- **py-clob-client-v2**: https://github.com/polymarket/py-clob-client

---

## License

Proprietary — for authorized use only.

Trading strategies and research are confidential.

---

**Status**: Ready for external robustness review  
**Last Updated**: 2026-07-01  
**Assessment**: See ROBUSTNESS_ASSESSMENT.md
