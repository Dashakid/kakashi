"""Configuration for the prediction market arbitrage engine."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv(
    "ARB_DATABASE_URL",
    "postgresql+asyncpg://arbuser:arbpass@localhost:5432/arbdb",
)

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("ARB_REDIS_URL", "redis://localhost:6379/0")
# Key TTLs (seconds)
REDIS_ORDERBOOK_TTL: int = int(os.getenv("REDIS_ORDERBOOK_TTL", "10"))
REDIS_MARKET_TTL: int = int(os.getenv("REDIS_MARKET_TTL", "60"))

# ── Polymarket ────────────────────────────────────────────────────────────────
POLY_GAMMA_URL: str = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com")
POLY_CLOB_URL: str = os.getenv("POLY_CLOB_URL", "https://clob.polymarket.com")
POLY_WS_URL: str = os.getenv("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/")
POLY_MARKETS_LIMIT: int = int(os.getenv("POLY_MARKETS_LIMIT", "500"))

# ── Kalshi ────────────────────────────────────────────────────────────────────
KALSHI_REST_URL: str = os.getenv("KALSHI_REST_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_WS_URL: str = os.getenv("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
KALSHI_API_KEY: str = os.getenv("KALSHI_API_KEY", "")

# Private key: prefer inline env var, fall back to file path
_secret_inline: str = os.getenv("KALSHI_API_SECRET", "")
_secret_path: str = os.getenv("KALSHI_API_SECRET_PATH", "")
if _secret_inline:
    KALSHI_API_SECRET: str = _secret_inline
elif _secret_path and Path(_secret_path).exists():
    KALSHI_API_SECRET = Path(_secret_path).read_text().strip()
else:
    KALSHI_API_SECRET = ""
# Kalshi demo environment (no real money)
KALSHI_DEMO_REST_URL: str = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_DEMO_WS_URL: str = "wss://demo-api.kalshi.co/trade-api/ws/v2"
USE_KALSHI_DEMO: bool = os.getenv("USE_KALSHI_DEMO", "true").lower() == "true"

# ── Trading ───────────────────────────────────────────────────────────────────
PAPER_TRADING: bool = os.getenv("PAPER_TRADING", "true").lower() == "true"
INITIAL_CAPITAL: float = float(os.getenv("ARB_INITIAL_CAPITAL", "1000.0"))
MAX_POSITION_SIZE: float = float(os.getenv("ARB_MAX_POSITION_SIZE", "200.0"))
MIN_PROFIT_CENTS: float = float(os.getenv("ARB_MIN_PROFIT_CENTS", "2.0"))
POLY_FEE_PCT: float = float(os.getenv("POLY_FEE_PCT", "0.02"))    # 2 % taker fee
KALSHI_FEE_PCT: float = float(os.getenv("KALSHI_FEE_PCT", "0.07"))  # 7 % taker fee

# ── Market matching ───────────────────────────────────────────────────────────
MATCH_CONFIDENCE_THRESHOLD: float = float(os.getenv("MATCH_CONFIDENCE_THRESHOLD", "0.85"))

# ── Ingestion ─────────────────────────────────────────────────────────────────
MARKET_POLL_INTERVAL: int = int(os.getenv("MARKET_POLL_INTERVAL", "300"))  # seconds
ORDERBOOK_POLL_INTERVAL: float = float(os.getenv("ORDERBOOK_POLL_INTERVAL", "2.0"))

# ── FastAPI ───────────────────────────────────────────────────────────────────
API_HOST: str = os.getenv("ARB_API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("ARB_API_PORT", "8001"))
