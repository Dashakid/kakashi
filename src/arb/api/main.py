"""FastAPI ingestion service — Phase 1 entry point.

Run with:
    uvicorn src.arb.api.main:app --host 0.0.0.0 --port 8001 --reload

Or via Docker:
    docker compose up arb-ingestor
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from loguru import logger

from src.arb.config import API_HOST, API_PORT, PAPER_TRADING
from src.arb.db.connection import init_db, close_pool, close_redis, get_redis
from src.arb.db.repository import MarketRepo, HeartbeatRepo
from src.arb.feeds.feed_manager import FeedManager

# Global feed manager (shared between lifespan and routes)
_feed_manager: FeedManager | None = None
_feed_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    global _feed_manager, _feed_task

    logger.info("Arb ingestor starting up…")
    logger.info(f"Paper trading mode: {PAPER_TRADING}")

    # Apply DB schema
    await init_db()

    # Start feed manager in background
    _feed_manager = FeedManager()
    _feed_task = asyncio.create_task(_feed_manager.run(), name="feed_manager")

    await HeartbeatRepo.upsert("api", "running", "FastAPI started")

    yield

    # Shutdown
    logger.info("Arb ingestor shutting down…")
    if _feed_task:
        _feed_task.cancel()
        try:
            await _feed_task
        except asyncio.CancelledError:
            pass
    if _feed_manager:
        await _feed_manager.close()
    await close_pool()
    await close_redis()


app = FastAPI(
    title="Prediction Market Arb — Data Ingestor",
    description="Phase 1: Pulls live market and order-book data from Polymarket and Kalshi.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Meta"])
async def health() -> dict[str, str]:
    return {"status": "ok", "paper_trading": str(PAPER_TRADING)}


# ── Markets ───────────────────────────────────────────────────────────────────

@app.get("/markets/{platform}", tags=["Markets"])
async def list_markets(
    platform: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Return active markets for a platform from the database."""
    if platform not in ("polymarket", "kalshi"):
        raise HTTPException(status_code=400, detail="platform must be 'polymarket' or 'kalshi'")

    markets = await MarketRepo.get_active(platform)
    page = markets[offset : offset + limit]
    return {
        "platform": platform,
        "total": len(markets),
        "offset": offset,
        "limit": limit,
        "markets": page,
    }


# ── Cached order books ────────────────────────────────────────────────────────

@app.get("/orderbook/{platform}/{market_id}", tags=["Order Books"])
async def get_order_book(platform: str, market_id: str) -> dict[str, Any]:
    """Return the latest cached order book from Redis for a market."""
    if platform == "polymarket":
        key = f"poly:ob:{market_id}"
    elif platform == "kalshi":
        key = f"kalshi:ob:{market_id}"
    else:
        raise HTTPException(status_code=400, detail="unknown platform")

    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        raise HTTPException(status_code=404, detail="order book not yet cached")

    import json
    return json.loads(raw)


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/status", tags=["Meta"])
async def status() -> dict[str, Any]:
    """Return live counts of tracked markets and feed health."""
    if _feed_manager is None:
        return {"error": "feed manager not running"}

    return {
        "polymarket_tokens": len(_feed_manager.poly._token_to_market),
        "kalshi_tickers": len(_feed_manager.kalshi._tickers),
        "poly_db_ids": len(_feed_manager._poly_id_map),
        "kalshi_db_ids": len(_feed_manager._kalshi_id_map),
        "feed_task_alive": _feed_task is not None and not _feed_task.done(),
        "paper_trading": PAPER_TRADING,
    }


# ── Manual trigger (useful for testing) ──────────────────────────────────────

@app.post("/refresh", tags=["Meta"])
async def trigger_refresh() -> dict[str, str]:
    """Trigger an immediate market list refresh from both platforms."""
    if _feed_manager is None:
        raise HTTPException(status_code=503, detail="feed manager not running")

    asyncio.create_task(
        asyncio.gather(
            _feed_manager.poly.fetch_markets(),
            _feed_manager.kalshi.fetch_markets(),
        )
    )
    return {"status": "refresh triggered"}
