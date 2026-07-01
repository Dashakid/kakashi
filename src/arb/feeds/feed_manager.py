"""Feed manager — coordinates Polymarket + Kalshi feeds and persists to DB."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.arb.config import MARKET_POLL_INTERVAL, ORDERBOOK_POLL_INTERVAL
from src.arb.feeds.polymarket_feed import PolymarketFeed
from src.arb.feeds.kalshi_feed import KalshiFeed
from src.arb.db.repository import MarketRepo, OrderBookRepo, HeartbeatRepo
from src.arb.db.connection import db_conn


class FeedManager:
    """
    Orchestrates both feeds:
      1. Periodic market list refresh (REST) → DB upsert
      2. Real-time order book updates (WS + REST fallback) → DB insert + Redis cache
    """

    def __init__(self) -> None:
        self.poly = PolymarketFeed()
        self.kalshi = KalshiFeed()

        # Internal maps: external_id -> internal DB id (filled after first upsert)
        self._poly_id_map: dict[str, int] = {}
        self._kalshi_id_map: dict[str, int] = {}

        # Register callbacks
        self.poly.on_markets(self._on_poly_markets)
        self.poly.on_order_book(self._on_order_book)
        self.kalshi.on_markets(self._on_kalshi_markets)
        self.kalshi.on_order_book(self._on_order_book)

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def _on_poly_markets(self, markets: list[dict]) -> None:
        asyncio.create_task(self._persist_markets("polymarket", markets, self._poly_id_map))

    def _on_kalshi_markets(self, markets: list[dict]) -> None:
        asyncio.create_task(self._persist_markets("kalshi", markets, self._kalshi_id_map))

    def _on_order_book(self, ob: dict) -> None:
        asyncio.create_task(self._persist_order_book(ob))

    # ── Persistence ───────────────────────────────────────────────────────────

    async def _persist_markets(
        self, platform: str, markets: list[dict], id_map: dict[str, int]
    ) -> None:
        from datetime import datetime, timezone

        def _parse_dt(val: str | None):
            if not val:
                return None
            try:
                # Handle trailing Z (Python < 3.11 doesn't accept it in fromisoformat)
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                return None

        for m in markets:
            try:
                db_id = await MarketRepo.upsert(
                    platform_slug=platform,
                    external_id=m["external_id"],
                    title=m["title"],
                    description=m.get("description", ""),
                    resolution_rules=m.get("resolution_rules", ""),
                    category=m.get("category", ""),
                    end_date=_parse_dt(m.get("end_date")),
                    status=m.get("status", "active"),
                    yes_bid=m.get("yes_bid"),
                    yes_ask=m.get("yes_ask"),
                    no_bid=m.get("no_bid"),
                    no_ask=m.get("no_ask"),
                    volume_24h=m.get("volume_24h"),
                    liquidity=m.get("liquidity"),
                    title_tokens=_tokenise(m["title"]),
                )
                id_map[m["external_id"]] = db_id
            except Exception as exc:
                logger.debug(f"Market upsert failed [{platform}:{m.get('external_id')}]: {exc}")

        await HeartbeatRepo.upsert(
            f"ingestor_{platform}", "running", f"{len(markets)} markets"
        )
        logger.debug(f"Persisted {len(markets)} {platform} markets")

    async def _persist_order_book(self, ob: dict) -> None:
        platform = ob.get("platform", "")
        ext_id = ob.get("external_id")
        if not ext_id:
            return

        id_map = self._poly_id_map if platform == "polymarket" else self._kalshi_id_map
        market_db_id = id_map.get(ext_id)
        if not market_db_id:
            return  # market not yet persisted; skip

        try:
            # YES side
            await OrderBookRepo.insert(
                market_id=market_db_id,
                side="yes",
                bids=ob.get("bids", []),
                asks=ob.get("asks", []),
                best_bid=ob.get("best_bid"),
                best_ask=ob.get("best_ask"),
                source=ob.get("source", "poll"),
            )
        except Exception as exc:
            logger.debug(f"OrderBook insert failed: {exc}")

    # ── Market polling loop ───────────────────────────────────────────────────

    async def _poll_markets(self) -> None:
        while True:
            try:
                await asyncio.gather(
                    self.poly.fetch_markets(),
                    self.kalshi.fetch_markets(),
                )
            except Exception as exc:
                logger.error(f"Market poll error: {exc}")
            await asyncio.sleep(MARKET_POLL_INTERVAL)

    # ── REST order book polling (fallback when WS is down) ────────────────────

    async def _poll_order_books_poly(self) -> None:
        """Iterate over known Poly token IDs and poll REST order books."""
        while True:
            tokens = list(self.poly._token_to_market.keys())
            if tokens:
                for token_id in tokens[:200]:  # cap to avoid hammering
                    try:
                        await self.poly.fetch_order_book(token_id)
                    except Exception as exc:
                        logger.debug(f"Poly OB poll error {token_id}: {exc}")
                    await asyncio.sleep(0.05)  # ~20 req/s
            await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

    async def _poll_order_books_kalshi(self) -> None:
        """Iterate over known Kalshi tickers and poll REST order books."""
        while True:
            tickers = list(self.kalshi._tickers)
            if tickers:
                for ticker in tickers[:200]:
                    try:
                        await self.kalshi.fetch_order_book(ticker)
                    except Exception as exc:
                        logger.debug(f"Kalshi OB poll error {ticker}: {exc}")
                    await asyncio.sleep(0.05)
            await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Start all feed tasks and run until cancelled."""
        logger.info("FeedManager starting…")

        # Initial market fetch before starting WS
        try:
            await asyncio.gather(
                self.poly.fetch_markets(),
                self.kalshi.fetch_markets(),
            )
        except Exception as exc:
            logger.warning(f"Initial market fetch partial failure: {exc}")

        # Start WebSocket listeners
        await self.poly.start_ws()
        await self.kalshi.start_ws()

        # Start background polling tasks
        tasks = [
            asyncio.create_task(self._poll_markets(), name="poll_markets"),
            asyncio.create_task(self._poll_order_books_poly(), name="poll_ob_poly"),
            asyncio.create_task(self._poll_order_books_kalshi(), name="poll_ob_kalshi"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("FeedManager shutting down")
        finally:
            for t in tasks:
                t.cancel()
            await self.poly.close()
            await self.kalshi.close()

    async def close(self) -> None:
        await self.poly.close()
        await self.kalshi.close()


def _tokenise(text: str) -> list[str]:
    """Simple whitespace tokeniser for title_tokens column (TF-IDF in Phase 2)."""
    import re
    return [w.lower() for w in re.findall(r"[a-z0-9']+", text.lower())]
