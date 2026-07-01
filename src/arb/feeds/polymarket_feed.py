"""Polymarket data feed — REST polling + WebSocket order book stream."""

from __future__ import annotations

import asyncio
import json
import time
from typing import AsyncIterator, Callable, Any

import aiohttp
import websockets
from loguru import logger

from src.arb.config import (
    POLY_GAMMA_URL,
    POLY_CLOB_URL,
    POLY_WS_URL,
    POLY_MARKETS_LIMIT,
    ORDERBOOK_POLL_INTERVAL,
)
from src.arb.db.connection import redis_set_json
from src.arb.config import REDIS_ORDERBOOK_TTL, REDIS_MARKET_TTL

# Callback type: receives normalised market or order-book dicts
MarketCallback = Callable[[list[dict]], None]
OrderBookCallback = Callable[[dict], None]


def _normalise_market(raw: dict) -> dict | None:
    """Convert a Polymarket Gamma API market record to our canonical shape."""
    # Support both token-list and outcomes/outcomePrices shapes (see repo notes)
    yes_bid = yes_ask = no_bid = no_ask = None
    try:
        tokens = raw.get("tokens")
        if tokens and isinstance(tokens, list):
            for tok in tokens:
                outcome = str(tok.get("outcome", "")).upper()
                if outcome == "YES":
                    yes_bid = float(tok.get("price", 0) or 0)
                    yes_ask = yes_bid  # Gamma gives mid, not full book
                elif outcome == "NO":
                    no_bid = float(tok.get("price", 0) or 0)
                    no_ask = no_bid
        else:
            outcomes_raw = raw.get("outcomes", "[]")
            prices_raw = raw.get("outcomePrices", "[]")
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            price_map = {str(o).upper(): float(p) for o, p in zip(outcomes, prices)}
            yes_bid = yes_ask = price_map.get("YES")
            no_bid = no_ask = price_map.get("NO")
    except Exception as exc:
        logger.debug(f"Poly price parse error for {raw.get('id')}: {exc}")

    end_ts = raw.get("endDate") or raw.get("end_date_iso")

    return {
        "platform": "polymarket",
        "external_id": str(raw.get("id") or raw.get("conditionId", "")),
        "title": raw.get("question") or raw.get("title", ""),
        "description": raw.get("description", ""),
        "resolution_rules": raw.get("resolutionSource", ""),
        "category": raw.get("category", ""),
        "end_date": end_ts,
        "status": "active" if raw.get("active", True) else "closed",
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "volume_24h": float(raw.get("volume24hrClob") or raw.get("volume24h") or 0),
        "liquidity": float(raw.get("liquidityClob") or raw.get("liquidity") or 0),
    }


class PolymarketFeed:
    """Pulls market data from Polymarket REST API and streams order books via WebSocket."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._ws_task: asyncio.Task | None = None
        self._market_callbacks: list[MarketCallback] = []
        self._ob_callbacks: list[OrderBookCallback] = []
        # token_id -> market external_id; built during market fetch
        self._token_to_market: dict[str, str] = {}

    def on_markets(self, cb: MarketCallback) -> None:
        self._market_callbacks.append(cb)

    def on_order_book(self, cb: OrderBookCallback) -> None:
        self._ob_callbacks.append(cb)

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Use Google DNS (8.8.8.8) to bypass router/ISP DNS blocking of polymarket
            resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])
            connector = aiohttp.TCPConnector(resolver=resolver)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "arb-bot/1.0"},
            )
        return self._session

    # ── REST: fetch all active markets ────────────────────────────────────────

    async def fetch_markets(self) -> list[dict]:
        """Paginate Gamma API and return normalised market records."""
        session = await self._session_get()
        markets: list[dict] = []
        offset = 0
        limit = min(POLY_MARKETS_LIMIT, 100)  # Gamma max page = 100

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            try:
                async with session.get(
                    f"{POLY_GAMMA_URL}/markets", params=params
                ) as resp:
                    resp.raise_for_status()
                    page: list[dict] = await resp.json()
            except Exception as exc:
                logger.error(f"Poly fetch_markets error (offset={offset}): {exc}")
                break

            if not page:
                break

            for raw in page:
                norm = _normalise_market(raw)
                if norm and norm["title"]:
                    markets.append(norm)
                    # Map CLOB token IDs → external_id for WS subscriptions
                    for tok in raw.get("tokens") or []:
                        tid = tok.get("token_id")
                        if tid:
                            self._token_to_market[str(tid)] = norm["external_id"]

            if len(page) < limit:
                break
            offset += limit

        logger.info(f"Polymarket: fetched {len(markets)} active markets")

        # Cache in Redis
        await redis_set_json("poly:markets:active", markets, REDIS_MARKET_TTL)

        for cb in self._market_callbacks:
            cb(markets)

        return markets

    # ── REST: order book for a single market ──────────────────────────────────

    async def fetch_order_book(self, token_id: str) -> dict | None:
        """Fetch order book for a YES or NO token from the CLOB REST endpoint."""
        session = await self._session_get()
        url = f"{POLY_CLOB_URL}/book"
        params = {"token_id": token_id}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                data = await resp.json()
        except Exception as exc:
            logger.debug(f"Poly order_book fetch error ({token_id}): {exc}")
            return None

        bids = [{"price": float(l["price"]), "size": float(l["size"])} for l in data.get("bids", [])]
        asks = [{"price": float(l["price"]), "size": float(l["size"])} for l in data.get("asks", [])]
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None

        ob = {
            "platform": "polymarket",
            "token_id": token_id,
            "external_id": self._token_to_market.get(token_id),
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "ts": time.time(),
        }

        # Cache
        await redis_set_json(f"poly:ob:{token_id}", ob, REDIS_ORDERBOOK_TTL)
        for cb in self._ob_callbacks:
            cb(ob)

        return ob

    # ── WebSocket: real-time price stream ─────────────────────────────────────

    async def _ws_connect(self, token_ids: list[str]) -> None:
        """Subscribe to CLOB WebSocket for price_change events."""
        if not token_ids:
            return

        uri = f"{POLY_WS_URL}market"
        subscriptions = [{"assets_ids": token_ids[i:i+50]} for i in range(0, len(token_ids), 50)]

        for sub_batch in subscriptions:
            await self._ws_listen(uri, sub_batch["assets_ids"])

    async def _ws_listen(self, uri: str, asset_ids: list[str]) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=30,
                    extra_headers={"User-Agent": "arb-bot/1.0"},
                ) as ws:
                    # Subscribe
                    subscribe_msg = json.dumps({
                        "auth": {},
                        "markets": [],
                        "assets_ids": asset_ids,
                        "type": "market",
                    })
                    await ws.send(subscribe_msg)
                    logger.info(f"Poly WS subscribed to {len(asset_ids)} tokens")
                    backoff = 1  # reset on success

                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_ws_message(msg)
                        except json.JSONDecodeError:
                            pass
            except Exception as exc:
                logger.warning(f"Poly WS disconnected: {exc}. Reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ws_message(self, msg: Any) -> None:
        event_type = msg.get("event_type") if isinstance(msg, dict) else None
        if event_type not in ("book", "price_change"):
            return

        for item in (msg.get("data") if isinstance(msg.get("data"), list) else [msg]):
            token_id = item.get("asset_id")
            if not token_id:
                continue

            bids = [{"price": float(l[0]), "size": float(l[1])} for l in item.get("buys", [])]
            asks = [{"price": float(l[0]), "size": float(l[1])} for l in item.get("sells", [])]
            best_bid = bids[0]["price"] if bids else None
            best_ask = asks[0]["price"] if asks else None

            ob = {
                "platform": "polymarket",
                "token_id": token_id,
                "external_id": self._token_to_market.get(token_id),
                "bids": bids,
                "asks": asks,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "ts": time.time(),
                "source": "ws",
            }

            await redis_set_json(f"poly:ob:{token_id}", ob, REDIS_ORDERBOOK_TTL)
            for cb in self._ob_callbacks:
                cb(ob)

    async def start_ws(self) -> None:
        """Start WebSocket listener for all known tokens."""
        token_ids = list(self._token_to_market.keys())
        if not token_ids:
            logger.warning("Poly WS: no token IDs yet; run fetch_markets first")
            return
        self._ws_task = asyncio.create_task(
            self._ws_connect(token_ids), name="poly_ws"
        )

    async def close(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()
