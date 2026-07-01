"""Kalshi data feed — REST polling + WebSocket order book stream.

Kalshi V2 API docs: https://trading-api.kalshi.com/trade-api/v2
WebSocket docs: https://trading-api.kalshi.com/trade-api/ws/v2
"""

from __future__ import annotations

import asyncio
import time
from base64 import b64encode
from typing import Callable, Any
import json

import aiohttp
import websockets
from loguru import logger

from src.arb.config import (
    KALSHI_REST_URL,
    KALSHI_WS_URL,
    KALSHI_DEMO_REST_URL,
    KALSHI_DEMO_WS_URL,
    KALSHI_API_KEY,
    KALSHI_API_SECRET,
    USE_KALSHI_DEMO,
    REDIS_ORDERBOOK_TTL,
    REDIS_MARKET_TTL,
)
from src.arb.db.connection import redis_set_json

MarketCallback = Callable[[list[dict]], None]
OrderBookCallback = Callable[[dict], None]

# Pick demo or live endpoints
_REST_BASE = KALSHI_DEMO_REST_URL if USE_KALSHI_DEMO else KALSHI_REST_URL
_WS_BASE = KALSHI_DEMO_WS_URL if USE_KALSHI_DEMO else KALSHI_WS_URL


def _kalshi_auth_headers(method: str, path: str) -> dict[str, str]:
    """
    Build Kalshi RSA-PKCS1v15 request signature headers (REST API).
    https://trading-api.kalshi.com/docs/#section/Authentication
    """
    if not KALSHI_API_KEY or not KALSHI_API_SECRET:
        return {}

    ts_ms = str(int(time.time() * 1000))
    msg = (ts_ms + method.upper() + path).encode()

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        private_key = serialization.load_pem_private_key(
            KALSHI_API_SECRET.encode(), password=None
        )
        sig = private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
        sig_b64 = b64encode(sig).decode()
    except Exception as exc:
        logger.warning(f"Kalshi RSA sign failed: {exc}")
        return {}

    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "Content-Type": "application/json",
    }


def _kalshi_ws_auth_headers(path: str) -> dict[str, str]:
    """
    Build Kalshi RSA-PSS signature headers for WebSocket connections.
    The WS endpoint requires PSS padding (different from REST which uses PKCS1v15).
    """
    if not KALSHI_API_KEY or not KALSHI_API_SECRET:
        return {}

    ts_ms = str(int(time.time() * 1000))
    msg = (ts_ms + "GET" + path).encode()

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        private_key = serialization.load_pem_private_key(
            KALSHI_API_SECRET.encode(), password=None
        )
        pss = padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        )
        sig = private_key.sign(msg, pss, hashes.SHA256())
        sig_b64 = b64encode(sig).decode()
    except Exception as exc:
        logger.warning(f"Kalshi WS RSA-PSS sign failed: {exc}")
        return {}

    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
    }


def _normalise_market(raw: dict) -> dict | None:
    """Convert a Kalshi V2 market record to our canonical shape."""
    yes_bid = yes_ask = no_bid = no_ask = None
    try:
        # Kalshi reports prices in cents (0-99)
        yes_bid = float(raw.get("yes_bid") or 0) / 100
        yes_ask = float(raw.get("yes_ask") or 0) / 100
        no_bid = float(raw.get("no_bid") or 0) / 100
        no_ask = float(raw.get("no_ask") or 0) / 100
    except Exception:
        pass

    status_raw = raw.get("status", "")
    status = "active" if status_raw in ("open", "active") else "closed"

    return {
        "platform": "kalshi",
        "external_id": raw.get("ticker") or raw.get("id", ""),
        "title": raw.get("title") or raw.get("question", ""),
        "description": raw.get("subtitle", ""),
        "resolution_rules": raw.get("resolution_source", ""),
        "category": raw.get("category", ""),
        "end_date": raw.get("close_time") or raw.get("expiration_time"),
        "status": status,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "volume_24h": float(raw.get("volume_24h") or 0),
        "liquidity": float(raw.get("open_interest") or 0),
    }


class KalshiFeed:
    """Pulls market data from Kalshi REST API and streams order books via WebSocket."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._ws_task: asyncio.Task | None = None
        self._market_callbacks: list[MarketCallback] = []
        self._ob_callbacks: list[OrderBookCallback] = []
        # ticker -> internal market id (filled after fetch_markets)
        self._tickers: set[str] = set()
        self._ws_msg_id = 1

    def on_markets(self, cb: MarketCallback) -> None:
        self._market_callbacks.append(cb)

    def on_order_book(self, cb: OrderBookCallback) -> None:
        self._ob_callbacks.append(cb)

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "arb-bot/1.0"},
            )
        return self._session

    # ── REST: fetch all active markets ────────────────────────────────────────

    async def fetch_markets(self) -> list[dict]:
        session = await self._session_get()
        markets: list[dict] = []
        cursor = None
        limit = 200
        max_pages = 10   # cap at 2 000 markets; full corpus takes minutes

        for _page in range(max_pages):
            path = "/markets"
            params: dict[str, Any] = {"limit": limit, "status": "open"}
            if cursor:
                params["cursor"] = cursor

            # Kalshi signature must include query string in the path
            from urllib.parse import urlencode
            qs = urlencode(params)
            signed_path = f"{path}?{qs}"

            auth_headers = _kalshi_auth_headers("GET", signed_path)
            try:
                async with session.get(
                    f"{_REST_BASE}{path}",
                    params=params,
                    headers=auth_headers,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except Exception as exc:
                logger.error(f"Kalshi fetch_markets error: {exc}")
                break

            page = data.get("markets", [])
            for raw in page:
                norm = _normalise_market(raw)
                if norm and norm["title"]:
                    markets.append(norm)
                    self._tickers.add(norm["external_id"])

            cursor = data.get("cursor")
            if not cursor or not page:
                break

        logger.info(f"Kalshi: fetched {len(markets)} active markets")
        await redis_set_json("kalshi:markets:active", markets, REDIS_MARKET_TTL)

        for cb in self._market_callbacks:
            cb(markets)

        return markets

    # ── REST: order book ──────────────────────────────────────────────────────

    async def fetch_order_book(self, ticker: str) -> dict | None:
        session = await self._session_get()
        path = f"/markets/{ticker}/orderbook"
        auth_headers = _kalshi_auth_headers("GET", path)
        try:
            async with session.get(
                f"{_REST_BASE}{path}", headers=auth_headers
            ) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                data = await resp.json()
        except Exception as exc:
            logger.debug(f"Kalshi order_book fetch error ({ticker}): {exc}")
            return None

        ob_data = data.get("orderbook", data)
        # Kalshi prices in cents → normalise to 0-1
        bids = [{"price": float(l[0]) / 100, "size": float(l[1])} for l in ob_data.get("yes", [])]
        asks = [{"price": float(l[0]) / 100, "size": float(l[1])} for l in ob_data.get("no", [])]
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None

        ob = {
            "platform": "kalshi",
            "ticker": ticker,
            "external_id": ticker,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "ts": time.time(),
        }

        await redis_set_json(f"kalshi:ob:{ticker}", ob, REDIS_ORDERBOOK_TTL)
        for cb in self._ob_callbacks:
            cb(ob)

        return ob

    # ── WebSocket ─────────────────────────────────────────────────────────────

    async def _ws_listen(self, tickers: list[str]) -> None:
        """WebSocket listener using aiohttp (avoids websockets library Python 3.14 compat issues)."""
        backoff = 1
        while True:
            try:
                path = "/trade-api/ws/v2"
                ws_hdrs = _kalshi_ws_auth_headers(path)

                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        _WS_BASE,
                        headers=ws_hdrs,
                        heartbeat=20,
                        receive_timeout=30,
                    ) as ws:
                        # Subscribe in batches of 50
                        for i in range(0, len(tickers), 50):
                            batch = tickers[i : i + 50]
                            sub = {
                                "id": self._ws_msg_id,
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["orderbook_delta"],
                                    "market_tickers": batch,
                                },
                            }
                            self._ws_msg_id += 1
                            await ws.send_str(json.dumps(sub))

                        logger.info(f"Kalshi WS subscribed to {len(tickers)} tickers")
                        backoff = 1

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    await self._handle_ws_message(data)
                                except json.JSONDecodeError:
                                    pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as exc:
                logger.warning(f"Kalshi WS disconnected: {exc}. Reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ws_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return
        msg_type = msg.get("type")
        if msg_type not in ("orderbook_snapshot", "orderbook_delta"):
            return

        data = msg.get("msg", {})
        ticker = data.get("market_ticker")
        if not ticker:
            return

        # Parse levels: [[price_cents, size], ...]
        yes_levels = data.get("yes", [])
        no_levels = data.get("no", [])
        bids = [{"price": float(l[0]) / 100, "size": float(l[1])} for l in yes_levels]
        asks = [{"price": float(l[0]) / 100, "size": float(l[1])} for l in no_levels]
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None

        ob = {
            "platform": "kalshi",
            "ticker": ticker,
            "external_id": ticker,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "ts": time.time(),
            "source": "ws",
        }

        await redis_set_json(f"kalshi:ob:{ticker}", ob, REDIS_ORDERBOOK_TTL)
        for cb in self._ob_callbacks:
            cb(ob)

    async def start_ws(self) -> None:
        tickers = list(self._tickers)
        if not tickers:
            logger.warning("Kalshi WS: no tickers yet; run fetch_markets first")
            return
        self._ws_task = asyncio.create_task(
            self._ws_listen(tickers), name="kalshi_ws"
        )

    async def close(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()
