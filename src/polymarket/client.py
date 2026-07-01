"""Polymarket API client for fetching market data and executing trades."""

import requests
import aiohttp
import asyncio
import time
import random
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from loguru import logger

import os
from src.config import (
    GAMMA_API_URL, 
    DATA_API_URL, 
    TRADE_API_URL,
    RELAYER_API_KEY,
    RELAYER_WALLET_ADDRESS
)

# CLOB client (loaded lazily so the rest of the bot still imports if missing)
try:
    from py_clob_client_v2 import (
        ApiCreds,
        ClobClient,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
        Side,
    )
    _CLOB_AVAILABLE = True
except Exception:
    _CLOB_AVAILABLE = False


@dataclass
class Market:
    """Polymarket contract data."""
    id: str
    title: str
    description: str
    outcomes: List[str]
    best_bid: float
    best_ask: float
    last_price: float
    volume: float
    liquidity: float
    timestamp: datetime


@dataclass
class OrderBook:
    """Order book snapshot for a market."""
    market_id: str
    bids: List[tuple]  # [(price, size), ...]
    asks: List[tuple]  # [(price, size), ...]
    mid_price: float
    timestamp: datetime


class PolymarketClient:
    """Client for interacting with Polymarket API with Relayer support."""

    def __init__(self, relayer_key: Optional[str] = None, wallet: Optional[str] = None):
        """Initialize Polymarket client with Relayer."""
        self.relayer_key = relayer_key or RELAYER_API_KEY
        self.wallet = wallet or RELAYER_WALLET_ADDRESS
        self._require_explicit_api_creds = (
            os.getenv("POLY_REQUIRE_EXPLICIT_API_CREDS", "1").strip().lower()
            not in {"0", "false", "no", "off"}
        )
        self.gamma_url = GAMMA_API_URL
        self.data_url = DATA_API_URL
        self.trade_url = TRADE_API_URL
        self.session = None
        self._clob: Optional["ClobClient"] = None
        self._clob_order_options = self._build_clob_order_options()
        self._clob_submit_lock = asyncio.Lock()
        # Best-effort in-process idempotency cache to prevent duplicate posts
        # from retries/timeouts in the same bot process.
        self._recent_orders: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _build_clob_order_options() -> Optional["PartialCreateOrderOptions"]:
        raw_tick = str(os.getenv("POLY_TICK_SIZE", "0.01"))
        if raw_tick in {"0.1", "0.01", "0.001", "0.0001"}:
            return PartialCreateOrderOptions(tick_size=raw_tick)
        return None

    async def initialize(self):
        """Initialize async session with timeout."""
        timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_read=5)
        self.session = aiohttp.ClientSession(timeout=timeout)
        # Initialize the CLOB signer client (used for live orders).
        self._init_clob()

    def _init_clob(self) -> None:
        """Build a py-clob-client instance authed with the user's private key.
        Safe to call multiple times — no-op if already built or deps missing.
        """
        if self._clob is not None or not _CLOB_AVAILABLE:
            return
        priv_key = os.getenv("POLY_PRIVATE_KEY") or os.getenv("RELAYER_PRIVATE_KEY") or ""
        funder = os.getenv("POLY_FUNDER_ADDRESS") or os.getenv("RELAYER_WALLET_ADDRESS") or ""
        api_key = os.getenv("POLY_API_KEY") or os.getenv("RELAYER_API_KEY") or ""
        api_secret = os.getenv("POLY_API_SECRET") or os.getenv("RELAYER_API_SECRET") or ""
        api_passphrase = os.getenv("POLY_API_PASSPHRASE") or os.getenv("RELAYER_API_PASSPHRASE") or ""
        if not priv_key:
            logger.warning("CLOB: no POLY_PRIVATE_KEY in env — live orders disabled")
            return
        try:
            host = "https://clob.polymarket.com"
            chain_id = 137  # Polygon mainnet
            # Derive the actual Ethereum address from the private key
            try:
                from eth_account import Account as _EthAccount
                derived_address = _EthAccount.from_key(priv_key).address
            except Exception:
                derived_address = None
            # Choose signature type based on whether funder matches key
            # signature_type=0 (EOA): funder MUST equal address(private_key)
            # signature_type=2 (proxy/Safe): EOA signs on behalf of proxy funder
            if derived_address and funder and funder.lower() != derived_address.lower():
                # Funder ≠ key → use Polymarket proxy. Try sig_type=2 (Gnosis Safe-style proxy).
                # Some Polymarket email-signup proxies are Safe contracts; fall back to 1 didn't work.
                sig_type = int(os.getenv("POLY_SIG_TYPE", "2"))
                logger.info(f"CLOB: funder {funder[:10]}... ≠ key {derived_address[:10]}... → using signature_type={sig_type}")
            else:
                # Funder matches key (or no funder) → plain EOA → use signature_type=0
                sig_type = 0
                if derived_address:
                    funder = derived_address  # ensure exact match
                logger.info(f"CLOB: using signature_type=0 (EOA), funder={funder[:10] if funder else 'none'}...")
            client = ClobClient(host, key=priv_key, chain_id=chain_id, funder=funder, signature_type=sig_type)
            # Default to strict explicit creds to avoid server-side version drift.
            if api_key and api_secret and api_passphrase:
                client.set_api_creds(ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase))
            elif self._require_explicit_api_creds:
                logger.error(
                    "CLOB: explicit API creds required but missing. "
                    "Set POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE "
                    "(or RELAYER_* equivalents)"
                )
                return
            else:
                creds = client.create_or_derive_api_key()
                client.set_api_creds(creds)
                logger.info(f"CLOB: derived API creds (key={creds.api_key[:8]}…)")
            self._clob = client
            logger.info(f"CLOB: signer client ready | funder={funder[:10]}…")
        except Exception as e:
            # Surface as error, not warning — if CLOB fails to init, live orders are disabled
            # and the operator must know immediately.
            logger.error(f"CLOB init failed: {e} — live orders will be disabled")

    async def close(self):
        """Close async session."""
        if self.session:
            await self.session.close()

    async def get_usdc_balance(self) -> Optional[float]:
        """Return the wallet's USDC balance on Polygon via public RPC.
        Tries both native USDC and USDC.e contracts across multiple RPC endpoints.
        Returns None on failure."""
        funder = os.getenv("POLY_FUNDER_ADDRESS") or os.getenv("RELAYER_WALLET_ADDRESS") or ""
        if not funder:
            return None
        # balanceOf(address) ABI selector
        data = "0x70a08231" + funder.lower().replace("0x", "").zfill(64)
        # Try native USDC first (newer Polymarket accounts), then USDC.e (bridged)
        contracts = [
            "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # native USDC on Polygon
            "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e (bridged)
        ]
        rpcs = [
            "https://polygon.llamarpc.com",
            "https://rpc.ankr.com/polygon",
            "https://polygon-rpc.com",
        ]
        for rpc in rpcs:
            for contract in contracts:
                try:
                    payload = {"jsonrpc": "2.0", "method": "eth_call",
                               "params": [{"to": contract, "data": data}, "latest"], "id": 1}
                    async with self.session.post(rpc, json=payload,
                                                 timeout=aiohttp.ClientTimeout(total=8)) as r:
                        result = (await r.json()).get("result", "0x0")
                        bal = int(result, 16) / 1_000_000  # USDC has 6 decimals
                        if bal > 0:
                            logger.info(f"💰 USDC balance: ${bal:.2f} (contract {contract[:10]}... via {rpc})")
                            return bal
                except Exception as e:
                    logger.debug(f"RPC balanceOf failed ({rpc}, {contract[:10]}...): {e}")
        return None

    async def get_markets(self, search_term: Optional[str] = None) -> List[Market]:
        """Fetch active markets from Gamma API, optionally filtered by search_term.
        
        Args:
            search_term: Optional keyword to filter by question/slug. If None, returns ALL markets.
        
        NOTE: The Gamma API ?search= param is silently ignored and returns
        random newest markets. Instead, we fetch top active markets sorted by
        volume and filter client-side by keyword matching on the question field.
        """
        try:
            url = f"{self.gamma_url}/markets"
            params = {
                "active": "true",
                "closed": "false",
                "limit": 200,
                "order": "volume24hr",
                "ascending": "false",
            }
            
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if isinstance(data, list):
                        markets_data = data
                    elif isinstance(data, dict):
                        markets_data = data.get("markets", data.get("data", data.get("results", [])))
                    else:
                        markets_data = []
                    
                    # Client-side filter: if search_term provided, filter by keyword
                    if search_term:
                        keyword = search_term.lower()
                        matched = [
                            m for m in markets_data
                            if isinstance(m, dict) and (
                                keyword in m.get("question", "").lower()
                                or keyword in m.get("slug", "").lower()
                            )
                        ]
                        logger.info(f"Gamma API: {len(markets_data)} active markets, "
                                    f"{len(matched)} match '{search_term}'")
                    else:
                        # No search term: return ALL markets
                        matched = [m for m in markets_data if isinstance(m, dict)]
                        logger.info(f"Gamma API: Fetched {len(matched)} active markets (no filter)")
                    
                    return [self._parse_market(m) for m in matched]
            
            return []
            
        except Exception as e:
            logger.debug(f"Polymarket markets fetch failed (OK if using Kraken-only): {str(e)[:50]}")
            return []

    async def get_market(self, market_id: str) -> Optional[Market]:
        """Fetch specific market data."""
        try:
            url = f"{self.gamma_url}/markets/{market_id}"
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_market(data)
                else:
                    logger.error(f"Error fetching market {market_id}: {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception fetching market: {e}")
            return None

    async def get_order_book(self, market_id: str) -> Optional[OrderBook]:
        """Fetch order book for a market."""
        try:
            # Try Trade API orderbook endpoint first
            url = f"{self.trade_url}/orderbooks/{market_id}"
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_orderbook(data, market_id)
                elif resp.status == 404:
                    # Try alternative format: /markets/{id}/book
                    url = f"{self.gamma_url}/markets/{market_id}/book"
                    async with self.session.get(url) as resp2:
                        if resp2.status == 200:
                            data = await resp2.json()
                            return self._parse_orderbook(data, market_id)
                        else:
                            logger.debug(f"Orderbook not found for {market_id}: {resp2.status}")
                            return None
                else:
                    logger.error(f"Error fetching orderbook: {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception fetching orderbook: {e}")
            return None

    async def place_order(
        self,
        market_id: str,
        outcome: int,
        size: float,
        price: float,
        side: str = "BUY",
        token_id: Optional[str] = None,
        client_order_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a real order on Polymarket via the CLOB.

        Args:
            market_id: condition_id (kept for backward compat / logging)
            outcome:   0 = Yes, 1 = No  (used only if token_id not given)
            size:      shares (must be > 0)
            price:     0–1 limit price
            side:      "BUY" or "SELL"
            token_id:  CLOB ERC-1155 token id. STRONGLY preferred — caller
                       should pass the `asset` field from the positions API
                       directly. If omitted, we resolve it via Gamma using
                       market_id + outcome index.
            client_order_key: deterministic idempotency key from strategy layer
                       (e.g. market+outcome+wallet+time bucket).
        """
        if self._clob is None:
            self._init_clob()
        if self._clob is None:
            logger.error("place_order: CLOB client not initialized (check POLY_PRIVATE_KEY)")
            return {}

        try:
            # Resolve token_id from condition_id if needed
            tid = token_id
            if not tid:
                tid = await self._resolve_token_id(market_id, outcome)
            if not tid:
                logger.error(f"place_order: could not resolve token_id for {market_id} outcome={outcome}")
                return {}

            key = client_order_key or f"{market_id}|{outcome}|{side.upper()}|{tid}|{int(time.time()//5)}"
            self._prune_recent_orders()
            cached = self._recent_orders.get(key)
            if cached and (time.time() - float(cached.get("ts", 0))) < 120:
                logger.info(f"CLOB idempotency hit: key={key}")
                return dict(cached.get("resp") or {})

            norm_price = self._normalize_price(float(price), side)
            norm_size = self._normalize_size(float(size))
            if norm_size <= 0:
                logger.error(f"place_order: normalized size invalid ({norm_size}) for raw size={size}")
                return {}

            side_const = Side.BUY if side.upper() == "BUY" else Side.SELL
            order_args = OrderArgs(
                price=norm_price,
                size=norm_size,
                side=side_const,
                token_id=str(tid),
            )

            # CLOB SDK is sync — run in executor so we don't block the loop.
            # Timeout prevents the loop from hanging if the CLOB server stalls.
            resp = await self._submit_clob_order(order_args)
            if self._has_order_version_mismatch(resp):
                raise RuntimeError(f"order_version_mismatch: {resp}")
            if isinstance(resp, dict) and resp.get("error"):
                raise RuntimeError(f"clob_error: {resp}")

            resp = self._normalize_order_response(resp)
            order_id = resp.get("orderId") or resp.get("id") or resp.get("order_id")
            if order_id:
                self._recent_orders[key] = {"ts": time.time(), "resp": dict(resp)}
            logger.info(f"CLOB order placed: {resp}")
            return resp or {}
        except Exception as e:
            if self._is_order_version_mismatch_exception(e):
                logger.warning("CLOB order_version_mismatch detected; refreshing session and retrying once")
                retry_resp = await self._retry_after_refresh(order_args)
                retry_resp = self._normalize_order_response(retry_resp)
                retry_id = retry_resp.get("orderId") or retry_resp.get("id") or retry_resp.get("order_id")
                if retry_id:
                    self._recent_orders[key] = {"ts": time.time(), "resp": dict(retry_resp)}
                    logger.info(f"CLOB retry succeeded: {retry_resp}")
                    return retry_resp
                logger.error(f"CLOB retry failed (no order id): {retry_resp}")
                return {}
            logger.error(f"CLOB place_order error: {e}")
            return {}

    async def _retry_after_refresh(self, order_args: "OrderArgs") -> Dict[str, Any]:
        """Refresh auth/session state and retry once after short jittered backoff."""
        self._refresh_clob_session()
        await asyncio.sleep(random.uniform(0.15, 0.4))
        return await self._submit_clob_order(order_args)

    async def _submit_clob_order(self, order_args: "OrderArgs") -> Dict[str, Any]:
        async with self._clob_submit_lock:
            loop = asyncio.get_event_loop()
            def _post_order() -> Dict[str, Any]:
                return self._clob.create_and_post_order(
                    order_args=order_args,
                    options=self._clob_order_options,
                    order_type=OrderType.GTC,
                )
            return await asyncio.wait_for(
                loop.run_in_executor(None, _post_order),
                timeout=15,
            )

    def _refresh_clob_session(self) -> None:
        """Refresh auth creds without rebuilding signer/session state."""
        if self._clob is None:
            self._init_clob()
            return

        api_key = os.getenv("POLY_API_KEY") or os.getenv("RELAYER_API_KEY") or ""
        api_secret = os.getenv("POLY_API_SECRET") or os.getenv("RELAYER_API_SECRET") or ""
        api_passphrase = os.getenv("POLY_API_PASSPHRASE") or os.getenv("RELAYER_API_PASSPHRASE") or ""
        try:
            if api_key and api_secret and api_passphrase:
                self._clob.set_api_creds(
                    ApiCreds(
                        api_key=api_key,
                        api_secret=api_secret,
                        api_passphrase=api_passphrase,
                    )
                )
            elif self._require_explicit_api_creds:
                raise RuntimeError(
                    "explicit API creds required; refusing derive/rotate in refresh path"
                )
            else:
                creds = self._clob.create_or_derive_api_key()
                self._clob.set_api_creds(creds)
            logger.info("CLOB: refreshed API creds on existing client")
        except Exception as e:
            logger.warning(f"CLOB: refresh creds failed (keeping current session): {e}")

    @staticmethod
    def _is_order_version_mismatch_exception(exc: Exception) -> bool:
        s = str(exc).lower()
        return "order_version_mismatch" in s

    @staticmethod
    def _has_order_version_mismatch(resp: Any) -> bool:
        if isinstance(resp, dict):
            err = str(resp.get("error", "")).lower()
            msg = str(resp.get("message", "")).lower()
            return "order_version_mismatch" in err or "order_version_mismatch" in msg
        return False

    @staticmethod
    def _normalize_order_response(resp: Any) -> Dict[str, Any]:
        out = resp if isinstance(resp, dict) else {}
        if "orderID" in out and "orderId" not in out:
            out["orderId"] = out["orderID"]
        return out

    @staticmethod
    def _normalize_price(raw_price: float, side: str) -> float:
        tick = float(os.getenv("POLY_PRICE_TICK", "0.001"))
        p = max(0.001, min(0.999, raw_price))
        # Bias by side: BUY rounds down, SELL rounds up.
        if side.upper() == "BUY":
            p = (int(p / tick)) * tick
        else:
            p = (int((p + tick - 1e-12) / tick)) * tick
        return round(max(0.001, min(0.999, p)), 6)

    @staticmethod
    def _normalize_size(raw_size: float) -> float:
        step = float(os.getenv("POLY_SIZE_STEP", "0.0001"))
        s = max(0.0, raw_size)
        s = (int(s / step)) * step
        return round(s, 8)

    def _prune_recent_orders(self) -> None:
        now = time.time()
        self._recent_orders = {
            k: v for k, v in self._recent_orders.items()
            if now - float(v.get("ts", 0)) < 300
        }

    async def _resolve_token_id(self, condition_id: str, outcome_idx: int) -> Optional[str]:
        """Look up the ERC-1155 token id for a (market, outcome) pair."""
        try:
            url = f"{self.gamma_url}/markets"
            params = {"condition_ids": condition_id, "limit": 1}
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                rows = data if isinstance(data, list) else data.get("data", [])
                if not rows:
                    return None
                tokens_field = rows[0].get("clobTokenIds", "[]")
                import json as _json
                if isinstance(tokens_field, str):
                    tokens = _json.loads(tokens_field)
                else:
                    tokens = tokens_field or []
                if outcome_idx < len(tokens):
                    return str(tokens[outcome_idx])
        except Exception as e:
            logger.warning(f"_resolve_token_id failed: {e}")
        return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order."""
        try:
            url = f"{self.trade_url}/orders/{order_id}"
            headers = {"Authorization": f"Bearer {self.relayer_key}"}
            
            async with self.session.delete(url, headers=headers) as resp:
                if resp.status in [200, 204]:
                    logger.info(f"Order {order_id} cancelled")
                    return True
                else:
                    logger.error(f"Error cancelling order: {resp.status}")
                    return False
        except Exception as e:
            logger.error(f"Exception cancelling order: {e}")
            return False

    def _parse_market(self, data: Dict) -> Market:
        """Parse market data from API response."""
        import json as _json
        # Gamma API returns outcomePrices as a JSON-encoded string array e.g. '["0.73","0.27"]'
        # Index 0 = YES price, index 1 = NO price
        outcome_prices = []
        op = data.get("outcomePrices", "[]")
        if isinstance(op, str):
            try:
                outcome_prices = _json.loads(op)
            except Exception:
                pass
        elif isinstance(op, list):
            outcome_prices = op
        yes_price = float(outcome_prices[0]) if outcome_prices else 0.0

        # Prefer explicit bid/ask (camelCase or snake_case), fall back to outcome price ± 1¢
        raw_bid = data.get("bestBid", data.get("best_bid", data.get("bid", None)))
        raw_ask = data.get("bestAsk", data.get("best_ask", data.get("ask", None)))
        if raw_bid is not None and float(raw_bid) > 0:
            best_bid = float(raw_bid)
        elif yes_price > 0:
            best_bid = max(0.01, yes_price - 0.01)
        else:
            best_bid = 0.0
        if raw_ask is not None and float(raw_ask) > 0 and float(raw_ask) > best_bid:
            best_ask = float(raw_ask)
        elif yes_price > 0:
            best_ask = min(0.99, yes_price + 0.01)
        else:
            best_ask = 0.0

        last_price = float(
            data.get("lastTradePrice",
            data.get("last_price",
            data.get("price", yes_price)))
        ) or yes_price

        return Market(
            id=data.get("id", data.get("market_id", "")),
            title=data.get("title", data.get("question", "")),
            description=data.get("description", data.get("description_short", "")),
            outcomes=data.get("outcomes", data.get("outcome_labels", [])),
            best_bid=best_bid,
            best_ask=best_ask,
            last_price=last_price,
            volume=float(data.get("volume24hr", data.get("volume", data.get("volume_24h", 0)))),
            liquidity=float(data.get("liquidity", 0)),
            timestamp=datetime.utcnow(),
        )

    def _parse_orderbook(self, data: Dict, market_id: str) -> OrderBook:
        """Parse order book data from API response."""
        bids = [(float(b[0]), float(b[1])) for b in data.get("bids", [])]
        asks = [(float(a[0]), float(a[1])) for a in data.get("asks", [])]
        
        mid_price = 0.5
        if bids and asks:
            mid_price = (bids[0][0] + asks[0][0]) / 2
        
        return OrderBook(
            market_id=market_id,
            bids=bids,
            asks=asks,
            mid_price=mid_price,
            timestamp=datetime.utcnow(),
        )
