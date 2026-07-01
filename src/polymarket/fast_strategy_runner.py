"""
Fast (1-hour) Polymarket strategy runner for SOL, AVAX, XRP, and other assets.
Complements the main 4-hour BTC/ETH relay with faster signals on alternative assets.
"""

import asyncio
import random
from datetime import datetime
from typing import Dict, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from src.polymarket.market_discovery import MarketDiscovery
from src.polymarket.signal_matcher import SignalMatcher
from src.polymarket.order_placer import OrderPlacer
from src.data.kraken_client import KrakenClient
from src.trading.scalp_strategy import ScalpStrategy, TradeSignal
from src.notifications.discord_webhook import get_discord_client
from src.notifications.alerts import BotAlert
from src.polymarket import live_rules
from src.ml.models import PricePredictor


class FastPolymarketRunner:
    """
    Fast strategy runner for Polymarket.
    
    - Monitors 1-hour candles on alternative assets (SOL, AVAX, XRP, DOT, etc.)
    - Independent from BTC/ETH 4-hour relay
    - More frequent signals (higher volume, potentially lower win rate)
    - Uses Bollinger Bands + RSI on Kraken data, executes on Polymarket
    """
    
    def __init__(
        self,
        discovery: MarketDiscovery,
        signal_matcher: SignalMatcher,
        order_placer: OrderPlacer,
    ):
        """Initialize fast strategy runner."""
        self.discovery = discovery
        self.signal_matcher = signal_matcher
        self.order_placer = order_placer
        
        # Assets to monitor with 1-hour candles
        self.fast_assets = {
            'SOL': 'Solana',
            'AVAX': 'Avalanche', 
            'XRP': 'XRP',
            'DOT': 'Polkadot',
            'LINK': 'Chainlink',
            'ADA': 'Cardano',
        }
        
        self.strategy = ScalpStrategy(
            bb_period=20, bb_std=2.0, rsi_period=14,
            ml_predictor=PricePredictor(),
        )
        self.scheduler = AsyncIOScheduler()
        self.active_positions: Dict[str, dict] = {}
        self.last_signals = {}  # Track last signal per asset to avoid duplicates
        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl = 0.0
        # Real spot price cache per asset — refreshed in manage_positions
        self._spot_cache: Dict[str, float] = {}
        self._spot_fetch_time: Dict[str, float] = {}
        
    def start(self):
        """Start the fast strategy scheduler."""
        logger.info("🚀 Starting fast 1-hour Polymarket strategy...")
        
        # Check for 1-hour signals every 60 minutes
        self.scheduler.add_job(
            self.check_fast_signals,
            'interval',
            minutes=60,
            id='fast_signals',
            name='1-Hour Fast Signal Check',
            max_instances=1,
        )
        
        # Manage positions every 5 minutes (tighter for faster trades)
        self.scheduler.add_job(
            self.manage_positions,
            'interval',
            minutes=5,
            id='fast_position_mgmt',
            name='Fast Position Management',
            max_instances=1,
        )
        
        self.scheduler.start()
        logger.info("✅ Fast strategy scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Fast strategy scheduler stopped")
    
    async def check_fast_signals(self):
        """Check for 1-hour signals on fast assets."""
        try:
            logger.info("⚡ Checking 1-hour signals on fast assets...")
            
            for asset, display_name in self.fast_assets.items():
                try:
                    await self._check_asset_signal(asset, display_name)
                except Exception as asset_err:
                    logger.warning(f"⚠️ Error checking {asset}: {type(asset_err).__name__}")
                    continue
        
        except asyncio.CancelledError:
            logger.info("Fast signal check cancelled")
        except Exception as e:
            logger.error(f"❌ Error in check_fast_signals: {type(e).__name__}: {e}", exc_info=False)
    
    async def _check_asset_signal(self, asset: str, display_name: str):
        """Check 1-hour signal for a specific asset."""
        try:
            # Map asset to Kraken pair
            kraken_pair_map = {
                'SOL': 'SOLUSD',
                'AVAX': 'AVAXUSD',
                'XRP': 'XRPUSD',
                'DOT': 'DOTUSD',
                'LINK': 'LINKUSD',
                'ADA': 'ADAUSD',
            }
            
            kraken_pair = kraken_pair_map.get(asset)
            if not kraken_pair:
                logger.warning(f"No Kraken pair for {asset}")
                return
            
            # Fetch 1-hour candles — need 48h to give BB period 20 enough data
            try:
                async with KrakenClient() as kraken:
                    df_1h = await asyncio.wait_for(
                        kraken.get_dataframe(pair=kraken_pair, interval=60, lookback_hours=48),
                        timeout=10.0
                    )
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Kraken timeout fetching {asset} 1-hour data")
                return
            except Exception as fetch_err:
                logger.warning(f"⚠️ Failed to fetch {asset} data: {type(fetch_err).__name__}")
                return
            
            if df_1h is None or df_1h.empty:
                logger.debug(f"No 1-hour data for {asset}")
                return
            
            current_price = df_1h['close'].iloc[-1]
            logger.info(f"⚡ 1H {asset}: ${current_price:,.2f} ({len(df_1h)} candles)")
            
            # Get signal using scalp strategy
            try:
                signal_result = self.strategy.get_signal(df_1h, df_1h)  # Use 1h as both timeframes
            except Exception as sig_err:
                logger.warning(f"❌ Signal error for {asset}: {type(sig_err).__name__}")
                return
            
            if signal_result.signal == TradeSignal.NO_SIGNAL:
                logger.debug(f"No signal for {asset} this cycle")
                return
            
            if signal_result.confidence < 0.50:
                logger.debug(f"{asset} confidence too low: {signal_result.confidence:.0%}")
                return
            
            # Avoid duplicate signals within 30 minutes
            last_time = self.last_signals.get(asset, 0)
            current_time = datetime.now().timestamp()
            if current_time - last_time < 1800:  # 30 minutes
                logger.debug(f"Skipping duplicate signal for {asset}")
                return
            
            # **SIGNAL FIRED!**
            logger.warning(f"⚡ 1H SIGNAL FIRED FOR {asset}!")
            logger.warning(f"   Direction: {signal_result.signal.value}")
            logger.warning(f"   Entry: ${signal_result.entry_price:,.2f}")
            logger.warning(f"   Stop Loss: ${signal_result.stop_loss:,.2f}")
            logger.warning(f"   Take Profit: ${signal_result.take_profit:,.2f}")
            logger.warning(f"   Confidence: {signal_result.confidence:.0%}")
            
            self.last_signals[asset] = current_time
            
            # Try to find matching Polymarket market and execute
            await self._execute_fast_signal(asset, signal_result, current_price)
        
        except Exception as e:
            logger.error(f"❌ Error checking {asset}: {type(e).__name__}: {e}", exc_info=False)
    
    async def _execute_fast_signal(self, asset: str, signal_result, current_price: float):
        """Open a paper position. Outcome recorded when price hits TP/SL in manage_positions."""
        try:
            if signal_result.signal == TradeSignal.BUY_YES:
                trade_signal = "BUY_YES"
            elif signal_result.signal == TradeSignal.BUY_NO:
                trade_signal = "BUY_NO"
            else:
                return

            import uuid
            trade_id = str(uuid.uuid4())[:8]
            self.active_positions[trade_id] = {
                "trade_id": trade_id,
                "asset": asset,
                "signal": trade_signal,
                "entry_price": signal_result.entry_price,
                "stop_loss": signal_result.stop_loss,
                "take_profit": signal_result.take_profit,
                "confidence": signal_result.confidence,
                "open_time": datetime.utcnow().timestamp(),
                "kraken_pair": {
                    "SOL": "SOLUSD", "AVAX": "AVAXUSD", "XRP": "XRPUSD",
                    "DOT": "DOTUSD", "LINK": "LINKUSD", "ADA": "ADAUSD",
                }.get(asset, ""),
            }
            logger.warning(
                f"📋 1H POSITION OPENED | {asset} {trade_signal} | "
                f"entry=${signal_result.entry_price:,.4f} | "
                f"TP=${signal_result.take_profit:,.4f} SL=${signal_result.stop_loss:,.4f} | "
                f"conf={signal_result.confidence:.0%}"
            )

        except Exception as e:
            logger.warning(f"Failed to open {asset} position: {e}")
    
    async def manage_positions(self):
        """Close positions when real Kraken spot hits TP or SL. Max hold 4 hours."""
        try:
            if not self.active_positions:
                return

            now = datetime.utcnow().timestamp()
            MAX_HOLD_SECONDS = 4 * 3600  # 4 hours

            for trade_id in list(self.active_positions.keys()):
                pos = self.active_positions[trade_id]
                asset = pos["asset"]
                pair = pos["kraken_pair"]
                if not pair:
                    continue

                # Refresh spot price once per 5-minute cycle per asset (cached)
                cache_age = now - self._spot_fetch_time.get(pair, 0)
                if cache_age >= 290:  # just under 5 min scheduler interval
                    try:
                        async with KrakenClient() as kraken:
                            df = await asyncio.wait_for(
                                kraken.get_dataframe(pair=pair, interval=5, lookback_hours=1),
                                timeout=8.0,
                            )
                        if not df.empty:
                            self._spot_cache[pair] = float(df["close"].iloc[-1])
                            self._spot_fetch_time[pair] = now
                    except Exception as fetch_err:
                        logger.debug(f"Spot refresh skipped for {pair}: {fetch_err}")

                current_spot = self._spot_cache.get(pair)
                if current_spot is None:
                    continue

                entry_price = pos["entry_price"]
                take_profit = pos["take_profit"]
                stop_loss   = pos["stop_loss"]
                signal      = pos["signal"]

                if signal == "BUY_YES":
                    hit_target = current_spot >= take_profit
                    hit_stop   = current_spot <= stop_loss
                else:  # BUY_NO: profit when price falls
                    hit_target = current_spot <= take_profit
                    hit_stop   = current_spot >= stop_loss

                timed_out    = (now - pos["open_time"]) >= MAX_HOLD_SECONDS
                if not (hit_target or hit_stop or timed_out):
                    continue

                # Close at real market price
                exit_price = current_spot
                if signal == "BUY_YES":
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:
                    pnl_pct = (entry_price - exit_price) / entry_price

                # Deduct real Polymarket taker fees.
                # NOTE: pnl_pct here is a Kraken spot % move (not a contract price move),
                # but the fee is still charged on Polymarket contract notional (~$10).
                # We apply ROUND_TRIP_FEE (2 × 1% = 2%) as a flat %‑of‑notional drag.
                gross_pnl_pct = pnl_pct
                pnl_pct = gross_pnl_pct - live_rules.ROUND_TRIP_FEE
                is_win    = bool(pnl_pct > 0)
                dollar_pnl = pnl_pct * 10.0
                reason    = "TARGET" if hit_target else ("STOP" if hit_stop else "TIMEOUT")

                outcome = "✅ WIN" if is_win else "❌ LOSS"
                held_min = (now - pos["open_time"]) / 60
                logger.warning(
                    f"{outcome} [{trade_id}] Poly Fast {asset} {reason}: {signal} | "
                    f"${entry_price:,.4f} → ${exit_price:,.4f} | "
                    f"gross={gross_pnl_pct:+.2%} fee={live_rules.ROUND_TRIP_FEE:.1%} net={pnl_pct:+.2%} | {held_min:.0f}m"
                )

                if is_win:
                    self._session_wins += 1
                else:
                    self._session_losses += 1
                self._session_pnl += pnl_pct

                try:
                    all_wins   = self._session_wins
                    all_losses = self._session_losses
                    await BotAlert.trade_result(
                        bot_name=f"Poly Fast {asset}",
                        asset=asset,
                        signal_type=signal,
                        pnl_pct=pnl_pct,
                        is_win=is_win,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        session_wins=self._session_wins,
                        session_losses=self._session_losses,
                        session_pnl=self._session_pnl,
                        all_time_wins=all_wins,
                        all_time_losses=all_losses,
                        streak=0,
                        dollar_pnl=dollar_pnl,
                    )
                except Exception as discord_err:
                    logger.debug(f"Discord error: {discord_err}")

                del self.active_positions[trade_id]

        except Exception as e:
            logger.error(f"❌ Error managing fast positions: {type(e).__name__}: {e}")
    
    async def run_forever(self):
        """Run the fast strategy indefinitely."""
        # Start the scheduler
        try:
            self.start()
            # Run signal check immediately on startup — don't wait 60 minutes
            await asyncio.sleep(3)
            await self.check_fast_signals()
        except Exception as start_err:
            logger.error(f"❌ Failed to start fast strategy: {type(start_err).__name__}", exc_info=True)
            raise
        
        # Main loop
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        try:
            while True:
                try:
                    await asyncio.sleep(1)
                    consecutive_errors = 0
                except asyncio.CancelledError:
                    logger.info("Fast strategy cancelled")
                    raise
                except Exception as inner_err:
                    consecutive_errors += 1
                    logger.error(
                        f"❌ Error in fast strategy loop ({consecutive_errors}/{max_consecutive_errors}): "
                        f"{type(inner_err).__name__}",
                        exc_info=False
                    )
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("Too many errors. Stopping fast strategy.")
                        raise
                    
                    backoff_time = min(2 ** (consecutive_errors - 1), 30)
                    await asyncio.sleep(backoff_time)
        
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("⏹️  Fast strategy shutting down...")
        finally:
            try:
                self.stop()
            except Exception:
                pass
            logger.info("Fast strategy stopped")
