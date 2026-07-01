"""All-day trading scheduler and strategy runner."""

import asyncio
import random
from datetime import datetime, time
from typing import Dict, List, Optional
from dataclasses import dataclass
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger
import numpy as np
import pandas as pd

from src.polymarket.client import PolymarketClient
from src.data.store import DataStore, OHLCV
from src.trading.engine import TradingEngine, OrderSide, OrderStatus
from src.trading.scalp_strategy import ScalpStrategy, TradeSignal
from src.trading.arbitrage import ArbitrageDetector
from src.data.kraken_client import KrakenClient
from src.config import PREDICTION_THRESHOLD
from src.tracking.performance_tracker import PerformanceTracker
from src.notifications.alerts import BotAlert
from src.ml.models import PricePredictor

# Note: get_discord_client import moved to inside methods to avoid module-level hang


class AllDayStrategyRunner:
    """
    Runs Percoco and arbitrage strategies 24/7.
    
    Monitors:
    - 1-minute charts for Percoco entry signals
    - 15-minute charts for bias confirmation
    - Order books for arbitrage opportunities
    - Market maker fees and rebates
    """

    def __init__(
        self,
        client: PolymarketClient,
        store: DataStore,
        engine: TradingEngine,
        kraken_pair: str = "XXBTZUSD",  # Default to BTC, can be XETHZUSD for ETH
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        """Initialize strategy runner."""
        self.client = client
        self.store = store
        self.engine = engine
        self.kraken_pair = kraken_pair  # Store the Kraken pair for data fetching
        self.loop = loop or asyncio.get_event_loop()  # Store the event loop for scheduler
        
        self.percoco = ScalpStrategy(
            bb_period=20, bb_std=2.0, rsi_period=14,
            ml_predictor=PricePredictor(),
        )
        self.arbitrage = ArbitrageDetector(min_return=0.01)
        
        self.scheduler = BackgroundScheduler()
        self.active_positions: Dict[str, dict] = {}
        self.trade_log = []
        
        # TRACKING & LEARNING
        self.tracker = PerformanceTracker()  # Track $500 → growth
        self.trade_history = []  # Complete record of all trades
        self.win_rate_by_confidence = {}  # {confidence_level: {wins, losses, win_rate}}
        self.win_rate_by_bias = {}  # {bias_type: {wins, losses, win_rate}}
        self.dynamic_min_confidence = 0.50  # Starts loose, tightens if needed

        # Real-time spot price cache for position tracking
        self._current_spot_price = None
        self._last_price_fetch = datetime.utcfromtimestamp(0)

    def start(self):
        """Start the all-day scheduler."""
        logger.info("🚀 Starting all-day strategy runner...")
        
        # Track if Polymarket is available
        self.polymarket_available = True
        
        # Add jobs to run throughout the day
        # 4-HOUR STRATEGY: Check for direction signals every 1 hour
        logger.debug("Adding percoco signal check job (every 60 minutes)")
        self.scheduler.add_job(
            self._check_percoco_signals_sync,  # Use wrapper, not async method
            'interval',
            minutes=60,
            id='percoco_check',
            name='4-Hour Signal Check',
            max_instances=1,
        )
        
        # Check for arbitrage every 30 seconds (skip if previous still running)
        # NOTE: Disabled if Polymarket is not accessible - core strategy uses Kraken data
        if self.polymarket_available:
            logger.debug("Adding arbitrage check job (every 30 seconds)")
            self.scheduler.add_job(
                self._check_arbitrage_sync,  # Use wrapper, not async method
                'interval',
                seconds=30,
                id='arbitrage_check',
                name='Arbitrage Detection',
                max_instances=1,
            )
        
        # FAST: Manage positions every 20 seconds (quick exits for scalps)
        logger.debug("Adding position management job (every 20 seconds)")
        self.scheduler.add_job(
            self._manage_positions_sync,  # Use wrapper, not async method
            'interval',
            seconds=20,
            id='position_management',
            name='Position Management',
            max_instances=1,
        )
        
        # LEARNING: Evaluate win rates every 10 minutes
        logger.debug("Adding learning evaluation job (every 10 minutes)")
        self.scheduler.add_job(
            self._learn_from_trades_sync,  # Use wrapper, not async method
            'interval',
            minutes=10,
            id='learning_eval',
            name='Learning Evaluation',
            max_instances=1,
        )
        
        # Log performance metrics every hour
        logger.debug("Adding performance logging job (every hour)")
        self.scheduler.add_job(
            self._log_performance_sync,  # Use wrapper, not async method
            'interval',
            hours=1,
            id='performance_logging',
            name='Performance Logging',
            max_instances=1,
        )
        
        logger.debug("Calling scheduler.start()")
        self.scheduler.start()
        logger.debug("All-day scheduler started successfully")
        logger.info("✅ All-day scheduler started")

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    # WRAPPER METHODS FOR SCHEDULER (must be sync, not async)
    # These schedule async methods on the event loop
    
    def _check_percoco_signals_sync(self):
        """Synchronous wrapper to schedule async signal check on event loop."""
        logger.info("🔄 4H signal check triggered by scheduler")
        try:
            logger.debug("Scheduling signal check on event loop")
            # Schedule the async method on the event loop (thread-safe)
            future = asyncio.run_coroutine_threadsafe(
                self.check_percoco_signals(),
                self.loop
            )
            logger.debug("Waiting for signal check to complete (timeout=65s)")
            # Wait for completion with timeout
            result = future.result(timeout=65)
            logger.debug("Signal check completed successfully")
        except asyncio.TimeoutError:
            logger.debug("Signal check TIMED OUT")
            logger.error("⏱️ Signal check timed out")
        except Exception as e:
            logger.debug(f"Signal check wrapper ERROR: {type(e).__name__}: {e}")
            logger.error(f"❌ Signal check wrapper error: {type(e).__name__}: {e}")

    def _manage_positions_sync(self):
        """Synchronous wrapper to schedule async position management on event loop."""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.manage_positions(),
                self.loop
            )
            future.result(timeout=25)
        except asyncio.TimeoutError:
            logger.error("⏱️ Position management timed out")
        except Exception as e:
            logger.error(f"❌ Position management wrapper error: {type(e).__name__}: {e}")

    def _check_arbitrage_sync(self):
        """Synchronous wrapper to schedule async arbitrage check on event loop."""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.check_arbitrage(),
                self.loop
            )
            future.result(timeout=35)
        except asyncio.TimeoutError:
            logger.error("⏱️ Arbitrage check timed out")
        except Exception as e:
            logger.error(f"❌ Arbitrage check wrapper error: {type(e).__name__}: {e}")

    def _learn_from_trades_sync(self):
        """Synchronous wrapper to schedule async learning on event loop."""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.learn_from_trades(),
                self.loop
            )
            future.result(timeout=95)
        except asyncio.TimeoutError:
            logger.error("⏱️ Learning evaluation timed out")
        except Exception as e:
            logger.error(f"❌ Learning evaluation wrapper error: {type(e).__name__}: {e}")

    def _log_performance_sync(self):
        """Synchronous wrapper to schedule async performance logging on event loop."""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.log_performance(),
                self.loop
            )
            future.result(timeout=35)
        except asyncio.TimeoutError:
            logger.error("⏱️ Performance logging timed out")
        except Exception as e:
            logger.error(f"❌ Performance logging wrapper error: {type(e).__name__}: {e}")


    async def check_percoco_signals(self):
        """Check for 4-hour direction trade signals with retry logic."""
        try:
            asset_name = "Bitcoin" if self.kraken_pair == "XXBTZUSD" else "Ethereum"
            logger.info(f"🔍 Checking 4H {asset_name} signals from Kraken...")

            # Fetch TRUE 4-hour (240 min) candles — 20 candles = 80 hours of context,
            # giving the BB/RSI proper signal resolution for a 4-hour hold.
            # Also fetch daily candles for longer-term trend bias.
            df_4h = None
            df_1d = None

            for retry_attempt in range(3):
                try:
                    async with KrakenClient() as kraken:
                        df_4h = await asyncio.wait_for(
                            kraken.get_dataframe(pair=self.kraken_pair, interval=240, lookback_hours=480),
                            timeout=12.0
                        )
                        df_1d = await asyncio.wait_for(
                            kraken.get_dataframe(pair=self.kraken_pair, interval=1440, lookback_hours=720),
                            timeout=12.0
                        )
                    break  # Success, exit retry loop
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Kraken timeout (attempt {retry_attempt + 1}/3)")
                    if retry_attempt < 2:
                        await asyncio.sleep(2 ** retry_attempt)
                    else:
                        raise
                except Exception as e:
                    logger.warning(f"⚠️ Kraken error (attempt {retry_attempt + 1}/3): {type(e).__name__}: {str(e)[:100]}")
                    if retry_attempt < 2:
                        await asyncio.sleep(2 ** retry_attempt)
                    else:
                        raise

            if df_4h is None or df_4h.empty:
                logger.warning("❌ Kraken returned no 4-hour data after retries")
                return

            if df_1d is None or df_1d.empty:
                logger.warning("No daily data available, using 4h as trend reference")
                df_1d = df_4h

            # Rename for compatibility (strategy param names are df_5m / df_1h but any OHLCV works)
            df_1h = df_4h

            current_price = df_1h['close'].iloc[-1]
            logger.info(f"✅ Got {len(df_1h)} 4H candles | {asset_name}: ${current_price:,.2f}")

            # Use Bollinger Bands + RSI 4-hour direction strategy
            try:
                signal_result = self.percoco.get_signal(df_1h, df_1d)
            except Exception as sig_err:
                logger.error(f"❌ Signal calculation error: {type(sig_err).__name__}: {sig_err}", exc_info=False)
                return
            
            # Log the signal
            logger.info(f"📊 4H Signal: {signal_result.signal.value} | Confidence: {signal_result.confidence:.0%} | RSI: {signal_result.rsi:.0f}")

            # LEARNING FILTER: Use dynamic confidence threshold
            if signal_result.signal == TradeSignal.NO_SIGNAL:
                logger.info(f"  ↳ No signal — RSI={signal_result.rsi:.0f}, BB={signal_result.bb_pct:+.1f}%")
                return

            if signal_result.confidence < self.dynamic_min_confidence:
                logger.info(f"  ↳ Confidence {signal_result.confidence:.0%} below threshold {self.dynamic_min_confidence:.0%} — skipping")
                return

            # Signal passed! Log it clearly
            logger.info(f"🎯 4-HOUR SIGNAL FIRED: {signal_result.signal.value}")
            logger.info(f"   Entry: ${signal_result.entry_price:,.2f}  |  SL: ${signal_result.stop_loss:,.2f}  |  TP: ${signal_result.take_profit:,.2f}")
            logger.info(f"   Confidence: {signal_result.confidence:.0%}  |  RSI: {signal_result.rsi:.0f}  |  {signal_result.reasoning}")
            
            # Record the signal (paper trade)
            await self._paper_trade_signal(signal_result)
            
        except asyncio.CancelledError:
            logger.info("Signal check cancelled")
        except (ConnectionError, TimeoutError, OSError) as net_err:
            logger.warning(f"🌐 Network error fetching signals: {type(net_err).__name__}")
        except Exception as e:
            logger.error(f"❌ Unexpected error in check_percoco_signals: {type(e).__name__}: {e}", exc_info=True)
    
    async def _paper_trade_signal(self, signal_result):
        """Record a paper trade signal for backtesting."""
        try:
            # DEDUP GUARD: Don't open a new position if one is already active
            if self.active_positions:
                logger.info(f"⏭️  Skipping signal — {len(self.active_positions)} position(s) already open")
                return

            trade_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "signal": signal_result.signal.value,
                "entry_price": signal_result.entry_price,
                "stop_loss": signal_result.stop_loss,
                "take_profit": signal_result.take_profit,
                "confidence": signal_result.confidence,
                "rsi": signal_result.rsi,
                "bb_pct": signal_result.bb_pct,
                "uptrend": signal_result.uptrend,
                "volume_conf": signal_result.volume_conf,
                "reasoning": signal_result.reasoning,
                "entry_time": datetime.utcnow(),
            }
            self.trade_log.append(trade_entry)
            
            # **CRITICAL FIX**: Add to active positions for exit tracking
            import uuid
            trade_id = str(uuid.uuid4())[:8]
            self.active_positions[trade_id] = {
                "trade_id": trade_id,
                "signal": signal_result.signal.value,
                "entry_price": signal_result.entry_price,
                "entry_time": datetime.utcnow(),
                "stop_loss": signal_result.stop_loss,
                "take_profit": signal_result.take_profit,
                "confidence": signal_result.confidence,
                "status": "open",
                "steps": 0,
            }
            logger.info(f"Recorded paper trade [{trade_id}]: {signal_result.signal.value} at ${signal_result.entry_price:,.2f}")
            
            # Send Discord notification if webhook is configured
            from src.notifications.discord_webhook import get_discord_client
            discord = get_discord_client()
            if discord.enabled:
                asset = "ETH" if "ETH" in self.kraken_pair else "BTC"
                bot_name = f"{asset} Bot"
                
                asyncio.create_task(
                    discord.send_trade_alert(
                        bot_name=bot_name,
                        signal=signal_result.signal.value,
                        entry_price=signal_result.entry_price,
                        stop_loss=signal_result.stop_loss,
                        take_profit=signal_result.take_profit,
                        confidence=signal_result.confidence,
                        asset=asset,
                        rsi=signal_result.rsi,
                        bb_pct=signal_result.bb_pct,
                        uptrend=signal_result.uptrend,
                        volume_conf=signal_result.volume_conf,
                        reasoning=signal_result.reasoning,
                    )
                )
        except Exception as e:
            logger.error(f"Error recording paper trade: {e}")
    
    async def _send_startup_notification(self):
        """Send bot startup notification to Discord."""
        try:
            await asyncio.sleep(1)  # Wait a moment for scheduler to fully initialize
            from src.notifications.discord_webhook import get_discord_client
            discord = get_discord_client()
            if discord.enabled:
                asset = "ETH" if "ETH" in self.kraken_pair else "BTC"
                bot_name = f"{asset} Bot"
                await discord.send_startup_alert(
                    bot_name=bot_name,
                    strategy="4-Hour Bollinger Bands + RSI",
                    mode="Paper Trading"
                )
        except Exception as e:
            logger.warning(f"Could not send Discord startup notification: {e}")

    async def check_arbitrage(self):
        """Check for arbitrage opportunities (simplified to not block)."""
        try:
            # Skip if Polymarket not available
            if not self.polymarket_available:
                return
                
            # Try with short timeout
            try:
                markets = await asyncio.wait_for(
                    self.client.get_markets("Bitcoin"),
                    timeout=3.0  # 3 second timeout
                )
            except asyncio.TimeoutError:
                logger.debug("Polymarket request timed out")
                self.polymarket_available = False
                return
                
            if not markets or len(markets) < 2:
                return
            
            # Pick 1-2 random markets and check spreads
            sample = random.sample(markets, min(2, len(markets)))
            
            for market in sample:
                # Just log the price, don't fetch orderbook
                if hasattr(market, 'last_price'):
                    logger.debug(f"Market {market.id[:8]}: ${market.last_price:.4f}")
            
            logger.debug("Arbitrage check completed")
            
        except Exception as e:
            # Fail silently for non-critical arbitrage checks
            logger.debug(f"Arbitrage check skipped: network issue (OK for Kraken-only strategy)")


    async def manage_positions(self):
        """Track open positions against real Kraken spot price data.

        Entry price is the actual BTC/ETH spot price recorded when the signal fired.
        Each 20-second tick we compare the current real Kraken price against the
        stop_loss and take_profit levels (also stored as spot prices) to determine
        outcome honesty — no random coin flips.
        """
        try:
            if not self.active_positions:
                return

            # Refresh spot price at most once per minute to respect Kraken rate limits.
            now = datetime.utcnow()
            cache_age = (now - self._last_price_fetch).total_seconds()
            if cache_age >= 60:
                try:
                    async with KrakenClient() as kraken:
                        df = await asyncio.wait_for(
                            kraken.get_dataframe(pair=self.kraken_pair, interval=5, lookback_hours=1),
                            timeout=8.0,
                        )
                    if not df.empty:
                        self._current_spot_price = float(df["close"].iloc[-1])
                        self._last_price_fetch = now
                        logger.debug(f"Spot price refreshed: {self.kraken_pair} = {self._current_spot_price:,.2f}")
                except Exception as price_err:
                    logger.debug(f"Spot price refresh skipped: {price_err}")

            current_spot = self._current_spot_price
            if current_spot is None:
                return  # Cannot evaluate positions without a real price

            # Force-close positions held longer than 4 hours (240 min)
            MAX_HOLD_MINUTES = 240
            asset = "ETH" if "ETH" in self.kraken_pair else "BTC"

            for trade_id in list(self.active_positions.keys()):
                pos = self.active_positions[trade_id]
                if pos["status"] != "open":
                    del self.active_positions[trade_id]
                    continue

                pos["steps"] += 1
                entry_price = pos["entry_price"]   # real BTC/ETH spot price at entry
                take_profit = pos["take_profit"]   # real spot price take-profit level
                stop_loss   = pos["stop_loss"]     # real spot price stop-loss level
                signal      = pos["signal"]        # "BUY_YES" or "BUY_NO"

                # BUY_YES = long (profit when BTC rises)
                # BUY_NO  = short (profit when BTC falls)
                if signal == "BUY_YES":
                    hit_target = current_spot >= take_profit
                    hit_stop   = current_spot <= stop_loss
                else:
                    hit_target = current_spot <= take_profit
                    hit_stop   = current_spot >= stop_loss

                minutes_held = pos["steps"] * 20 / 60
                timed_out    = minutes_held >= MAX_HOLD_MINUTES

                if not (hit_target or hit_stop or timed_out):
                    continue

                # --- Close position at real market price ---
                exit_price = current_spot
                if signal == "BUY_YES":
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:  # BUY_NO: profit when price falls
                    pnl_pct = (entry_price - exit_price) / entry_price
                is_win = pnl_pct > 0

                reason  = "TARGET" if hit_target else ("STOP" if hit_stop else f"TIMEOUT({minutes_held:.0f}m)")
                outcome = "✅ WIN" if is_win else "❌ LOSS"
                logger.info(
                    f"{outcome} [{trade_id}] {reason}: {signal} | "
                    f"${entry_price:,.2f} → ${exit_price:,.2f} | {pnl_pct:+.2%} | {minutes_held:.0f}m held"
                )

                try:
                    session_wins   = 0
                    session_losses = 0
                    all_wins       = 0
                    all_losses     = 0
                    session_pnl    = 0.0
                    streak         = 0
                    await BotAlert.trade_result(
                        bot_name=f"{asset} Bot",
                        asset=asset,
                        signal_type=signal,
                        pnl_pct=pnl_pct,
                        is_win=is_win,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        session_wins=session_wins,
                        session_losses=session_losses,
                        session_pnl=session_pnl,
                        all_time_wins=all_wins,
                        all_time_losses=all_losses,
                        streak=streak,
                    )
                except Exception as _alert_err:
                    logger.debug(f"Discord alert error: {_alert_err}")

                pos["status"] = "closed"
                del self.active_positions[trade_id]

        except Exception as e:
            logger.error(f"Error managing positions: {type(e).__name__}: {e}")

    def _record_trade(self, order_id, signal, entry, exit, pnl, confidence, win):
        """Record trade for LEARNING SYSTEM."""
        trade_record = {
            "order_id": order_id,
            "signal": signal,
            "entry": entry,
            "exit": exit,
            "pnl": pnl,
            "confidence": confidence,
            "win": win,
            "timestamp": datetime.utcnow(),
        }
        self.trade_history.append(trade_record)
        
        # ROBUST TRACKING: Record to tracker
        self.tracker.record_trade(
            order_id=order_id,
            signal=signal,
            entry=entry,
            exit=exit,
            pnl_pct=pnl,
            confidence=confidence,
            win=win,
            size=3,  # Position size
        )
        
        logger.info(f"📊 RECORDED: {signal} @ {confidence:.0%} = {'+' if win else ''}{pnl*100:.2f}% PnL")
        
        # Send win/loss result to Discord
        try:
            from src.notifications.discord_webhook import get_discord_client
            discord = get_discord_client()
            if discord.enabled:
                asset = "ETH" if "ETH" in self.kraken_pair else "BTC"

                session_wins = sum(1 for t in self.trade_history if t.get("win"))
                session_losses = sum(1 for t in self.trade_history if not t.get("win"))

                # Calculate duration from matching trade_log entry
                duration_minutes = 0
                for tl in reversed(self.trade_log):
                    if abs(tl.get("entry_price", 0) - entry) < 1 and tl.get("signal") == signal:
                        entry_time = tl.get("entry_time")
                        if entry_time:
                            duration_minutes = int((datetime.utcnow() - entry_time).total_seconds() / 60)
                        break

                asyncio.run_coroutine_threadsafe(
                    discord.send_trade_result(
                        bot_name=f"{asset} Bot",
                        signal=signal,
                        entry_price=entry,
                        exit_price=exit,
                        pnl_pct=pnl,
                        confidence=confidence,
                        win=win,
                        asset=asset,
                        session_wins=session_wins,
                        session_losses=session_losses,
                        duration_minutes=duration_minutes,
                    ),
                    self.loop,
                )
        except Exception as e:
            logger.warning(f"Could not send trade result to Discord: {e}")

    async def learn_from_trades(self):
        """LEARNING SYSTEM: Analyze trades & adapt thresholds (every 10 min)."""
        try:
            if len(self.trade_history) < 5:
                logger.debug(f"Learning: {len(self.trade_history)} trades (need 5+)")
                return
            
            # Calculate win rate by confidence level
            high_conf = [t for t in self.trade_history if t['confidence'] >= 0.75]
            med_conf = [t for t in self.trade_history if 0.60 <= t['confidence'] < 0.75]
            low_conf = [t for t in self.trade_history if t['confidence'] < 0.60]
            
            def calc_wr(trades):
                if not trades:
                    return 0
                return sum(1 for t in trades if t['win']) / len(trades)
            
            high_wr = calc_wr(high_conf)
            med_wr = calc_wr(med_conf)
            low_wr = calc_wr(low_conf)
            
            # ADAPT: Increase threshold if low-conf is losing
            if low_wr < 0.30 and len(low_conf) > 3:
                self.dynamic_min_confidence = min(0.70, self.dynamic_min_confidence + 0.02)
                logger.warning(
                    f"📈 LEARNING: Low conf trades losing ({low_wr:.0%}). "
                    f"Raising threshold to {self.dynamic_min_confidence:.0%}"
                )
            
            # ADAPT: Decrease threshold if we're not getting enough trades
            if len(self.trade_history) < 10 and self.dynamic_min_confidence > 0.50:
                self.dynamic_min_confidence = max(0.50, self.dynamic_min_confidence - 0.01)
                logger.warning(
                    f"📉 LEARNING: Only {len(self.trade_history)} trades. "
                    f"Lowering threshold to {self.dynamic_min_confidence:.0%}"
                )
            
            # Log learning status
            total_trades = len(self.trade_history)
            total_wins = sum(1 for t in self.trade_history if t['win'])
            win_rate = total_wins / total_trades if total_trades > 0 else 0
            
            logger.info(
                f"🧠 LEARNING STATUS:\n"
                f"  Total trades: {total_trades}\n"
                f"  Win rate: {win_rate:.1%}\n"
                f"  High conf (75%+): {high_wr:.1%} ({len(high_conf)} trades)\n"
                f"  Med conf (60-75%): {med_wr:.1%} ({len(med_conf)} trades)\n"
                f"  Low conf (<60%): {low_wr:.1%} ({len(low_conf)} trades)\n"
                f"  Dynamic threshold: {self.dynamic_min_confidence:.0%}"
            )
        
        except Exception as e:
            logger.error(f"Error in learning: {e}")

    async def log_performance(self):
        """Log portfolio performance metrics and send Discord heartbeat."""
        try:
            # Use tracker for comprehensive metrics
            self.tracker.print_summary()

            metrics = self.engine.get_portfolio_metrics()
            asset = "ETH" if "ETH" in self.kraken_pair else "BTC"
            bot_name = f"{asset} Bot"

            total_trades = len(self.trade_history)
            total_wins = sum(1 for t in self.trade_history if t.get("win"))
            win_rate_pct = (total_wins / total_trades * 100) if total_trades > 0 else 0.0
            open_positions = len(self.active_positions)

            logger.info(
                f"\n{'='*60}\n"
                f"📊 HOURLY STATUS — {bot_name}\n"
                f"  Balance: ${metrics.total_balance:.2f}\n"
                f"  P&L: ${metrics.total_pnl:.2f} ({metrics.total_pnl_pct*100:.2f}%)\n"
                f"  Open Positions: {open_positions}\n"
                f"  Total Trades: {total_trades} | Win Rate: {win_rate_pct:.1f}%\n"
                f"  Confidence Threshold: {self.dynamic_min_confidence:.0%}\n"
                f"{'='*60}\n"
            )

            # Send hourly heartbeat to Discord so you know the bot is alive
            try:
                from src.notifications.discord_webhook import get_discord_client
                discord = get_discord_client()
                if discord.enabled:
                    total = len(self.trade_history)
                    wins = sum(1 for t in self.trade_history if t.get("win"))
                    record_str = f"{wins}W/{total - wins}L" if total else ""
                    color = 0x00AA44 if metrics.total_pnl >= 0 else 0xFF4444
                    pnl_sign = "+" if metrics.total_pnl >= 0 else ""
                    embed = {
                        "title": f"⏱️ {bot_name} — Hourly Update",
                        "color": color,
                        "fields": [
                            {"name": "💰 Balance", "value": f"${metrics.total_balance:,.2f}", "inline": True},
                            {"name": "📈 P&L", "value": f"{pnl_sign}${metrics.total_pnl:.2f} ({pnl_sign}{metrics.total_pnl_pct*100:.2f}%)", "inline": True},
                            {"name": "📊 Open Positions", "value": str(open_positions), "inline": True},
                            {"name": "🏆 Session Record", "value": record_str if record_str else "No trades yet", "inline": False},
                            {"name": "🔍 Confidence Threshold", "value": f"{self.dynamic_min_confidence:.0%}", "inline": True},
                            {"name": "✅ Status", "value": "🟢 Running", "inline": True},
                        ],
                        "footer": {"text": f"Paper Trading | {bot_name} | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    await discord.send_message(embed=embed)
                    logger.info("📨 Sent hourly Discord heartbeat")
            except Exception as discord_err:
                logger.debug(f"Discord heartbeat skipped: {discord_err}")

        except Exception as e:
            logger.error(f"Error logging performance: {e}")

    def _generate_synthetic_data(self, n: int = 50) -> pd.DataFrame:
        """Generate synthetic OHLCV data for testing."""
        try:
            bases = 0.45 + np.random.randn(n) * 0.05
            df = pd.DataFrame({
                'open': np.clip(bases, 0.1, 0.9),
                'high': np.clip(bases + np.abs(np.random.randn(n) * 0.02), 0.1, 0.9),
                'low': np.clip(bases - np.abs(np.random.randn(n) * 0.02), 0.1, 0.9),
                'close': np.clip(bases + np.random.randn(n) * 0.01, 0.1, 0.9),
                'volume': np.random.randint(100, 10000, n),
            })
            return df
        except Exception as e:
            logger.error(f"Error generating synthetic data: {e}")
            return pd.DataFrame()

    async def run_forever(self):
        """Run the strategy runner indefinitely."""
        # Start the scheduler (sync)
        self.start()
        
        # Send startup notification now that we're in async context
        try:
            await self._send_startup_notification()
        except Exception as e:
            logger.warning(f"Failed to send startup notification: {e}")
        
        # Main loop with error resilience
        try:
            while True:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.info("Trading loop cancelled")
                    raise
                except Exception as inner_e:
                    logger.error(f"Error in sleep/wait cycle: {inner_e}", exc_info=False)
                    await asyncio.sleep(1)  # Prevent tight loop on repeated errors
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutting down...")
            self.stop()
        except Exception as e:
            logger.error(f"Unexpected error in run_forever: {e}", exc_info=True)
            self.stop()
            raise

