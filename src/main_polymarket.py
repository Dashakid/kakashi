"""Main entry point for the Polymarket trading bot - HYBRID MODE."""

import asyncio
import os
import signal
from datetime import datetime
from loguru import logger

from src.config import LOG_LEVEL, PAPER_TRADING
from src.polymarket.orchestrator import PolymarketOrchestrator
from src.polymarket.client import PolymarketClient
from src.polymarket.wallet_manager import CredentialManager, WalletManager
from src.polymarket.order_placer import OrderPlacer
from src.polymarket.exit_manager import ExitManager
from src.polymarket.market_discovery import MarketDiscovery
from src.polymarket.signal_matcher import SignalMatcher
from src.polymarket.fast_strategy_runner import FastPolymarketRunner
from src.tracking.performance_tracker import PerformanceTracker
from src.notifications.alerts import BotAlert


# Configure logging for Polymarket bot
LOG_FILE_POLYMARKET = "logs/trading_polymarket.log"
os.makedirs(os.path.dirname(LOG_FILE_POLYMARKET) or "logs", exist_ok=True)
logger.remove()
logger.add(
    LOG_FILE_POLYMARKET,
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)
logger.add(
    lambda msg: print(msg.rstrip(), flush=True),
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
)


async def main():
    """Main entry point - runs Polymarket direction strategy."""
    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    logger.info(f"🚀 Starting Polymarket Direction Bot [{mode}]")
    logger.info("📈 AI Direction Detection Strategy - Multi-Asset")
    logger.info("⚡ Trading direction markets on Polymarket")
    
    # Initialize components
    logger.info("🔧 Initializing Polymarket components...")
    try:
        client = PolymarketClient()
        logger.info("  ✅ PolymarketClient initialized")
        
        # **CRITICAL**: Initialize async session
        await client.initialize()
        logger.info("  ✅ PolymarketClient async session ready")
        
        cred_manager = CredentialManager()
        logger.info("  ✅ CredentialManager initialized")
        
        order_placer = OrderPlacer(client, max_order_size=5000.0)
        logger.info("  ✅ OrderPlacer initialized")
        
        wallet_mgr = WalletManager(client, cred_manager)
        logger.info("  ✅ WalletManager initialized")
        
        orchestrator = PolymarketOrchestrator(order_placer, wallet_mgr)
        logger.info("  ✅ PolymarketOrchestrator initialized")
        
        tracker = PerformanceTracker()
        logger.info("  ✅ PerformanceTracker initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize components: {e}", exc_info=True)
        raise
    
    # Send startup alert to Discord
    await BotAlert.startup(
        bot_name="Polymarket Bot",
        strategy="Hybrid Mode: 4-Hour Relay + 1-Hour Fast Strategy",
        mode=mode
    )
    
    try:
        # Show strategy details - HYBRID MODE
        logger.info("=" * 70)
        logger.info("POLYMARKET HYBRID STRATEGY - 4H RELAY + 1H FAST:")
        logger.info("=" * 70)
        logger.info("┌─ 4-HOUR RELAY (BTC/ETH only - synced with main bots):")
        logger.info("│  ├─ Signal: Bollinger Bands + RSI on 4-hour Kraken data")
        logger.info("│  ├─ Frequency: ~2 signals/day per asset (high conviction)")
        logger.info("│  ├─ Trade: Execute matching BTC/ETH markets on Polymarket")
        logger.info("│  └─ Win Rate Target: 55%+")
        logger.info("└─ 1-HOUR FAST (SOL, AVAX, XRP, DOT, LINK, ADA):")
        logger.info("   ├─ Signal: Bollinger Bands + RSI on 1-hour Kraken data")
        logger.info("   ├─ Frequency: ~2-4 signals/day per asset (faster turnaround)")
        logger.info("   ├─ Trade: Execute matching markets on Polymarket")
        logger.info("   └─ Win Rate Target: 50%+ (faster = higher variance)")
        logger.info("=" * 70)
        logger.info("EXPECTED OUTPUT: 10-15 total signals/day across hybrid strategy")
        logger.info("=" * 70 + "\n")
        
        # Initialize market discovery (now supports more assets)
        logger.info("🔍 Initializing market discovery (expanded to 8 assets)...")
        discovery = MarketDiscovery(client)
        markets = await discovery.discover_markets(force_refresh=True)
        logger.info(f"✅ Discovered {len(markets)} Polymarket direction markets")
        
        # Initialize signal matcher
        signal_matcher = SignalMatcher(discovery)
        logger.info("✅ Signal matcher ready")
        
        # Run market monitoring loop with error handling
        logger.info("🚀 Starting hybrid Polymarket strategy...")
        monitor_interval = 30  # Check every 30 seconds
        
        # **NEW: Start fast strategy runner for 1-hour signals**
        fast_runner = FastPolymarketRunner(discovery, signal_matcher, order_placer)
        fast_strategy_task = asyncio.create_task(fast_runner.run_forever())
        logger.info("✅ Fast 1-hour strategy runner started (parallel)")
        
        # Main relay loop (4-hour signals from BTC/ETH bots)
        try:
            while True:
                try:
                    # Periodically refresh market discovery
                    logger.debug("📊 Refreshing market discoveries...")
                    markets = await orchestrator.market_discovery.discover_markets()
                    active_markets = [m for m in markets.values() if m.is_active() and m.is_liquid()]
                    logger.debug(f"💰 Found {len(active_markets)} active liquid markets")
                    
                    # Check and process active positions
                    active_positions = orchestrator.get_active_positions()
                    for position in active_positions:
                        logger.debug(f"📈 Monitoring position: {position['market_id']}")
                        try:
                            exits = orchestrator.check_and_execute_exits(
                                position['order_id'], 
                                position.get('current_price', 0)
                            )
                            if exits:
                                logger.info(f"✅ Executed {len(exits)} exits")
                        except Exception as e:
                            logger.warning(f"Error checking exits: {e}")
                    
                    # Wait before next check
                    await asyncio.sleep(monitor_interval)
                    
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}", exc_info=False)
                    await asyncio.sleep(monitor_interval)
        except Exception as e:
            logger.error(f"❌ Market monitoring crashed: {type(e).__name__}: {e}", exc_info=True)
        finally:
            # Clean up fast strategy on exit
            if not fast_strategy_task.done():
                fast_strategy_task.cancel()
                try:
                    await fast_strategy_task
                except asyncio.CancelledError:
                    logger.info("Fast strategy task cancelled")
    
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down gracefully...")
        await BotAlert.shutdown(bot_name="Polymarket Bot", reason="Manual shutdown (KeyboardInterrupt)")
    
    except Exception as e:
        logger.error(f"❌ Critical error: {e}", exc_info=True)
        # NOTE: Do NOT send crash alert here - bot_monitor handles crash detection and alerting
        # Sending alerts here causes duplicate notifications
    
    finally:
        logger.info("Polymarket trading bot stopped")
        # Close client session
        if client and client.session:
            await client.close()
            logger.info("Closed Polymarket client session")


if __name__ == "__main__":
    import sys
    
    # Handle Docker graceful shutdown
    def shutdown_handler(signum, frame):
        """Handle SIGTERM (Docker) and SIGINT (Ctrl+C)."""
        logger.info(f"⏹️  Received signal {signum}. Shutting down gracefully...")
        raise KeyboardInterrupt("Signal received")
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)  # Docker stop
    signal.signal(signal.SIGINT, shutdown_handler)   # Ctrl+C
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️  Bot interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unhandled error: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)