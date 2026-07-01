"""Main entry point for the trading bot."""

import asyncio
import os
import signal
from datetime import datetime
from loguru import logger

from src.config import LOG_LEVEL, LOG_FILE, PAPER_TRADING
from src.polymarket.client import PolymarketClient
from src.data.store import DataStore, DataCollector
from src.ml.models import PricePredictor
from src.trading.engine import TradingEngine
from src.simulator.backtest import PaperTradingSimulator
from src.simulator.all_day_runner import AllDayStrategyRunner
from src.notifications.alerts import BotAlert


# EARLY OUTPUT FOR DEBUGGING (before any blocking operations)
print("🤖 Trading bot starting... (Python process initialized)", flush=True)

# Configure logging
os.makedirs(os.path.dirname(LOG_FILE) or "logs", exist_ok=True)
logger.remove()
logger.add(
    LOG_FILE,
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)
logger.add(
    lambda msg: print(msg.rstrip(), flush=True),
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
)


async def main():
    """Main entry point - runs 4-hour Bitcoin direction strategy."""
    print("🤖 Starting async main()...", flush=True)
    
    try:
        # FIRST log message to detect if we reach here
        logger.info(f"🚀 Starting 4-Hour Bitcoin Direction Bot")
        logger.info("⏳ Initializing components...")
        
        # Initialize components
        logger.debug("📦 Creating PolymarketClient...")
        client = PolymarketClient()
        
        logger.debug("💾 Creating DataStore (initializing database)...")
        try:
            store = DataStore()
            logger.debug("✅ DataStore initialized")
        except Exception as db_err:
            logger.error(f"❌ Database initialization failed: {db_err}")
            logger.error("Check if data/trading.db is corrupted. Try: rm data/trading.db")
            raise
        
        logger.debug("🎯 Creating TradingEngine...")
        engine = TradingEngine()
        logger.info("✅ All components initialized")
        
        mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
        logger.info(f"📈 Bollinger Bands + RSI Strategy on 4-Hour Candles [{mode}]")
        logger.info("⚡ Trading BTC direction markets on Polymarket (via Kraken data)")
        
        # Send startup alert to Discord
        await BotAlert.startup(
            bot_name="BTC Bot",
            strategy="4-Hour Bollinger Bands + RSI",
            mode=mode
        )
        
        # Try to connect to Polymarket (optional for 4-hour Kraken validation)
        try:
            # Timeout after 5 seconds to avoid hanging
            logger.info("Connecting to Polymarket...")
            await asyncio.wait_for(client.initialize(), timeout=5.0)
            markets = await asyncio.wait_for(client.get_markets("Bitcoin"), timeout=5.0)
            logger.info(f"✅ Polymarket connected. Found {len(markets)} Bitcoin markets")
        except asyncio.TimeoutError:
            logger.warning("⏱️  Polymarket timeout (5s). Running Kraken-only validation mode...")
            markets = []
        except Exception as e:
            logger.warning(f"⚠️  Polymarket connection failed ({type(e).__name__}). Running Kraken-only validation mode...")
            markets = []
        
        # Initialize strategy runner (works with Kraken data even if Polymarket fails)
        logger.info("Initializing strategy runner...")
        try:
            # Pass the current event loop to the runner so scheduler can use it
            runner = AllDayStrategyRunner(
                client, 
                store, 
                engine,
                loop=asyncio.get_running_loop()
            )
            logger.info("✅ Strategy runner initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize strategy runner: {e}", exc_info=True)
            raise
        
        # Show 4-hour strategy details
        logger.info("=" * 60)
        logger.info("4-HOUR BITCOIN DIRECTION STRATEGY:")
        logger.info("├─ 📊 Bollinger Bands (20-period, 2.0 std dev)")
        logger.info("├─ 📈 RSI (14-period) for extreme detection")
        logger.info("├─ 💫 Momentum confirmation (5-period)")
        logger.info("├─ 📰 Volume signal weighting")
        logger.info("├─ ✅ Entry: Price at BB extremes + RSI extreme + momentum")
        logger.info("├─ 🎯 Target: 4% profit (1:2.6 risk/reward ratio)")
        logger.info("├─ 🛑 Stop: 1.5% loss")
        logger.info("├─ 📋 Expected: 6 signals/day, 55%+ win rate")
        logger.info("└─ 💰 Growth target: $500 → $5,000 in ~80 days")
        logger.info("=" * 60)
        
        # Run 4-hour strategy with better error handling
        try:
            logger.info("Starting main trading loop...")
            await runner.run_forever()
        except Exception as e:
            logger.error(f"❌ Trading loop error: {type(e).__name__}: {e}", exc_info=True)
            raise
    
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down gracefully...")
        await BotAlert.shutdown(bot_name="BTC Bot", reason="Manual shutdown (KeyboardInterrupt)")
    
    except Exception as e:
        logger.error(f"❌ Critical error: {e}", exc_info=True)
        # NOTE: Do NOT send crash alert here - bot_monitor handles crash detection and alerting
        # Sending alerts here causes duplicate notifications
    
    finally:
        # Clean shutdown - ensure resources are freed
        try:
            logger.info("Cleaning up resources...")
            await client.close()
        except Exception as e:
            logger.warning(f"Error closing client: {e}")
        logger.info("Trading bot stopped")


async def run_backtest():
    """Run a backtest before going live (optional)."""
    logger.info("🔍 Running backtest on historical data...")
    
    client = PolymarketClient()
    await client.initialize()
    
    store = DataStore()
    predictor = PricePredictor()
    engine = TradingEngine()
    simulator = PaperTradingSimulator(engine, predictor, store)
    
    try:
        markets = await client.get_markets("Bitcoin")
        if markets:
            market = markets[0]
            logger.info(f"Backtesting: {market.title}")
            
            result = await simulator.run_backtest(market.id, days=30)
            if result:
                logger.info(
                    f"\n{'='*50}\n"
                    f"📊 BACKTEST RESULTS\n"
                    f"Start Balance: ${result.start_balance:.2f}\n"
                    f"End Balance: ${result.end_balance:.2f}\n"
                    f"Return: {result.total_return_pct*100:.2f}%\n"
                    f"Trades: {result.num_trades}\n"
                    f"Win Rate: {result.win_rate*100:.2f}%\n"
                    f"Best Trade: ${result.best_trade:.2f}\n"
                    f"Worst Trade: ${result.worst_trade:.2f}\n"
                    f"{'='*50}\n"
                )
    finally:
        await client.close()


if __name__ == "__main__":
    import sys
    
    # Handle Docker graceful shutdown
    shutdown_event = asyncio.Event()
    
    def shutdown_handler(signum, frame):
        """Handle SIGTERM (Docker) and SIGINT (Ctrl+C)."""
        logger.info(f"⏹️  Received signal {signum}. Shutting down gracefully...")
        shutdown_event.set()
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)  # Docker stop
    signal.signal(signal.SIGINT, shutdown_handler)   # Ctrl+C
    
    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        asyncio.run(run_backtest())
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Bot interrupted by user")
        except Exception as e:
            logger.error(f"❌ Uncaught exception in main: {type(e).__name__}: {e}", exc_info=True)
            sys.exit(1)
