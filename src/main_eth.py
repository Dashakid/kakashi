"""Main entry point for the Ethereum trading bot - MARKET MAKER EDITION."""

import asyncio
import os
import signal
from datetime import datetime
from loguru import logger

from src.config import LOG_LEVEL, PAPER_TRADING
from src.polymarket.client import PolymarketClient
from src.polymarket.market_discovery import MarketDiscovery
from src.polymarket.eth_market_maker import ETHMarketMakerRunner
from src.notifications.alerts import BotAlert


# EARLY OUTPUT FOR DEBUGGING (before any blocking operations)
print("🤖 ETH Market Maker bot starting... (Python process initialized)", flush=True)

# Configure logging
LOG_FILE_ETH = "logs/trading_eth.log"
os.makedirs(os.path.dirname(LOG_FILE_ETH) or "logs", exist_ok=True)
logger.remove()
logger.add(
    LOG_FILE_ETH,
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)
logger.add(
    lambda msg: print(msg.rstrip(), flush=True),
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
)


async def main():
    """Main entry point - runs ETH market maker strategy on Polymarket."""
    print("🤖 ETH: Starting async main()...", flush=True)
    
    try:
        # FIRST log message to detect if we reach here
        logger.info(f"🚀 Starting ETH Market Maker Bot [PAPER_TRADING={PAPER_TRADING}]")
        logger.info("💰 Strategy: Provide liquidity by quoting both sides of ETH markets")
        logger.info("📊 Capture spreads instead of predicting direction")
        logger.info("⏳ Initializing components...")
        
        # Initialize Polymarket client
        logger.debug("📦 Creating PolymarketClient...")
        client = PolymarketClient()
        await client.initialize()
        logger.info("  ✅ PolymarketClient initialized with async session")
        
        # Initialize market discovery
        logger.debug("🔍 Creating MarketDiscovery...")
        discovery = MarketDiscovery(client)
        logger.info("  ✅ MarketDiscovery initialized")
        
        # Discover initial markets
        logger.info("🔍 Discovering ETH markets on Polymarket... (force refresh)")
        markets = await discovery.discover_markets(force_refresh=True)
        eth_markets = [m for m in markets.values() if m.asset == 'ETH' and m.market_type == 'direction']
        logger.info(f"✅ Found {len(eth_markets)} ETH direction markets")
        
        # Initialize market maker runner
        logger.debug("🎯 Creating ETH Market Maker Runner...")
        market_maker = ETHMarketMakerRunner(discovery, client)
        logger.info("  ✅ ETH Market Maker initialized")
        
        logger.info("🎯 Starting market maker loop...")
        logger.info("  - 5-min market refresh")
        logger.info("  - 10-sec quote generation (30% probability)")
        logger.info("  - 20-sec profitable position closures")
        logger.info("📢 Tracking to unified win/loss tracker")
        logger.info("📨 Discord alerts for >$0.10 wins")
        logger.info("")
        logger.info("=" * 60)
        
        # Send startup alert to Discord
        mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
        await BotAlert.startup(
            bot_name="ETH Market Maker",
            strategy="Dual-sided liquidity provision (2-5¢ spreads)",
            mode=mode
        )
        
        # Show market maker strategy details
        logger.info("=" * 60)
        logger.info("ETH MARKET MAKER STRATEGY:")
        logger.info("├─ 💡 Strategy: Dual-sided liquidity provision")
        logger.info("├─ 📊 Quote both YES and NO on direction markets")
        logger.info("├─ 💰 Capture spreads: 2-5 cents per trade")
        logger.info("├─ ⏱️  Quote generation: Every 10 seconds (30% probability)")
        logger.info("├─ 📈 Position close: Every 20 seconds if profitable")
        logger.info("├─ 🎯 Target P&L: +0.5% to +2% per trade")
        logger.info("├─ 📋 Expected: 1000+ quotes/day, ~40-50% winning quotes")
        logger.info("└─ 💎 High volume, low margin model (~$85k/month benchmark)")
        logger.info("=" * 60)
        
        # Run market maker strategy with better error handling
        try:
            logger.info("Starting market maker trading loop...")
            await market_maker.run_forever()
        except Exception as e:
            logger.error(f"❌ Trading loop error: {type(e).__name__}: {e}", exc_info=True)
            raise
    
    except KeyboardInterrupt:
        logger.info("⏹️  Shutting down gracefully...")
        await BotAlert.shutdown(bot_name="ETH Market Maker", reason="Manual shutdown (KeyboardInterrupt)")
    
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
        logger.info("ETH market maker bot stopped")


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
        logger.info("ETH Market Maker Bot interrupted by user")
    except Exception as e:
        logger.error(f"❌ Uncaught exception in main: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)
