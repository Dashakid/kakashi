"""TradingView webhook receiver — listens for signals from TradingView Pine Script alerts."""
import asyncio
import uvicorn
import os
import signal
from loguru import logger
from src.config import LOG_LEVEL

LOG_FILE = "logs/webhook.log"
os.makedirs("logs", exist_ok=True)
logger.remove()
logger.add(LOG_FILE, level=LOG_LEVEL, format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")
logger.add(lambda msg: print(msg.rstrip(), flush=True), level=LOG_LEVEL, format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

if __name__ == "__main__":
    logger.info("🎣 Starting TradingView webhook server on port 8080...")
    uvicorn.run("src.webhook.tradingview_server:app", host="0.0.0.0", port=8080, reload=False)
