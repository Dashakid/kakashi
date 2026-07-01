"""
Main entry: Kakashi v2 — Basket Consensus Strategy.

PAPER MODE ONLY.  Live trading is hardcoded off in strategy.py.

Run with:
    python3 -m src.main_kakashi_v2

Environment variables (optional):
    KAKASHI_V2_WEBHOOK_URL   — dedicated Discord webhook for this bot;
                               falls back to DISCORD_WEBHOOK_URL if not set.
"""

import asyncio
from loguru import logger

from src.polymarket.basket.strategy import BasketStrategy


async def main() -> None:
    logger.add(
        "logs/kakashi_v2.log",
        rotation="50 MB",
        retention="14 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )
    bot = BasketStrategy()
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
