"""
Main entry: Kakashi Bot (Top Trader Follower).

Run with: python3 -m src.main_kakashi
"""

import asyncio
from loguru import logger

from src.polymarket.kakashi_bot import KakashiBot


async def main() -> None:
    logger.add(
        "logs/kakashi.log",
        rotation="50 MB",
        retention="14 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )
    bot = KakashiBot()
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
