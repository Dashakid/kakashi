"""Main orchestrator — runs all phases in a single process.

Usage:
    python -m src.arb.main

Phases launched as concurrent asyncio tasks:
  Phase 1 — FeedManager  (market polling + WS order books → DB/Redis)
  Phase 2 — MatchingEngine loop  (TF-IDF market matching → matched_pairs)
  Phase 3 — ArbDetector loop  (scan pairs for ¢ discrepancies → arb_opportunities)
  Phase 4 — ExecutionBot loop  (paper/live trade execution)

Phase 5 (Streamlit) runs as a separate process (see docker-compose.yml).
"""

from __future__ import annotations

import asyncio
import signal
import sys

from loguru import logger

from src.arb.config import PAPER_TRADING
from src.arb.db.connection import init_db, close_pool, close_redis
from src.arb.db.repository import HeartbeatRepo
from src.arb.feeds.feed_manager import FeedManager
from src.arb.matching.engine import MatchingEngine
from src.arb.detection.detector import ArbDetector
from src.arb.execution.bot import ExecutionBot


async def main() -> None:
    logger.info("=" * 60)
    logger.info("Arb Engine starting")
    logger.info(f"  Paper trading: {PAPER_TRADING}")
    logger.info("=" * 60)

    # ── Init DB schema ─────────────────────────────────────────────────────
    await init_db()
    await HeartbeatRepo.upsert("orchestrator", "starting")

    # ── Instantiate components ─────────────────────────────────────────────
    feed_manager = FeedManager()
    matching_engine = MatchingEngine()
    detector = ArbDetector()
    exec_bot = ExecutionBot()

    # Wire detector → exec_bot: whenever detector finds an opp, submit it
    _original_scan = detector.scan_once

    async def _scan_and_submit() -> list:
        opps = await _original_scan()
        for opp in opps:
            exec_bot.submit(opp)
        return opps

    detector.scan_once = _scan_and_submit  # type: ignore[method-assign]

    # ── Tasks ──────────────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(feed_manager.run(),           name="phase1_feed"),
        asyncio.create_task(matching_engine.run_loop(600), name="phase2_match"),
        asyncio.create_task(detector.run_loop(2.0),        name="phase3_detect"),
        asyncio.create_task(exec_bot.run_loop(),           name="phase4_exec"),
    ]

    await HeartbeatRepo.upsert("orchestrator", "running", f"{len(tasks)} tasks")
    logger.info("All phases running")

    # ── Graceful shutdown ─────────────────────────────────────────────────
    loop = asyncio.get_running_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info(f"Signal {sig.name} received — shutting down")
        for t in tasks:
            t.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        logger.info("Cleaning up connections…")
        await feed_manager.close()
        await close_pool()
        await close_redis()
        logger.info("Arb Engine stopped")


if __name__ == "__main__":
    asyncio.run(main())
