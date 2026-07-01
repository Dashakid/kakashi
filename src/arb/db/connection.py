"""Async database and Redis connection management."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Any

import asyncpg
import redis.asyncio as aioredis
from loguru import logger

from src.arb.config import DATABASE_URL, REDIS_URL


# ── PostgreSQL pool ───────────────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Return (or lazily initialise) the shared asyncpg connection pool."""
    global _pool
    if _pool is None:
        # Strip the SQLAlchemy driver prefix if present
        dsn = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        logger.info("PostgreSQL pool initialised")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL pool closed")


@asynccontextmanager
async def db_conn() -> AsyncIterator[asyncpg.Connection]:
    """Async context manager that yields a connection from the pool."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


# ── Redis client ──────────────────────────────────────────────────────────────

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
        logger.info("Redis client initialised")
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None
        logger.info("Redis client closed")


# ── Convenience helpers ───────────────────────────────────────────────────────

async def redis_set_json(key: str, value: Any, ttl: int | None = None) -> None:
    r = await get_redis()
    serialised = json.dumps(value, default=str)
    if ttl:
        await r.setex(key, ttl, serialised)
    else:
        await r.set(key, serialised)


async def redis_get_json(key: str) -> Any | None:
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        return None
    return json.loads(raw)


# ── DB initialisation ─────────────────────────────────────────────────────────

async def init_db() -> None:
    """Apply schema DDL idempotently on startup."""
    from src.arb.db.schema import ALL_SQL  # local import avoids circular

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Execute each statement individually to handle multi-statement scripts
        for statement in ALL_SQL.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    await conn.execute(stmt)
                except Exception as exc:
                    logger.warning(f"DDL statement skipped ({exc}): {stmt[:60]!r}")
    logger.info("Database schema initialised")
