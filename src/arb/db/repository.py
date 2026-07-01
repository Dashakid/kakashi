"""Async repository — thin CRUD wrappers over asyncpg."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.arb.db.connection import db_conn


class MarketRepo:
    """CRUD for the `markets` table."""

    @staticmethod
    async def upsert(
        *,
        platform_slug: str,
        external_id: str,
        title: str,
        description: str = "",
        resolution_rules: str = "",
        category: str = "",
        end_date: datetime | None = None,
        status: str = "active",
        yes_bid: float | None = None,
        yes_ask: float | None = None,
        no_bid: float | None = None,
        no_ask: float | None = None,
        volume_24h: float | None = None,
        liquidity: float | None = None,
        title_tokens: list[str] | None = None,
    ) -> int:
        """Upsert a market row; returns the internal market id."""
        async with db_conn() as conn:
            platform_id: int = await conn.fetchval(
                "SELECT id FROM platforms WHERE slug = $1", platform_slug
            )
            if platform_id is None:
                raise ValueError(f"Unknown platform slug: {platform_slug!r}")

            row_id: int = await conn.fetchval(
                """
                INSERT INTO markets
                    (platform_id, external_id, title, description, resolution_rules,
                     category, end_date, status,
                     yes_bid, yes_ask, no_bid, no_ask,
                     volume_24h, liquidity, title_tokens, last_seen, updated_at)
                VALUES
                    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW(),NOW())
                ON CONFLICT (platform_id, external_id)
                DO UPDATE SET
                    title            = EXCLUDED.title,
                    description      = EXCLUDED.description,
                    resolution_rules = EXCLUDED.resolution_rules,
                    category         = EXCLUDED.category,
                    end_date         = EXCLUDED.end_date,
                    status           = EXCLUDED.status,
                    yes_bid          = EXCLUDED.yes_bid,
                    yes_ask          = EXCLUDED.yes_ask,
                    no_bid           = EXCLUDED.no_bid,
                    no_ask           = EXCLUDED.no_ask,
                    volume_24h       = EXCLUDED.volume_24h,
                    liquidity        = EXCLUDED.liquidity,
                    title_tokens     = EXCLUDED.title_tokens,
                    last_seen        = NOW(),
                    updated_at       = NOW()
                RETURNING id
                """,
                platform_id,
                external_id,
                title,
                description,
                resolution_rules,
                category,
                end_date,
                status,
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                volume_24h,
                liquidity,
                title_tokens,
            )
        return row_id

    @staticmethod
    async def get_active(platform_slug: str) -> list[dict[str, Any]]:
        async with db_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT m.id, m.external_id, m.title, m.description,
                       m.resolution_rules, m.category, m.end_date,
                       m.yes_bid, m.yes_ask, m.no_bid, m.no_ask,
                       m.volume_24h, m.liquidity, m.title_tokens
                FROM markets m
                JOIN platforms p ON p.id = m.platform_id
                WHERE p.slug = $1 AND m.status = 'active'
                ORDER BY m.volume_24h DESC NULLS LAST
                """,
                platform_slug,
            )
        return [dict(r) for r in rows]


class OrderBookRepo:
    """Inserts order book snapshots."""

    @staticmethod
    async def insert(
        *,
        market_id: int,
        side: str,
        bids: list[dict],
        asks: list[dict],
        best_bid: float | None,
        best_ask: float | None,
        source: str = "poll",
    ) -> None:
        import json

        mid = None
        spread = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2
            spread = best_ask - best_bid

        async with db_conn() as conn:
            await conn.execute(
                """
                INSERT INTO order_books
                    (market_id, side, bids, asks, best_bid, best_ask,
                     mid_price, spread, source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                market_id,
                side,
                json.dumps(bids),
                json.dumps(asks),
                best_bid,
                best_ask,
                mid,
                spread,
                source,
            )


class HeartbeatRepo:
    @staticmethod
    async def upsert(component: str, status: str, message: str = "") -> None:
        async with db_conn() as conn:
            await conn.execute(
                """
                INSERT INTO bot_heartbeat (component, status, message, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (component)
                DO UPDATE SET status=$2, message=$3, updated_at=NOW()
                """,
                component,
                status,
                message,
            )
