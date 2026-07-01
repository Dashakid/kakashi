"""Phase 2 — Market Matching Engine.

Uses TF-IDF cosine similarity to semantically match equivalent markets across
Polymarket and Kalshi.  Sentence-transformer upgrade is drop-in (same interface).

Entry: MatchingEngine.run_once() or MatchingEngine.run_loop()
"""

from __future__ import annotations

import asyncio
import re
from typing import NamedTuple

import numpy as np
from loguru import logger
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.arb.config import MATCH_CONFIDENCE_THRESHOLD
from src.arb.db.connection import db_conn


# ── Data structures ───────────────────────────────────────────────────────────

class MarketRecord(NamedTuple):
    db_id: int
    external_id: str
    title: str
    description: str
    resolution_rules: str
    category: str


class MatchedPair(NamedTuple):
    poly_db_id: int
    kalshi_db_id: int
    confidence: float
    method: str = "tfidf"


# ── Text preprocessing ────────────────────────────────────────────────────────

_STOPWORDS = {
    "will", "the", "a", "an", "be", "is", "are", "was", "were", "in", "on",
    "at", "to", "by", "of", "for", "with", "or", "and", "it", "this", "that",
    "by", "end", "before", "after", "between", "about", "if", "when", "who",
    "which", "what", "how", "as", "from",
}

def _clean(text: str) -> str:
    """Lowercase, strip punctuation, remove stopwords."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [t for t in text.split() if t and t not in _STOPWORDS]
    return " ".join(tokens)


def _market_text(m: MarketRecord) -> str:
    """Combine title + description + resolution into a single document."""
    parts = [m.title, m.description or "", m.resolution_rules or ""]
    return _clean(" ".join(p for p in parts if p))


# ── Matching engine ───────────────────────────────────────────────────────────

class MatchingEngine:
    """
    Loads active markets from both platforms, computes TF-IDF similarity,
    and writes matched pairs (confidence >= threshold) to the DB.
    """

    def __init__(self, threshold: float = MATCH_CONFIDENCE_THRESHOLD) -> None:
        self.threshold = threshold

    # ── DB helpers ────────────────────────────────────────────────────────────

    async def _fetch_markets(self, platform_slug: str) -> list[MarketRecord]:
        async with db_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT m.id, m.external_id, m.title,
                       COALESCE(m.description,'') AS description,
                       COALESCE(m.resolution_rules,'') AS resolution_rules,
                       COALESCE(m.category,'') AS category
                FROM markets m
                JOIN platforms p ON p.id = m.platform_id
                WHERE p.slug = $1 AND m.status = 'active'
                ORDER BY m.volume_24h DESC NULLS LAST
                """,
                platform_slug,
            )
        return [
            MarketRecord(
                db_id=r["id"],
                external_id=r["external_id"],
                title=r["title"],
                description=r["description"],
                resolution_rules=r["resolution_rules"],
                category=r["category"],
            )
            for r in rows
        ]

    async def _save_pairs(self, pairs: list[MatchedPair]) -> int:
        """Upsert matched pairs; returns count of new/updated rows."""
        if not pairs:
            return 0
        async with db_conn() as conn:
            # Use executemany for efficiency
            data = [
                (p.poly_db_id, p.kalshi_db_id, p.confidence, p.method)
                for p in pairs
            ]
            await conn.executemany(
                """
                INSERT INTO matched_pairs
                    (poly_market_id, kalshi_market_id, confidence, match_method, is_active)
                VALUES ($1, $2, $3, $4, TRUE)
                ON CONFLICT (poly_market_id, kalshi_market_id)
                DO UPDATE SET
                    confidence   = EXCLUDED.confidence,
                    match_method = EXCLUDED.match_method,
                    is_active    = TRUE,
                    updated_at   = NOW()
                """,
                data,
            )
        return len(pairs)

    async def _deactivate_stale(
        self,
        active_poly_ids: set[int],
        active_kalshi_ids: set[int],
    ) -> None:
        """Mark pairs where either market is no longer active."""
        if not active_poly_ids and not active_kalshi_ids:
            return
        async with db_conn() as conn:
            await conn.execute(
                """
                UPDATE matched_pairs SET is_active = FALSE
                WHERE is_active = TRUE
                  AND (
                      poly_market_id   NOT IN (SELECT unnest($1::bigint[]))
                   OR kalshi_market_id NOT IN (SELECT unnest($2::bigint[]))
                  )
                """,
                list(active_poly_ids),
                list(active_kalshi_ids),
            )

    # ── Core matching ─────────────────────────────────────────────────────────

    def _compute_matches(
        self,
        poly: list[MarketRecord],
        kalshi: list[MarketRecord],
    ) -> list[MatchedPair]:
        if not poly or not kalshi:
            return []

        poly_docs = [_market_text(m) for m in poly]
        kalshi_docs = [_market_text(m) for m in kalshi]

        # Fit TF-IDF on combined corpus so vocabulary is shared
        vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=1,
            sublinear_tf=True,
            max_features=50_000,
        )
        all_docs = poly_docs + kalshi_docs
        tfidf_matrix = vectorizer.fit_transform(all_docs)

        poly_matrix = tfidf_matrix[: len(poly_docs)]
        kalshi_matrix = tfidf_matrix[len(poly_docs) :]

        # Cosine similarity: shape (n_poly, n_kalshi)
        sim_matrix: np.ndarray = cosine_similarity(poly_matrix, kalshi_matrix)

        pairs: list[MatchedPair] = []
        # For each Poly market take at most 1 best Kalshi match above threshold
        for pi, poly_market in enumerate(poly):
            best_ki = int(np.argmax(sim_matrix[pi]))
            score = float(sim_matrix[pi, best_ki])
            if score >= self.threshold:
                pairs.append(
                    MatchedPair(
                        poly_db_id=poly_market.db_id,
                        kalshi_db_id=kalshi[best_ki].db_id,
                        confidence=round(score, 4),
                    )
                )

        # Deduplicate: if multiple Poly markets map to the same Kalshi market,
        # keep only the highest-confidence one
        best_by_kalshi: dict[int, MatchedPair] = {}
        for p in pairs:
            existing = best_by_kalshi.get(p.kalshi_db_id)
            if existing is None or p.confidence > existing.confidence:
                best_by_kalshi[p.kalshi_db_id] = p

        return list(best_by_kalshi.values())

    # ── Public interface ──────────────────────────────────────────────────────

    async def run_once(self) -> int:
        """Run one full matching pass. Returns count of matched pairs saved."""
        poly = await self._fetch_markets("polymarket")
        kalshi = await self._fetch_markets("kalshi")

        if not poly or not kalshi:
            logger.warning(
                f"Matching skipped — poly={len(poly)}, kalshi={len(kalshi)} markets"
            )
            return 0

        logger.info(
            f"Matching {len(poly)} Poly × {len(kalshi)} Kalshi markets "
            f"(threshold={self.threshold})"
        )

        pairs = self._compute_matches(poly, kalshi)
        saved = await self._save_pairs(pairs)

        await self._deactivate_stale(
            {m.db_id for m in poly}, {m.db_id for m in kalshi}
        )

        logger.info(f"Matching complete — {saved} pairs at ≥{self.threshold:.0%} confidence")
        return saved

    async def run_loop(self, interval_seconds: int = 600) -> None:
        """Run matching in a loop every `interval_seconds`."""
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Matching loop error: {exc}")
            await asyncio.sleep(interval_seconds)
