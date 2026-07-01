"""PostgreSQL DDL for the prediction market arbitrage engine.

Run via:
    psql -U arbuser -d arbdb -f schema.sql
Or let the app auto-apply with init_db().
"""

CREATE_EXTENSIONS = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
"""

CREATE_TABLES = """
-- ── Platforms ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS platforms (
    id          SMALLSERIAL PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,   -- 'polymarket' | 'kalshi'
    display     TEXT NOT NULL,
    rest_base   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO platforms (slug, display, rest_base)
VALUES
    ('polymarket', 'Polymarket', 'https://gamma-api.polymarket.com'),
    ('kalshi',     'Kalshi',    'https://trading-api.kalshi.com/trade-api/v2')
ON CONFLICT (slug) DO NOTHING;

-- ── Markets ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS markets (
    id              BIGSERIAL PRIMARY KEY,
    platform_id     SMALLINT NOT NULL REFERENCES platforms(id),
    external_id     TEXT NOT NULL,          -- platform-native market ID
    slug            TEXT,
    title           TEXT NOT NULL,
    description     TEXT,
    resolution_rules TEXT,
    category        TEXT,
    sub_category    TEXT,
    end_date        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'active',  -- active | closed | resolved
    -- Best prices at last poll (cents, 0-100)
    yes_bid         NUMERIC(6,4),
    yes_ask         NUMERIC(6,4),
    no_bid          NUMERIC(6,4),
    no_ask          NUMERIC(6,4),
    volume_24h      NUMERIC(18,4),
    liquidity       NUMERIC(18,4),
    -- Embedding stored as float array for quick TF-IDF (Phase 2 uses pgvector)
    title_tokens    TEXT[],
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_markets_platform  ON markets(platform_id);
CREATE INDEX IF NOT EXISTS idx_markets_status    ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_end_date  ON markets(end_date);
CREATE INDEX IF NOT EXISTS idx_markets_title_trgm ON markets USING GIN (title gin_trgm_ops);

-- ── Order book snapshots ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_books (
    id          BIGSERIAL PRIMARY KEY,
    market_id   BIGINT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    side        TEXT NOT NULL CHECK (side IN ('yes', 'no')),
    -- JSONB arrays: [{"price": 0.62, "size": 150}, ...]
    bids        JSONB NOT NULL DEFAULT '[]',
    asks        JSONB NOT NULL DEFAULT '[]',
    best_bid    NUMERIC(6,4),
    best_ask    NUMERIC(6,4),
    mid_price   NUMERIC(6,4),
    spread      NUMERIC(6,4),
    source      TEXT NOT NULL DEFAULT 'poll',  -- 'poll' | 'ws'
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ob_market_time ON order_books(market_id, captured_at DESC);

-- ── Matched market pairs ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matched_pairs (
    id              BIGSERIAL PRIMARY KEY,
    poly_market_id  BIGINT NOT NULL REFERENCES markets(id),
    kalshi_market_id BIGINT NOT NULL REFERENCES markets(id),
    confidence      NUMERIC(5,4) NOT NULL,      -- 0.00–1.00
    match_method    TEXT NOT NULL DEFAULT 'tfidf',  -- 'tfidf' | 'transformer'
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (poly_market_id, kalshi_market_id)
);

CREATE INDEX IF NOT EXISTS idx_pairs_active ON matched_pairs(is_active);

-- ── Arbitrage opportunities ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS arb_opportunities (
    id              BIGSERIAL PRIMARY KEY,
    pair_id         BIGINT NOT NULL REFERENCES matched_pairs(id),
    -- Which side to buy on each platform
    poly_side       TEXT NOT NULL CHECK (poly_side IN ('yes', 'no')),
    kalshi_side     TEXT NOT NULL CHECK (kalshi_side IN ('yes', 'no')),
    poly_ask        NUMERIC(6,4) NOT NULL,    -- cost to buy on Polymarket (cents)
    kalshi_ask      NUMERIC(6,4) NOT NULL,    -- cost to buy on Kalshi (cents)
    combined_cost   NUMERIC(6,4) NOT NULL,    -- should be < 100 for arb
    gross_profit    NUMERIC(6,4),             -- 100 - combined_cost
    poly_fee        NUMERIC(6,4),
    kalshi_fee      NUMERIC(6,4),
    net_profit      NUMERIC(6,4),             -- gross - fees
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL DEFAULT 'detected'  -- detected | executed | expired | failed
);

CREATE INDEX IF NOT EXISTS idx_arb_pair      ON arb_opportunities(pair_id);
CREATE INDEX IF NOT EXISTS idx_arb_status    ON arb_opportunities(status);
CREATE INDEX IF NOT EXISTS idx_arb_detected  ON arb_opportunities(detected_at DESC);

-- ── Paper trades ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS paper_trades (
    id              BIGSERIAL PRIMARY KEY,
    opportunity_id  BIGINT REFERENCES arb_opportunities(id),
    platform        TEXT NOT NULL,  -- 'polymarket' | 'kalshi'
    market_id       BIGINT NOT NULL REFERENCES markets(id),
    side            TEXT NOT NULL,  -- 'yes' | 'no'
    direction       TEXT NOT NULL,  -- 'buy' | 'sell'
    price_cents     NUMERIC(6,4) NOT NULL,
    contracts       NUMERIC(12,4) NOT NULL,
    notional_usd    NUMERIC(12,4) NOT NULL,
    fee_usd         NUMERIC(12,4) NOT NULL DEFAULT 0,
    pnl_usd         NUMERIC(12,4),            -- filled when position closes
    status          TEXT NOT NULL DEFAULT 'open',  -- open | closed | cancelled
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ
);

-- ── Bot state / heartbeat ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bot_heartbeat (
    id          SMALLSERIAL PRIMARY KEY,
    component   TEXT NOT NULL UNIQUE,   -- 'ingestor' | 'matcher' | 'detector'
    status      TEXT NOT NULL DEFAULT 'starting',
    message     TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

ALL_SQL = CREATE_EXTENSIONS + "\n" + CREATE_TABLES
