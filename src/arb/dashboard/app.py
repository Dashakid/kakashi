"""Phase 5 — Streamlit Dashboard.

Run:
    streamlit run src/arb/dashboard/app.py

Shows:
  - Bot status & capital
  - Live arbitrage opportunities (last 100)
  - Executed trades & P&L
  - Market coverage (Poly vs Kalshi)
  - Matched pairs table
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import redis
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
_DB_URL = os.getenv("ARB_DATABASE_URL", "postgresql://arbuser:arbpass@localhost:5432/arbdb")
_REDIS_URL = os.getenv("ARB_REDIS_URL", "redis://localhost:6379/0")

# Streamlit page config
st.set_page_config(
    page_title="Arb Engine Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB helpers (sync psycopg2 — Streamlit is not async) ───────────────────────

@st.cache_resource
def _get_db():
    # Strip asyncpg dialect if present
    dsn = _DB_URL.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(dsn)


def _query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    conn = _get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        conn.rollback()
        return []


@st.cache_resource
def _get_redis():
    return redis.from_url(_REDIS_URL, decode_responses=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("⚡ Arb Engine")
    st.caption("Prediction Market Arbitrage Bot")
    st.divider()

    refresh = st.button("🔄 Refresh", use_container_width=True)
    auto_refresh = st.toggle("Auto-refresh (10s)", value=False)
    st.divider()

    paper_mode = os.getenv("PAPER_TRADING", "true").lower() == "true"
    if paper_mode:
        st.success("🧻 PAPER TRADING")
    else:
        st.error("🔴 LIVE TRADING")

    st.divider()
    st.caption(f"DB: `{_DB_URL.split('@')[-1] if '@' in _DB_URL else _DB_URL}`")

if auto_refresh:
    st.empty()  # trigger rerun via component trick
    import time as _t
    _t.sleep(10)
    st.rerun()

# ── Page header ───────────────────────────────────────────────────────────────

st.title("📊 Prediction Market Arbitrage Dashboard")
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

# ── Row 1: KPI cards ──────────────────────────────────────────────────────────

col1, col2, col3, col4, col5 = st.columns(5)

market_counts = _query(
    """
    SELECT p.slug, COUNT(*) AS cnt
    FROM markets m
    JOIN platforms p ON p.id = m.platform_id
    WHERE m.status='active'
    GROUP BY p.slug
    """
)
mc = {r["slug"]: r["cnt"] for r in market_counts}

pairs_count = _query("SELECT COUNT(*) AS cnt FROM matched_pairs WHERE is_active=TRUE")[0]["cnt"]

opp_today = _query(
    "SELECT COUNT(*) AS cnt FROM arb_opportunities WHERE detected_at > NOW() - INTERVAL '24h'"
)[0]["cnt"]

total_pnl = _query(
    "SELECT COALESCE(SUM(pnl_usd), 0) AS total FROM paper_trades WHERE status='closed'"
)[0]["total"] or 0.0

open_trades = _query("SELECT COUNT(*) AS cnt FROM paper_trades WHERE status='open'")[0]["cnt"]

with col1:
    st.metric("Poly Markets", f"{mc.get('polymarket', 0):,}")
with col2:
    st.metric("Kalshi Markets", f"{mc.get('kalshi', 0):,}")
with col3:
    st.metric("Matched Pairs", f"{pairs_count:,}")
with col4:
    st.metric("Opps (24h)", f"{opp_today:,}")
with col5:
    st.metric("Net P&L", f"${float(total_pnl):.2f}", delta=f"{open_trades} open")

st.divider()

# ── Row 2: Live opportunities ─────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(
    ["🎯 Live Opportunities", "📋 Trade History", "🔗 Matched Pairs", "❤️ Bot Status"]
)

with tab1:
    st.subheader("Recent Arbitrage Opportunities")

    opps = _query(
        """
        SELECT
            ao.id,
            ao.detected_at,
            mp.poly_title,
            ao.poly_side,
            ao.kalshi_side,
            ao.poly_ask,
            ao.kalshi_ask,
            ao.combined_cost,
            ao.gross_profit,
            ao.net_profit,
            ao.status,
            mp.confidence
        FROM arb_opportunities ao
        JOIN matched_pairs mp2 ON mp2.id = ao.pair_id
        JOIN (
            SELECT mp3.id, pm.title AS poly_title, mp3.confidence
            FROM matched_pairs mp3
            JOIN markets pm ON pm.id = mp3.poly_market_id
        ) mp ON mp.id = ao.pair_id
        ORDER BY ao.detected_at DESC
        LIMIT 100
        """
    )

    if opps:
        df = pd.DataFrame(opps)
        df["detected_at"] = pd.to_datetime(df["detected_at"]).dt.strftime("%H:%M:%S")
        df["combined_cost"] = df["combined_cost"].apply(lambda x: f"{x:.1f}¢")
        df["net_profit"] = df["net_profit"].apply(lambda x: f"+{x:.2f}¢" if x else "—")
        df["confidence"] = df["confidence"].apply(lambda x: f"{x*100:.0f}%")

        # Colour-code status
        def _colour_status(val: str) -> str:
            colours = {"detected": "🟡", "executed": "🟢", "failed": "🔴", "expired": "⚫"}
            return f"{colours.get(val, '')} {val}"

        df["status"] = df["status"].apply(_colour_status)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No opportunities detected yet. Markets are being ingested…")

with tab2:
    st.subheader("Executed Trades (Paper)")

    trades = _query(
        """
        SELECT
            pt.id,
            pt.opened_at,
            pt.platform,
            m.title,
            pt.side,
            pt.direction,
            pt.price_cents,
            pt.contracts,
            pt.notional_usd,
            pt.fee_usd,
            pt.pnl_usd,
            pt.status
        FROM paper_trades pt
        JOIN markets m ON m.id = pt.market_id
        ORDER BY pt.opened_at DESC
        LIMIT 200
        """
    )

    if trades:
        df = pd.DataFrame(trades)
        df["opened_at"] = pd.to_datetime(df["opened_at"]).dt.strftime("%Y-%m-%d %H:%M")
        df["price_cents"] = df["price_cents"].apply(lambda x: f"{x:.1f}¢")
        df["notional_usd"] = df["notional_usd"].apply(lambda x: f"${x:.2f}")
        df["pnl_usd"] = df["pnl_usd"].apply(
            lambda x: f"${float(x):.2f}" if x is not None else "—"
        )

        # P&L summary
        total = float(_query(
            "SELECT COALESCE(SUM(notional_usd),0) AS t FROM paper_trades"
        )[0]["t"] or 0)
        wins = _query(
            "SELECT COUNT(*) AS c FROM paper_trades WHERE pnl_usd > 0"
        )[0]["c"]
        losses = _query(
            "SELECT COUNT(*) AS c FROM paper_trades WHERE pnl_usd < 0"
        )[0]["c"]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Notional", f"${total:.2f}")
        c2.metric("Winning Trades", wins)
        c3.metric("Losing Trades", losses)

        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No trades executed yet.")

with tab3:
    st.subheader("Active Matched Pairs")

    pairs = _query(
        """
        SELECT
            mp.id,
            pm.title AS poly_title,
            km.title AS kalshi_title,
            mp.confidence,
            mp.match_method,
            mp.updated_at
        FROM matched_pairs mp
        JOIN markets pm ON pm.id = mp.poly_market_id
        JOIN markets km ON km.id = mp.kalshi_market_id
        WHERE mp.is_active = TRUE
        ORDER BY mp.confidence DESC
        LIMIT 500
        """
    )

    if pairs:
        df = pd.DataFrame(pairs)
        df["confidence"] = df["confidence"].apply(lambda x: f"{float(x)*100:.1f}%")
        df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Confidence distribution
        confs = [float(p["confidence"].replace("%", "")) for p in pairs]
        chart_df = pd.DataFrame({"confidence_%": confs})
        st.bar_chart(chart_df["confidence_%"].value_counts().sort_index())
    else:
        st.info("No matched pairs yet. Run the matching engine first.")

with tab4:
    st.subheader("Bot Component Status")

    heartbeats = _query(
        "SELECT component, status, message, updated_at FROM bot_heartbeat ORDER BY component"
    )

    if heartbeats:
        for hb in heartbeats:
            age_s = (
                datetime.now(timezone.utc) - hb["updated_at"].replace(tzinfo=timezone.utc)
            ).total_seconds()
            icon = "🟢" if age_s < 60 else "🟡" if age_s < 300 else "🔴"
            st.write(
                f"{icon} **{hb['component']}** — {hb['status']} "
                f"| {hb['message']} "
                f"| last seen {int(age_s)}s ago"
            )
    else:
        st.info("No heartbeat data yet.")

    st.divider()
    st.subheader("Redis Cache Spot-Check")
    try:
        r = _get_redis()
        poly_keys = len(r.keys("poly:ob:*"))
        kalshi_keys = len(r.keys("kalshi:ob:*"))
        st.write(f"Poly order book keys cached: **{poly_keys}**")
        st.write(f"Kalshi order book keys cached: **{kalshi_keys}**")
    except Exception as exc:
        st.error(f"Redis unreachable: {exc}")
