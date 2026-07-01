"""Unified trade logger — Supabase (Postgres REST) primary, MongoDB secondary, JSONL fallback.

Priority order:
  1. Supabase — set SUPABASE_URL and SUPABASE_KEY in .env
  2. MongoDB  — set MONGODB_URI in .env
  3. JSONL    — data/all_trades.jsonl (always works, never crashes a bot)

Supabase table: trades
Run this SQL once in the Supabase SQL editor to create it:

  create table trades (
    id           bigserial primary key,
    entered_at   timestamptz not null,
    bot          text        not null,
    market       text        not null,
    outcome      text        not null,
    entry_price  numeric(10,5),
    size_usd     numeric(12,2),
    status       text        default 'open',
    exit_price   numeric(10,5),
    exit_at      timestamptz,
    pnl_usd      numeric(12,2),
    pnl_pct      numeric(10,5),
    is_win       boolean,
    resolved_via text
  );

  -- optional: enable RLS and create a policy if you want a public dashboard
  alter table trades enable row level security;
  create policy "service role full access"
    on trades for all using (true) with check (true);

resolved_via guide:
  "gamma_api"   — Gamma API redeemable flag fired  (REAL outcome)
  "wallet_exit" — mirror wallet stopped holding
  "price"       — price-target hit (phantom bug indicator)
  "timeout"     — 14-day max-hold
  None          — still open
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# JSONL fallback path
_TRADE_LOG = Path(__file__).resolve().parent.parent.parent / "data" / "all_trades.jsonl"

# Cached flags so we only probe env once per process
_supabase_url: Optional[str] = None
_supabase_key: Optional[str] = None
_supabase_ready: Optional[bool] = None   # None = not checked yet

# Legacy MongoDB fallback
_mongo_client = None
_mongo_col = None


def _load_env_once() -> None:
    """Load .env if not already loaded (idempotent)."""
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# Supabase helpers (pure stdlib — no extra package needed)
# ---------------------------------------------------------------------------

def _supabase_init() -> bool:
    """Return True if Supabase creds are available."""
    global _supabase_url, _supabase_key, _supabase_ready
    if _supabase_ready is not None:
        return _supabase_ready
    _load_env_once()
    _supabase_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    _supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
    _supabase_ready = bool(_supabase_url and _supabase_key)
    return _supabase_ready


def _supabase_insert(doc: dict) -> Optional[str]:
    """POST a row to the Supabase `trades` table. Returns the new row id or None."""
    if not _supabase_init():
        return None
    url = f"{_supabase_url}/rest/v1/trades"
    # Convert datetime objects to ISO strings
    payload = {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in doc.items()
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "apikey": _supabase_key,
            "Authorization": f"Bearer {_supabase_key}",
            "Prefer": "return=representation",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            result = json.loads(resp.read())
            if isinstance(result, list) and result:
                return str(result[0].get("id"))
            return None
    except Exception:
        return None


def _supabase_update(row_id: str, update: dict) -> bool:
    """PATCH an existing row in the Supabase `trades` table."""
    if not _supabase_init() or not row_id:
        return False
    url = f"{_supabase_url}/rest/v1/trades?id=eq.{row_id}"
    payload = {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in update.items()
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "apikey": _supabase_key,
            "Authorization": f"Bearer {_supabase_key}",
        },
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status in (200, 204)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# MongoDB fallback helpers (unchanged)
# ---------------------------------------------------------------------------

def _collection():
    """Return the pymongo Collection, or None if MongoDB is not configured."""
    global _mongo_client, _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    _load_env_once()
    uri = os.getenv("MONGODB_URI")
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5_000)
        _mongo_col = _mongo_client["contract_trading"]["trades"]
        return _mongo_col
    except Exception:
        return None


def _jsonl_append(doc: dict) -> None:
    """Fallback: append a dict to the JSONL file."""
    _TRADE_LOG.parent.mkdir(parents=True, exist_ok=True)
    # convert datetime objects to ISO strings for JSON
    safe = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in doc.items()}
    with open(_TRADE_LOG, "a") as f:
        f.write(json.dumps(safe) + "\n")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def open_trade(
    bot: str,
    market: str,
    outcome: str,
    entry_price: float,
    size_usd: float,
    entered_at: Optional[datetime] = None,
) -> Optional[str]:
    """
    Insert an open position.
    Returns a row id string that can be passed to close_trade(), or None.
    Priority: Supabase → MongoDB → JSONL.
    """
    doc = {
        "entered_at":  (entered_at or datetime.now(timezone.utc)),
        "bot":         bot,
        "market":      market,
        "outcome":     outcome,
        "entry_price": round(entry_price, 5),
        "size_usd":    round(size_usd, 2),
        "status":      "open",
        "exit_price":  None,
        "exit_at":     None,
        "pnl_usd":     None,
        "pnl_pct":     None,
        "is_win":      None,
        "resolved_via": None,
    }
    try:
        # 1. Supabase
        row_id = _supabase_insert(doc)
        if row_id:
            return f"supa:{row_id}"
        # 2. MongoDB
        col = _collection()
        if col is not None:
            result = col.insert_one(doc)
            return f"mongo:{result.inserted_id}"
        # 3. JSONL
        _jsonl_append(doc)
        return None
    except Exception:
        return None


def close_trade(
    trade_id: Optional[str],
    exit_price: float,
    is_win: bool,
    resolved_via: str,
    entry_price: Optional[float] = None,
    size_usd: Optional[float] = None,
) -> None:
    """
    Update an open trade document with exit data.
    trade_id is the string returned by open_trade() — prefixed with 'supa:', 'mongo:', or None.
    """
    try:
        now = datetime.now(timezone.utc)
        update: dict = {
            "status":       "closed",
            "exit_price":   round(exit_price, 5),
            "exit_at":      now,
            "is_win":       is_win,
            "resolved_via": resolved_via,
        }
        if entry_price is not None and size_usd is not None and entry_price > 0:
            pnl_pct = (exit_price - entry_price) / entry_price
            update["pnl_pct"] = round(pnl_pct, 5)
            update["pnl_usd"] = round(size_usd * pnl_pct, 2)

        if trade_id and trade_id.startswith("supa:"):
            _supabase_update(trade_id[5:], update)
        elif trade_id and trade_id.startswith("mongo:"):
            from bson import ObjectId
            col = _collection()
            if col is not None:
                col.update_one({"_id": ObjectId(trade_id[6:])}, {"$set": update})
        else:
            record = {"trade_id": trade_id, **update} if trade_id else update
            _jsonl_append(record)
    except Exception:
        pass


def log_trade(
    bot: str,
    market: str,
    outcome: str,
    entry_price: float,
    exit_price: float,
    size_usd: float,
    is_win: bool,
    resolved_via: str,
    entered_at: Optional[datetime] = None,
) -> None:
    """
    Insert a fully-resolved trade in one shot (used by kakashi and v2 which
    only fire at close time).  Never raises.
    Pass entered_at to record the actual open time (defaults to now if omitted).
    Priority: Supabase → MongoDB → JSONL.
    """
    try:
        now = datetime.now(timezone.utc)
        open_ts = entered_at or now
        entry_safe = max(entry_price, 1e-9)
        pnl_pct = (exit_price - entry_price) / entry_safe
        doc = {
            "entered_at":   open_ts,
            "bot":          bot,
            "market":       market,
            "outcome":      outcome,
            "entry_price":  round(entry_price, 5),
            "size_usd":     round(size_usd, 2),
            "status":       "closed",
            "exit_price":   round(exit_price, 5),
            "exit_at":      now,
            "pnl_pct":      round(pnl_pct, 5),
            "pnl_usd":      round(size_usd * pnl_pct, 2),
            "is_win":       is_win,
            "resolved_via": resolved_via,
        }
        # 1. Supabase
        if _supabase_insert(doc):
            return
        # 2. MongoDB
        col = _collection()
        if col is not None:
            col.insert_one(doc)
            return
        # 3. JSONL
        _jsonl_append(doc)
    except Exception:
        pass  # never crash a live bot over logging
