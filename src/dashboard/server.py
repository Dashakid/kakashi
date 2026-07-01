"""
Dashboard API server — reads bot state files and serves a live UI.

Run standalone:   python -m src.dashboard
Accessible at:    http://localhost:8080
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

# Resolve paths relative to project root (two levels up from this file)
_ROOT = Path(__file__).parent.parent.parent
_DATA = _ROOT / "data"
_LOGS = _ROOT / "logs"
_HTML = Path(__file__).parent / "index.html"

app = FastAPI(title="Kakashi Bot Dashboard", docs_url=None, redoc_url=None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return None


def _age_seconds(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


def _status_from_age(age: Optional[float]) -> str:
    if age is None:
        return "offline"
    if age < 120:
        return "online"
    if age < 600:
        return "stale"
    return "offline"


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/status")
def get_status() -> Dict:
    hb = _read_json(_DATA / "heartbeat.json")

    if hb:
        age = _age_seconds(hb.get("last_poll"))
        return {
            "status": _status_from_age(age),
            "last_poll": hb.get("last_poll"),
            "mode": hb.get("mode", "paper"),
            "pid": hb.get("pid"),
            "wallets_tracked": hb.get("wallets_tracked", 0),
            "active_signals": hb.get("active_signals", 0),
            "age_seconds": int(age) if age is not None else None,
        }

    # Fallback: infer from state file mtime
    state_path = _DATA / "kakashi_state.json"
    if state_path.exists():
        mtime = datetime.fromtimestamp(state_path.stat().st_mtime, timezone.utc)
        age = (datetime.now(timezone.utc) - mtime).total_seconds()
        return {
            "status": _status_from_age(age),
            "last_poll": mtime.isoformat(),
            "mode": "unknown",
            "pid": None,
            "wallets_tracked": 0,
            "active_signals": 0,
            "age_seconds": int(age),
        }

    return {
        "status": "offline",
        "last_poll": None,
        "mode": "unknown",
        "pid": None,
        "wallets_tracked": 0,
        "active_signals": 0,
        "age_seconds": None,
    }


@app.get("/api/positions")
def get_positions() -> List[Dict]:
    state = _read_json(_DATA / "kakashi_state.json")
    if not state:
        return []

    rows = []
    for pos in state.get("open_positions", {}).values():
        rows.append({
            "market": pos.get("market_title", "")[:70],
            "outcome": pos.get("outcome", ""),
            "wallet": pos.get("wallet_label") or pos.get("wallet_address", "")[:10],
            "entry_price": round(pos.get("entry_price", 0), 4),
            "size_usd": round(pos.get("paper_size_usd", 0), 2),
            "opened_ts": pos.get("opened_ts", ""),
            "closed": False,
        })

    rows.sort(key=lambda r: r["opened_ts"], reverse=True)
    return rows


@app.get("/api/wallets")
def get_wallets() -> List[Dict]:
    state = _read_json(_DATA / "kakashi_state.json")
    if not state:
        return []

    rows = []
    for addr, w in state.get("top_wallets", {}).items():
        resolved = w.get("resolved_trades", 0)
        wr = w.get("win_rate", 0.0)
        rows.append({
            "label": w.get("label", addr[:10]),
            "address": addr,
            "resolved": resolved,
            "win_rate": round(wr * 100, 1),
            "realized_pnl": round(w.get("realized_pnl_total", 0), 2),
            "volume_7d": round(w.get("volume_7d", 0), 2),
            "trades_7d": w.get("trades_7d", 0),
        })

    rows.sort(key=lambda r: r["realized_pnl"], reverse=True)
    return rows


@app.get("/api/stats")
def get_stats() -> Dict:
    raw = _read_json(_DATA / "win_loss_stats.json")
    if not raw:
        return {"bots": [], "totals": {"trades": 0, "wins": 0, "losses": 0, "avg_pnl_pct": 0.0, "win_rate": 0.0}}

    totals: Dict[str, float] = {"trades": 0, "wins": 0, "losses": 0, "pnl_sum": 0.0}
    bots = []
    for bot_name, s in raw.items():
        t = s.get("total_trades", 0)
        w = s.get("wins", 0)
        l = s.get("losses", 0)
        pnl_sum = s.get("total_pnl", 0.0)
        avg_pnl = (pnl_sum / t * 100) if t else 0.0
        totals["trades"] += t
        totals["wins"] += w
        totals["losses"] += l
        totals["pnl_sum"] += pnl_sum
        bots.append({
            "name": bot_name,
            "trades": t,
            "wins": w,
            "losses": l,
            "win_rate": round(s.get("win_rate", 0) * 100, 1),
            "avg_pnl_pct": round(avg_pnl, 2),
            "avg_win_pct": round(s.get("avg_win_pnl", 0) * 100, 2),
            "avg_loss_pct": round(s.get("avg_loss_pnl", 0) * 100, 2),
        })

    bots.sort(key=lambda b: b["trades"], reverse=True)
    total_trades = totals["trades"]
    totals["win_rate"] = round(totals["wins"] / total_trades * 100, 1) if total_trades else 0.0
    # avg P&L per trade — meaningful regardless of number of bots or trades
    totals["avg_pnl_pct"] = round(totals["pnl_sum"] / total_trades * 100, 2) if total_trades else 0.0
    return {"bots": bots, "totals": totals}


@app.get("/api/logs")
def get_logs(lines: int = Query(default=80, le=500)) -> Dict:
    log_file = _LOGS / "kakashi.log"
    if not log_file.exists():
        return {"lines": []}
    try:
        with open(log_file, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            chunk = min(size, lines * 220)
            f.seek(max(0, size - chunk))
            raw = f.read().decode("utf-8", errors="replace")
        return {"lines": raw.splitlines()[-lines:]}
    except Exception as exc:
        return {"lines": [f"Error reading log: {exc}"]}


@app.get("/api/trades")
def get_trades(limit: int = Query(default=50, le=200)) -> List[Dict]:
    db = _DATA / "analytics.db"
    if not db.exists():
        return []
    try:
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY rowid DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    if _HTML.exists():
        return HTMLResponse(_HTML.read_text())
    return HTMLResponse("<h1>index.html not found next to server.py</h1>", status_code=500)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(host: str = "0.0.0.0", port: int = 8080) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")
