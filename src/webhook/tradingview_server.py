"""
TradingView Webhook Server.

Receives alerts from TradingView Pine Script strategies and records them
as paper trades, with full Discord notifications.

TradingView Alert Message Format (JSON):
{
  "ticker": "BTCUSD",
  "action": "long",        // "long" or "short"
  "price": 85000,
  "confidence": 0.72,      // optional, 0.0-1.0
  "rsi": 32,               // optional
  "bb_pct": -2.1,          // optional, % from mid BB
  "reason": "BB lower touch + RSI oversold",  // optional
  "bot": "BTC Bot"         // optional, defaults to ticker-based name
}

Setup Instructions sent back in /health endpoint.
"""

import os
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="TradingView Webhook Receiver", version="1.0.0")

# Track received signals in memory
_signal_log = []
_session_stats = {"received": 0, "processed": 0, "errors": 0}


class TVAlert(BaseModel):
    """TradingView alert payload."""
    ticker: str = "BTCUSD"
    action: str = "long"          # "long" or "short"
    price: float = 0.0
    confidence: float = 0.65
    rsi: float = 0.0
    bb_pct: float = 0.0
    reason: str = ""
    bot: Optional[str] = None
    timeframe: str = "4H"


def _ticker_to_asset(ticker: str) -> str:
    """Map TradingView ticker to asset name."""
    ticker = ticker.upper()
    if "BTC" in ticker or "XBT" in ticker:
        return "BTC"
    elif "ETH" in ticker:
        return "ETH"
    elif "SOL" in ticker:
        return "SOL"
    elif "XRP" in ticker:
        return "XRP"
    elif "AVAX" in ticker:
        return "AVAX"
    elif "LINK" in ticker:
        return "LINK"
    elif "ADA" in ticker:
        return "ADA"
    elif "DOT" in ticker:
        return "DOT"
    return ticker.replace("USD", "").replace("USDT", "")[:6]


@app.get("/health")
async def health():
    """Health check + setup instructions."""
    return {
        "status": "ok",
        "server": "TradingView Webhook Receiver",
        "time": datetime.utcnow().isoformat(),
        "session_stats": _session_stats,
        "recent_signals": _signal_log[-5:],
        "setup": {
            "step1": "In TradingView, open your strategy/indicator",
            "step2": "Click 'Alerts' → 'Create Alert'",
            "step3": "Set Condition to your strategy signal",
            "step4": "Under 'Notifications', enable 'Webhook URL'",
            "step5": "Set URL to: http://YOUR_IP:8080/webhook/tradingview",
            "step6": "Set Message to JSON (see example below)",
            "example_message": '{"ticker": "{{ticker}}", "action": "long", "price": {{close}}, "confidence": 0.72, "rsi": 32, "reason": "BB + RSI signal"}',
            "note": "Use ngrok (ngrok http 8080) to expose locally, or run on a VPS"
        }
    }


@app.post("/webhook/tradingview")
async def receive_alert(request: Request):
    """Receive and process a TradingView alert."""
    _session_stats["received"] += 1

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    try:
        alert = TVAlert(**body)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid alert format: {e}")

    asset = _ticker_to_asset(alert.ticker)
    bot_name = alert.bot or f"{asset} Bot (TradingView)"
    signal_type = "BUY_YES" if alert.action.lower() in ("long", "buy", "buy_yes") else "BUY_NO"
    is_long = signal_type == "BUY_YES"

    logger.info(
        f"📡 TradingView Alert | {asset} {alert.action.upper()} | "
        f"${alert.price:,.2f} | RSI={alert.rsi:.0f} | conf={alert.confidence:.0%}"
    )

    # Build stop/take-profit levels
    if is_long:
        stop_loss   = alert.price * 0.985   # 1.5% stop
        take_profit = alert.price * 1.04    # 4% target
    else:
        stop_loss   = alert.price * 1.015
        take_profit = alert.price * 0.96

    reason = alert.reason or f"TradingView {alert.timeframe} signal | RSI={alert.rsi:.0f}"

    # Record to win/loss tracker
    signal_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "asset": asset,
        "bot": bot_name,
        "signal": signal_type,
        "price": alert.price,
        "confidence": alert.confidence,
        "rsi": alert.rsi,
        "bb_pct": alert.bb_pct,
        "reason": reason,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
    }
    _signal_log.append(signal_entry)
    if len(_signal_log) > 100:
        _signal_log.pop(0)

    # Send Discord notification
    discord_sent = False
    try:
        from src.notifications.discord_webhook import get_discord_client
        discord = get_discord_client()
        if discord.enabled:
            color = 0x00C853 if is_long else 0xFF1744
            action_emoji = "🟢" if is_long else "🔴"
            direction_label = "LONG" if is_long else "SHORT"
            asset_emoji = "₿" if asset == "BTC" else "Ξ" if asset == "ETH" else "📈"

            risk = abs(alert.price - stop_loss)
            reward = abs(take_profit - alert.price)
            rr = reward / risk if risk > 0 else 0

            rsi_label = "Deeply Oversold 🔥" if alert.rsi < 25 else (
                "Oversold" if alert.rsi < 40 else (
                "Deeply Overbought 🔥" if alert.rsi > 75 else (
                "Overbought" if alert.rsi > 60 else "Neutral")))

            fields = [
                {"name": "Action", "value": f"{action_emoji} {direction_label} @ ${alert.price:,.2f}", "inline": False},
                {"name": "Stop Loss", "value": f"${stop_loss:,.2f}  (−1.5%)", "inline": True},
                {"name": "Take Profit", "value": f"${take_profit:,.2f}  (+4%)", "inline": True},
                {"name": "Risk/Reward", "value": f"1 : {rr:.1f}", "inline": True},
            ]
            if alert.rsi > 0:
                fields.append({"name": "RSI (14)", "value": f"{alert.rsi:.1f} — {rsi_label}", "inline": True})
            if alert.bb_pct != 0:
                bb_label = f"{abs(alert.bb_pct):.1f}% {'below lower' if is_long else 'above upper'} BB"
                fields.append({"name": "Bollinger Band", "value": bb_label, "inline": True})
            fields.append({"name": "Confidence", "value": f"{alert.confidence*100:.0f}%", "inline": True})
            fields.append({"name": "Timeframe", "value": alert.timeframe, "inline": True})
            if reason:
                fields.append({"name": "Reasoning", "value": reason, "inline": False})

            embed = {
                "title": f"{asset_emoji} {asset} TradingView Signal",
                "color": color,
                "fields": fields,
                "footer": {"text": f"Paper Trading | {bot_name} | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                "timestamp": datetime.utcnow().isoformat(),
            }
            discord_sent = await discord.send_message(embed=embed)
    except Exception as disc_err:
        logger.warning(f"Discord notification failed: {disc_err}")

    _session_stats["processed"] += 1
    logger.info(f"✅ Alert processed | Discord: {'sent' if discord_sent else 'skipped'}")

    return JSONResponse({
        "status": "ok",
        "asset": asset,
        "signal": signal_type,
        "price": alert.price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "discord_sent": discord_sent,
        "timestamp": datetime.utcnow().isoformat(),
    })


@app.get("/signals")
async def get_signals():
    """Get recent signals."""
    return {"signals": _signal_log[-20:], "stats": _session_stats}
