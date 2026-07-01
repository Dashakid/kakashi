"""Discord webhook integration for real-time bot notifications."""

import os
import json
import aiohttp
from datetime import datetime
from typing import Optional, Dict, Any
from loguru import logger


# ---------------------------------------------------------------------------
# Shared aiohttp session — one connection pool per process.
# Created lazily on first use so it is always inside a running event loop.
# ---------------------------------------------------------------------------
_shared_session: Optional[aiohttp.ClientSession] = None


def _get_shared_session() -> Optional[aiohttp.ClientSession]:
    """Return (creating if needed) a long-lived aiohttp session for Discord posts."""
    global _shared_session
    if _shared_session is None or _shared_session.closed:
        try:
            _shared_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10, connect=5)
            )
        except Exception:
            return None
    return _shared_session


class DiscordWebhook:
    """Send notifications to Discord via webhook."""
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize Discord webhook client.
        
        Args:
            webhook_url: Discord webhook URL. Can also be set via DISCORD_WEBHOOK_URL env var
        """
        self.webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)
        
        if not self.enabled:
            logger.warning("⚠️  Discord webhook not configured. Set DISCORD_WEBHOOK_URL to enable notifications")
    
    async def send_message(self, content: str = "", embed: Dict[str, Any] = None) -> bool:
        """
        Send a message to Discord.
        
        Args:
            content: Plain text message
            embed: Discord embed dict for rich formatting
        
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            payload = {
                "content": content if content else None,
                "embeds": [embed] if embed else None
            }
            
            # Remove None values
            payload = {k: v for k, v in payload.items() if v is not None}

            # Reuse the module-level session to avoid opening a new TCP connection
            # (and leaking file descriptors) on every alert.
            session = _get_shared_session()
            if session is None or session.closed:
                # Fallback: create a one-shot session (e.g. called outside an event loop)
                async with aiohttp.ClientSession() as fallback_session:
                    async with fallback_session.post(
                        self.webhook_url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status in [200, 204]:
                            return True
                        logger.error(f"Discord webhook failed: {response.status}")
                        return False
            else:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status in [200, 204]:
                        return True
                    logger.error(f"Discord webhook failed: {response.status}")
                    return False
        
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            return False
    
    def create_trade_embed(
        self,
        bot_name: str,
        signal: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        confidence: float,
        asset: str = "BTC",
        rsi: float = 0.0,
        bb_pct: float = 0.0,
        uptrend: bool = True,
        volume_conf: float = 0.5,
        reasoning: str = "",
    ) -> Dict[str, Any]:
        """Create a Discord embed for a trade signal (weather-style layout)."""
        is_long = "YES" in signal
        color = 0x00C853 if is_long else 0xFF1744
        asset_emoji = "₿" if asset == "BTC" else "Ξ" if asset == "ETH" else "📈"
        direction_label = "LONG" if is_long else "SHORT"
        action_emoji = "🟢" if is_long else "🔴"

        risk = abs(entry_price - stop_loss)
        reward = abs(take_profit - entry_price)
        rr_ratio = reward / risk if risk > 0 else 0

        risk_pct = (risk / entry_price) * 100
        reward_pct = (reward / entry_price) * 100

        # RSI label
        if rsi < 25:
            rsi_label = "Deeply Oversold 🔥"
        elif rsi < 35:
            rsi_label = "Oversold"
        elif rsi > 75:
            rsi_label = "Deeply Overbought 🔥"
        elif rsi > 65:
            rsi_label = "Overbought"
        else:
            rsi_label = "Neutral"

        # BB label
        bb_abs = abs(bb_pct)
        if is_long:
            bb_label = f"{bb_abs:.1f}% below lower band"
        else:
            bb_label = f"{bb_abs:.1f}% above upper band"

        trend_label = "📈 Uptrend" if uptrend else "📉 Downtrend"
        vol_label = f"{volume_conf * 2:.1f}× average"

        if not reasoning:
            reasoning = f"RSI={rsi:.0f} | BB={bb_pct:+.1f}% | {trend_label}"

        embed = {
            "title": f"{asset_emoji} {asset} 4-Hour Direction Signal",
            "color": color,
            "fields": [
                {
                    "name": "Action",
                    "value": f"{action_emoji} {direction_label} @ ${entry_price:,.2f}",
                    "inline": False,
                },
                {
                    "name": "Stop Loss",
                    "value": f"${stop_loss:,.2f}  (−{risk_pct:.2f}%)",
                    "inline": True,
                },
                {
                    "name": "Take Profit",
                    "value": f"${take_profit:,.2f}  (+{reward_pct:.2f}%)",
                    "inline": True,
                },
                {
                    "name": "Risk / Reward",
                    "value": f"1 : {rr_ratio:.1f}",
                    "inline": True,
                },
                {
                    "name": "RSI (14)",
                    "value": f"{rsi:.1f} — {rsi_label}",
                    "inline": True,
                },
                {
                    "name": "Bollinger Band",
                    "value": bb_label,
                    "inline": True,
                },
                {
                    "name": "Trend Bias",
                    "value": trend_label,
                    "inline": True,
                },
                {
                    "name": "Volume",
                    "value": vol_label,
                    "inline": True,
                },
                {
                    "name": "Confidence",
                    "value": f"{confidence * 100:.0f}%",
                    "inline": True,
                },
                {
                    "name": "Reasoning",
                    "value": reasoning,
                    "inline": False,
                },
                {
                    "name": "Estimated Hold",
                    "value": "~4 hours",
                    "inline": False,
                },
            ],
            "footer": {
                "text": f"Paper Trading | {bot_name} | {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
            },
            "timestamp": datetime.now().isoformat(),
        }

        return embed
    
    def create_trade_result_embed(
        self,
        bot_name: str,
        signal: str,
        entry_price: float,
        exit_price: float,
        pnl_pct: float,
        confidence: float,
        win: bool,
        asset: str = "BTC",
        session_wins: int = 0,
        session_losses: int = 0,
        duration_minutes: int = 0,
    ) -> Dict[str, Any]:
        """Create a Discord embed for a completed trade result (win or loss)."""
        is_long = "YES" in signal
        outcome_emoji = "✅" if win else "❌"
        outcome_label = "WIN" if win else "LOSS"
        color = 0x00C853 if win else 0xFF1744
        asset_emoji = "₿" if asset == "BTC" else "Ξ" if asset == "ETH" else "📈"
        direction_label = "LONG" if is_long else "SHORT"

        pnl_sign = "+" if pnl_pct >= 0 else ""
        pnl_display = f"{pnl_sign}{pnl_pct * 100:.2f}%"

        total = session_wins + session_losses
        win_rate = (session_wins / total * 100) if total > 0 else 0.0
        record_str = f"{session_wins}W / {session_losses}L — {win_rate:.0f}% win rate"

        duration_str = ""
        if duration_minutes > 0:
            h, m = divmod(duration_minutes, 60)
            duration_str = f"{h}h {m}m" if h else f"{m}m"

        fields = [
            {"name": "Outcome", "value": f"{outcome_emoji} {outcome_label}  ({pnl_display})", "inline": False},
            {"name": "Direction", "value": f"{'🟢' if is_long else '🔴'} {direction_label}", "inline": True},
            {"name": "Entry → Exit", "value": f"${entry_price:,.2f} → ${exit_price:,.2f}", "inline": True},
            {"name": "P&L", "value": pnl_display, "inline": True},
            {"name": "Confidence at Entry", "value": f"{confidence * 100:.0f}%", "inline": True},
        ]
        if duration_str:
            fields.append({"name": "Duration", "value": duration_str, "inline": True})
        if total > 0:
            fields.append({"name": "Session Record", "value": record_str, "inline": False})

        return {
            "title": f"{asset_emoji} {asset} Trade Closed",
            "color": color,
            "fields": fields,
            "footer": {
                "text": f"Paper Trading | {bot_name} | {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
            },
            "timestamp": datetime.now().isoformat(),
        }

    def create_startup_embed(
        self,
        bot_name: str,
        strategy: str = "4-Hour Bollinger Bands + RSI",
        mode: str = "Paper Trading"
    ) -> Dict[str, Any]:
        """
        Create a Discord embed for bot startup.
        
        Args:
            bot_name: Name of the bot
            strategy: Strategy name
            mode: Trading mode (Paper Trading, Live Trading)
        
        Returns:
            Discord embed dict
        """
        embed = {
            "title": f"🚀 {bot_name} Started",
            "color": 0x0099FF,
            "fields": [
                {
                    "name": "📈 Strategy",
                    "value": strategy,
                    "inline": False
                },
                {
                    "name": "🔄 Mode",
                    "value": mode,
                    "inline": True
                },
                {
                    "name": "🎯 Timeframe",
                    "value": "4-Hour",
                    "inline": True
                },
                {
                    "name": "⏰ Status",
                    "value": "Monitoring for signals...",
                    "inline": False
                }
            ],
            "footer": {
                "text": f"Bot initialized | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return embed
    
    def create_stats_embed(
        self,
        bot_name: str,
        total_signals: int,
        win_rate: float,
        avg_pnl: float,
        confidence_avg: float
    ) -> Dict[str, Any]:
        """
        Create a Discord embed for performance stats.
        
        Args:
            bot_name: Name of the bot
            total_signals: Total signals generated
            win_rate: Win rate percentage
            avg_pnl: Average P&L per trade
            confidence_avg: Average confidence
        
        Returns:
            Discord embed dict
        """
        # Color code based on performance
        if win_rate >= 55:
            color = 0x00FF00  # Green
        elif win_rate >= 50:
            color = 0xFFFF00  # Yellow
        else:
            color = 0xFF0000  # Red
        
        embed = {
            "title": f"📊 {bot_name} Performance Update",
            "color": color,
            "fields": [
                {
                    "name": "🎯 Signals",
                    "value": f"{total_signals}",
                    "inline": True
                },
                {
                    "name": "✅ Win Rate",
                    "value": f"{win_rate:.1f}%",
                    "inline": True
                },
                {
                    "name": "💰 Avg P&L",
                    "value": f"{avg_pnl:+.2f}%",
                    "inline": True
                },
                {
                    "name": "💡 Avg Confidence",
                    "value": f"{confidence_avg:.1f}%",
                    "inline": True
                }
            ],
            "footer": {
                "text": f"Performance metrics | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return embed
    
    def create_error_embed(
        self,
        bot_name: str,
        error_msg: str,
        error_type: str = "Error"
    ) -> Dict[str, Any]:
        """
        Create a Discord embed for error notifications.
        
        Args:
            bot_name: Name of the bot
            error_msg: Error message
            error_type: Type of error
        
        Returns:
            Discord embed dict
        """
        embed = {
            "title": f"⚠️ {bot_name} {error_type}",
            "color": 0xFF0000,
            "fields": [
                {
                    "name": "Error Details",
                    "value": f"```\n{error_msg[:500]}\n```",
                    "inline": False
                }
            ],
            "footer": {
                "text": f"Error logged | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return embed
    
    async def send_trade_alert(
        self,
        bot_name: str,
        signal: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        confidence: float,
        asset: str = "BTC",
        rsi: float = 0.0,
        bb_pct: float = 0.0,
        uptrend: bool = True,
        volume_conf: float = 0.5,
        reasoning: str = "",
    ) -> bool:
        """Send a trade signal alert to Discord."""
        embed = self.create_trade_embed(
            bot_name=bot_name,
            signal=signal,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            confidence=confidence,
            asset=asset,
            rsi=rsi,
            bb_pct=bb_pct,
            uptrend=uptrend,
            volume_conf=volume_conf,
            reasoning=reasoning,
        )
        return await self.send_message(embed=embed)

    async def send_trade_result(
        self,
        bot_name: str,
        signal: str,
        entry_price: float,
        exit_price: float,
        pnl_pct: float,
        confidence: float,
        win: bool,
        asset: str = "BTC",
        session_wins: int = 0,
        session_losses: int = 0,
        duration_minutes: int = 0,
    ) -> bool:
        """Send a trade result (win/loss) notification to Discord."""
        embed = self.create_trade_result_embed(
            bot_name=bot_name,
            signal=signal,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_pct=pnl_pct,
            confidence=confidence,
            win=win,
            asset=asset,
            session_wins=session_wins,
            session_losses=session_losses,
            duration_minutes=duration_minutes,
        )
        return await self.send_message(embed=embed)
    
    async def send_startup_alert(
        self,
        bot_name: str,
        strategy: str = "4-Hour Bollinger Bands + RSI",
        mode: str = "Paper Trading"
    ) -> bool:
        """Send bot startup notification to Discord."""
        embed = self.create_startup_embed(bot_name, strategy, mode)
        return await self.send_message(embed=embed)
    
    async def send_stats_update(
        self,
        bot_name: str,
        total_signals: int,
        win_rate: float,
        avg_pnl: float,
        confidence_avg: float
    ) -> bool:
        """Send performance stats update to Discord."""
        embed = self.create_stats_embed(
            bot_name=bot_name,
            total_signals=total_signals,
            win_rate=win_rate,
            avg_pnl=avg_pnl,
            confidence_avg=confidence_avg
        )
        return await self.send_message(embed=embed)
    
    async def send_error_alert(
        self,
        bot_name: str,
        error_msg: str,
        error_type: str = "Error"
    ) -> bool:
        """Send error notification to Discord."""
        embed = self.create_error_embed(bot_name, error_msg, error_type)
        return await self.send_message(embed=embed)


# Global singleton instance (optional)
_discord_client: Optional[DiscordWebhook] = None


def get_discord_client(webhook_url: Optional[str] = None) -> DiscordWebhook:
    """Get or create global Discord webhook client.

    The singleton is rebuilt whenever:
    - it hasn't been created yet, OR
    - the caller passes an explicit webhook_url, OR
    - the singleton was built with enabled=False but DISCORD_WEBHOOK_URL is now set
      (handles the case where the env var is set after first import, e.g. Kakashi
      sets os.environ["DISCORD_WEBHOOK_URL"] inside run_forever()).
    """
    global _discord_client
    current_env_url = os.getenv("DISCORD_WEBHOOK_URL")
    if (
        _discord_client is None
        or webhook_url is not None
        or (not _discord_client.enabled and current_env_url)
        or (webhook_url is None and current_env_url and _discord_client.webhook_url != current_env_url)
    ):
        _discord_client = DiscordWebhook(webhook_url or current_env_url)
    return _discord_client


async def send_discord_alert(
    message: str = "",
    embed: Dict[str, Any] = None
) -> bool:
    """
    Convenience function to send a Discord alert using global client.
    
    Args:
        message: Plain text message
        embed: Discord embed dict
    
    Returns:
        True if sent successfully
    """
    client = get_discord_client()
    return await client.send_message(message, embed)
