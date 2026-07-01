"""Unified alert system for trading bots."""

from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum
from loguru import logger

from src.notifications.discord_webhook import get_discord_client


class AlertType(Enum):
    """Alert types for bots."""
    STARTUP = "STARTUP"
    SHUTDOWN = "SHUTDOWN"
    SIGNAL = "SIGNAL"
    ERROR = "ERROR"
    TRADE = "TRADE"


class BotAlert:
    """Unified alert system for all bots."""
    
    @staticmethod
    async def startup(bot_name: str, strategy: str, mode: str) -> bool:
        """Send startup notification."""
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return False
            
            embed = {
                "title": f"🚀 {bot_name} Started",
                "description": strategy,
                "color": 0x00AA00,  # Green
                "fields": [
                    {"name": "Mode", "value": mode, "inline": True},
                    {"name": "Status", "value": "✅ Running", "inline": True},
                    {"name": "⏰ Time", "value": datetime.now().strftime("%H:%M:%S"), "inline": True}
                ],
                "timestamp": datetime.now().isoformat()
            }
            
            await discord.send_message(embed=embed)
            logger.info(f"✅ Sent {AlertType.STARTUP.value} notification")
            return True
        except Exception as e:
            logger.warning(f"Could not send startup alert: {e}")
            return False
    
    @staticmethod
    async def shutdown(bot_name: str, reason: str = "Manual shutdown") -> bool:
        """Send shutdown notification."""
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return False
            
            embed = {
                "title": f"⏹️ {bot_name} Stopped",
                "color": 0xFF9900,  # Orange
                "fields": [
                    {"name": "Reason", "value": reason, "inline": False},
                    {"name": "Status", "value": "Gracefully stopped", "inline": True},
                    {"name": "⏰ Time", "value": datetime.now().strftime("%H:%M:%S"), "inline": True}
                ],
                "timestamp": datetime.now().isoformat()
            }
            
            await discord.send_message(embed=embed)
            logger.info(f"✅ Sent {AlertType.SHUTDOWN.value} notification")
            return True
        except Exception as e:
            logger.warning(f"Could not send shutdown alert: {e}")
            return False
    
    @staticmethod
    async def signal(bot_name: str, signal_type: str, price: float, confidence: float, details: Optional[Dict[str, Any]] = None) -> bool:
        """Send trade signal notification.
        
        NOTE: Signal alerts are the ONLY alerts sent individually by bots.
        All crash/error alerts are handled by bot_monitor only.
        """
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return False
            
            # Color: Green for BUY, Red for SELL, Yellow for NEUTRAL
            color_map = {
                "BUY": 0x00FF00,
                "SELL": 0xFF0000,
                "NEUTRAL": 0xFFFF00,
                "LONG": 0x00FF00,
                "SHORT": 0xFF0000,
                "COPY_YES": 0x00C853,
                "COPY_NO": 0xFF6D00,
            }
            color = color_map.get(signal_type.upper(), 0x0099FF)
            
            fields = [
                {"name": "Signal", "value": signal_type, "inline": True},
                {"name": "Confidence", "value": f"{confidence*100:.0f}%", "inline": True},
                {"name": "Price", "value": f"${price:,.2f}", "inline": True},
            ]
            
            if details:
                for key, value in details.items():
                    fields.append({"name": key, "value": str(value)[:1024], "inline": False})
            
            embed = {
                "title": f"📊 {bot_name} Signal",
                "description": details.get("Market", f"{signal_type} Signal Detected") if details and "Market" in details else f"{signal_type} Signal Detected",
                "color": color,
                "fields": fields,
                "timestamp": datetime.now().isoformat()
            }
            
            await discord.send_message(embed=embed)
            logger.info(f"✅ Sent {AlertType.SIGNAL.value} notification")
            return True
        except Exception as e:
            logger.warning(f"Could not send signal alert: {e}")
            return False
    
    @staticmethod
    async def trade(bot_name: str, side: str, size: float, entry_price: float, target_price: float, stop_price: float) -> bool:
        """Send trade execution notification."""
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return False
            
            # Color: Green for BUY, Red for SELL
            color = 0x00FF00 if side.upper() in ["BUY", "LONG"] else 0xFF0000
            
            embed = {
                "title": f"🎯 {bot_name} Trade Executed",
                "description": f"{side.upper()} {size} units",
                "color": color,
                "fields": [
                    {"name": "Side", "value": side.upper(), "inline": True},
                    {"name": "Size", "value": f"{size:.4f}", "inline": True},
                    {"name": "Entry Price", "value": f"${entry_price:,.2f}", "inline": True},
                    {"name": "Target", "value": f"${target_price:,.2f}", "inline": True},
                    {"name": "Stop", "value": f"${stop_price:,.2f}", "inline": True},
                    {"name": "Risk/Reward", "value": f"1:{(target_price-entry_price)/(entry_price-stop_price):.2f}", "inline": True},
                ],
                "timestamp": datetime.now().isoformat()
            }
            
            await discord.send_message(embed=embed)
            logger.info(f"✅ Sent {AlertType.TRADE.value} notification")
            return True
        except Exception as e:
            logger.warning(f"Could not send trade alert: {e}")
            return False

    @staticmethod
    async def trade_result(
        bot_name: str,
        asset: str,
        signal_type: str,
        pnl_pct: float,
        is_win: bool,
        entry_price: float,
        exit_price: float,
        session_wins: int,
        session_losses: int,
        session_pnl: float,
        all_time_wins: int,
        all_time_losses: int,
        streak: int = 0,
        dollar_pnl: float = None,
        account_balance: float = None,
        starting_capital: float = None,
        is_live: bool = False,
    ) -> bool:
        """Send a trade result notification with running W/L stats."""
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return False

            color = 0x00FF7F if is_win else 0xFF4444
            outcome_emoji = "✅" if is_win else "❌"
            outcome_label = "WIN" if is_win else "LOSS"
            pnl_str = f"{pnl_pct:+.2%}"
            if dollar_pnl is not None:
                pnl_str = f"{'+' if dollar_pnl >= 0 else ''}${dollar_pnl:.2f} ({pnl_pct:+.2%})"

            session_total = session_wins + session_losses
            session_wr = (session_wins / session_total * 100) if session_total > 0 else 0
            all_time_total = all_time_wins + all_time_losses
            all_time_wr = (all_time_wins / all_time_total * 100) if all_time_total > 0 else 0

            streak_str = ""
            if streak >= 3:
                streak_str = f"🔥 {streak} in a row!" if is_win else f"💀 {streak} losses in a row"

            session_pnl_str = f"{session_pnl:+.2%}"

            # Prices under 1.0 are contract probabilities (Polymarket), not dollar prices
            price_fmt = (lambda p: f"{p:.3f} ({p*100:.0f}%)") if entry_price <= 1.0 else (lambda p: f"${p:,.2f}")

            fields = [
                {"name": "Result", "value": f"{outcome_emoji} {outcome_label}", "inline": True},
                {"name": "P&L", "value": pnl_str, "inline": True},
                {"name": "Signal", "value": signal_type, "inline": True},
                {"name": "Entry", "value": price_fmt(entry_price), "inline": True},
                {"name": "Exit", "value": price_fmt(exit_price), "inline": True},
                {"name": "\u200b", "value": "\u200b", "inline": True},
                {"name": "📅 This Session", "value": f"{session_wins}W / {session_losses}L ({session_wr:.0f}%) | P&L: {session_pnl_str}", "inline": False},
                {"name": "🏆 All Time", "value": f"{all_time_wins}W / {all_time_losses}L ({all_time_wr:.0f}%)", "inline": False},
            ]

            if account_balance is not None and starting_capital is not None:
                total_return = account_balance - starting_capital
                fields.append({
                    "name": "💰 Account",
                    "value": f"${account_balance:.2f} ({'+' if total_return >= 0 else ''}${total_return:.2f} from ${starting_capital:.0f})",
                    "inline": False,
                })

            if streak_str:
                fields.append({"name": "Streak", "value": streak_str, "inline": False})

            embed = {
                "title": f"{outcome_emoji} {bot_name} | {asset} | {pnl_str}",
                "color": color,
                "fields": fields,
                "footer": {"text": f"{'🔴 Live' if is_live else '📄 Paper'} trading | {datetime.now().strftime('%H:%M:%S')}"},
                "timestamp": datetime.now().isoformat()
            }

            await discord.send_message(embed=embed)
            return True
        except Exception as e:
            logger.warning(f"Could not send trade_result alert: {e}")
            return False
