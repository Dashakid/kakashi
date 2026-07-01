"""
Unified Win/Loss Tracker for all trading bots.
Tracks performance across BTC, ETH, Polymarket, and Weather bots.
Persists data to ensure accurate historical records.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional
from loguru import logger


@dataclass
class TradeEntry:
    """Single trade record."""
    timestamp: str
    bot_name: str
    asset: str
    signal_type: str  # BUY_YES, BUY_NO, WEATHER_UP, WEATHER_DOWN, etc
    entry_price: float
    exit_price: float
    pnl_pct: float
    confidence: float
    is_win: bool
    duration_minutes: int = 0
    notes: str = ""
    dollar_pnl: Optional[float] = None
    # Entry-time market features for ML training
    market_features: Optional[Dict] = None


@dataclass
class BotStats:
    """Statistics for a single bot."""
    bot_name: str
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    ties: int = 0  # Break-even trades
    win_rate: float = 0.0
    loss_rate: float = 0.0
    avg_win_pnl: float = 0.0
    avg_loss_pnl: float = 0.0
    total_pnl: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0


@dataclass
class AggregatedStats:
    """Overall stats across all bots."""
    total_trades: int = 0
    total_wins: int = 0
    total_losses: int = 0
    total_ties: int = 0
    overall_win_rate: float = 0.0
    total_pnl: float = 0.0
    bot_stats: Dict[str, BotStats] = field(default_factory=dict)


# Minimum net P&L to count as a real win. Filters floating-point near-zero
# results (e.g. fee exactly cancelling gross) from inflating the win rate.
WIN_THRESHOLD = 0.001   # 0.1% net profit required


class WinLossTracker:
    """Unified tracker for all bot performance."""
    
    def __init__(self, data_dir: str = "data"):
        """Initialize tracker."""
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.trades_file = self.data_dir / "win_loss_history.jsonl"
        self.stats_file = self.data_dir / "win_loss_stats.json"
        
        self.trades: List[TradeEntry] = []
        self.bot_stats: Dict[str, BotStats] = {}
        
        self._load_history()
        self._calculate_stats()
    
    def _load_history(self):
        """Load all previous trades from file."""
        if not self.trades_file.exists():
            logger.info("📊 No trade history found - starting fresh")
            return
        
        try:
            skipped = 0
            with open(self.trades_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        trade_dict = json.loads(line)
                        # Tolerate old records missing market_features
                        trade_dict.setdefault('market_features', None)
                        trade = TradeEntry(**trade_dict)
                        self.trades.append(trade)
                    except (json.JSONDecodeError, TypeError, Exception) as line_err:
                        skipped += 1
                        logger.warning(
                            f"📊 Skipping corrupt trade history line {line_num}: "
                            f"{str(line_err)[:60]} — '{line.strip()[:50]}'"
                        )

            if skipped:
                logger.warning(f"📊 Loaded {len(self.trades)} trades ({skipped} corrupt lines skipped)")
            else:
                logger.info(f"📊 Loaded {len(self.trades)} historical trades")
        except Exception as e:
            logger.error(f"❌ Failed to load trade history: {e}")
    
    def record_trade(
        self,
        bot_name: str,
        asset: str,
        signal_type: str,
        entry_price: float,
        exit_price: float,
        pnl_pct: float,
        confidence: float,
        is_win: bool,
        duration_minutes: int = 0,
        notes: str = "",
        dollar_pnl: Optional[float] = None,
        market_features: Optional[Dict] = None,
    ) -> TradeEntry:
        """
        Record a completed trade.
        
        Args:
            bot_name: Name of the bot that executed the trade (BTC, ETH, Polymarket, Weather)
            asset: Asset traded (BTC, ETH, etc)
            signal_type: Type of signal (BUY_YES, BUY_NO, WEATHER_UP, etc)
            entry_price: Entry price
            exit_price: Exit price when trade closed
            pnl_pct: P&L as percentage (e.g., 0.02 for +2%)
            confidence: Confidence level at entry (0.0-1.0)
            is_win: Whether trade was profitable
            duration_minutes: How long trade was held
            notes: Additional notes
        
        Returns:
            The trade entry that was recorded
        """
        # Compute is_win from pnl_pct — the tracker is the source of truth.
        # This prevents callers from inflating win rates via floating-point
        # near-zero positives or hard-coded True/False values.
        is_win = pnl_pct > WIN_THRESHOLD

        trade = TradeEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            bot_name=bot_name,
            asset=asset,
            signal_type=signal_type,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_pct=pnl_pct,
            confidence=confidence,
            is_win=is_win,
            duration_minutes=duration_minutes,
            notes=notes,
            dollar_pnl=dollar_pnl,
            market_features=market_features,
        )
        
        # Write to disk FIRST — if the process is killed between disk write
        # and the in-memory append, the trade is still on disk and will be
        # reloaded on the next startup.  The previous order (append then save)
        # meant a kill between the two lines permanently lost the trade.
        self._save_trade(trade)
        self.trades.append(trade)
        self._calculate_stats()
        
        # Log the trade
        outcome = "✅ WIN" if is_win else "❌ LOSS"
        logger.info(
            f"📊 TRADE RECORDED | {bot_name:12} | {asset:5} | {outcome} | "
            f"${entry_price:,.0f} → ${exit_price:,.0f} | {pnl_pct:+.2%} | "
            f"Conf: {confidence:.0%}"
        )
        
        return trade
    
    def _save_trade(self, trade: TradeEntry):
        """Persist trade to file."""
        try:
            with open(self.trades_file, 'a') as f:
                f.write(json.dumps(asdict(trade)) + '\n')
        except Exception as e:
            logger.error(f"❌ Failed to save trade: {e}")
            # Dead-letter queue — persist failed writes for manual recovery
            dead_letter_file = self.data_dir / "dead_letter_trades.jsonl"
            try:
                with open(dead_letter_file, 'a') as dlf:
                    dlf.write(json.dumps({
                        "error": str(e),
                        "trade": asdict(trade),
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                    }) + '\n')
                logger.warning(f"Trade saved to dead-letter queue: {dead_letter_file}")
            except Exception as dlq_err:
                logger.critical(f"Dead-letter write also failed: {dlq_err} — trade LOST: {trade}")
    
    def _calculate_stats(self):
        """Recalculate all statistics."""
        self.bot_stats = {}
        
        # Group trades by bot
        trades_by_bot: Dict[str, List[TradeEntry]] = {}
        for trade in self.trades:
            if trade.bot_name not in trades_by_bot:
                trades_by_bot[trade.bot_name] = []
            trades_by_bot[trade.bot_name].append(trade)
        
        # Calculate per-bot stats
        for bot_name, bot_trades in trades_by_bot.items():
            stats = self._calculate_bot_stats(bot_name, bot_trades)
            self.bot_stats[bot_name] = stats
        
        # Save aggregated stats
        self._save_stats()
    
    def _calculate_bot_stats(self, bot_name: str, trades: List[TradeEntry]) -> BotStats:
        """Calculate stats for a single bot."""
        stats = BotStats(bot_name=bot_name)
        
        if not trades:
            return stats
        
        stats.total_trades = len(trades)
        stats.wins = sum(1 for t in trades if t.pnl_pct > WIN_THRESHOLD)
        stats.losses = sum(1 for t in trades if t.pnl_pct < -WIN_THRESHOLD)
        stats.ties = stats.total_trades - stats.wins - stats.losses
        
        stats.win_rate = (stats.wins / stats.total_trades) if stats.total_trades > 0 else 0.0
        stats.loss_rate = (stats.losses / stats.total_trades) if stats.total_trades > 0 else 0.0
        
        # Calculate P&L metrics
        if stats.wins > 0:
            winning_trades = [t.pnl_pct for t in trades if t.pnl_pct > WIN_THRESHOLD]
            stats.avg_win_pnl = sum(winning_trades) / len(winning_trades)
            stats.best_trade = max(winning_trades)
        
        if stats.losses > 0:
            losing_trades = [t.pnl_pct for t in trades if t.pnl_pct < -WIN_THRESHOLD]
            stats.avg_loss_pnl = sum(losing_trades) / len(losing_trades)
            stats.worst_trade = min(losing_trades)
        
        stats.total_pnl = sum(t.pnl_pct for t in trades)
        
        # Calculate consecutive streaks
        current_win_streak = 0
        current_loss_streak = 0
        
        for trade in trades:
            if trade.pnl_pct > WIN_THRESHOLD:
                current_win_streak += 1
                current_loss_streak = 0
                stats.max_consecutive_wins = max(stats.max_consecutive_wins, current_win_streak)
                stats.consecutive_wins = current_win_streak
            elif trade.pnl_pct < -WIN_THRESHOLD:
                current_loss_streak += 1
                current_win_streak = 0
                stats.max_consecutive_losses = max(stats.max_consecutive_losses, current_loss_streak)
                stats.consecutive_losses = current_loss_streak
            else:
                # Tie / break-even — reset streaks
                current_win_streak = 0
                current_loss_streak = 0
        
        return stats
    
    def _save_stats(self):
        """Save current stats to file (atomic write — temp + rename)."""
        try:
            # Convert stats to dict for JSON serialization
            stats_dict = {
                bot_name: {
                    'total_trades': stats.total_trades,
                    'wins': stats.wins,
                    'losses': stats.losses,
                    'ties': stats.ties,
                    'win_rate': stats.win_rate,
                    'loss_rate': stats.loss_rate,
                    'avg_win_pnl': stats.avg_win_pnl,
                    'avg_loss_pnl': stats.avg_loss_pnl,
                    'total_pnl': stats.total_pnl,
                    'best_trade': stats.best_trade,
                    'worst_trade': stats.worst_trade,
                    'consecutive_wins': stats.consecutive_wins,
                    'consecutive_losses': stats.consecutive_losses,
                    'max_consecutive_wins': stats.max_consecutive_wins,
                    'max_consecutive_losses': stats.max_consecutive_losses,
                }
                for bot_name, stats in self.bot_stats.items()
            }
            tmp = self.stats_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(stats_dict, indent=2))
            tmp.replace(self.stats_file)
        except Exception as e:
            logger.error(f"❌ Failed to save stats: {e}")
    
    def get_aggregated_stats(self) -> AggregatedStats:
        """Get overall statistics across all bots."""
        agg = AggregatedStats(bot_stats=self.bot_stats.copy())
        
        for stats in self.bot_stats.values():
            agg.total_trades += stats.total_trades
            agg.total_wins += stats.wins
            agg.total_losses += stats.losses
            agg.total_ties += stats.ties
            agg.total_pnl += stats.total_pnl
        
        agg.overall_win_rate = (
            (agg.total_wins / agg.total_trades) if agg.total_trades > 0 else 0.0
        )
        
        return agg
    
    def get_bot_stats(self, bot_name: str) -> Optional[BotStats]:
        """Get stats for a specific bot."""
        return self.bot_stats.get(bot_name)
    
    def get_track_record_string(self) -> str:
        """
        Get a formatted track record string.
        Example: "12W / 5L / 1T (70.6%)"
        """
        agg = self.get_aggregated_stats()
        
        if agg.total_trades == 0:
            return "0W / 0L (No trades yet)"
        
        tie_str = f" / {agg.total_ties}T" if agg.total_ties > 0 else ""
        record = f"{agg.total_wins}W / {agg.total_losses}L{tie_str}"
        win_pct = agg.overall_win_rate * 100
        
        return f"{record} ({win_pct:.1f}%)"
    
    def get_track_record_detailed(self) -> str:
        """Get detailed track record with per-bot breakdown."""
        lines = [
            "📊 === WIN/LOSS TRACKER ===",
            "",
            f"Overall Record: {self.get_track_record_string()}",
            ""
        ]
        
        # Add per-bot stats
        if self.bot_stats:
            lines.append("Per-Bot Breakdown:")
            for bot_name, stats in sorted(self.bot_stats.items()):
                if stats.total_trades > 0:
                    win_pct = stats.win_rate * 100
                    record = f"{stats.wins}W / {stats.losses}L"
                    if stats.ties > 0:
                        record += f" / {stats.ties}T"
                    pnl = f"+{stats.total_pnl:.1%}" if stats.total_pnl >= 0 else f"{stats.total_pnl:.1%}"
                    lines.append(f"  • {bot_name:12} | {record:15} ({win_pct:5.1f}%) | P&L: {pnl}")
        
        return "\n".join(lines)
    
    def get_discord_summary(self) -> str:
        """Get summary formatted for Discord webhook."""
        agg = self.get_aggregated_stats()
        
        if agg.total_trades == 0:
            return "No trades recorded yet"
        
        return f"{agg.total_wins}W / {agg.total_losses}L — {agg.overall_win_rate:.0%} win rate"


# Global singleton instance
_tracker_instance: Optional[WinLossTracker] = None


def get_tracker() -> WinLossTracker:
    """Get or create global tracker instance."""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = WinLossTracker()
    return _tracker_instance
