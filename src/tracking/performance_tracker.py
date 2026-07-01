"""
Real-time performance tracking and analytics for trading bot.
Tracks P&L, win rate, projections, and learning progress.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict
from loguru import logger


@dataclass
class TradeRecord:
    """Single trade record."""
    timestamp: str
    order_id: str
    signal: str
    entry_price: float
    exit_price: float
    pnl_pct: float
    confidence: float
    win: bool
    position_size: int


@dataclass
class DailyMetrics:
    """Daily performance metrics."""
    date: str
    starting_balance: float
    ending_balance: float
    daily_pnl: float
    daily_pnl_pct: float
    trades_executed: int
    trades_won: int
    win_rate: float
    avg_confidence: float


class PerformanceTracker:
    """Track and analyze bot performance."""
    
    def __init__(self, data_dir: str = "data"):
        """Initialize tracker."""
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.trades_file = self.data_dir / "trades.jsonl"
        self.metrics_file = self.data_dir / "daily_metrics.json"
        self.dashboard_file = self.data_dir / "dashboard.json"
        
        # Session tracking
        self.session_start_balance = 500.0  # Initial $500
        self.current_balance = 500.0
        self.trades: List[TradeRecord] = []
        self.daily_metrics: List[DailyMetrics] = []
        
        self._load_history()
    
    def _load_history(self):
        """Load previous trade history."""
        if self.trades_file.exists():
            with open(self.trades_file) as f:
                for line in f:
                    trade = json.loads(line)
                    self.trades.append(TradeRecord(**trade))
    
    def record_trade(self, order_id: str, signal: str, entry: float, exit: float,
                    pnl_pct: float, confidence: float, win: bool, size: int):
        """Record a completed trade."""
        trade = TradeRecord(
            timestamp=datetime.utcnow().isoformat(),
            order_id=order_id,
            signal=signal,
            entry_price=entry,
            exit_price=exit,
            pnl_pct=pnl_pct,
            confidence=confidence,
            win=win,
            position_size=size,
        )
        
        self.trades.append(trade)
        
        # Update balance
        self.current_balance *= (1 + pnl_pct)
        
        # Log and save
        logger.info(f"📊 TRADE RECORDED: {signal} | ${self.current_balance:.2f} (+{pnl_pct*100:.2f}%)")
        self._save_trade(trade)
        self._update_dashboard()
    
    def _save_trade(self, trade: TradeRecord):
        """Save trade to file."""
        with open(self.trades_file, 'a') as f:
            f.write(json.dumps(asdict(trade)) + '\n')
    
    def get_daily_summary(self) -> Dict:
        """Get today's performance summary."""
        today = datetime.utcnow().date().isoformat()
        today_trades = [t for t in self.trades if t.timestamp.startswith(today)]
        
        if not today_trades:
            return {
                'date': today,
                'trades': 0,
                'wins': 0,
                'win_rate': 0,
                'daily_pnl': 0,
                'daily_pnl_pct': 0,
            }
        
        wins = sum(1 for t in today_trades if t.win)
        daily_pnl = sum(t.pnl_pct for t in today_trades)
        
        return {
            'date': today,
            'trades': len(today_trades),
            'wins': wins,
            'win_rate': wins / len(today_trades),
            'daily_pnl': daily_pnl * 100,  # Percentage
            'daily_pnl_pct': daily_pnl,
            'avg_confidence': sum(t.confidence for t in today_trades) / len(today_trades),
        }
    
    def get_performance_summary(self) -> Dict:
        """Get overall performance metrics."""
        if not self.trades:
            return {
                'status': 'LEARNING',
                'trades_total': 0,
                'account_balance': self.current_balance,
                'total_pnl': 0,
                'total_pnl_pct': 0,
                'win_rate': 0,
            }
        
        wins = sum(1 for t in self.trades if t.win)
        total_pnl = self.current_balance - self.session_start_balance
        total_pnl_pct = (self.current_balance / self.session_start_balance) - 1
        
        return {
            'status': 'ACTIVE',
            'trades_total': len(self.trades),
            'account_balance': self.current_balance,
            'total_pnl': total_pnl,
            'total_pnl_pct': total_pnl_pct,
            'win_rate': wins / len(self.trades) if self.trades else 0,
            'avg_confidence': sum(t.confidence for t in self.trades) / len(self.trades),
            'session_start': self.session_start_balance,
        }
    
    def get_projections(self) -> Dict:
        """Project future balance assuming consistent returns."""
        summary = self.get_performance_summary()
        
        if summary['trades_total'] < 5:
            return {'status': 'INSUFFICIENT_DATA', 'message': 'Need 5+ trades for projection'}
        
        # Calculate daily return from trades
        daily_trades = []
        current_day = None
        day_pnl = 0
        
        for trade in self.trades:
            trade_day = trade.timestamp.split('T')[0]
            if trade_day != current_day:
                if current_day is not None:
                    daily_trades.append(day_pnl)
                current_day = trade_day
                day_pnl = 0
            day_pnl += trade.pnl_pct
        
        if daily_trades:
            avg_daily_return = sum(daily_trades) / len(daily_trades)
        else:
            avg_daily_return = summary['total_pnl_pct'] / max(1, len(self.trades) / 50)
        
        # Project forward
        projections = {}
        current_bal = self.current_balance
        
        timeframes = {
            'week': 7,
            'month': 30,
            'quarter': 90,
        }
        
        for period, days in timeframes.items():
            projected = current_bal * ((1 + avg_daily_return) ** days)
            projections[period] = {
                'balance': projected,
                'pnl': projected - self.current_balance,
                'daily_return_pct': avg_daily_return * 100,
                'days': days,
            }
        
        return {
            'status': 'ACTIVE',
            'current_balance': self.current_balance,
            'avg_daily_return_pct': avg_daily_return * 100,
            'projections': projections,
        }
    
    def _update_dashboard(self):
        """Update dashboard JSON file."""
        daily = self.get_daily_summary()
        overall = self.get_performance_summary()
        projections = self.get_projections()
        
        dashboard = {
            'timestamp': datetime.utcnow().isoformat(),
            'daily': daily,
            'overall': overall,
            'projections': projections,
        }
        
        with open(self.dashboard_file, 'w') as f:
            json.dump(dashboard, f, indent=2)
    
    def print_summary(self):
        """Print formatted summary to logs."""
        summary = self.get_performance_summary()
        daily = self.get_daily_summary()
        proj = self.get_projections()
        
        logger.info(
            f"\n{'='*70}\n"
            f"💰 PERFORMANCE SUMMARY\n"
            f"{'='*70}\n"
            f"Account Balance:     ${summary['account_balance']:.2f}\n"
            f"Total P&L:           ${summary['total_pnl']:.2f} ({summary['total_pnl_pct']*100:+.2f}%)\n"
            f"Total Trades:        {summary['trades_total']}\n"
            f"Win Rate:            {summary['win_rate']*100:.1f}%\n"
            f"Avg Confidence:      {summary['avg_confidence']*100:.0f}%\n"
            f"\n"
            f"TODAY'S METRICS\n"
            f"{'─'*70}\n"
            f"Trades Today:        {daily['trades']}\n"
            f"Wins Today:          {daily['wins']} ({daily['win_rate']*100:.1f}%)\n"
            f"Daily P&L:           {daily['daily_pnl_pct']*100:+.2f}%\n"
            f"\n"
            f"PROJECTIONS (assuming {proj.get('avg_daily_return_pct', 0):.2f}% daily return)\n"
            f"{'─'*70}\n"
            f"Week:   ${proj.get('projections', {}).get('week', {}).get('balance', 0):.2f}\n"
            f"Month:  ${proj.get('projections', {}).get('month', {}).get('balance', 0):.2f}\n"
            f"3Mo:    ${proj.get('projections', {}).get('quarter', {}).get('balance', 0):.2f}\n"
            f"{'='*70}\n"
        )
