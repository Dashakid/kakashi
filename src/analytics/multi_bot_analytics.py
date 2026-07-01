"""
Centralized multi-bot analytics engine.
Tracks performance across all bots, markets, and identifies best/worst performers.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from loguru import logger


@dataclass
class Trade:
    """Trade record with bot attribution."""
    trade_id: str                    # Unique ID
    bot_id: str                      # "btc-bot", "eth-bot", etc.
    market_id: str                   # Polymarket market ID
    asset: str                       # BTC, ETH, SOL
    
    timestamp: str                   # Signal timestamp (ISO)
    signal: str                      # BUY_YES or BUY_NO
    entry_price: float               # Market price at entry
    entry_amount: float              # $ invested
    confidence: float                # Signal confidence 0-100
    
    executed_price: float            # Actual filled price
    executed_amount: float           # Actual size
    
    exit_timestamp: Optional[str]    # When position closed
    exit_price: Optional[float]      # Exit price
    exit_reason: str                 # PROFIT / LOSS / TIME / MANUAL
    
    pnl_amount: float                # $ profit/loss
    pnl_pct: float                   # % return
    win: bool                        # True if profitable
    
    live: bool                       # Live or paper trading
    created_at: str                  # DB timestamp


class MultiBoTAnalytics:
    """Central analytics engine for all trading bots."""
    
    def __init__(self, db_path: str = "data/analytics.db"):
        """Initialize analytics database."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self.conn = None
        self._init_db()
    
    def _init_db(self):
        """Create database schema if not exists."""
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Main trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                bot_id TEXT NOT NULL,
                market_id TEXT,
                asset TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                signal TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_amount REAL NOT NULL,
                confidence REAL NOT NULL,
                executed_price REAL,
                executed_amount REAL,
                exit_timestamp TEXT,
                exit_price REAL,
                exit_reason TEXT,
                pnl_amount REAL NOT NULL,
                pnl_pct REAL NOT NULL,
                win INTEGER NOT NULL,
                live INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Daily metrics cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_metrics (
                id INTEGER PRIMARY KEY,
                bot_id TEXT NOT NULL,
                date TEXT NOT NULL,
                trades_executed INTEGER,
                trades_won INTEGER,
                win_rate REAL,
                daily_pnl REAL,
                daily_pnl_pct REAL,
                avg_confidence REAL,
                UNIQUE(bot_id, date)
            )
        ''')
        
        # Market performance cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_performance (
                id INTEGER PRIMARY KEY,
                market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                trades_count INTEGER,
                win_count INTEGER,
                win_rate REAL,
                avg_pnl_pct REAL,
                total_pnl REAL,
                last_updated TEXT,
                UNIQUE(market_id)
            )
        ''')
        
        # Bot rankings cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_rankings (
                id INTEGER PRIMARY KEY,
                bot_id TEXT NOT NULL UNIQUE,
                total_trades INTEGER,
                total_wins INTEGER,
                win_rate REAL,
                total_pnl REAL,
                total_pnl_pct REAL,
                avg_confidence REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                last_updated TEXT
            )
        ''')
        
        self.conn.commit()
        logger.info(f"✅ Analytics DB initialized: {self.db_path}")
    
    def record_trade(self, trade: Trade) -> bool:
        """Record a trade from a bot."""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO trades (
                    trade_id, bot_id, market_id, asset, timestamp,
                    signal, entry_price, entry_amount, confidence,
                    executed_price, executed_amount, exit_timestamp,
                    exit_price, exit_reason, pnl_amount, pnl_pct,
                    win, live
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade.trade_id, trade.bot_id, trade.market_id, trade.asset,
                trade.timestamp, trade.signal, trade.entry_price, trade.entry_amount,
                trade.confidence, trade.executed_price, trade.executed_amount,
                trade.exit_timestamp, trade.exit_price, trade.exit_reason,
                trade.pnl_amount, trade.pnl_pct, int(trade.win), int(trade.live)
            ))
            self.conn.commit()
            logger.debug(f"📊 Trade recorded: {trade.bot_id} {trade.signal} {trade.pnl_pct:+.2%}")
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"ℹ️ Trade {trade.trade_id} already exists")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to record trade: {e}")
            return False
    
    # =====================
    # RANKING QUERIES
    # =====================
    
    def get_bot_rankings(self) -> List[Dict]:
        """Rank all bots by win rate and P&L."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                bot_id,
                COUNT(*) as total_trades,
                SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as total_wins,
                ROUND(CAST(SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) as win_rate,
                ROUND(SUM(pnl_pct), 4) as total_pnl_pct,
                ROUND(SUM(pnl_amount), 2) as total_pnl_amount,
                ROUND(AVG(confidence), 1) as avg_confidence
            FROM trades
            GROUP BY bot_id
            ORDER BY win_rate DESC, total_pnl_pct DESC
        ''')
        
        rankings = []
        for row in cursor.fetchall():
            rankings.append({
                'bot_id': row[0],
                'total_trades': row[1],
                'total_wins': row[2],
                'win_rate': row[3],
                'total_pnl_pct': row[4],
                'total_pnl_amount': row[5],
                'avg_confidence': row[6],
                'rank': len(rankings) + 1
            })
        
        return rankings
    
    # =====================
    # MARKET PERFORMANCE
    # =====================
    
    def get_market_performance(self, min_trades: int = 3) -> List[Dict]:
        """Identify best and worst performing markets."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                market_id,
                asset,
                COUNT(*) as trades,
                SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as wins,
                ROUND(CAST(SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) as win_rate,
                ROUND(AVG(pnl_pct), 4) as avg_pnl_pct,
                ROUND(SUM(pnl_amount), 2) as total_pnl
            FROM trades
            GROUP BY market_id
            HAVING COUNT(*) >= ?
            ORDER BY win_rate DESC
        ''', (min_trades,))
        
        markets = []
        for row in cursor.fetchall():
            markets.append({
                'market_id': row[0],
                'asset': row[1],
                'trades': row[2],
                'wins': row[3],
                'win_rate': row[4],
                'avg_pnl_pct': row[5],
                'total_pnl': row[6]
            })
        
        return markets
    
    # =====================
    # SIGNAL QUALITY ANALYSIS
    # =====================
    
    def analyze_by_confidence(self) -> Dict:
        """Analyze win rate by signal confidence level."""
        cursor = self.conn.cursor()
        
        confidence_ranges = [
            (80, 100, '80-100%'),
            (70, 80, '70-80%'),
            (60, 70, '60-70%'),
            (0, 60, '<60%')
        ]
        
        results = {}
        for min_conf, max_conf, label in confidence_ranges:
            cursor.execute('''
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as wins,
                    ROUND(CAST(SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) as win_rate,
                    ROUND(AVG(pnl_pct), 4) as avg_pnl_pct
                FROM trades
                WHERE confidence >= ? AND confidence < ?
            ''', (min_conf, max_conf))
            
            row = cursor.fetchone()
            if row[0] > 0:  # Only include buckets with trades
                results[label] = {
                    'trades': row[0],
                    'wins': row[1],
                    'win_rate': row[2],
                    'avg_pnl_pct': row[3]
                }
        
        return results
    
    # =====================
    # DAILY PERFORMANCE
    # =====================
    
    def get_daily_summary(self, bot_id: str, days: int = 7) -> List[Dict]:
        """Get daily P&L for a bot."""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as trades,
                SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as wins,
                ROUND(CAST(SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) as win_rate,
                ROUND(SUM(pnl_pct), 4) as daily_pnl_pct,
                ROUND(SUM(pnl_amount), 2) as daily_pnl_amount,
                ROUND(AVG(confidence), 1) as avg_confidence
            FROM trades
            WHERE bot_id = ? AND datetime(timestamp) >= datetime('now', '-' || ? || ' days')
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        ''', (bot_id, days))
        
        return [dict(row) for row in cursor.fetchall()]
    
    # =====================
    # IDENTIFICATION FUNCTIONS
    # =====================
    
    def identify_best_bot(self) -> Optional[Dict]:
        """Identify the best performing bot."""
        rankings = self.get_bot_rankings()
        return rankings[0] if rankings else None
    
    def identify_worst_bot(self) -> Optional[Dict]:
        """Identify the worst performing bot."""
        rankings = self.get_bot_rankings()
        return rankings[-1] if rankings else None
    
    def identify_best_markets(self, min_trades: int = 5) -> List[Dict]:
        """Get markets with >55% win rate (positive edge)."""
        markets = self.get_market_performance(min_trades)
        return [m for m in markets if m['win_rate'] >= 0.55]
    
    def identify_bad_markets(self, max_win_rate: float = 0.45) -> List[Dict]:
        """Get markets with <45% win rate (should disable)."""
        markets = self.get_market_performance(min_trades=3)
        return [m for m in markets if m['win_rate'] < max_win_rate]
    
    def get_optimal_confidence_threshold(self) -> float:
        """Find confidence level that maximizes win rate."""
        conf_analysis = self.analyze_by_confidence()
        
        if not conf_analysis:
            return 60.0
        
        # Find highest win rate
        best_threshold = None
        best_win_rate = 0
        
        for threshold_range, stats in conf_analysis.items():
            if stats['win_rate'] > best_win_rate:
                best_win_rate = stats['win_rate']
                # Extract min value from threshold_range
                min_val = int(threshold_range.split('-')[0])
                best_threshold = min_val
        
        return best_threshold or 60.0
    
    # =====================
    # SUMMARY REPORT
    # =====================
    
    def generate_report(self) -> str:
        """Generate comprehensive performance report."""
        rankings = self.get_bot_rankings()
        best_markets = self.identify_best_markets()
        bad_markets = self.identify_bad_markets()
        conf_analysis = self.analyze_by_confidence()
        best_bot = self.identify_best_bot()
        worst_bot = self.identify_worst_bot()
        opt_confidence = self.get_optimal_confidence_threshold()
        
        report = f"""
╔════════════════════════════════════════════════════════╗
║     MULTI-BOT ANALYTICS REPORT ({datetime.now().strftime('%Y-%m-%d %H:%M')})   ║
╚════════════════════════════════════════════════════════╝

📊 BOT RANKINGS
{'─'*60}
"""
        
        for i, rank in enumerate(rankings, 1):
            emoji = '🥇' if i == 1 else '🥈' if i == 2 else '🥉' if i == 3 else f'  {i}.'
            report += f"""
{emoji} {rank['bot_id']:15} | Trades: {rank['total_trades']:3d} | Win: {rank['win_rate']:.1%} | P&L: {rank['total_pnl_pct']:+.2%} | Confidence: {rank['avg_confidence']:.0f}%
"""
        
        report += f"""
🎯 SIGNAL QUALITY ANALYSIS
{'─'*60}
"""
        for conf_range, stats in sorted(conf_analysis.items(), reverse=True):
            report += f"""
{conf_range:12} → Trades: {stats['trades']:3d} | Win: {stats['win_rate']:.1%} | Avg P&L: {stats['avg_pnl_pct']:+.2%}
"""
        
        report += f"""
✅ BEST MARKETS (Win Rate ≥55%)
{'─'*60}
"""
        if best_markets:
            for market in best_markets[:5]:
                report += f"""
{market['asset']} {market['market_id'][:40]:40} | Trades: {market['trades']} | Win: {market['win_rate']:.1%}
"""
        else:
            report += "\nNo markets with 55%+ win rate yet.\n"
        
        report += f"""
❌ BAD MARKETS (Win Rate <45%)
{'─'*60}
"""
        if bad_markets:
            for market in bad_markets[:5]:
                report += f"""
{market['asset']} {market['market_id'][:40]:40} | Trades: {market['trades']} | Win: {market['win_rate']:.1%} [DISABLE]
"""
        else:
            report += "\nNo bad markets identified.\n"
        
        report += f"""
💡 OPTIMIZATION RECOMMENDATIONS
{'─'*60}
• Optimal confidence threshold: {opt_confidence:.0f}%+ (highest win rate)
• Best performing bot: {best_bot['bot_id'] if best_bot else 'N/A'} ({best_bot['win_rate']:.1%} win rate)
• Worst performing bot: {worst_bot['bot_id'] if worst_bot else 'N/A'} ({worst_bot['win_rate']:.1%} win rate)
{'='*60}
"""
        
        return report
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
