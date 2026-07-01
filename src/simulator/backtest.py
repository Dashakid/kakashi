"""Paper trading simulator for backtesting strategies."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import pandas as pd
from loguru import logger

from src.trading.engine import TradingEngine, OrderSide
from src.ml.models import PricePredictor
from src.data.store import DataStore


@dataclass
class SimulationResult:
    """Backtesting result."""
    start_balance: float
    end_balance: float
    total_return: float
    total_return_pct: float
    num_trades: int
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float
    best_trade: float
    worst_trade: float


class PaperTradingSimulator:
    """Simulator for paper trading and backtesting."""

    def __init__(
        self,
        engine: TradingEngine,
        predictor: PricePredictor,
        store: DataStore,
    ):
        """Initialize simulator."""
        self.engine = engine
        self.predictor = predictor
        self.store = store
        self.strategy_logs: List[Dict] = []

    async def run_backtest(
        self,
        market_id: str,
        days: int = 30,
    ) -> Optional[SimulationResult]:
        """Run backtest on historical data."""
        try:
            # Get historical data
            df = self.store.to_dataframe(market_id, days)
            if df.empty:
                logger.warning(f"No data for {market_id}")
                return None
            
            logger.info(f"Running backtest for {market_id} ({len(df)} candles)")
            
            # Reset engine
            self.engine.balance = self.engine.initial_balance
            self.engine.positions.clear()
            self.engine.orders.clear()
            self.engine.trades.clear()
            
            # Simulate each candle
            for idx, (timestamp, row) in enumerate(df.iterrows()):
                price = row["close"]
                
                # Get prediction
                historical_df = df.iloc[:idx+1]
                prediction = self.predictor.predict(historical_df)
                
                # Simple strategy: buy if prediction > 0.55, sell if < 0.45
                if prediction > 0.55 and market_id not in self.engine.positions:
                    order_id = f"{market_id}_{timestamp}_buy"
                    self.engine.place_order(order_id, market_id, OrderSide.BUY, 10, price)
                    self.engine.execute_order(order_id, price)
                    
                    self.strategy_logs.append({
                        "timestamp": timestamp,
                        "action": "BUY",
                        "price": price,
                        "prediction": prediction,
                    })
                
                elif prediction < 0.45 and market_id in self.engine.positions:
                    self.engine.close_position(market_id, price)
                    
                    self.strategy_logs.append({
                        "timestamp": timestamp,
                        "action": "SELL",
                        "price": price,
                        "prediction": prediction,
                    })
                
                # Update prices
                self.engine.update_prices({market_id: price})
            
            # Close any remaining positions
            if market_id in self.engine.positions:
                last_price = df.iloc[-1]["close"]
                self.engine.close_position(market_id, last_price)
            
            # Calculate metrics
            metrics = self.engine.get_portfolio_metrics()
            
            result = SimulationResult(
                start_balance=self.engine.initial_balance,
                end_balance=metrics.total_balance,
                total_return=metrics.total_pnl,
                total_return_pct=metrics.total_pnl_pct,
                num_trades=len(self.engine.trades),
                win_rate=metrics.win_rate,
                max_drawdown=metrics.max_drawdown,
                sharpe_ratio=metrics.sharpe_ratio,
                best_trade=max([t[2] for t in self.engine.trades], default=0),
                worst_trade=min([t[2] for t in self.engine.trades], default=0),
            )
            
            logger.info(f"Backtest complete: {result.total_return_pct*100:.2f}% return")
            return result
        except Exception as e:
            logger.error(f"Error running backtest: {e}")
            return None

    def get_strategy_log(self) -> pd.DataFrame:
        """Get logs of all strategy actions."""
        if not self.strategy_logs:
            return pd.DataFrame()
        
        return pd.DataFrame(self.strategy_logs)

    async def live_paper_trade(self, market_id: str, check_interval: int = 60):
        """Run live paper trading (simulated with real data)."""
        logger.info(f"Starting paper trading for {market_id}")
        
        while True:
            try:
                df = self.store.to_dataframe(market_id, days=30)
                if df.empty:
                    continue
                
                prediction = self.predictor.predict(df)
                last_price = df.iloc[-1]["close"]
                
                # Execute strategy logic
                if prediction > 0.55 and market_id not in self.engine.positions:
                    order_id = f"{market_id}_{datetime.utcnow()}_buy"
                    self.engine.place_order(order_id, market_id, OrderSide.BUY, 10, last_price)
                    self.engine.execute_order(order_id, last_price)
                
                elif prediction < 0.45 and market_id in self.engine.positions:
                    self.engine.close_position(market_id, last_price)
                
                self.engine.update_prices({market_id: last_price})
                
                # Log metrics
                metrics = self.engine.get_portfolio_metrics()
                logger.info(
                    f"Balance: {metrics.total_balance:.2f} | "
                    f"PnL: {metrics.total_pnl:.2f} ({metrics.total_pnl_pct*100:.2f}%) | "
                    f"Prediction: {prediction:.2f}"
                )
                
                await asyncio.sleep(check_interval)
            except Exception as e:
                logger.error(f"Error in paper trading: {e}")
                await asyncio.sleep(check_interval)


from dataclasses import dataclass
import asyncio
