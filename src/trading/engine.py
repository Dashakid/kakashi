"""Trading engine for managing positions and execution."""

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from enum import Enum
from loguru import logger

from src.config import (
    INITIAL_BALANCE,
    MAX_POSITION_SIZE,
    RISK_PER_TRADE,
)


class OrderSide(Enum):
    """Order side (buy/sell)."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status."""
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass
class Order:
    """Trade order."""
    order_id: str
    market_id: str
    side: OrderSide
    size: float
    price: float
    timestamp: datetime
    status: OrderStatus = OrderStatus.PENDING
    filled_size: float = 0.0
    filled_price: float = 0.0


@dataclass
class Position:
    """Active trading position."""
    market_id: str
    side: OrderSide
    size: float
    entry_price: float
    entry_time: datetime
    current_price: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    
    def update_price(self, price: float):
        """Update position with current market price."""
        self.current_price = price
        if self.side == OrderSide.BUY:
            self.pnl = (price - self.entry_price) * self.size
            self.pnl_pct = (price - self.entry_price) / self.entry_price
        else:
            self.pnl = (self.entry_price - price) * self.size
            self.pnl_pct = (self.entry_price - price) / self.entry_price


@dataclass
class PortfolioMetrics:
    """Portfolio performance metrics."""
    total_balance: float
    available_cash: float
    positions_value: float
    total_pnl: float
    total_pnl_pct: float
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float


class TradingEngine:
    """Core trading engine for managing positions and execution."""

    def __init__(self, initial_balance: float = INITIAL_BALANCE):
        """Initialize trading engine."""
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions: Dict[str, Position] = {}
        self.orders: List[Order] = []
        self.trades: List[tuple] = []  # (entry_price, exit_price, pnl)
        self.max_position_size = MAX_POSITION_SIZE
        self.risk_per_trade = RISK_PER_TRADE
        # Equity curve: list of (portfolio_value, timestamp) snapshots after each executed trade.
        # Used to compute rolling Sharpe ratio and maximum drawdown.
        self._equity_curve: List[float] = [initial_balance]

    def place_order(
        self,
        order_id: str,
        market_id: str,
        side: OrderSide,
        size: float,
        price: float,
    ) -> bool:
        """Place a trade order."""
        try:
            # Check if we have enough cash for buy orders
            if side == OrderSide.BUY:
                required_cash = size * price
                if required_cash > self.balance:
                    logger.warning(f"Insufficient cash for order {order_id}")
                    return False
                
                # Check position size limits
                if size > self.max_position_size:
                    logger.warning(f"Order size {size} exceeds max position size")
                    return False
            
            order = Order(
                order_id=order_id,
                market_id=market_id,
                side=side,
                size=size,
                price=price,
                timestamp=datetime.now(timezone.utc),
            )
            
            self.orders.append(order)
            logger.info(f"Order placed: {order_id} {side.value} {size} @ {price}")
            return True
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return False

    def execute_order(self, order_id: str, filled_price: Optional[float] = None) -> bool:
        """Execute a pending order."""
        try:
            order = next((o for o in self.orders if o.order_id == order_id), None)
            if not order:
                logger.warning(f"Order not found: {order_id}")
                return False
            
            filled_price = filled_price or order.price
            
            # Open or update position
            if order.market_id in self.positions:
                pos = self.positions[order.market_id]
                if pos.side == order.side:
                    # Increase position
                    pos.size += order.size
                    pos.entry_price = (
                        (pos.entry_price * (pos.size - order.size) + filled_price * order.size)
                        / pos.size
                    )
                else:
                    # Close or reverse position
                    if order.size >= pos.size:
                        self.trades.append((pos.entry_price, filled_price, (filled_price - pos.entry_price) * pos.size))
                        remaining = order.size - pos.size
                        if remaining > 0:
                            self.positions[order.market_id] = Position(
                                market_id=order.market_id,
                                side=order.side,
                                size=remaining,
                                entry_price=filled_price,
                                entry_time=datetime.now(timezone.utc),
                                current_price=filled_price,
                            )
                        else:
                            del self.positions[order.market_id]
                    else:
                        pos.size -= order.size
                        self.trades.append((pos.entry_price, filled_price, (filled_price - pos.entry_price) * order.size))
            else:
                # New position
                self.positions[order.market_id] = Position(
                    market_id=order.market_id,
                    side=order.side,
                    size=order.size,
                    entry_price=filled_price,
                    entry_time=datetime.now(timezone.utc),
                    current_price=filled_price,
                )
            
            # Update balance
            if order.side == OrderSide.BUY:
                self.balance -= order.size * filled_price
            else:
                self.balance += order.size * filled_price
            
            order.status = OrderStatus.FILLED
            order.filled_size = order.size
            order.filled_price = filled_price

            # Snapshot equity for Sharpe / drawdown calculations
            pos_value = sum(p.size * p.current_price for p in self.positions.values())
            self._equity_curve.append(self.balance + pos_value)

            logger.info(f"Order executed: {order_id} {order.size} @ {filled_price}")
            return True
        except Exception as e:
            logger.error(f"Error executing order: {e}")
            return False

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        try:
            order = next((o for o in self.orders if o.order_id == order_id), None)
            if not order:
                logger.warning(f"Order not found: {order_id}")
                return False
            
            order.status = OrderStatus.CANCELLED
            logger.info(f"Order cancelled: {order_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return False

    def update_prices(self, market_prices: Dict[str, float]):
        """Update all positions with current market prices."""
        for market_id, price in market_prices.items():
            if market_id in self.positions:
                self.positions[market_id].update_price(price)

    def get_portfolio_metrics(self) -> PortfolioMetrics:
        """Calculate current portfolio metrics."""
        positions_value = sum(
            pos.size * pos.current_price for pos in self.positions.values()
        )
        total_pnl = sum(pos.pnl for pos in self.positions.values())
        
        closed_trades_pnl = [t[2] for t in self.trades]
        total_pnl += sum(closed_trades_pnl)
        
        total_balance = self.balance + positions_value
        total_pnl_pct = (total_balance - self.initial_balance) / self.initial_balance
        
        win_rate = 0.0
        if self.trades:
            wins = sum(1 for t in closed_trades_pnl if t > 0)
            win_rate = wins / len(closed_trades_pnl)
        
        return PortfolioMetrics(
            total_balance=total_balance,
            available_cash=self.balance,
            positions_value=positions_value,
            total_pnl=total_pnl,
            total_pnl_pct=total_pnl_pct,
            win_rate=win_rate,
            max_drawdown=self._calculate_max_drawdown(),
            sharpe_ratio=self._calculate_sharpe(),
        )

    def _calculate_max_drawdown(self) -> float:
        """Maximum peak-to-trough drawdown from the equity curve (as a positive fraction)."""
        curve = self._equity_curve
        if len(curve) < 2:
            return 0.0
        peak = curve[0]
        max_dd = 0.0
        for value in curve[1:]:
            if value > peak:
                peak = value
            if peak > 0:
                dd = (peak - value) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd

    def _calculate_sharpe(self, risk_free_rate: float = 0.0) -> float:
        """Annualised Sharpe ratio from per-trade equity returns (proxy for daily returns)."""
        curve = self._equity_curve
        if len(curve) < 3:
            return 0.0
        returns = [
            (curve[i] - curve[i - 1]) / curve[i - 1]
            for i in range(1, len(curve))
            if curve[i - 1] != 0
        ]
        if len(returns) < 2:
            return 0.0
        n = len(returns)
        mean_r = sum(returns) / n
        variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
        std_r = math.sqrt(variance)
        if std_r == 0:
            return 0.0
        # Scale to annual: assume ~252 trades/year as approximation
        return (mean_r - risk_free_rate) / std_r * math.sqrt(252)

    def close_position(self, market_id: str, price: float) -> bool:
        """Close a position at given price."""
        try:
            if market_id not in self.positions:
                logger.warning(f"No position for {market_id}")
                return False
            
            pos = self.positions[market_id]
            side = OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY
            
            order_id = f"close_{market_id}_{datetime.now(timezone.utc).timestamp()}"
            if self.place_order(order_id, market_id, side, pos.size, price):
                return self.execute_order(order_id, price)
            
            return False
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False
