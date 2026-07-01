"""
Exit Logic & Profit-Taking - Manage position exits and profit targets

Handles:
- Take profit orders at target levels
- Stop loss management
- Trailing stops
- Time-based exits
- Partial profit-taking
- Risk/reward ratio calculations
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
from loguru import logger

from src.polymarket.order_placer import PolymarketOrder, OrderStatus, OrderType, OrderPlacer
from src.polymarket.signal_matcher import SignalMatch


class ExitReason(str, Enum):
    """Reasons for exiting a position"""
    TAKE_PROFIT = "take_profit"        # Hit profit target
    STOP_LOSS = "stop_loss"            # Hit stop loss
    TIME_BASED = "time_based"          # Exit after time period
    SIGNAL_REVERSAL = "signal_reversal"  # Opposite signal received
    MANUAL = "manual"                  # User manually closed
    RISK_LIMIT = "risk_limit"          # Risk management limit
    TRAILING_STOP = "trailing_stop"    # Trailing stop triggered


@dataclass
class ExitTarget:
    """Exit target configuration"""
    
    # Target type
    name: str  # e.g., "TP1", "TP2", "SL"
    target_price: float  # Price level to exit at
    profit_percent: float  # Profit % at this price
    
    # Execution
    percent_of_position: float = 1.0  # % of position to close (default: all)
    order_type: OrderType = OrderType.LIMIT
    
    # Tracking
    is_active: bool = True
    triggered_at: Optional[datetime] = None
    exit_order_id: Optional[str] = None


@dataclass
class PositionExit:
    """Record of a position exit"""
    
    exit_id: str
    order_id: str  # Original order that created the position
    exit_reason: ExitReason
    
    # Exit details
    exit_price: float
    quantity_closed: float  # Amount actually closed
    realized_pnl: float  # Profit/loss on this exit
    realized_pnl_percent: float  # % return
    
    # Costs
    entry_cost: float  # Cost basis
    exit_value: float  # Value at exit
    
    # Timing
    entry_time: datetime
    exit_time: datetime = field(default_factory=datetime.utcnow)
    hold_duration: timedelta = field(default_factory=lambda: timedelta())
    
    def calculate_hold_duration(self):
        """Calculate how long position was held"""
        self.hold_duration = self.exit_time - self.entry_time


class ExitManager:
    """Manages position exits and profit-taking"""
    
    def __init__(self, order_placer: OrderPlacer):
        """
        Initialize exit manager
        
        Args:
            order_placer: OrderPlacer instance for executing exit orders
        """
        self.order_placer = order_placer
        
        # Exit tracking
        self.exit_targets: Dict[str, List[ExitTarget]] = {}  # order_id -> [targets]
        self.position_exits: Dict[str, PositionExit] = {}    # exit_id -> exit
        
        # Configuration
        self.default_take_profit: float = 0.20  # Default 20% profit target
        self.default_stop_loss: float = -0.10  # Default 10% stop loss
        self.trailing_stop_percent: float = 0.05  # Trailing stop at 5% below peak
        
        logger.info(f"✅ ExitManager initialized (TP: {self.default_take_profit:.0%}, SL: {self.default_stop_loss:.0%})")
    
    def set_default_targets(self, take_profit: float = 0.20, stop_loss: float = -0.10):
        """
        Set default exit targets
        
        Args:
            take_profit: Default profit target (e.g., 0.20 for 20%)
            stop_loss: Default stop loss (e.g., -0.10 for 10% loss)
        """
        self.default_take_profit = take_profit
        self.default_stop_loss = stop_loss
        logger.info(f"✅ Default exit targets set (TP: {take_profit:.0%}, SL: {stop_loss:.0%})")
    
    def create_exit_targets(
        self,
        order_id: str,
        entry_price: float,
        take_profit_percent: Optional[float] = None,
        stop_loss_percent: Optional[float] = None,
        use_tiered_exits: bool = False
    ) -> List[ExitTarget]:
        """
        Create exit targets for an order
        
        Args:
            order_id: Order ID to attach targets to
            entry_price: Entry price
            take_profit_percent: Profit target (default: default_take_profit)
            stop_loss_percent: Stop loss (default: default_stop_loss)
            use_tiered_exits: Create multiple tiered exit targets
            
        Returns:
            List of ExitTarget objects
        """
        
        if take_profit_percent is None:
            take_profit_percent = self.default_take_profit
        
        if stop_loss_percent is None:
            stop_loss_percent = self.default_stop_loss
        
        targets = []
        
        # Calculate price levels
        tp_price = entry_price * (1 + take_profit_percent)
        sl_price = entry_price * (1 + stop_loss_percent)
        
        if use_tiered_exits:
            # Create 3-tier exit: TP1 (50% at half profit), TP2 (50% at full profit), SL
            tp1_price = entry_price * (1 + take_profit_percent / 2)
            
            targets = [
                ExitTarget(
                    name="TP1",
                    target_price=tp1_price,
                    profit_percent=take_profit_percent / 2,
                    percent_of_position=0.5,
                    order_type=OrderType.LIMIT
                ),
                ExitTarget(
                    name="TP2",
                    target_price=tp_price,
                    profit_percent=take_profit_percent,
                    percent_of_position=1.0,
                    order_type=OrderType.LIMIT
                ),
                ExitTarget(
                    name="SL",
                    target_price=sl_price,
                    profit_percent=stop_loss_percent,
                    percent_of_position=1.0,
                    order_type=OrderType.MARKET
                )
            ]
        else:
            # Simple two-target exit: TP and SL
            targets = [
                ExitTarget(
                    name="TP",
                    target_price=tp_price,
                    profit_percent=take_profit_percent,
                    percent_of_position=1.0,
                    order_type=OrderType.LIMIT
                ),
                ExitTarget(
                    name="SL",
                    target_price=sl_price,
                    profit_percent=stop_loss_percent,
                    percent_of_position=1.0,
                    order_type=OrderType.MARKET
                )
            ]
        
        self.exit_targets[order_id] = targets
        
        logger.info(
            f"✅ Exit targets created for {order_id}: "
            f"TP {take_profit_percent:.0%} @ ${tp_price:.2f}, "
            f"SL {stop_loss_percent:.0%} @ ${sl_price:.2f}"
        )
        
        return targets
    
    def check_exit_triggers(
        self,
        order_id: str,
        current_price: float,
        peak_price: Optional[float] = None
    ) -> List[ExitTarget]:
        """
        Check if any exit targets are triggered
        
        Args:
            order_id: Order ID to check
            current_price: Current market price
            peak_price: Peak price for trailing stop (optional)
            
        Returns:
            List of triggered targets
        """
        
        if order_id not in self.exit_targets:
            return []
        
        triggered = []
        targets = self.exit_targets[order_id]
        
        for target in targets:
            if not target.is_active:
                continue
            
            # Check if target is triggered
            is_triggered = False
            
            if "TP" in target.name or "profit" in target.name.lower():
                # Take profit: triggered when price >= target
                is_triggered = current_price >= target.target_price
            elif "SL" in target.name or "stop" in target.name.lower():
                # Stop loss: triggered when price <= target
                is_triggered = current_price <= target.target_price
            
            # Check trailing stop
            if peak_price and "TRAIL" in target.name.upper():
                trailing_price = peak_price * (1 - self.trailing_stop_percent)
                is_triggered = current_price <= trailing_price
            
            if is_triggered:
                target.triggered_at = datetime.utcnow()
                triggered.append(target)
        
        return triggered
    
    def execute_exit(
        self,
        order_id: str,
        exit_target: ExitTarget,
        exit_reason: ExitReason,
        current_price: float,
        position_size: float
    ) -> Optional[PositionExit]:
        """
        Execute an exit for a position
        
        Args:
            order_id: Original order ID
            exit_target: Exit target that triggered
            exit_reason: Reason for exit
            current_price: Current market price
            position_size: Size of original position
            
        Returns:
            PositionExit record if successful
        """
        
        # Get original order
        original_order = self.order_placer.get_order(order_id)
        if not original_order:
            logger.error(f"Original order not found: {order_id}")
            return None
        
        # Calculate exit amount
        quantity = position_size * exit_target.percent_of_position
        entry_cost = original_order.filled_amount * original_order.average_fill_price
        exit_value = quantity * current_price
        realized_pnl = exit_value - (entry_cost * exit_target.percent_of_position)
        realized_pnl_percent = (realized_pnl / (entry_cost * exit_target.percent_of_position)) if entry_cost > 0 else 0
        
        # Create exit record
        exit_id = f"exit-{order_id}-{len(self.position_exits)}"
        exit_record = PositionExit(
            exit_id=exit_id,
            order_id=order_id,
            exit_reason=exit_reason,
            exit_price=current_price,
            quantity_closed=quantity,
            realized_pnl=realized_pnl,
            realized_pnl_percent=realized_pnl_percent,
            entry_cost=entry_cost * exit_target.percent_of_position,
            exit_value=exit_value,
            entry_time=original_order.created_at
        )
        
        exit_record.calculate_hold_duration()
        
        # Store exit record
        self.position_exits[exit_id] = exit_record
        
        # Mark target as completed
        exit_target.is_active = False
        exit_target.exit_order_id = exit_id
        
        logger.info(
            f"✅ Position closed: {order_id} "
            f"({quantity:.0f} @ ${current_price:.2f}, "
            f"PnL: ${realized_pnl:.2f} ({realized_pnl_percent:.1%}), "
            f"Reason: {exit_reason.value})"
        )
        
        return exit_record
    
    def close_position_at_market(
        self,
        order_id: str,
        current_price: float,
        position_size: float,
        reason: ExitReason = ExitReason.MANUAL
    ) -> Optional[PositionExit]:
        """
        Immediately close a position at market price
        
        Args:
            order_id: Order ID to close
            current_price: Current market price
            position_size: Position size to close
            reason: Exit reason
            
        Returns:
            PositionExit record
        """
        
        market_exit = ExitTarget(
            name="MARKET_EXIT",
            target_price=current_price,
            profit_percent=0.0,
            percent_of_position=1.0,
            order_type=OrderType.MARKET
        )
        
        return self.execute_exit(
            order_id=order_id,
            exit_target=market_exit,
            exit_reason=reason,
            current_price=current_price,
            position_size=position_size
        )
    
    def trail_stop(
        self,
        order_id: str,
        current_price: float,
        peak_price: float,
        position_size: float,
        trail_percent: float = 0.05
    ) -> Optional[PositionExit]:
        """
        Check and potentially execute trailing stop
        
        Args:
            order_id: Order ID
            current_price: Current price
            peak_price: Peak price (for calculating trail)
            position_size: Position size
            trail_percent: Trailing stop % (default 5%)
            
        Returns:
            PositionExit if stop triggered
        """
        
        self.trailing_stop_percent = trail_percent
        trail_price = peak_price * (1 - trail_percent)
        
        if current_price <= trail_price:
            trail_exit = ExitTarget(
                name="TRAIL_STOP",
                target_price=current_price,
                profit_percent=((current_price / peak_price) - 1),
                percent_of_position=1.0,
                order_type=OrderType.MARKET
            )
            
            return self.execute_exit(
                order_id=order_id,
                exit_target=trail_exit,
                exit_reason=ExitReason.TRAILING_STOP,
                current_price=current_price,
                position_size=position_size
            )
        
        return None
    
    def get_profit_target_for_risk(
        self,
        entry_price: float,
        stop_loss_price: float,
        risk_reward_ratio: float = 2.0
    ) -> float:
        """
        Calculate profit target based on risk/reward ratio
        
        Args:
            entry_price: Entry price
            stop_loss_price: Stop loss price
            risk_reward_ratio: Risk/reward ratio (e.g., 2.0 for 1:2 R:R)
            
        Returns:
            Target profit price
        """
        
        risk = entry_price - stop_loss_price
        profit_target = entry_price + (risk * risk_reward_ratio)
        
        logger.info(
            f"📊 Position R:R Calculation: "
            f"Entry ${entry_price:.2f}, SL ${stop_loss_price:.2f}, "
            f"Risk ${risk:.2f}, TP ${profit_target:.2f} ({risk_reward_ratio}:1 R:R)"
        )
        
        return profit_target
    
    def get_exit_statistics(self) -> Dict:
        """
        Get summary statistics of all exits
        
        Returns:
            Dict with exit statistics
        """
        
        if not self.position_exits:
            return {
                'total_exits': 0,
                'total_pnl': 0.0,
                'avg_pnl_percent': 0.0,
                'winning_exits': 0,
                'losing_exits': 0,
                'win_rate': 0.0
            }
        
        exits = list(self.position_exits.values())
        total_pnl = sum(e.realized_pnl for e in exits)
        winning = sum(1 for e in exits if e.realized_pnl > 0)
        losing = sum(1 for e in exits if e.realized_pnl < 0)
        avg_pnl_percent = sum(e.realized_pnl_percent for e in exits) / len(exits) if exits else 0
        
        return {
            'total_exits': len(exits),
            'total_pnl': total_pnl,
            'avg_pnl_percent': avg_pnl_percent,
            'winning_exits': winning,
            'losing_exits': losing,
            'win_rate': (winning / len(exits) * 100) if exits else 0
        }
    
    def get_active_targets(self, order_id: str) -> List[ExitTarget]:
        """Get active exit targets for an order"""
        if order_id not in self.exit_targets:
            return []
        return [t for t in self.exit_targets[order_id] if t.is_active]
    
    def get_position_exits_by_reason(self, reason: ExitReason) -> List[PositionExit]:
        """Get exits filtered by reason"""
        return [e for e in self.position_exits.values() if e.exit_reason == reason]
