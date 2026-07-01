"""
Order Placement Module - Execute trades on Polymarket

Handles:
- Creating limit orders on matched markets
- Order validation and pre-flight checks
- Order status tracking
- Cancel and modify operations
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List
from datetime import datetime
from loguru import logger

from src.polymarket.signal_matcher import SignalMatch
from src.polymarket.client import PolymarketClient


class OrderStatus(str, Enum):
    """Order lifecycle states"""
    PENDING = "pending"          # Waiting to be sent
    SUBMITTED = "submitted"      # Sent to Polymarket API
    ACCEPTED = "accepted"        # Accepted by exchange
    FILLED = "filled"            # Partially or fully filled
    CANCELLED = "cancelled"      # User cancelled
    REJECTED = "rejected"        # Exchange rejected
    EXPIRED = "expired"          # Order expired
    ERROR = "error"              # Error occurred


class OrderType(str, Enum):
    """Order execution types"""
    LIMIT = "limit"              # Limit order at specified price
    MARKET = "market"            # Market order - execute at best price


@dataclass
class PolymarketOrder:
    """Represents a single order on Polymarket"""
    
    # Order identification
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None  # For idempotency
    
    # Market and outcome info
    market_id: str = ""
    outcome_name: str = ""        # e.g., "UP", "DOWN", "YES", "NO"
    outcome_id: int = 0           # Market outcome index
    
    # Order parameters
    size: float = 0.0             # Amount to buy (in shares/tokens)
    price: float = 0.0            # Limit price (0-1.0 for probability)
    order_type: OrderType = OrderType.LIMIT
    
    # Execution info
    status: OrderStatus = OrderStatus.PENDING
    filled_amount: float = 0.0    # Amount actually filled
    average_fill_price: float = 0.0
    
    # Cost and P&L tracking
    total_cost: float = 0.0       # Total $ spent
    current_value: float = 0.0    # Current market value
    unrealized_pnl: float = 0.0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    
    # Metadata
    signal_match_id: Optional[str] = None  # Reference to SignalMatch
    bot_id: str = ""
    signal_type: str = ""  # BUY_YES or BUY_NO
    confidence: float = 0.0
    market_edge: float = 0.0
    
    # Notes
    reason: str = ""


@dataclass
class OrderValidation:
    """Result of order validation"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class OrderPlacer:
    """Places and manages orders on Polymarket"""
    
    def __init__(self, client: PolymarketClient, max_order_size: float = 5000.0):
        """
        Initialize order placer
        
        Args:
            client: PolymarketClient instance
            max_order_size: Maximum USD amount per order
        """
        self.client = client
        self.max_order_size = max_order_size
        
        # Order tracking
        self.orders: Dict[str, PolymarketOrder] = {}  # order_id -> Order
        self.submitted_orders: List[str] = []  # List of submitted order IDs
        
        logger.info(f"✅ OrderPlacer initialized (max size: ${max_order_size})")
    
    def validate_order(self, signal_match: SignalMatch, size: float) -> OrderValidation:
        """
        Validate an order before submission
        
        Args:
            signal_match: SignalMatch from matcher
            size: Position size in USD
            
        Returns:
            OrderValidation with errors/warnings
        """
        errors = []
        warnings = []
        
        # Check market exists
        if not signal_match.matched_market:
            errors.append("No matched market found")
        
        # Check size
        if size <= 0:
            errors.append(f"Invalid size: {size}")
        
        if size > self.max_order_size:
            errors.append(f"Size ${size} exceeds max ${self.max_order_size}")
        
        # Check outcome
        if signal_match.outcome_to_buy not in [0, 1]:
            errors.append(f"Invalid outcome ID: {signal_match.outcome_to_buy}")
        
        # Check confidence
        if signal_match.confidence < 50:
            warnings.append(f"Low confidence ({signal_match.confidence:.0f}%)")
        
        # Check edge
        if signal_match.market_edge < 0.01:  # Less than 1% edge
            warnings.append(f"Thin edge ({signal_match.market_edge:.1%})")
        
        # Check liquidity
        liquidity = signal_match.matched_market.liquidity
        if liquidity < size * 0.1:  # Liquidity < 10% of order size
            warnings.append(f"Low liquidity vs order size (${liquidity:.0f} vs ${size})")
        
        is_valid = len(errors) == 0
        return OrderValidation(is_valid=is_valid, errors=errors, warnings=warnings)
    
    def create_order(
        self,
        signal_match: SignalMatch,
        size: float,
        order_type: OrderType = OrderType.LIMIT,
        limit_price: Optional[float] = None
    ) -> Optional[PolymarketOrder]:
        """
        Create an order from a signal match
        
        Args:
            signal_match: SignalMatch from matcher
            size: Position size in USD
            order_type: LIMIT or MARKET
            limit_price: Limit price (required for LIMIT orders)
            
        Returns:
            PolymarketOrder if valid, None otherwise
        """
        
        # Validate
        validation = self.validate_order(signal_match, size)
        if not validation.is_valid:
            logger.error(f"Order validation failed: {', '.join(validation.errors)}")
            return None
        
        if validation.warnings:
            logger.warning(f"Order warnings: {', '.join(validation.warnings)}")
        
        # Get market and outcome info
        market = signal_match.matched_market
        outcome_id = signal_match.outcome_to_buy
        outcome_names = market.outcomes
        
        if outcome_id >= len(outcome_names):
            logger.error(f"Outcome ID {outcome_id} out of range")
            return None
        
        outcome_name = outcome_names[outcome_id]
        
        # Determine price
        if order_type == OrderType.LIMIT:
            if limit_price is None:
                limit_price = market.best_bid if signal_match.signal == "BUY_YES" else market.best_ask
            price = limit_price
        else:  # MARKET
            # Use current market price for market orders
            price = market.current_price
        
        # Calculate cost
        total_cost = size  # Assuming 1-to-1 cost ratio (market price in USD)
        
        # Create order object
        order = PolymarketOrder(
            order_id=None,  # Will be assigned by Polymarket API
            market_id=market.market_id,
            outcome_name=outcome_name,
            outcome_id=outcome_id,
            size=size,
            price=price,
            order_type=order_type,
            status=OrderStatus.PENDING,
            total_cost=total_cost,
            signal_match_id=signal_match.bot_id,  # Use bot_id as reference
            bot_id=signal_match.bot_id,
            signal_type=signal_match.signal,
            confidence=signal_match.confidence,
            market_edge=signal_match.market_edge,
            reason=f"Signal match: {market.title}"
        )
        
        logger.info(
            f"✅ Order created: {signal_match.bot_id} {signal_match.signal} "
            f"{outcome_name} on {market.title} (size: ${size:.0f}, price: {price:.2f})"
        )
        
        return order
    
    def submit_order(self, order: PolymarketOrder) -> bool:
        """
        Submit order to Polymarket API
        
        Args:
            order: PolymarketOrder to submit
            
        Returns:
            True if submitted successfully
        """
        
        if order.status != OrderStatus.PENDING:
            logger.error(f"Cannot submit order in {order.status} state")
            return False
        
        try:
            # Call Polymarket API
            api_response = self.client.place_order(
                market_id=order.market_id,
                outcome_id=order.outcome_id,
                side="buy",  # Always buy for now
                size=order.size,
                price=order.price,
                order_type=order.order_type.value
            )
            
            # Update order with API response
            order.order_id = api_response.get("order_id")
            order.client_order_id = api_response.get("client_order_id")
            order.status = OrderStatus.SUBMITTED
            order.submitted_at = datetime.utcnow()
            
            # Track order
            if order.order_id:
                self.orders[order.order_id] = order
                self.submitted_orders.append(order.order_id)
            
            logger.info(
                f"✅ Order submitted: {order.order_id} "
                f"({order.bot_id} {order.signal_type} {order.outcome_name})"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to submit order: {e}")
            order.status = OrderStatus.ERROR
            return False
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if cancelled successfully
        """
        
        if order_id not in self.orders:
            logger.error(f"Order {order_id} not found")
            return False
        
        order = self.orders[order_id]
        
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.EXPIRED]:
            logger.warning(f"Cannot cancel order in {order.status} state")
            return False
        
        try:
            # Call API to cancel
            self.client.cancel_order(order_id)
            
            order.status = OrderStatus.CANCELLED
            logger.info(f"✅ Order cancelled: {order_id}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """
        Get current status of an order
        
        Args:
            order_id: Order ID to query
            
        Returns:
            OrderStatus or None if not found
        """
        
        if order_id not in self.orders:
            logger.warning(f"Order {order_id} not found")
            return None
        
        try:
            # Query API for latest status
            api_response = self.client.get_order(order_id)
            
            # Update order status
            order = self.orders[order_id]
            status_str = api_response.get("status", "unknown").lower()
            
            # Map API status to our OrderStatus enum
            if status_str == "filled":
                order.status = OrderStatus.FILLED
                order.filled_amount = api_response.get("filled_amount", 0)
                order.average_fill_price = api_response.get("avg_fill_price", 0)
                order.filled_at = datetime.utcnow()
            elif status_str == "cancelled":
                order.status = OrderStatus.CANCELLED
            elif status_str == "accepted":
                order.status = OrderStatus.ACCEPTED
            elif status_str == "rejected":
                order.status = OrderStatus.REJECTED
            elif status_str == "expired":
                order.status = OrderStatus.EXPIRED
            
            return order.status
        
        except Exception as e:
            logger.error(f"Failed to get order status {order_id}: {e}")
            return None
    
    def get_order(self, order_id: str) -> Optional[PolymarketOrder]:
        """
        Get order details
        
        Args:
            order_id: Order ID
            
        Returns:
            PolymarketOrder or None if not found
        """
        return self.orders.get(order_id)
    
    def get_submitted_orders(self) -> List[PolymarketOrder]:
        """Get all submitted orders"""
        return [self.orders[oid] for oid in self.submitted_orders if oid in self.orders]
    
    def get_open_orders(self) -> List[PolymarketOrder]:
        """Get all open orders (submitted but not filled)"""
        return [
            order for order in self.orders.values()
            if order.status in [OrderStatus.SUBMITTED, OrderStatus.ACCEPTED]
        ]
    
    def get_filled_orders(self) -> List[PolymarketOrder]:
        """Get all filled orders"""
        return [
            order for order in self.orders.values()
            if order.status == OrderStatus.FILLED
        ]
    
    def place_order_from_signal(
        self,
        signal_match: SignalMatch,
        order_type: OrderType = OrderType.LIMIT
    ) -> Optional[PolymarketOrder]:
        """
        End-to-end flow: create and submit order from signal match
        
        Args:
            signal_match: SignalMatch from matcher
            order_type: Order type (LIMIT or MARKET)
            
        Returns:
            PolymarketOrder if successful, None otherwise
        """
        
        # Create order
        order = self.create_order(
            signal_match=signal_match,
            size=signal_match.recommended_size,
            order_type=order_type,
            limit_price=signal_match.matched_market.best_bid
        )
        
        if order is None:
            return None
        
        # Submit order
        if self.submit_order(order):
            return order
        
        return None
    
    def place_orders_batch(
        self,
        signal_matches: List[SignalMatch],
        order_type: OrderType = OrderType.LIMIT
    ) -> List[PolymarketOrder]:
        """
        Place multiple orders from signal matches
        
        Args:
            signal_matches: List of SignalMatch objects
            order_type: Order type for all orders
            
        Returns:
            List of successfully placed orders
        """
        
        placed_orders = []
        
        for signal_match in signal_matches:
            order = self.place_order_from_signal(signal_match, order_type)
            if order:
                placed_orders.append(order)
        
        logger.info(f"✅ Batch placed {len(placed_orders)}/{len(signal_matches)} orders")
        return placed_orders
    
    def get_portfolio_value(self, current_prices: Dict[str, float]) -> Dict[str, float]:
        """
        Calculate current portfolio value
        
        Args:
            current_prices: Dict of {market_id: current_price}
            
        Returns:
            Dict with total_cost, current_value, unrealized_pnl, roi%
        """
        
        total_cost = 0.0
        current_value = 0.0
        
        for order in self.get_filled_orders():
            cost = order.filled_amount * order.average_fill_price
            total_cost += cost
            
            current_price = current_prices.get(order.market_id, order.average_fill_price)
            value = order.filled_amount * current_price
            current_value += value
        
        unrealized_pnl = current_value - total_cost
        roi = (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0.0
        
        return {
            "total_cost": total_cost,
            "current_value": current_value,
            "unrealized_pnl": unrealized_pnl,
            "roi_percent": roi
        }
    
    def set_max_order_size(self, size: float):
        """Update maximum order size"""
        self.max_order_size = size
        logger.info(f"Max order size updated to ${size:.0f}")
