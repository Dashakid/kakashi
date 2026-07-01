"""
Market Maker Strategy for Polymarket.

Instead of predicting direction, we act as a liquidity provider:
- Quote both YES and NO sides of crypto direction markets
- Capture the spread on each trade
- Scale volume based on inventory and profitability

Example:
  Bitcoin UP market trading at: YES=0.52, NO=0.48
  We quote: BID=0.50 for YES, ASK=0.54 for YES
  
  Someone sells YES at 0.50 (we buy)
  Later, someone buys YES at 0.54 (we sell)
  Profit: 0.54 - 0.50 = 0.04 per contract × 100 contracts = $4
  
  Do this 1000x per day = $4000/day = $120k/month
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import random


@dataclass
class MarketMakerQuote:
    """A quote on both sides of a market."""
    market_id: str
    market_title: str
    asset: str
    
    # Our prices
    bid_price: float      # Price we'll BUY at (lower)
    ask_price: float      # Price we'll SELL at (higher)
    bid_size: float       # Amount we'll buy
    ask_size: float       # Amount we'll sell
    
    # Market data
    mid_price: float      # Market mid
    market_bid: float     # Market's best bid
    market_ask: float     # Market's best ask
    spread: float         # Ask - bid (our profit margin)
    
    # Metadata
    confidence: float     # How confident in this quote (0-1)
    created_at: str = None
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()


@dataclass  
class TradeExecution:
    """A trade we executed as market maker."""
    trade_id: str
    market_id: str
    market_title: str
    asset: str
    
    action: str              # BUY or SELL (from our perspective)
    side: str                # YES or NO
    execution_price: float   # Price we got filled at
    size: float              # Amount
    
    # Counter trade info
    counter_action: Optional[str] = None  # If we closed this position (BUY then SELL)
    counter_price: Optional[float] = None
    counter_time: Optional[str] = None
    
    # P&L
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    is_closed: bool = False
    
    created_at: str = None
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()


class MarketMakerStrategy:
    """
    Market making bot for Polymarket.
    
    Continuously quotes both sides of liquid markets.
    Records every trade for high-volume, low-margin profit model.
    """
    
    def __init__(
        self,
        assets: List[str] = None,
        base_spread_usd: float = 0.02,  # $0.02 spread per contract
        max_inventory_per_side: float = 1000.0,  # Max contracts to hold
        min_market_spread: float = 0.05,  # Only quote if market spread > 5 cents
        min_conviction: float = 0.40,  # Quote if we think price is wrong by >40%
    ):
        """Initialize market maker strategy."""
        self.assets = assets or ['ETH', 'BTC']
        self.base_spread_usd = base_spread_usd
        self.max_inventory_per_side = max_inventory_per_side
        self.min_market_spread = min_market_spread
        self.min_conviction = min_conviction
        
        # Track our positions
        self.inventory: Dict[str, Dict[str, float]] = {}  # {market_id: {YES: qty, NO: qty}}
        self.open_trades: Dict[str, TradeExecution] = {}  # {trade_id: execution}
        self.closed_trades: List[TradeExecution] = []
        
        # Performance tracking
        self.total_pnl = 0.0
        self.wins = 0
        self.losses = 0
        self.trades_this_session = 0
        
    def generate_quotes(self, markets: List) -> List[MarketMakerQuote]:
        """
        Generate quotes for a list of markets.
        
        Returns list of MarketMakerQuote objects representing both sides.
        """
        quotes = []
        
        if not markets:
            logger.warning("❌ No markets provided to generate_quotes")
            return quotes
        
        logger.debug(f"🔍 generate_quotes received {len(markets)} markets")
        
        for i, market in enumerate(markets):
            try:
                # Get market data
                market_id = market.get('id', '')
                title = market.get('title', '')
                market_bid = market.get('best_bid', 0.5)
                market_ask = market.get('best_ask', 0.5)
                
                logger.debug(f"  Market {i}: id={market_id}, title={title[:40]}, bid={market_bid}, ask={market_ask}")
                
                if not market_id or not title:
                    logger.debug(f"  ❌ Skipping - missing id or title")
                    continue
                
                # Skip if market spread is too tight (not worth quoting)
                market_spread = market_ask - market_bid
                
                # If spread is 0 or negative, use default 0.5 mid price
                if market_spread <= 0:
                    logger.debug(f"  ⚠️  Zero/negative spread ({market_spread}), using 0.5 mid price")
                    mid_price = 0.5
                elif market_spread < self.min_market_spread:
                    logger.debug(f"  ❌ Spread {market_spread} too tight (min={self.min_market_spread}), skipping")
                    continue
                else:
                    mid_price = (market_bid + market_ask) / 2.0
                
                market_id = market.get('id', '')
                
                # Calculate our quotes (tighter than market to be competitive)
                our_spread = self.base_spread_usd
                
                # BID side: we buy (lower price)
                bid_price = mid_price - (our_spread / 2)
                bid_price = max(0.01, min(0.99, bid_price))
                
                # ASK side: we sell (higher price)
                ask_price = mid_price + (our_spread / 2)
                ask_price = max(0.01, min(0.99, ask_price))
                
                # Size based on inventory and confidence
                current_inventory = self.inventory.get(market_id, {})
                yes_qty = current_inventory.get('YES', 0)
                no_qty = current_inventory.get('NO', 0)
                
                # Adjust size based on inventory imbalance
                bid_size = self.max_inventory_per_side * 0.5  # Start conservative
                ask_size = self.max_inventory_per_side * 0.5
                
                # If we're long YES, reduce bid size
                if yes_qty > 0:
                    bid_size *= (1 - (yes_qty / self.max_inventory_per_side))
                
                # If we're long NO, reduce ask size  
                if no_qty > 0:
                    ask_size *= (1 - (no_qty / self.max_inventory_per_side))
                
                # Create quote
                quote = MarketMakerQuote(
                    market_id=market_id,
                    market_title=market.get('title', '')[:50],
                    asset='ETH',
                    bid_price=bid_price,
                    ask_price=ask_price,
                    bid_size=max(1, bid_size),
                    ask_size=max(1, ask_size),
                    mid_price=mid_price,
                    market_bid=market_bid,
                    market_ask=market_ask,
                    spread=our_spread,
                    confidence=random.uniform(0.65, 0.85),  # Baseline confidence
                )
                
                quotes.append(quote)
                logger.debug(f"Generated quote for {quote.market_title}: BID={bid_price:.2f}, ASK={ask_price:.2f}")
                
            except Exception as e:
                logger.warning(f"Error generating quote: {e}")
                continue
        
        logger.info(f"Generated {len(quotes)} market maker quotes")
        return quotes
    
    def execute_trade(
        self,
        market_id: str,
        market_title: str,
        side: str,  # YES or NO
        action: str,  # BUY or SELL
        price: float,
        size: float,
    ) -> TradeExecution:
        """
        Execute a trade (simulated for now).
        
        In production, this would:
        1. Submit order to Polymarket
        2. Wait for fill
        3. Return execution details
        
        For now, simulate immediate fill at slightly better price.
        """
        import uuid
        
        # Generate trade ID
        trade_id = str(uuid.uuid4())[:8]
        
        # Realistic fill: limit orders fill at exactly the posted price.
        # No artificial discount/premium — that was hiding a structural loss.
        # Polymarket maker fee is 0%; taker fee ~2% applied on exit instead.
        execution_price = max(0.01, min(0.99, price))
        
        # Create execution
        execution = TradeExecution(
            trade_id=trade_id,
            market_id=market_id,
            market_title=market_title,
            asset='ETH',
            action=action,
            side=side,
            execution_price=execution_price,
            size=size,
        )
        
        # Track in open trades
        self.open_trades[trade_id] = execution
        self.trades_this_session += 1
        
        # Update inventory
        if market_id not in self.inventory:
            self.inventory[market_id] = {'YES': 0.0, 'NO': 0.0}
        
        if action == 'BUY':
            self.inventory[market_id][side] += size
        else:
            self.inventory[market_id][side] -= size
        
        logger.info(f"Trade {trade_id}: {action} {size} {side} @ {execution_price:.3f}")
        return execution
    
    def close_trade(
        self,
        opening_trade_id: str,
        counter_price: float,
    ) -> Optional[TradeExecution]:
        """
        Close an open position by trading the opposite side.
        
        Calculate P&L and move to closed trades.
        """
        if opening_trade_id not in self.open_trades:
            logger.warning(f"Trade {opening_trade_id} not found")
            return None
        
        open_trade = self.open_trades.pop(opening_trade_id)
        
        # Determine counter action
        if open_trade.action == 'BUY':
            counter_action = 'SELL'
        else:
            counter_action = 'BUY'
        
        # GROSS P&L only — fees are applied externally in eth_market_maker.py
        # via live_rules.fee_drag() to avoid double-counting.
        if open_trade.action == 'BUY':
            pnl_per_contract = counter_price - open_trade.execution_price
        else:
            pnl_per_contract = open_trade.execution_price - counter_price
        
        pnl = pnl_per_contract * open_trade.size
        pnl_pct = pnl_per_contract / open_trade.execution_price if open_trade.execution_price > 0 else 0
        
        # Mark as closed
        open_trade.counter_action = counter_action
        open_trade.counter_price = counter_price
        open_trade.counter_time = datetime.now().isoformat()
        open_trade.pnl = pnl
        open_trade.pnl_pct = pnl_pct
        open_trade.is_closed = True
        
        # Move to closed trades
        self.closed_trades.append(open_trade)
        
        # Update stats
        self.total_pnl += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        
        logger.info(f"Trade {opening_trade_id} closed: {counter_action} @ {counter_price:.3f} | P&L: {pnl:+.4f} ({pnl_pct:+.2%})")
        return open_trade
    
    def get_session_stats(self) -> Dict:
        """Get current session statistics."""
        return {
            'total_trades': len(self.closed_trades) + len(self.open_trades),
            'closed_trades': len(self.closed_trades),
            'open_positions': len(self.open_trades),
            'wins': self.wins,
            'losses': self.losses,
            'win_rate': self.wins / (self.wins + self.losses) if (self.wins + self.losses) > 0 else 0,
            'total_pnl': self.total_pnl,
            'avg_pnl_per_trade': self.total_pnl / len(self.closed_trades) if self.closed_trades else 0,
            'trades_this_session': self.trades_this_session,
        }
