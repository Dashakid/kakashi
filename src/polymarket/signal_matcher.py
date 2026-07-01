"""
Signal-to-market matching engine.
Connects trading signals from bots to available Polymarket markets.
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from loguru import logger

from src.polymarket.market_discovery import MarketDiscovery, DiscoveredMarket


@dataclass
class SignalMatch:
    """A trading signal matched to a Polymarket market."""
    bot_id: str
    signal: str                      # "BUY_YES" or "BUY_NO"
    asset: str                       # BTC, ETH, etc.
    confidence: float                # Signal confidence 0-100
    entry_price: float               # Current price
    matched_market: DiscoveredMarket
    market_edge: float               # Our edge vs market odds
    outcome_to_buy: int              # Which outcome ID to buy (0=YES, 1=NO)
    recommended_size: float          # Position size recommendation
    reasoning: str                   # Why this market was chosen


class SignalMatcher:
    """Match trading signals to Polymarket markets."""
    
    def __init__(self, discovery: MarketDiscovery):
        """Initialize signal matcher."""
        self.discovery = discovery
        self.min_liquidity = 50.0     # Min market liquidity in $
        self.min_edge = 0.02          # Min edge required to trade (2%)
        self.max_spread = 0.10        # Max bid/ask spread (10%)
        self.min_confidence = 60.0    # Min signal confidence to trade
    
    def match_signal(
        self,
        bot_id: str,
        signal: str,
        asset: str,
        confidence: float,
        entry_price: float,
        position_size: float = 100.0
    ) -> Optional[SignalMatch]:
        """
        Match a trading signal to a market.
        
        Args:
            bot_id: Which bot generated signal (btc-bot, eth-bot)
            signal: BUY_YES or BUY_NO
            asset: BTC, ETH, SOL
            confidence: Signal confidence 0-100
            entry_price: Current market price
            position_size: $ size to allocate
        
        Returns:
            SignalMatch if found, None if no suitable market
        """
        
        # Validate signal
        if confidence < self.min_confidence:
            logger.warning(f"Signal confidence {confidence:.0f}% below floor {self.min_confidence:.0f}%")
            return None
        
        if signal not in ["BUY_YES", "BUY_NO"]:
            logger.error(f"Invalid signal: {signal}")
            return None
        
        # Find best market for this signal
        best_match = None
        best_edge = -1
        
        candidates = self._get_candidate_markets(asset)
        
        if not candidates:
            logger.warning(f"No suitable markets found for {asset}")
            return None
        
        # Evaluate each candidate
        for market in candidates:
            # Check spread
            spread = (market.best_ask - market.best_bid) / market.current_price
            if spread > self.max_spread:
                logger.debug(f"Market {market.market_id} spread too wide: {spread:.1%}")
                continue
            
            # Calculate edge
            if signal == "BUY_YES":
                signal_prob = confidence / 100.0
                market_prob = market.probability_yes
            else:
                signal_prob = confidence / 100.0
                market_prob = market.probability_no
            
            edge = signal_prob - market_prob
            
            if edge < self.min_edge:
                logger.debug(f"Market {market.market_id} edge {edge:.1%} below minimum {self.min_edge:.1%}")
                continue
            
            # This is a good candidate
            if edge > best_edge:
                best_edge = edge
                best_match = (market, edge)
        
        if not best_match:
            logger.warning(f"No markets meet edge requirement ({self.min_edge:.1%})")
            return None
        
        market, edge = best_match
        
        # Determine which outcome to buy
        if signal == "BUY_YES":
            outcome_to_buy = market.outcome_yes_id
            outcome_name = "YES"
        else:
            outcome_to_buy = market.outcome_no_id
            outcome_name = "NO"
        
        # Size recommendation based on edge and confidence
        size_multiplier = min(2.0, edge * 10)  # Max 2x position sizing
        recommended_size = position_size * size_multiplier
        
        match = SignalMatch(
            bot_id=bot_id,
            signal=signal,
            asset=asset,
            confidence=confidence,
            entry_price=entry_price,
            matched_market=market,
            market_edge=edge,
            outcome_to_buy=outcome_to_buy,
            recommended_size=recommended_size,
            reasoning=f"{bot_id} {signal} on {asset} (conf: {confidence:.0f}%) matched to market '{market.title[:50]}' with {edge:.1%} edge"
        )
        
        logger.info(f"✅ Signal matched: {match.reasoning}")
        return match
    
    def _get_candidate_markets(self, asset: str) -> List[DiscoveredMarket]:
        """Get candidate markets for an asset."""
        markets = self.discovery.filter_by_asset(asset)
        markets = [m for m in markets if m.is_active() and m.is_liquid(self.min_liquidity)]
        
        # Sort by liquidity (higher = better)
        return sorted(markets, key=lambda m: m.liquidity, reverse=True)
    
    def match_signals_batch(
        self,
        signals: List[Dict]
    ) -> List[SignalMatch]:
        """
        Match multiple signals at once.
        
        Args:
            signals: List of signal dicts with keys:
                - bot_id, signal, asset, confidence, entry_price, position_size
        
        Returns:
            List of matched signals
        """
        matches = []
        for sig in signals:
            match = self.match_signal(
                bot_id=sig.get('bot_id'),
                signal=sig.get('signal'),
                asset=sig.get('asset'),
                confidence=sig.get('confidence'),
                entry_price=sig.get('entry_price'),
                position_size=sig.get('position_size', 100.0)
            )
            if match:
                matches.append(match)
        
        return matches
    
    # =====================
    # CONFIGURATION
    # =====================
    
    def set_min_liquidity(self, liquidity: float):
        """Set minimum market liquidity requirement."""
        self.min_liquidity = liquidity
        logger.info(f"Min liquidity set to ${liquidity:.0f}")
    
    def set_min_edge(self, edge: float):
        """Set minimum edge requirement (0-1)."""
        self.min_edge = edge
        logger.info(f"Min edge set to {edge:.1%}")
    
    def set_min_confidence(self, confidence: float):
        """Set minimum signal confidence floor."""
        self.min_confidence = confidence
        logger.info(f"Min confidence set to {confidence:.0f}%")
    
    # =====================
    # REPORTING
    # =====================
    
    def print_match(self, match: SignalMatch):
        """Pretty-print a signal match."""
        print(f"""
╔════════════════════════════════════════════════════════╗
║               SIGNAL MATCH DETAILS                     ║
╠════════════════════════════════════════════════════════╣
║ Bot:               {match.bot_id:40}║
║ Signal:            {match.signal:40}║
║ Asset:             {match.asset:40}║
║ Confidence:        {match.confidence:40.0f}%║
║                                                        ║
║ Market Title:      {match.matched_market.title[:43]:43}║
║ Liquidity:         ${match.matched_market.liquidity:>42,.0f}║
║ Current Price:     ${match.entry_price:>42,.2f}║
║ Bid/Ask:           ${match.matched_market.best_bid:.2f} / ${match.matched_market.best_ask:.2f}{' '*30}║
║                                                        ║
║ Market Edge:       {match.market_edge:>42.1%}║
║ Outcome to Buy:    {['YES', 'NO'][match.outcome_to_buy]:40}║
║ Recommended Size:  ${match.recommended_size:>42,.0f}║
║                                                        ║
║ Reasoning:         {match.reasoning[:43]:43}║
╚════════════════════════════════════════════════════════╝
""")
