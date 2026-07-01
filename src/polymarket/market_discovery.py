"""
Polymarket market discovery engine.
Discovers all active BTC/ETH markets and caches them for rapid access.
"""

import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict, field
from loguru import logger

from src.polymarket.client import PolymarketClient, Market


@dataclass
class DiscoveredMarket:
    """A discovered Polymarket market ready for trading."""
    market_id: str
    title: str
    description: str
    asset: str                       # BTC or ETH
    market_type: str                 # "direction" | "price_target" | "price_range"
    outcomes: List[str]              # The two outcomes (e.g., ["UP", "DOWN"])
    outcome_yes_id: int              # ID for YES outcome
    outcome_no_id: int               # ID for NO outcome
    current_price: float             # Mid-price
    best_bid: float
    best_ask: float
    volume_24h: float
    liquidity: float                 # Combined liquidity
    open_interest: float
    closes_at: Optional[str]         # Market close time (ISO)
    probability_yes: float           # Market's implied probability for YES
    probability_no: float            # Market's implied probability for NO
    discovered_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def __hash__(self):
        return hash(self.market_id)
    
    def is_liquid(self, min_liquidity: float = 100.0) -> bool:
        """Check if market has sufficient liquidity."""
        return self.liquidity >= min_liquidity
    
    def is_active(self) -> bool:
        """Check if market hasn't closed yet."""
        if not self.closes_at:
            return True
        closes = datetime.fromisoformat(self.closes_at)
        return closes > datetime.now()
    
    def get_edge(self, signal_probability: float) -> float:
        """
        Calculate edge if we have a signal probability.
        
        Args:
            signal_probability: Our model's prediction (0-1)
        
        Returns:
            Edge (positive = in our favor)
        """
        market_prob = self.probability_yes
        if signal_probability > 0.5:
            # We think YES, compare to market's YES probability
            edge = signal_probability - market_prob
        else:
            # We think NO, compare to market's NO probability
            edge = (1 - signal_probability) - (1 - market_prob)
        return edge


class MarketDiscovery:
    """Discovers and caches Polymarket markets for BTC/ETH."""
    
    def __init__(self, client: PolymarketClient, cache_file: str = "data/market_cache.json"):
        """Initialize discovery engine."""
        self.client = client
        self.cache_file = Path(cache_file)
        self.cache_file.parent.mkdir(exist_ok=True)
        
        self.markets: Dict[str, DiscoveredMarket] = {}
        self.last_refresh = None
        self.cache_ttl = timedelta(hours=1)  # Refresh cache every hour
        
        self._load_cache()
    
    def _load_cache(self):
        """Load markets from cache file."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file) as f:
                    data = json.load(f)
                    self.markets = {
                        m['market_id']: DiscoveredMarket(**m)
                        for m in data.get('markets', [])
                    }
                    self.last_refresh = datetime.fromisoformat(data.get('last_refresh'))
                    logger.info(f"✅ Loaded {len(self.markets)} markets from cache")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
                self.markets = {}
    
    def _save_cache(self):
        """Save markets to cache file."""
        try:
            cache_data = {
                'markets': [asdict(m) for m in self.markets.values()],
                'last_refresh': datetime.now().isoformat(),
                'total_count': len(self.markets)
            }
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
            logger.debug(f"✅ Saved {len(self.markets)} markets to cache")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    async def discover_markets(self, force_refresh: bool = False) -> Dict[str, DiscoveredMarket]:
        """
        Discover all BTC/ETH markets. Uses cache if <1 hour old.
        
        Args:
            force_refresh: Force API refresh even if cache is fresh
            
        Returns:
            Dict of {market_id: DiscoveredMarket}
        """
        
        # Check if cache is still fresh
        if not force_refresh and self.last_refresh:
            age = datetime.now() - self.last_refresh
            if age < self.cache_ttl:
                logger.info(f"ℹ️ Using cached markets ({len(self.markets)} total)")
                return self.markets
        
        logger.info("🔍 Discovering markets from Polymarket API...")
        
        discovered = {}
        
        try:
            # FIXED: Fetch ALL markets at once (no search term = no filter)
            logger.info(f"  Fetching all active Polymarket direction markets...")
            markets = await self.client.get_markets(search_term=None)
            logger.info(f"  ✅ Fetched {len(markets)} markets from API")
            
            # Parse and filter for BTC/ETH direction markets
            for market in markets:
                # Try to auto-detect asset from title
                parsed = self._parse_market_auto_detect(market)
                if parsed:
                    discovered[parsed.market_id] = parsed
                    if len(discovered) <= 20:  # Log first 20
                        logger.debug(f"    Found: [{parsed.asset}] {parsed.title[:50]}")
            
        except Exception as e:
            logger.error(f"  Error fetching markets: {type(e).__name__}: {e}")
        
        # Update cache
        self.markets = discovered
        self.last_refresh = datetime.now()
        self._save_cache()
        
        logger.info(f"✅ Discovered {len(self.markets)} live markets")
        return self.markets
    
    def _parse_market_auto_detect(self, market: Market) -> Optional[DiscoveredMarket]:
        """Parse Market into DiscoveredMarket with AUTO-DETECTION of asset and type."""
        try:
            title_lower = market.title.lower()
            description_lower = (market.description or "").lower()
            question_text = title_lower + " " + description_lower
            
            # AUTO-DETECT asset from title/description
            asset = None
            if any(x in question_text for x in ['bitcoin', ' btc', 'btc:', 'bitcoin:']):
                asset = 'BTC'
            elif any(x in question_text for x in ['ethereum', ' eth', 'eth:', 'ethereum:']):
                asset = 'ETH'
            elif any(x in question_text for x in ['solana', ' sol', 'sol:']):
                asset = 'SOL'
            elif any(x in question_text for x in ['avalanche', ' avax', 'avax:']):
                asset = 'AVAX'
            elif any(x in question_text for x in ['ripple', ' xrp', 'xrp:']):
                asset = 'XRP'
            elif any(x in question_text for x in ['polkadot', ' dot', 'dot:']):
                asset = 'DOT'
            elif any(x in question_text for x in ['chainlink', ' link', 'link:']):
                asset = 'LINK'
            elif any(x in question_text for x in ['cardano', ' ada', 'ada:']):
                asset = 'ADA'
            
            # If no crypto asset detected, skip
            if not asset:
                return None
            
            # Determine market type from title
            if 'direction' in title_lower or 'up or down' in title_lower or any(x in title_lower for x in ['higher', 'lower', 'increase', 'decrease']):
                market_type = 'direction'
            elif 'reach' in title_lower or 'above' in title_lower or 'below' in title_lower:
                market_type = 'price_target'
            elif 'between' in title_lower or 'range' in title_lower:
                market_type = 'price_range'
            else:
                # Default to direction if it's any kind of market
                market_type = 'direction'
            
            # Calculate probabilities from prices (assuming binary outcomes)
            # Use last_price (set from outcomePrices[0] in client.py) as YES probability
            prob_yes = market.last_price if market.last_price > 0 else 0.5
            prob_no = 1.0 - prob_yes
            
            discovered = DiscoveredMarket(
                market_id=market.id,
                title=market.title,
                description=market.description or "",
                asset=asset,
                market_type=market_type,
                outcomes=market.outcomes,
                outcome_yes_id=0,
                outcome_no_id=1,
                current_price=market.last_price,
                best_bid=market.best_bid,
                best_ask=market.best_ask,
                volume_24h=market.volume,
                liquidity=market.liquidity,
                open_interest=0,
                closes_at=None,
                probability_yes=prob_yes,
                probability_no=prob_no
            )
            
            return discovered
        except Exception as e:
            logger.debug(f"Failed to parse market: {e}")
            return None
    
    def _parse_market(self, market: Market, asset: str) -> Optional[DiscoveredMarket]:
        """Parse Market into DiscoveredMarket."""
        try:
            # Determine market type from title
            title_lower = market.title.lower()
            
            if 'direction' in title_lower or 'up or down' in title_lower:
                market_type = 'direction'
            elif 'reach' in title_lower or 'above' in title_lower or 'below' in title_lower:
                market_type = 'price_target'
            elif 'between' in title_lower or 'range' in title_lower:
                market_type = 'price_range'
            else:
                market_type = 'direction'  # Default
            
            # Calculate probabilities from prices (assuming binary outcomes)
            # Use last_price (set from outcomePrices[0] in client.py) as YES probability
            prob_yes = market.last_price if market.last_price > 0 else 0.5
            prob_no = 1.0 - prob_yes
            
            discovered = DiscoveredMarket(
                market_id=market.id,
                title=market.title,
                description=market.description or "",
                asset=asset,
                market_type=market_type,
                outcomes=market.outcomes,
                outcome_yes_id=0,
                outcome_no_id=1,
                current_price=market.last_price,
                best_bid=market.best_bid,
                best_ask=market.best_ask,
                volume_24h=market.volume,
                liquidity=market.liquidity,
                open_interest=0,
                closes_at=None,
                probability_yes=prob_yes,
                probability_no=prob_no
            )
            
            return discovered
        except Exception as e:
            logger.debug(f"Failed to parse market: {e}")
            return None
    
    # =====================
    # FILTERING & QUERYING
    # =====================
    
    def filter_by_asset(self, asset: str) -> List[DiscoveredMarket]:
        """Get all markets for an asset."""
        return [m for m in self.markets.values() if m.asset == asset]
    
    def filter_by_type(self, market_type: str) -> List[DiscoveredMarket]:
        """Get all markets of a specific type."""
        return [m for m in self.markets.values() if m.market_type == market_type]
    
    def filter_by_liquidity(self, min_liquidity: float = 100.0) -> List[DiscoveredMarket]:
        """Get all markets with sufficient liquidity."""
        return [m for m in self.markets.values() if m.is_liquid(min_liquidity)]
    
    def filter_active_only(self) -> List[DiscoveredMarket]:
        """Get only active (not closed) markets."""
        return [m for m in self.markets.values() if m.is_active()]
    
    def search(self, keyword: str) -> List[DiscoveredMarket]:
        """Search markets by title/description."""
        keyword_lower = keyword.lower()
        return [
            m for m in self.markets.values()
            if keyword_lower in m.title.lower() or keyword_lower in m.description.lower()
        ]
    
    def get_best_market_for_signal(
        self,
        asset: str,
        signal_confidence: float,
        market_type: str = "direction",
        min_liquidity: float = 100.0
    ) -> Optional[DiscoveredMarket]:
        """
        Find the best market for a given signal.
        
        Prioritizes by:
        1. Market type match
        2. Liquidity
        3. Implied edge (our confidence vs market price)
        """
        candidates = [
            m for m in self.markets.values()
            if m.asset == asset
            and m.is_active()
            and m.is_liquid(min_liquidity)
            and m.market_type == market_type
        ]
        
        if not candidates:
            return None
        
        # Sort by liquidity (higher is better)
        candidates.sort(key=lambda m: m.liquidity, reverse=True)
        
        best = candidates[0]
        logger.debug(f"Best market for {asset}: {best.title[:50]} (liquidity: ${best.liquidity:.0f})")
        return best
    
    # =====================
    # REPORTING
    # =====================
    
    def get_summary(self) -> Dict:
        """Get summary of discovered markets."""
        btc_markets = self.filter_by_asset('BTC')
        eth_markets = self.filter_by_asset('ETH')
        
        active = self.filter_active_only()
        liquid = self.filter_by_liquidity()
        
        return {
            'total_markets': len(self.markets),
            'btc_markets': len(btc_markets),
            'eth_markets': len(eth_markets),
            'active_markets': len(active),
            'liquid_markets': len(liquid),
            'by_type': {
                'direction': len(self.filter_by_type('direction')),
                'price_target': len(self.filter_by_type('price_target')),
                'price_range': len(self.filter_by_type('price_range')),
            },
            'last_refresh': self.last_refresh.isoformat() if self.last_refresh else None
        }
    
    def print_summary(self):
        """Print market discovery summary."""
        summary = self.get_summary()
        
        print(f"""
╔════════════════════════════════════════════════════╗
║          MARKET DISCOVERY SUMMARY                  ║
╠════════════════════════════════════════════════════╣
║ Total Markets:          {summary['total_markets']:3d}                          ║
║ BTC Markets:            {summary['btc_markets']:3d}                            ║
║ ETH Markets:            {summary['eth_markets']:3d}                            ║
║ Active Markets:         {summary['active_markets']:3d}                          ║
║ Liquid Markets (>$100): {summary['liquid_markets']:3d}                          ║
╠════════════════════════════════════════════════════╣
║ By Type:                                           ║
║   Direction:            {summary['by_type']['direction']:3d}                        ║
║   Price Target:         {summary['by_type']['price_target']:3d}                        ║
║   Price Range:          {summary['by_type']['price_range']:3d}                        ║
╠════════════════════════════════════════════════════╣
║ Last Refresh: {summary['last_refresh'][:19] if summary['last_refresh'] else 'Never':>40}  ║
╚════════════════════════════════════════════════════╝
""")
    
    def print_markets_for_asset(self, asset: str, limit: int = 10):
        """Print available markets for an asset."""
        markets = self.filter_by_asset(asset)
        active = [m for m in markets if m.is_active()]
        
        print(f"\n{'═'*80}")
        print(f"  {asset} MARKETS ({len(active)} active / {len(markets)} total)")
        print(f"{'═'*80}")
        
        for i, market in enumerate(active[:limit], 1):
            liquidity_bar = '█' * int(min(market.liquidity / 50, 10))
            print(f"""{i:2d}. {market.title[:56]:56} | ${market.liquidity:6.0f} {liquidity_bar}""")
        
        if len(active) > limit:
            print(f"\n   ... and {len(active) - limit} more markets")
        print()
