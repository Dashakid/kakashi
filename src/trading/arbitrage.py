"""Arbitrage detection and execution strategies for Polymarket."""

import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from loguru import logger


class ArbitrageType(Enum):
    """Types of arbitrage opportunities."""
    YES_NO_SUM = "yes_no_sum"  # When yes + no != 1.0
    LATENCY = "latency"  # Lag between Binance and Polymarket
    PROBABILITY_DIVERGENCE = "probability_divergence"  # vs CME/deribit


@dataclass
class ArbitrageOpportunity:
    """Represents an arbitrage opportunity."""
    type: ArbitrageType
    market_id: str
    yes_price: float
    no_price: float
    sum_price: float
    expected_return: float  # percentage
    confidence: float  # 0-1
    execution_time: float  # seconds available
    reason: str


class ArbitrageDetector:
    """Detect and analyze arbitrage opportunities on Polymarket."""

    def __init__(self, min_return: float = 0.01):
        """
        Initialize arbitrage detector.
        
        Args:
            min_return: Minimum return threshold (0.01 = 1%)
        """
        self.min_return = min_return

    def detect_yes_no_sum_arb(
        self,
        market_id: str,
        yes_price: float,
        no_price: float,
        tolerance: float = 0.001,  # 0.1% tolerance for small variations
    ) -> Optional[ArbitrageOpportunity]:
        """
        Detect Yes/No sum arbitrage.
        
        Math: In a binary market, Yes + No should always = $1.00
        Opportunity: If sum < $1.00, buy both for risk-free profit
        
        Example:
            Yes = $0.48, No = $0.49, Sum = $0.97
            Buy both for $0.97, receive $1.00 regardless of outcome
            Return: 3.09% = (1.00 - 0.97) / 0.97
        
        Args:
            market_id: Market ID
            yes_price: Current yes price
            no_price: Current no price
            tolerance: Acceptable price divergence
            
        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        sum_price = yes_price + no_price
        
        # Look for sum < 1.0 (buy both for profit)
        if sum_price >= 0.99 and sum_price <= 1.01:
            return None  # Prices are fair
        
        if sum_price >= 1.0:
            return None  # Sum > 1, no arb
        
        # Calculate expected return
        cost = sum_price
        payout = 1.0
        return_pct = (payout - cost) / cost
        
        if return_pct < self.min_return:
            return None  # Return too small
        
        logger.info(
            f"Yes/No sum arbitrage detected: "
            f"Yes=${yes_price:.4f} + No=${no_price:.4f} = ${sum_price:.4f} "
            f"({return_pct*100:.2f}% return)"
        )
        
        return ArbitrageOpportunity(
            type=ArbitrageType.YES_NO_SUM,
            market_id=market_id,
            yes_price=yes_price,
            no_price=no_price,
            sum_price=sum_price,
            expected_return=return_pct,
            confidence=0.98,  # Very high confidence (mathematical arbitrage)
            execution_time=60.0,  # Usually needs quick execution
            reason=f"Sum={sum_price:.4f} < 1.0, {return_pct*100:.2f}% risk-free return",
        )

    def detect_latency_arb(
        self,
        market_id: str,
        polymarket_price: float,
        external_price: float,
        external_source: str,  # "binance", "deribit", "cme"
        price_direction: str,  # "up" or "down"
        delay_seconds: float = 60.0,  # Expected delay
    ) -> Optional[ArbitrageOpportunity]:
        """
        Detect latency arbitrage between Polymarket and spot/futures exchanges.
        
        Setup:
        - Bitcoin spot price jumps $100+ on Binance
        - Polymarket "Will BTC be above $X in 15 mins?" lags in updating
        - Gap between fair price and Polymarket price = arbitrage
        
        Args:
            market_id: Market ID
            polymarket_price: Current Polymarket yes price
            external_price: Current external (Binance/deribit) price
            external_source: Source of external price
            price_direction: "up" if price moved up, "down" if moved down
            delay_seconds: Expected delay in seconds
            
        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        # Calculate fair price based on external move
        if price_direction == "up":
            # Price went up, yes contract should be worth more
            fair_price = min(0.95, external_price * 0.01)  # Rough approximation
            if polymarket_price < fair_price:
                return_pct = (fair_price - polymarket_price) / polymarket_price
                
                if return_pct < self.min_return:
                    return None
                
                logger.info(
                    f"Latency arbitrage (UP): Polymarket ${polymarket_price:.4f} "
                    f"vs fair ${fair_price:.4f} from {external_source}"
                )
                
                return ArbitrageOpportunity(
                    type=ArbitrageType.LATENCY,
                    market_id=market_id,
                    yes_price=polymarket_price,
                    no_price=1.0 - polymarket_price,
                    sum_price=1.0,
                    expected_return=return_pct,
                    confidence=0.70,  # Depends on execution speed
                    execution_time=delay_seconds,
                    reason=f"BTC {price_direction} on {external_source}, Polymarket lagging",
                )
        
        elif price_direction == "down":
            # Price went down, no contract should be worth more
            fair_price = max(0.05, 1.0 - (external_price * 0.01))
            if polymarket_price > fair_price:
                return_pct = (polymarket_price - fair_price) / polymarket_price
                
                if return_pct < self.min_return:
                    return None
                
                logger.info(
                    f"Latency arbitrage (DOWN): Polymarket ${polymarket_price:.4f} "
                    f"vs fair ${fair_price:.4f} from {external_source}"
                )
                
                return ArbitrageOpportunity(
                    type=ArbitrageType.LATENCY,
                    market_id=market_id,
                    yes_price=polymarket_price,
                    no_price=1.0 - polymarket_price,
                    sum_price=1.0,
                    expected_return=return_pct,
                    confidence=0.70,
                    execution_time=delay_seconds,
                    reason=f"BTC {price_direction} on {external_source}, Polymarket lagging",
                )
        
        return None

    def detect_probability_divergence(
        self,
        market_id: str,
        polymarket_price: float,
        cme_price: float,
        deribit_price: Optional[float] = None,
    ) -> Optional[ArbitrageOpportunity]:
        """
        Detect divergence between Polymarket and institutional prices.
        
        CME Bitcoin futures and Deribit options reflect "smart money" positioning.
        When Polymarket diverges significantly, it may be mispriced.
        
        Args:
            market_id: Market ID
            polymarket_price: Polymarket yes price (0-1)
            cme_price: CME implied probability (0-1)
            deribit_price: Deribit implied probability (0-1)
            
        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        divergences = []
        
        # Compare vs CME
        divergence_cme = abs(polymarket_price - cme_price)
        if divergence_cme > 0.05:  # 5% divergence
            divergences.append(("CME", divergence_cme, cme_price))
        
        # Compare vs Deribit
        if deribit_price is not None:
            divergence_deribit = abs(polymarket_price - deribit_price)
            if divergence_deribit > 0.05:
                divergences.append(("Deribit", divergence_deribit, deribit_price))
        
        if not divergences:
            return None
        
        # Use largest divergence
        source, divergence, fair_price = max(divergences, key=lambda x: x[1])
        
        # Determine if Polymarket is undervalued or overvalued
        if polymarket_price < fair_price:
            # Polymarket too cheap, buy yes
            return_pct = (fair_price - polymarket_price) / polymarket_price
            action = "BUY_YES"
        else:
            # Polymarket too expensive, buy no
            return_pct = (polymarket_price - fair_price) / polymarket_price
            action = "BUY_NO"
        
        if return_pct < self.min_return:
            return None
        
        logger.info(
            f"Probability divergence: Polymarket {polymarket_price:.2%} "
            f"vs {source} {fair_price:.2%} ({action})"
        )
        
        return ArbitrageOpportunity(
            type=ArbitrageType.PROBABILITY_DIVERGENCE,
            market_id=market_id,
            yes_price=polymarket_price,
            no_price=1.0 - polymarket_price,
            sum_price=1.0,
            expected_return=return_pct,
            confidence=0.65,  # Depends on which institutional market leads
            execution_time=300.0,  # More time to converge
            reason=f"Mismatch with {source}: Polymarket {polymarket_price:.2%} vs {fair_price:.2%}",
        )

    def scan_market(
        self,
        market_id: str,
        yes_price: float,
        no_price: float,
        external_price: Optional[float] = None,
        external_source: Optional[str] = None,
        cme_price: Optional[float] = None,
    ) -> List[ArbitrageOpportunity]:
        """
        Scan a single market for all types of arbitrage.
        
        Args:
            market_id: Market ID
            yes_price: Yes contract price
            no_price: No contract price
            external_price: External exchange price (for latency arb)
            external_source: External exchange name
            cme_price: CME institutional price (for divergence arb)
            
        Returns:
            List of detected arbitrage opportunities
        """
        opportunities = []
        
        # Check Yes/No sum arbitrage
        sum_arb = self.detect_yes_no_sum_arb(market_id, yes_price, no_price)
        if sum_arb:
            opportunities.append(sum_arb)
        
        # Check latency arbitrage
        if external_price and external_source:
            price_direction = "up" if external_price > 40000 else "down"  # Threshold
            latency_arb = self.detect_latency_arb(
                market_id,
                yes_price,
                external_price,
                external_source,
                price_direction,
            )
            if latency_arb:
                opportunities.append(latency_arb)
        
        # Check probability divergence
        if cme_price:
            divergence_arb = self.detect_probability_divergence(
                market_id,
                yes_price,
                cme_price,
            )
            if divergence_arb:
                opportunities.append(divergence_arb)
        
        return opportunities
