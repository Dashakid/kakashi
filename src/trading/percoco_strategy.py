"""Percoco Smart Money Concepts (SMC) strategy implementation."""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from loguru import logger

from src.ml.technical_analysis import TechnicalAnalysis, TechnicalIndicators


class TradeSignal(Enum):
    """Trade signal types."""
    BUY_YES = "buy_yes"
    BUY_NO = "buy_no"
    SELL = "sell"
    NO_SIGNAL = "no_signal"


@dataclass
class PercocolSignalResult:
    """Result of Percoco signal analysis."""
    signal: TradeSignal
    confidence: float  # 0.0 to 1.0
    entry_price: float
    stop_loss: float
    take_profit: float
    reason: str


class PercocolStrategy:
    """
    Craig Percoco's Smart Money Concepts (SMC) applied to Polymarket.
    
    Key principles:
    1. Fair Value Gap (FVG) detection on 15-min chart for bias
    2. One-candle entries on 1-min chart for precise timing
    3. EMA 20 filtering for trend confirmation
    4. Liquidity sweeps as entry triggers
    """

    def __init__(self, min_confidence: float = 0.65):
        """
        Initialize Percoco strategy.
        
        Args:
            min_confidence: Minimum confidence threshold for signals (0-1)
        """
        self.min_confidence = min_confidence
        self.tech = TechnicalAnalysis()

    def analyze_15min_bias(self, df_15m: pd.DataFrame) -> str:
        """
        Analyze 15-minute chart for overall bias using FVG.
        
        Args:
            df_15m: 15-minute OHLCV DataFrame
            
        Returns:
            'bullish', 'bearish', or 'neutral'
        """
        if len(df_15m) < 5:
            return "neutral"
        
        # Get FVGs
        fvgs = self.tech.calculate_fvg(df_15m)
        
        # Count unfilled bullish and bearish FVGs
        bullish_fvgs = [f for f in fvgs if f.type == 'bullish' and not f.filled]
        bearish_fvgs = [f for f in fvgs if f.type == 'bearish' and not f.filled]
        
        current_price = df_15m.iloc[-1]['close']
        
        # Check if price is graviting toward FVG
        bias = "neutral"
        
        if bullish_fvgs:
            closest_fvg = min(bullish_fvgs, key=lambda f: abs(f.high - current_price))
            if current_price < closest_fvg.high:
                bias = "bullish"
        
        if bearish_fvgs:
            closest_fvg = min(bearish_fvgs, key=lambda f: abs(f.low - current_price))
            if current_price > closest_fvg.low:
                bias = "bearish"
        
        logger.info(f"15-min bias: {bias} (Bullish FVGs: {len(bullish_fvgs)}, Bearish FVGs: {len(bearish_fvgs)})")
        return bias

    def detect_liquidity_sweep(self, df_1m: pd.DataFrame, lookback: int = 5) -> Optional[str]:
        """
        Detect aggressive one-candle liquidity sweeps on 1-minute chart.
        
        A liquidity sweep is an aggressive candle that breaks recent high/low
        with strong closing.
        
        Args:
            df_1m: 1-minute OHLCV DataFrame
            lookback: Number of candles to check for recent high/low
            
        Returns:
            'bullish' (broke resistance), 'bearish' (broke support), or None
        """
        if len(df_1m) < lookback + 1:
            return None
        
        current = df_1m.iloc[-1]
        previous = df_1m.iloc[-2]
        lookback_data = df_1m.iloc[-(lookback+1):-1]
        
        recent_high = lookback_data['high'].max()
        recent_low = lookback_data['low'].min()
        
        # Bullish sweep: close above recent high with strong body
        if current['close'] > recent_high and previous['close'] < recent_high:
            body_size = current['close'] - current['open']
            candle_range = current['high'] - current['low']
            if body_size > (candle_range * 0.6):  # Strong close
                logger.info("Bullish liquidity sweep detected")
                return "bullish"
        
        # Bearish sweep: close below recent low with strong body
        if current['close'] < recent_low and previous['close'] > recent_low:
            body_size = current['open'] - current['close']
            candle_range = current['high'] - current['low']
            if body_size > (candle_range * 0.6):  # Strong close
                logger.info("Bearish liquidity sweep detected")
                return "bearish"
        
        return None

    def ema_filter(self, price: float, indicators: TechnicalIndicators, atr: float) -> Tuple[bool, str]:
        """
        Apply EMA 20 filter for trade confirmation.
        
        Rules:
        - For BUY trades, price must be above EMA 20
        - Price should NOT be "hugging" the EMA (too close = no expansion)
        
        Args:
            price: Current price
            indicators: Technical indicators
            atr: Average True Range
            
        Returns:
            (is_valid, reason)
        """
        is_above = self.tech.is_price_above_ema(price, indicators.ema_20)
        is_hugging = self.tech.is_price_hugging_ema(price, indicators.ema_20, atr)
        
        if not is_above:
            return False, "Price below EMA 20"
        
        if is_hugging:
            return False, "Price hugging EMA 20 (no expansion)"
        
        return True, "EMA filter passed"

    def analyze_1min_entry(
        self,
        df_1m: pd.DataFrame,
        bias_15m: str,
    ) -> PercocolSignalResult:
        """
        Analyze 1-minute chart for specific entry signal.
        
        Uses:
        1. 15-min bias as direction filter
        2. 1-min liquidity sweep as trigger
        3. EMA 20 filter for confirmation
        4. RSI/MACD for overbought/oversold checks
        
        Args:
            df_1m: 1-minute OHLCV DataFrame
            bias_15m: Bias from 15-minute analysis
            
        Returns:
            PercocolSignalResult with signal and confidence
        """
        if len(df_1m) < 10:
            return PercocolSignalResult(
                signal=TradeSignal.NO_SIGNAL,
                confidence=0.0,
                entry_price=0,
                stop_loss=0,
                take_profit=0,
                reason="Insufficient data",
            )
        
        # Get indicators
        df = self.tech.get_indicators(df_1m)
        indicators = self.tech.get_latest_indicators(df)
        current_price = df.iloc[-1]['close']
        current_atr = indicators.atr
        
        # Detect liquidity sweep
        sweep = self.detect_liquidity_sweep(df_1m)
        
        if not sweep:
            return PercocolSignalResult(
                signal=TradeSignal.NO_SIGNAL,
                confidence=0.0,
                entry_price=0,
                stop_loss=0,
                take_profit=0,
                reason="No liquidity sweep detected",
            )
        
        # EMA filter
        ema_valid, ema_reason = self.ema_filter(current_price, indicators, current_atr)
        if not ema_valid:
            return PercocolSignalResult(
                signal=TradeSignal.NO_SIGNAL,
                confidence=0.0,
                entry_price=0,
                stop_loss=0,
                take_profit=0,
                reason=f"EMA filter failed: {ema_reason}",
            )
        
        # Check if sweep aligns with bias
        if sweep == "bullish" and bias_15m != "bearish":
            # RSI not too overbought
            rsi = indicators.rsi_14
            if rsi > 80:
                return PercocolSignalResult(
                    signal=TradeSignal.NO_SIGNAL,
                    confidence=0.0,
                    entry_price=0,
                    stop_loss=0,
                    take_profit=0,
                    reason="RSI overbought (>80)",
                )
            
            confidence = 0.75 if bias_15m == "bullish" else 0.65
            stop_loss = current_price - (current_atr * 1.5)
            take_profit = current_price + (current_atr * 2.0)
            
            return PercocolSignalResult(
                signal=TradeSignal.BUY_YES,
                confidence=confidence,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                reason=f"Bullish sweep + {bias_15m} bias on 15m (RSI: {rsi:.1f})",
            )
        
        elif sweep == "bearish" and bias_15m != "bullish":
            # RSI not too oversold
            rsi = indicators.rsi_14
            if rsi < 20:
                return PercocolSignalResult(
                    signal=TradeSignal.NO_SIGNAL,
                    confidence=0.0,
                    entry_price=0,
                    stop_loss=0,
                    take_profit=0,
                    reason="RSI oversold (<20)",
                )
            
            confidence = 0.75 if bias_15m == "bearish" else 0.65
            stop_loss = current_price + (current_atr * 1.5)
            take_profit = current_price - (current_atr * 2.0)
            
            return PercocolSignalResult(
                signal=TradeSignal.BUY_NO,
                confidence=confidence,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                reason=f"Bearish sweep + {bias_15m} bias on 15m (RSI: {rsi:.1f})",
            )
        
        return PercocolSignalResult(
            signal=TradeSignal.NO_SIGNAL,
            confidence=0.0,
            entry_price=0,
            stop_loss=0,
            take_profit=0,
            reason=f"Sweep direction ({sweep}) conflicts with bias ({bias_15m})",
        )

    def get_signal(
        self,
        df_1m: pd.DataFrame,
        df_15m: pd.DataFrame,
    ) -> PercocolSignalResult:
        """
        Get complete Percoco signal combining 1-min and 15-min analysis.
        
        Args:
            df_1m: 1-minute OHLCV DataFrame
            df_15m: 15-minute OHLCV DataFrame
            
        Returns:
            PercocolSignalResult
        """
        # Get 15-min bias
        bias = self.analyze_15min_bias(df_15m)
        
        # Get 1-min entry signal
        signal = self.analyze_1min_entry(df_1m, bias)
        
        # Only return signal if confidence exceeds threshold
        if signal.confidence < self.min_confidence:
            return PercocolSignalResult(
                signal=TradeSignal.NO_SIGNAL,
                confidence=signal.confidence,
                entry_price=signal.entry_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                reason=f"Confidence {signal.confidence:.2f} < threshold {self.min_confidence}",
            )
        
        return signal
