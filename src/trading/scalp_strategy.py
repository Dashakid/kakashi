"""5-Minute Bitcoin Scalping Strategy using Bollinger Bands + RSI."""

import pandas as pd
import numpy as np
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Tuple
from loguru import logger


class TradeSignal(Enum):
    """Trade signals."""
    BUY_YES = "BUY_YES"
    BUY_NO = "BUY_NO"
    NO_SIGNAL = "NO_SIGNAL"


@dataclass
class SignalResult:
    """Result from signal generation."""
    signal: TradeSignal
    confidence: float
    entry_price: float
    stop_loss: float
    take_profit: float
    # Indicator details for rich notifications
    rsi: float = 0.0
    bb_pct: float = 0.0        # % distance from mid-BB (negative = below lower band)
    uptrend: bool = True
    volume_conf: float = 0.5   # 0.5–1.0 volume confirmation ratio
    momentum: float = 0.0      # 3-period rate of change
    reasoning: str = ""        # Human-readable explanation


class ScalpStrategy:
    """
    4-Hour Bitcoin Direction Strategy.
    
    Uses Bollinger Bands for mean reversion + RSI for confirmation.
    Target: 55% win rate, 1:2 risk/reward ratio on 4-hour moves.
    """
    
    def __init__(self, bb_period: int = 20, bb_std: float = 2.0, rsi_period: int = 14,
                 ml_predictor=None):
        """Initialize 4-hour strategy.

        Args:
            ml_predictor: Optional ``PricePredictor`` instance.  When provided,
                the final confidence is a weighted blend: 70% BB/RSI base score
                + 30% ML probability — improving signal quality when a trained
                model is available without hard-depending on one.
        """
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.rsi_period = rsi_period
        self.min_confidence = 0.55  # Will be adjusted by learning system
        self.ml_predictor = ml_predictor  # Optional PricePredictor
    
    def _calculate_bollinger_bands(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands."""
        sma = df['close'].rolling(window=self.bb_period).mean()
        std = df['close'].rolling(window=self.bb_period).std()
        upper_band = sma + (std * self.bb_std)
        lower_band = sma - (std * self.bb_std)
        return upper_band, sma, lower_band
    
    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate RSI."""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_momentum(self, df: pd.DataFrame, period: int = 5) -> pd.Series:
        """Calculate simple momentum (rate of change)."""
        return df['close'].pct_change(period)
    
    def _calculate_volume_signal(self, df: pd.DataFrame) -> float:
        """
        Check if volume confirms the move.
        Returns 0.0-1.0 confidence based on volume relative to average.
        """
        current_vol = df['volume'].iloc[-1]
        avg_vol = df['volume'].iloc[-20:].mean()
        
        if avg_vol == 0:
            return 0.5
        
        vol_ratio = current_vol / avg_vol
        # High volume = more confidence (up to 1.0)
        # Low volume = less confidence (down to 0.5)
        return min(1.0, max(0.5, vol_ratio / 2.0))
    
    def get_signal(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> SignalResult:
        """
        Generate 4-hour direction signal from price data.
        
        Use intraday candles to predict 4-hour direction.
        Args:
            df_5m: Recent 5-min or 1-hour candles (for current momentum)
            df_1h: Longer term 1-hour+ data (for trend bias)
        
        Returns:
            SignalResult with BUY_YES (UP), BUY_NO (DOWN), or NO_SIGNAL
        """
        try:
            if len(df_5m) < self.bb_period + 5:
                logger.debug("Not enough data for signal")
                return SignalResult(
                    signal=TradeSignal.NO_SIGNAL,
                    confidence=0.0,
                    entry_price=0.0,
                    stop_loss=0.0,
                    take_profit=0.0
                )
            
            # Get indicators from current timeframe
            upper_bb, mid_bb, lower_bb = self._calculate_bollinger_bands(df_5m)
            rsi = self._calculate_rsi(df_5m, self.rsi_period)
            momentum = self._calculate_momentum(df_5m, period=3)
            volume_conf = self._calculate_volume_signal(df_5m)
            
            current_price = df_5m['close'].iloc[-1]
            current_rsi = rsi.iloc[-1]
            current_momentum = momentum.iloc[-1]
            
            # Get longer-term trend bias
            if len(df_1h) >= 5:
                h1_sma = df_1h['close'].rolling(20).mean().iloc[-1]
                h1_price = df_1h['close'].iloc[-1]
                uptrend = h1_price > h1_sma
            else:
                uptrend = True
            
            # --- 4-HOUR SIGNAL GENERATION ---
            signal = TradeSignal.NO_SIGNAL
            confidence = 0.0

            lower_val = lower_bb.iloc[-1]
            upper_val = upper_bb.iloc[-1]

            # Also check previous candle for confirmation (price crossed band)
            prev_price = df_5m['close'].iloc[-2] if len(df_5m) > 1 else current_price
            prev_rsi   = rsi.iloc[-2] if len(rsi) > 1 else current_rsi

            # BUY_YES (Long for 4 hours):
            #   - Price must actually TOUCH or pierce the lower Bollinger Band
            #   - RSI must be genuinely oversold (<= 40), not just below 65
            #   - Momentum must not be collapsing hard (avoid catching falling knife)
            at_lower_band = current_price <= lower_val * 1.002  # within 0.2% of lower band
            rsi_oversold  = current_rsi <= 40
            momentum_ok   = current_momentum > -0.03  # not in free-fall

            if at_lower_band and rsi_oversold and momentum_ok:
                signal = TradeSignal.BUY_YES
                confidence = 0.57
                # Stronger oversold → higher confidence
                if current_rsi <= 25:
                    confidence += 0.12  # deeply oversold
                elif current_rsi <= 32:
                    confidence += 0.07
                elif current_rsi <= 40:
                    confidence += 0.03
                # Trend alignment
                if uptrend:
                    confidence += 0.07
                # Volume confirmation
                confidence += (volume_conf - 0.5) * 0.08
                # Previous candle also oversold = stronger signal
                if prev_rsi <= 45:
                    confidence += 0.03
                confidence = min(0.88, confidence)

            # BUY_NO (Short for 4 hours):
            #   - Price must actually TOUCH or pierce the upper Bollinger Band
            #   - RSI must be genuinely overbought (>= 60), not just above 35
            #   - Momentum must not be spiking hard (avoid shorting strong breakouts)
            at_upper_band = current_price >= upper_val * 0.998  # within 0.2% of upper band
            rsi_overbought = current_rsi >= 60
            momentum_fading = current_momentum < 0.03  # not in breakout spike

            if not signal and at_upper_band and rsi_overbought and momentum_fading:
                signal = TradeSignal.BUY_NO
                confidence = 0.57
                # Stronger overbought → higher confidence
                if current_rsi >= 75:
                    confidence += 0.12  # deeply overbought
                elif current_rsi >= 68:
                    confidence += 0.07
                elif current_rsi >= 60:
                    confidence += 0.03
                # Trend alignment (shorting a downtrend)
                if not uptrend:
                    confidence += 0.07
                # Volume confirmation
                confidence += (volume_conf - 0.5) * 0.08
                # Previous candle also overbought = stronger signal
                if prev_rsi >= 55:
                    confidence += 0.03
                confidence = min(0.88, confidence)
            
            # --- ENTRY PRICE & RISK MANAGEMENT ---
            entry_price = current_price
            
            if signal == TradeSignal.BUY_YES:
                # Long - 4-hour hold, expect 2% move minimum
                stop_loss = entry_price * 0.985  # 1.5% stop (tighter for direction)
                take_profit = entry_price * 1.04  # 4% target (1:2.6 ratio)
            elif signal == TradeSignal.BUY_NO:
                # Short: entry_price is BTC/ETH spot price.
                # Profit when price falls; stop out if price rises.
                stop_loss   = entry_price * 1.015  # 1.5% stop (BTC rises against us)
                take_profit = entry_price * 0.96   # 4% target (BTC falls in our favour)
            else:
                stop_loss = 0.0
                take_profit = 0.0
            
            # --- REASONING STRING ---
            bb_pct = ((current_price - mid_bb.iloc[-1]) / mid_bb.iloc[-1]) * 100
            trend_label = "uptrend" if uptrend else "downtrend"
            vol_label = f"{volume_conf * 2:.1f}× avg volume"
            if signal == TradeSignal.BUY_YES:
                reasoning = (
                    f"Price {abs(bb_pct):.1f}% below lower BB + RSI={current_rsi:.0f} "
                    f"(oversold) + {trend_label} + {vol_label}"
                )
            elif signal == TradeSignal.BUY_NO:
                reasoning = (
                    f"Price {abs(bb_pct):.1f}% above upper BB + RSI={current_rsi:.0f} "
                    f"(overbought) + {trend_label} + {vol_label}"
                )
            else:
                reasoning = f"RSI={current_rsi:.0f} | BB={bb_pct:+.1f}% from mid | No extreme"
            
            logger.debug(
                f"4H Signal: {signal.value} | RSI: {current_rsi:.0f} | "
                f"BB Pos: {(current_price/mid_bb.iloc[-1])*100:.1f}% | Conf: {confidence:.0%}"
            )

            # --- OPTIONAL ML CONFIDENCE BLEND ---
            if signal != TradeSignal.NO_SIGNAL and self.ml_predictor is not None:
                try:
                    ml_prob = self.ml_predictor.predict(df_5m)  # 0.0–1.0 probability of up
                    # Map ML prob to a directional score aligned with our signal
                    if signal == TradeSignal.BUY_YES:
                        ml_score = ml_prob          # high prob-up is good for long
                    else:
                        ml_score = 1.0 - ml_prob    # low prob-up (i.e. down) is good for short
                    # Blend: 70% technicals + 30% ML
                    blended = 0.70 * confidence + 0.30 * ml_score
                    logger.debug(
                        f"ML blend: base={confidence:.3f} ml={ml_score:.3f} → {blended:.3f}"
                    )
                    confidence = min(0.95, blended)
                except Exception as ml_exc:
                    logger.debug(f"ML predictor skipped: {ml_exc}")
            
            return SignalResult(
                signal=signal,
                confidence=confidence,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                rsi=current_rsi,
                bb_pct=bb_pct,
                uptrend=uptrend,
                volume_conf=volume_conf,
                momentum=current_momentum,
                reasoning=reasoning,
            )
        
        except Exception as e:
            logger.error(f"Error in 4-hour signal generation: {e}")
            return SignalResult(
                signal=TradeSignal.NO_SIGNAL,
                confidence=0.0,
                entry_price=0.0,
                stop_loss=0.0,
                take_profit=0.0
            )
