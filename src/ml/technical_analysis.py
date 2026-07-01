"""Technical analysis and indicators for trading strategies."""

import numpy as np
import pandas as pd
from typing import Tuple, List, Optional
from dataclasses import dataclass


@dataclass
class FVG:
    """Fair Value Gap."""
    timestamp: float
    high: float
    low: float
    type: str  # 'bullish' or 'bearish'
    filled: bool = False


@dataclass
class TechnicalIndicators:
    """Container for technical indicators."""
    sma_20: float
    ema_20: float
    ema_9: float
    rsi_14: float
    macd: float
    macd_signal: float
    macd_histogram: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    atr: float
    volume: float


class TechnicalAnalysis:
    """Technical analysis tools for Percoco method."""

    @staticmethod
    def calculate_fvg(df: pd.DataFrame, window: int = 3) -> List[FVG]:
        """
        Detect Fair Value Gaps (FVG).
        
        FVG is a gap between candles that price hasn't yet filled.
        - Bullish FVG: When candle 1 high < candle 2 low (price jumped up)
        - Bearish FVG: When candle 1 low > candle 2 high (price jumped down)
        """
        fvgs = []
        
        for i in range(1, len(df) - 1):
            prev_high = df.iloc[i-1]['high']
            prev_low = df.iloc[i-1]['low']
            curr_high = df.iloc[i]['high']
            curr_low = df.iloc[i]['low']
            next_high = df.iloc[i+1]['high']
            next_low = df.iloc[i+1]['low']
            
            # Bullish FVG: gap up
            if prev_high < curr_low:
                gap_high = curr_low
                gap_low = prev_high
                # Check if filled in next candles
                filled = any(df.iloc[i:i+window]['low'] <= gap_low)
                fvgs.append(FVG(
                    timestamp=df.index[i].timestamp() if hasattr(df.index[i], 'timestamp') else float(i),
                    high=gap_high,
                    low=gap_low,
                    type='bullish',
                    filled=filled,
                ))
            
            # Bearish FVG: gap down
            if prev_low > curr_high:
                gap_high = prev_low
                gap_low = curr_high
                # Check if filled in next candles
                filled = any(df.iloc[i:i+window]['high'] >= gap_high)
                fvgs.append(FVG(
                    timestamp=df.index[i].timestamp() if hasattr(df.index[i], 'timestamp') else float(i),
                    high=gap_high,
                    low=gap_low,
                    type='bearish',
                    filled=filled,
                ))
        
        return fvgs

    @staticmethod
    def calculate_ema(series: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average."""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_sma(series: pd.Series, period: int) -> pd.Series:
        """Calculate Simple Moving Average."""
        return series.rolling(window=period).mean()

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def calculate_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal, adjust=False).mean()
        macd_histogram = macd - macd_signal
        return macd, macd_signal, macd_histogram

    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int = 20, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands."""
        sma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = sma + (std * num_std)
        lower = sma - (std * num_std)
        return upper, sma, lower

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        df = df.copy()
        df['tr1'] = df['high'] - df['low']
        df['tr2'] = abs(df['high'] - df['close'].shift())
        df['tr3'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        atr = df['tr'].rolling(window=period).mean()
        return atr

    @staticmethod
    def get_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators for a dataframe."""
        df = df.copy()
        
        # Moving averages
        df['sma_20'] = TechnicalAnalysis.calculate_sma(df['close'], 20)
        df['ema_20'] = TechnicalAnalysis.calculate_ema(df['close'], 20)
        df['ema_9'] = TechnicalAnalysis.calculate_ema(df['close'], 9)
        
        # Oscillators
        df['rsi_14'] = TechnicalAnalysis.calculate_rsi(df['close'], 14)
        
        # MACD
        df['macd'], df['macd_signal'], df['macd_histogram'] = TechnicalAnalysis.calculate_macd(df['close'])
        
        # Bollinger Bands
        df['bb_upper'], df['bb_middle'], df['bb_lower'] = TechnicalAnalysis.calculate_bollinger_bands(df['close'])
        
        # ATR
        df['atr'] = TechnicalAnalysis.calculate_atr(df)
        
        return df

    @staticmethod
    def get_latest_indicators(df: pd.DataFrame) -> TechnicalIndicators:
        """Get latest technical indicator values."""
        df = TechnicalAnalysis.get_indicators(df)
        latest = df.iloc[-1]
        
        return TechnicalIndicators(
            sma_20=float(latest.get('sma_20', 0)),
            ema_20=float(latest.get('ema_20', 0)),
            ema_9=float(latest.get('ema_9', 0)),
            rsi_14=float(latest.get('rsi_14', 0)),
            macd=float(latest.get('macd', 0)),
            macd_signal=float(latest.get('macd_signal', 0)),
            macd_histogram=float(latest.get('macd_histogram', 0)),
            bb_upper=float(latest.get('bb_upper', 0)),
            bb_middle=float(latest.get('bb_middle', 0)),
            bb_lower=float(latest.get('bb_lower', 0)),
            atr=float(latest.get('atr', 0)),
            volume=float(latest.get('volume', 0)),
        )

    @staticmethod
    def is_price_above_ema(price: float, ema_20: float) -> bool:
        """Check if price is above 20 EMA (expansion condition)."""
        return price > ema_20

    @staticmethod
    def is_price_hugging_ema(price: float, ema_20: float, atr: float) -> bool:
        """Check if price is 'hugging' the EMA (too close, avoid trading)."""
        distance = abs(price - ema_20)
        return distance < (atr * 0.25)  # Within 25% of ATR
