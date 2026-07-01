"""
Weather Arbitrage Strategy for Polymarket.

Exploits divergence between objective weather forecasts (NOAA/ECMWF)
and retail-driven Polymarket prices.

Example Edge:
- NOAA says tomorrow's high in NYC: 76°F (high confidence)
- Polymarket "75-79°F HIGH" trading at 0.25 (retail underpriced)
- Polymarket "70-74°F HIGH" trading at 0.72 (retail overpriced)
- BUY the 75-79 contract, SELL the 70-74 contract
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List
from loguru import logger
import pandas as pd


@dataclass
class WeatherForecast:
    """Weather forecast data."""
    location: str
    forecast_date: str
    high_temp: float
    low_temp: float
    confidence: float
    data_source: str  # NOAA, ECMWF, etc.
    retrieved_at: str


@dataclass
class MarketPrice:
    """Polymarket price data for weather range."""
    location: str
    range_label: str  # e.g., "70-74", "75-79"
    low: float
    high: float
    mid_price: float  # Current market price (YES share)
    volume_24h: float


@dataclass
class WeatherSignal:
    """Weather arbitrage signal."""
    signal_type: str  # BUY, SELL
    location: str
    forecast_temp: float
    market_range: str
    market_price: float
    conviction: float  # 0-1
    reasoning: str
    action: str  # e.g., "BUY 75-79 / SELL 70-74"


class WeatherArbitrageStrategy:
    """
    Detect and exploit weather prediction arbitrage.
    
    Method:
    1. Pull NOAA/ECMWF forecasts for major cities (NYC, London, Chicago, etc.)
    2. Parse Polymarket HIGH/LOW temperature range markets
    3. Detect divergence when forecast is well outside market probability
    4. Generate trading signals when edge > 3% (or configurable threshold)
    """
    
    def __init__(self):
        """Initialize weather strategy."""
        self.locations = {
            'NYC': {'lat': 40.7128, 'lon': -74.0060, 'polymarket_id': 'nyc-high-temp'},
            'London': {'lat': 51.5074, 'lon': -0.1278, 'polymarket_id': 'london-high-temp'},
            'Chicago': {'lat': 41.8781, 'lon': -87.6298, 'polymarket_id': 'chicago-high-temp'},
            'Tokyo': {'lat': 35.6762, 'lon': 139.6503, 'polymarket_id': 'tokyo-high-temp'},
            'Sydney': {'lat': -33.8688, 'lon': 151.2093, 'polymarket_id': 'sydney-high-temp'},
        }
        
        self.open_meteo_base = "https://api.open-meteo.com/v1/forecast"
        self.min_conviction = 0.60  # Minimum confidence to trade
        self.min_edge_pct = 0.03   # Minimum edge (3%)
    
    async def fetch_forecast(self, location: str) -> Optional[WeatherForecast]:
        """
        Fetch tomorrow's weather forecast from Open-Meteo (free NOAA proxy).
        
        Args:
            location: City name (NYC, London, etc.)
        
        Returns:
            WeatherForecast or None if error
        """
        try:
            if location not in self.locations:
                logger.warning(f"Location {location} not configured")
                return None
            
            loc_data = self.locations[location]
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            
            params = {
                'latitude': loc_data['lat'],
                'longitude': loc_data['lon'],
                'start_date': tomorrow,
                'end_date': tomorrow,
                'daily': 'temperature_2m_max,temperature_2m_min',
                'temperature_unit': 'fahrenheit',
                'timezone': 'auto',
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.open_meteo_base, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.warning(f"Open-Meteo error for {location}: {resp.status}")
                        return None
                    
                    data = await resp.json()
                    daily = data.get('daily', {})
                    
                    if not daily or not daily.get('temperature_2m_max'):
                        logger.warning(f"No forecast data for {location}")
                        return None
                    
                    high_temp = daily['temperature_2m_max'][0]
                    low_temp = daily['temperature_2m_min'][0]
                    
                    # Confidence based on forecast freshness
                    # (In real use, you'd check NOAA model agreement levels)
                    confidence = 0.75  # Conservative estimate
                    
                    logger.info(f"✅ {location} forecast: High {high_temp:.1f}°F, Low {low_temp:.1f}°F")
                    
                    return WeatherForecast(
                        location=location,
                        forecast_date=tomorrow,
                        high_temp=high_temp,
                        low_temp=low_temp,
                        confidence=confidence,
                        data_source="Open-Meteo (NOAA)",
                        retrieved_at=datetime.now().isoformat()
                    )
        
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Timeout fetching forecast for {location}")
            return None
        except Exception as e:
            logger.error(f"Error fetching forecast for {location}: {type(e).__name__}: {e}")
            return None
    
    def parse_market_ranges(self, location: str) -> List[MarketPrice]:
        """
        Parse typical Polymarket temperature ranges.
        Returns standard ranges (e.g., 70-74, 75-79, etc.)
        """
        ranges = []
        
        # Standard 5-degree ranges for most markets
        for low in range(50, 100, 5):
            high = low + 4
            ranges.append(MarketPrice(
                location=location,
                range_label=f"{low}-{high}",
                low=low,
                high=high,
                mid_price=0.0,  # Will be filled in from actual market data
                volume_24h=0.0
            ))
        
        return ranges
    
    def detect_divergence(
        self,
        forecast: WeatherForecast,
        market_prices: Dict[str, MarketPrice]
    ) -> Optional[WeatherSignal]:
        """
        Detect when forecast significantly diverges from market pricing.
        
        Example:
        - Forecast: High 76°F (high confidence 75%)
        - Market: 75-79°F contract trading at 0.30
        - Theoretical prob: 75-79 should be ~0.75 (if forecast correct)
        - Divergence: 0.75 - 0.30 = 0.45 (huge! 45 cents edge)
        - Signal: BUY 75-79 range contract
        
        Args:
            forecast: WeatherForecast with predicted high/low
            market_prices: Dict of range_label -> MarketPrice
        
        Returns:
            WeatherSignal if divergence found, None otherwise
        """
        try:
            high_temp = forecast.high_temp
            forecast_confidence = forecast.confidence
            
            best_signal = None
            best_edge = 0
            
            for range_label, market_price in market_prices.items():
                # Does the forecast fall in this range?
                forecast_in_range = market_price.low <= high_temp < market_price.high
                
                if forecast_in_range:
                    # What probability should this range have?
                    theoretical_prob = forecast_confidence
                    market_prob = market_price.mid_price
                    
                    edge = abs(theoretical_prob - market_prob)
                    
                    if edge > best_edge:
                        best_edge = edge
                        
                        if theoretical_prob > market_prob:
                            signal_type = "BUY"
                        else:
                            signal_type = "SELL"
                        
                        best_signal = WeatherSignal(
                            signal_type=signal_type,
                            location=forecast.location,
                            forecast_temp=high_temp,
                            market_range=range_label,
                            market_price=market_price.mid_price,
                            conviction=min(1.0, edge),
                            reasoning=f"Forecast: {high_temp:.1f}°F ({forecast_confidence:.0%} conf) | Market: {range_label}°F @ {market_price.mid_price:.2f}",
                            action=f"{signal_type} {range_label} @ {market_price.mid_price:.2f}"
                        )
            
            if best_signal and best_edge >= self.min_edge_pct:
                logger.warning(f"⚡ WEATHER SIGNAL: {best_signal.location}")
                logger.warning(f"   Direction: {best_signal.signal_type}")
                logger.warning(f"   Range: {best_signal.market_range}°F")
                logger.warning(f"   Forecast: {best_signal.forecast_temp:.1f}°F")
                logger.warning(f"   Market Price: {best_signal.market_price:.2f}")
                logger.warning(f"   Edge: {best_edge*100:.1f}%")
                return best_signal
            
            return None
        
        except Exception as e:
            logger.error(f"Error detecting divergence: {type(e).__name__}: {e}")
            return None
    
    async def run_analysis_cycle(self) -> List[WeatherSignal]:
        """
        Run one complete analysis cycle:
        1. Fetch forecasts for all locations
        2. Get market prices (mocked for now)
        3. Detect divergences
        4. Return signals
        """
        signals = []
        
        try:
            logger.info("🌡️ Starting weather analysis cycle...")
            
            # Fetch forecasts for all locations
            forecasts = []
            for location in self.locations.keys():
                forecast = await self.fetch_forecast(location)
                if forecast:
                    forecasts.append(forecast)
                await asyncio.sleep(0.5)  # Rate limit
            
            logger.info(f"✅ Fetched {len(forecasts)} forecasts")
            
            # For each forecast, check for divergence
            for forecast in forecasts:
                # In production: Fetch real Polymarket prices from CLOB API
                # For now: Mock market prices
                market_prices = self._mock_market_prices(forecast)
                
                signal = self.detect_divergence(forecast, market_prices)
                if signal:
                    signals.append(signal)
            
            logger.info(f"📊 Found {len(signals)} arbitrage signals")
            return signals
        
        except Exception as e:
            logger.error(f"Error in analysis cycle: {type(e).__name__}: {e}")
            return []
    
    def _mock_market_prices(self, forecast: WeatherForecast) -> Dict[str, MarketPrice]:
        """
        Mock Polymarket prices for testing.
        Realistic prices: ranges near forecast are ~0.70-0.80, far away are ~0.10-0.20
        """
        high_temp = forecast.high_temp
        prices = {}
        
        for low in range(50, 100, 5):
            high = low + 4
            range_label = f"{low}-{high}"
            
            # Calculate realistic market price based on distance from forecast
            distance_to_range = 0
            if high_temp < low:
                distance_to_range = low - high_temp
            elif high_temp >= high:
                distance_to_range = high_temp - high
            
            # Realistic pricing:
            # - Distance 0 (forecast in range): ~0.75 (retailer think it's 75% likely)
            # - Distance 1-2°F: ~0.55 (less likely but possible)
            # - Distance 3-5°F: ~0.30 (unlikely)
            # - Distance 5+°F: ~0.15 (very unlikely)
            if distance_to_range == 0:
                market_prob = 0.72  # Near forecast, slight retail bias (should be 0.75)
            elif distance_to_range <= 2:
                market_prob = 0.48  # 1-2° away, retail underprices
            elif distance_to_range <= 5:
                market_prob = 0.28  # Further away
            else:
                market_prob = 0.12  # Very far
            
            # Add small random variation (±0.02) to appear realistic
            import random
            market_prob += random.uniform(-0.02, 0.02)
            market_prob = max(0.05, min(0.95, market_prob))  # Clamp 0.05-0.95
            
            prices[range_label] = MarketPrice(
                location=forecast.location,
                range_label=range_label,
                low=low,
                high=high,
                mid_price=market_prob,
                volume_24h=random.randint(5000, 50000)  # Realistic volume
            )
        
        return prices
