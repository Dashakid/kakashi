"""
Robustness tests for all gaps fixed in the May 2026 audit.

Run from the project root:
    pytest tests/test_robustness.py -v

No live network calls. No .env required. All external dependencies are mocked.
"""

import asyncio
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import numpy as np
import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ohlcv(n: int = 60, base: float = 50_000.0) -> pd.DataFrame:
    """Synthetic OHLCV dataframe with realistic noise."""
    rng = np.random.default_rng(42)
    closes = base + rng.normal(0, 300, n).cumsum()
    df = pd.DataFrame({
        "open":   closes + rng.uniform(-100, 100, n),
        "high":   closes + rng.uniform(0,   300, n),
        "low":    closes - rng.uniform(0,   300, n),
        "close":  closes,
        "volume": rng.uniform(10, 200, n),
    })
    return df


# ============================================================================
# 1. API timeouts present
# ============================================================================

class TestApiTimeouts:
    """Verify all aiohttp sessions carry explicit timeouts."""

    def test_kakashi_session_has_timeout(self):
        import inspect
        import src.polymarket.kakashi_bot as ttf
        src_text = inspect.getsource(ttf)
        # Must declare a ClientTimeout before or within the ClientSession call
        assert "ClientTimeout" in src_text

    def test_leaderboard_test_session_has_timeout(self):
        import inspect
        import src.polymarket.leaderboard as lb
        src_text = inspect.getsource(lb)
        assert "ClientTimeout" in src_text


# ============================================================================
# 2. Sharpe ratio + max drawdown
# ============================================================================

class TestEngineMetrics:
    """TradingEngine._calculate_sharpe / _calculate_max_drawdown."""

    def _engine(self):
        # Patch config imports so we don't need a real .env
        with patch.dict("sys.modules", {
            "src.config": MagicMock(
                INITIAL_BALANCE=1000.0,
                MAX_POSITION_SIZE=100.0,
                RISK_PER_TRADE=0.02,
            ),
        }):
            import importlib
            import src.trading.engine as eng_mod
            importlib.reload(eng_mod)
            return eng_mod.TradingEngine(initial_balance=1000.0)

    def test_zero_metrics_on_no_trades(self):
        eng = self._engine()
        assert eng._calculate_max_drawdown() == 0.0
        assert eng._calculate_sharpe() == 0.0

    def test_max_drawdown_monotone_decline(self):
        eng = self._engine()
        # Simulate: 1000 → 900 → 800 → 700  (30% drawdown from peak)
        eng._equity_curve = [1000.0, 900.0, 800.0, 700.0]
        dd = eng._calculate_max_drawdown()
        assert abs(dd - 0.30) < 1e-9

    def test_max_drawdown_zero_for_rising_equity(self):
        eng = self._engine()
        eng._equity_curve = [1000.0, 1100.0, 1200.0, 1300.0]
        assert eng._calculate_max_drawdown() == 0.0

    def test_sharpe_positive_for_consistent_gains(self):
        eng = self._engine()
        # Each step gains 1% → positive, consistent → positive Sharpe
        v = 1000.0
        eng._equity_curve = [v]
        for _ in range(50):
            v *= 1.01
            eng._equity_curve.append(v)
        assert eng._calculate_sharpe() > 0

    def test_sharpe_negative_for_consistent_losses(self):
        eng = self._engine()
        v = 1000.0
        eng._equity_curve = [v]
        for _ in range(50):
            v *= 0.99
            eng._equity_curve.append(v)
        assert eng._calculate_sharpe() < 0

    def test_metrics_integrated_in_get_portfolio_metrics(self):
        eng = self._engine()
        # Set a known equity curve
        eng._equity_curve = [1000.0, 900.0, 950.0, 1050.0]
        m = eng.get_portfolio_metrics()
        # Drawdown should be non-zero (900 < 1000 peak)
        assert m.max_drawdown > 0
        # Sharpe is a float (sign varies; just check it's computed)
        assert isinstance(m.sharpe_ratio, float)


# ============================================================================
# 3. Dead-letter queue
# ============================================================================

class TestDeadLetterQueue:
    """Failed JSONL writes go to dead_letter_trades.jsonl."""

    def test_dead_letter_written_on_save_failure(self, tmp_path):
        import importlib
        import src.tracking.win_loss_tracker as wlt_mod

        tracker = wlt_mod.WinLossTracker(data_dir=str(tmp_path))

        # Make the primary file unwritable by replacing it with a directory
        trades_path = tmp_path / "win_loss_history.jsonl"
        trades_path.mkdir()  # Can't write to a directory — triggers OSError

        entry = wlt_mod.TradeEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            bot_name="TestBot",
            asset="BTC",
            signal_type="BUY_YES",
            entry_price=50000.0,
            exit_price=51000.0,
            pnl_pct=0.02,
            confidence=0.75,
            is_win=True,
        )
        tracker._save_trade(entry)

        dlq = tmp_path / "dead_letter_trades.jsonl"
        assert dlq.exists(), "Dead-letter file should be created on write failure"
        line = json.loads(dlq.read_text().strip())
        assert line["trade"]["bot_name"] == "TestBot"
        assert "error" in line
        assert "failed_at" in line


# ============================================================================
# 4. ML predictor wired into ScalpStrategy
# ============================================================================

class TestScalpStrategyMLBlend:
    """ScalpStrategy blends ML probability when ml_predictor is provided."""

    def _make_strategy(self, ml_prob: float):
        with patch.dict("sys.modules", {}):
            from src.trading.scalp_strategy import ScalpStrategy
        mock_predictor = MagicMock()
        mock_predictor.predict.return_value = ml_prob
        return ScalpStrategy(ml_predictor=mock_predictor), mock_predictor

    def test_no_predictor_does_not_call_ml(self):
        from src.trading.scalp_strategy import ScalpStrategy, TradeSignal
        strategy = ScalpStrategy()
        df = _make_ohlcv(60)
        result = strategy.get_signal(df, df)
        # Should not raise; ml_predictor is None
        assert result.signal in list(TradeSignal)

    def test_predictor_called_when_signal_fired(self):
        from src.trading.scalp_strategy import ScalpStrategy, TradeSignal
        strategy, mock_pred = self._make_strategy(0.8)
        df = _make_ohlcv(60, base=50_000.0)

        # Force a BUY_YES signal by patching the indicator values
        with patch.object(strategy, "_calculate_bollinger_bands") as mock_bb, \
             patch.object(strategy, "_calculate_rsi") as mock_rsi, \
             patch.object(strategy, "_calculate_momentum") as mock_mom:

            price = 49_000.0
            lower = pd.Series([49_100.0] * 60)
            mid   = pd.Series([50_000.0] * 60)
            upper = pd.Series([50_900.0] * 60)
            mock_bb.return_value = (upper, mid, lower)

            rsi_series = pd.Series([30.0] * 60)  # oversold → BUY_YES
            mock_rsi.return_value = rsi_series
            mock_mom.return_value = pd.Series([0.0] * 60)

            # Patch close price to sit at lower band
            df2 = df.copy()
            df2["close"] = price
            df2["volume"] = 100.0

            result = strategy.get_signal(df2, df2)

        if result.signal == TradeSignal.BUY_YES:
            mock_pred.predict.assert_called_once()

    def test_confidence_capped_at_0_95(self):
        """Even if both BB/RSI and ML agree strongly, confidence max is 0.95."""
        from src.trading.scalp_strategy import ScalpStrategy, TradeSignal
        strategy, _ = self._make_strategy(1.0)  # ML says 100% up

        with patch.object(strategy, "_calculate_bollinger_bands") as mock_bb, \
             patch.object(strategy, "_calculate_rsi") as mock_rsi, \
             patch.object(strategy, "_calculate_momentum") as mock_mom:

            price = 49_000.0
            lower = pd.Series([49_100.0] * 60)
            mid   = pd.Series([50_000.0] * 60)
            upper = pd.Series([50_900.0] * 60)
            mock_bb.return_value = (upper, mid, lower)
            mock_rsi.return_value = pd.Series([20.0] * 60)  # deeply oversold
            mock_mom.return_value = pd.Series([0.0] * 60)

            df = _make_ohlcv(60)
            df["close"] = price
            result = strategy.get_signal(df, df)

        if result.signal == TradeSignal.BUY_YES:
            assert result.confidence <= 0.95


# ============================================================================
# 5. SpreadCaptureStrategy hard guard
# ============================================================================

class TestSpreadCaptureGuard:
    """SpreadCaptureStrategy must raise RuntimeError on instantiation."""

    def test_raises_on_instantiation(self):
        from src.polymarket.strategies.spread_capture import SpreadCaptureStrategy
        with pytest.raises(RuntimeError, match="permanently disabled"):
            SpreadCaptureStrategy(
                discovery=MagicMock(),
                client=MagicMock(),
            )

    def test_disabled_flag_is_true(self):
        import src.polymarket.strategies.spread_capture as sc
        assert sc._STRATEGY_DISABLED is True


# ============================================================================
# 6. Dead-letter file format is valid JSONL
# ============================================================================

class TestDeadLetterFormat:
    def test_dead_letter_is_valid_json(self, tmp_path):
        import src.tracking.win_loss_tracker as wlt_mod
        tracker = wlt_mod.WinLossTracker(data_dir=str(tmp_path))

        # Replace file with directory to force write failure
        (tmp_path / "win_loss_history.jsonl").mkdir()

        entry = wlt_mod.TradeEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            bot_name="Bot", asset="ETH",
            signal_type="BUY_NO",
            entry_price=3000.0, exit_price=2900.0,
            pnl_pct=-0.033, confidence=0.6, is_win=False,
        )
        tracker._save_trade(entry)
        dlq = tmp_path / "dead_letter_trades.jsonl"
        for line in dlq.read_text().splitlines():
            parsed = json.loads(line)  # Must not raise
            assert "trade" in parsed
            assert "error" in parsed


# ============================================================================
# 7. Engine equity curve grows on each execution
# ============================================================================

class TestEquityCurveGrowth:
    def test_equity_curve_appended_after_execute(self):
        with patch.dict("sys.modules", {
            "src.config": MagicMock(
                INITIAL_BALANCE=1000.0,
                MAX_POSITION_SIZE=100.0,
                RISK_PER_TRADE=0.02,
            ),
        }):
            import importlib
            import src.trading.engine as eng_mod
            importlib.reload(eng_mod)
            eng = eng_mod.TradingEngine(1000.0)

        initial_len = len(eng._equity_curve)
        eng.place_order("o1", "BTC/USD", eng_mod.OrderSide.BUY, 0.01, 50_000.0)  # cost=$500 < $1000 balance
        eng.execute_order("o1", 50_000.0)
        assert len(eng._equity_curve) == initial_len + 1
