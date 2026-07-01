"""
Regression tests for the July 2026 robustness fixes:

1. Resolution detection uses outcomes/outcomePrices (Gamma has no
   winnerOutcome field) — trades can now actually close.
2. Snapshot returns None when no live price is extractable (no more silent
   slippage-filter bypass).
3. Entry price band filter rejects near-0 / near-1 entries.
4. Tracker save is atomic; corrupt state files are backed up, not wiped.
5. Consensus-exit grace logic closes trades when tracked wallets exit.

Run: pytest tests/test_kakashi_v2_fixes.py -v
"""

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeResp:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _FakeHttp:
    """aiohttp session stub returning a fixed payload for every GET."""

    def __init__(self, payload, status=200):
        self._payload = payload
        self._status = status

    def get(self, url):
        return _FakeResp(self._status, self._payload)


def _make_strategy(tmp_path, monkeypatch):
    """Build a BasketStrategy with tracker state redirected to tmp."""
    import src.polymarket.basket.tracker as tracker_mod
    monkeypatch.setattr(tracker_mod, "STATE_FILE", tmp_path / "state.json")
    from src.polymarket.basket.strategy import BasketStrategy
    return BasketStrategy()


# ============================================================================
# 1. Resolution detection (the trades-never-close bug)
# ============================================================================

class TestResolutionDetection:
    def test_resolved_win_via_outcome_prices(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        # Gamma-realistic resolved market: JSON-string outcomes/prices, no winnerOutcome
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": True,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0", "1"]',
        }])
        resolved, price = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is True
        assert price == 1.0

    def test_resolved_loss(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": True,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["1", "0"]',
        }])
        resolved, price = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is True
        assert price == 0.0

    def test_old_winner_outcome_payload_would_have_failed(self, tmp_path, monkeypatch):
        """The OLD code looked for winnerOutcome, which Gamma never returns.
        With realistic payloads it returned (False, 0) forever. New code closes."""
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": True,
            # No winnerOutcome / resolutionOutcome — this is what Gamma sends.
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.0005", "0.9995"]',
        }])
        resolved, price = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is True and price == 1.0

    def test_not_resolved_when_prices_indecisive(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": True,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.55", "0.45"]',   # not decisive
        }])
        resolved, _ = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is False

    def test_open_market_not_resolved(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": False,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.6", "0.4"]',
        }])
        resolved, _ = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is False

    def test_exact_match_beats_substring(self, tmp_path, monkeypatch):
        """'No' must not match inside e.g. 'North wins' by substring first."""
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": True,
            "outcomes": '["North wins", "No"]',
            "outcomePrices": '["1", "0"]',
        }])
        resolved, price = asyncio.run(strat._check_market_resolved("0xabc", "No"))
        assert resolved is True
        assert price == 0.0    # exact "No" index=1 → price 0 → loss


# ============================================================================
# 2. Snapshot never fabricates a price
# ============================================================================

class TestSnapshotPriceExtraction:
    def test_returns_none_when_no_price_fields(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "question": "Test market",
            # no tokens, no outcomes/outcomePrices → no live price
        }])
        snap = asyncio.run(
            strat._fetch_market_snapshot("0xabc", "No", 0.40, market_title="Test market")
        )
        assert snap is None

    def test_extracts_price_from_outcome_prices(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "question": "Test market",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.62", "0.38"]',
            "liquidity": "50000",
        }])
        snap = asyncio.run(
            strat._fetch_market_snapshot("0xabc", "No", 0.40, market_title="Test market")
        )
        assert snap is not None
        assert abs(snap.current_price - 0.38) < 1e-9

    def test_cache_respects_ttl(self, tmp_path, monkeypatch):
        from src.polymarket.basket.market_filter import MarketSnapshot
        strat = _make_strategy(tmp_path, monkeypatch)
        stale = MarketSnapshot(
            market_id="0xabc", market_title="t", outcome="No",
            current_price=0.5, volume_24h=0, open_interest=0,
            fetched_at=time.time() - 3600,   # 1h old
        )
        strat._market_cache["0xabc"] = stale
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "question": "t",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.3", "0.7"]',
        }])
        snap = asyncio.run(
            strat._fetch_market_snapshot("0xabc", "No", 0.5, market_title="t")
        )
        # Stale cache must have been bypassed and refreshed
        assert abs(snap.current_price - 0.7) < 1e-9


# ============================================================================
# 3. Price band filter
# ============================================================================

class TestPriceBandFilter:
    def _snap(self, price):
        from src.polymarket.basket.market_filter import MarketSnapshot
        return MarketSnapshot(
            market_id="x", market_title="t", outcome="Yes",
            current_price=price, volume_24h=0, open_interest=100_000,
            fetched_at=time.time(),
        )

    def test_rejects_near_one(self):
        from src.polymarket.basket.market_filter import run_filters
        result = run_filters(self._snap(0.97), consensus_price=0.97)
        assert not result.passed
        assert "band" in result.reason

    def test_rejects_near_zero(self):
        from src.polymarket.basket.market_filter import run_filters
        result = run_filters(self._snap(0.02), consensus_price=0.02)
        assert not result.passed

    def test_accepts_mid_band(self):
        from src.polymarket.basket.market_filter import run_filters
        result = run_filters(self._snap(0.55), consensus_price=0.55)
        assert result.passed


# ============================================================================
# 4. Tracker persistence hardening
# ============================================================================

class TestTrackerPersistence:
    def test_atomic_save_no_tmp_leftover(self, tmp_path, monkeypatch):
        import src.polymarket.basket.tracker as tracker_mod
        monkeypatch.setattr(tracker_mod, "STATE_FILE", tmp_path / "state.json")
        t = tracker_mod.BasketTracker()
        t.open_trade(
            basket="sports", market_id="0x1", market_title="m",
            outcome="No", entry_price=0.4, paper_size_usd=50.0,
        )
        assert (tmp_path / "state.json").exists()
        assert not (tmp_path / "state.json.tmp").exists()
        # File is valid JSON
        json.loads((tmp_path / "state.json").read_text())

    def test_corrupt_state_backed_up_not_wiped(self, tmp_path, monkeypatch):
        import src.polymarket.basket.tracker as tracker_mod
        state = tmp_path / "state.json"
        monkeypatch.setattr(tracker_mod, "STATE_FILE", state)
        state.write_text("{ this is not valid json !!!")
        tracker_mod.BasketTracker()
        backups = list(tmp_path.glob("*.corrupt.*"))
        assert len(backups) == 1
        assert "not valid json" in backups[0].read_text()


# ============================================================================
# 5. Consensus-exit close
# ============================================================================

class TestConsensusExit:
    def test_trade_closes_after_grace_when_consensus_gone(self, tmp_path, monkeypatch):
        import src.polymarket.basket.strategy as strat_mod
        strat = _make_strategy(tmp_path, monkeypatch)
        monkeypatch.setattr(strat_mod, "CONSENSUS_EXIT_GRACE_SECS", 0)

        trade_id = strat._tracker.open_trade(
            basket="sports", market_id="0xabc", market_title="Test market",
            outcome="No", entry_price=0.40, paper_size_usd=50.0,
        )

        # Market open (not resolved) but consensus key is absent this tick
        strat._live_consensus_keys = set()
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "question": "Test market",
            "closed": False,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.50", "0.50"]',
            "liquidity": "50000",
        }])

        with patch("src.tracking.trade_logger.log_trade"):
            asyncio.run(strat._check_open_positions())

        assert trade_id not in strat._tracker._state.open_trades
        closed = strat._tracker._state.closed_trades[-1]
        assert closed.close_reason == "consensus_exit"
        assert abs(closed.exit_price - 0.50) < 1e-9

    def test_trade_held_while_consensus_alive(self, tmp_path, monkeypatch):
        strat = _make_strategy(tmp_path, monkeypatch)
        trade_id = strat._tracker.open_trade(
            basket="sports", market_id="0xabc", market_title="Test market",
            outcome="No", entry_price=0.40, paper_size_usd=50.0,
        )
        strat._live_consensus_keys = {("sports", "0xabc", "No")}
        strat._http = _FakeHttp([{
            "conditionId": "0xabc",
            "closed": False,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.50", "0.50"]',
        }])
        asyncio.run(strat._check_open_positions())
        assert trade_id in strat._tracker._state.open_trades
