"""
ETH Market Maker Bot for Polymarket.

Replaces the failing direction-trading ETH bot with a market making approach.
Instead of predicting UP/DOWN, we provide liquidity on both sides and capture spreads.
"""

import asyncio
import random
from datetime import datetime
from typing import List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from src.trading.market_maker_strategy import MarketMakerStrategy, MarketMakerQuote
from src.polymarket.market_discovery import MarketDiscovery
from src.polymarket.client import PolymarketClient
from src.data.kraken_client import KrakenClient
from src.polymarket.market_discovery import MarketDiscoveryfrom src.notifications.discord_webhook import get_discord_client
from src.notifications.alerts import BotAlert
from src.polymarket.strategies.kraken_poly_arb import (
    fair_probability,
    _extract_strike,
    _days_to_expiry,
    FALLBACK_VOL,
)
from src.polymarket import live_rules


class ETHMarketMakerRunner:
    """
    Runs the ETH market maker strategy on Polymarket.
    
    - Continuously quotes both sides of ETH direction markets
    - Captures spreads on each trade (2-5 cents per contract)
    - Auto-closes positions for quick wins
    - Records trades to unified tracker
    """
    
    def __init__(
        self,
        discovery: MarketDiscovery,
        client: PolymarketClient,
        bot_name: str = "ETH Market Maker",
        trade_size_pct: float = None,  # e.g. 0.01 = 1% of balance per trade; None = fixed $10
    ):
        """Initialize ETH market maker runner."""
        self.discovery = discovery
        self.client = client
        self.bot_name = bot_name
        self.trade_size_pct = trade_size_pct
        self.strategy = MarketMakerStrategy(
            assets=['ETH'],
            base_spread_usd=0.04,  # $0.04 spread target (was 0.02 — too tight to clear fees)
            max_inventory_per_side=500.0,  # Max 500 contracts per side
            min_market_spread=0.08,  # Only quote if market spread >8 cents (was 5 — must clear 2c fees)
        )
        self.scheduler = AsyncIOScheduler()
        self.eth_markets: List = []
        self._session_wins = 0
        self._session_losses = 0
        self._session_pnl = 0.0
        self._current_streak = 0
        # Capital tracking
        self.STARTING_CAPITAL = 500.0   # $500 paper account
        self.TRADE_SIZE_USD = 10.0      # $10 per trade fixed (used when trade_size_pct is None)
        self._balance = self.STARTING_CAPITAL
        self._session_dollar_pnl = 0.0
        # Simulated price walk state: tracks current market price per open trade
        self._position_prices: dict = {}   # trade_id -> current simulated price
        self._position_steps: dict = {}    # trade_id -> # of 20-second ticks held
        self._position_features: dict = {} # trade_id -> entry-time market features
        # Real Kraken ETH momentum (-1 to +1), updated each market refresh
        self._kraken_momentum: float = 0.0
        # ETH spot price and realised vol — used by the BS directional filter
        self._eth_spot: float = 0.0
        self._eth_vol: float = FALLBACK_VOL
        
    def start(self):
        """Start the market maker scheduler."""
        logger.info("🚀 Starting ETH Market Maker strategy...")
        
        # Refresh market discovery every 5 minutes
        self.scheduler.add_job(
            self.refresh_markets,
            'interval',
            minutes=5,
            id='mm_refresh_markets',
            name='Refresh ETH Markets',
            max_instances=1,
        )
        
        # Generate and update quotes every 10 seconds
        self.scheduler.add_job(
            self.quote_loop,
            'interval',
            seconds=10,
            id='mm_quote_loop',
            name='Market Maker Quote Loop',
            max_instances=1,
        )
        
        # Close profitable positions every 20 seconds
        self.scheduler.add_job(
            self.close_profitable_positions,
            'interval',
            seconds=20,
            id='mm_close_positions',
            name='Close Profitable Positions',
            max_instances=1,
        )
        
        self.scheduler.start()
        logger.info("✅ ETH Market Maker scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("ETH Market Maker scheduler stopped")
    
    async def refresh_markets(self):
        """Refresh list of ETH markets from Polymarket."""
        try:
            logger.debug("🔄 Refreshing ETH markets...")
            # Always force-refresh to get real live Polymarket prices each 5-minute cycle
            all_markets = await self.discovery.discover_markets(
                force_refresh=True
            )
            
            # Filter for ANY active ETH markets (direction OR price_target)
            # ETH weekly direction markets expire — fall back to price target markets
            self.eth_markets = [
                m for m in all_markets.values()
                if m.asset == 'ETH' and m.is_active()
            ]

            if not self.eth_markets:
                # Broaden to any crypto direction market as fallback
                self.eth_markets = [
                    m for m in all_markets.values()
                    if m.market_type == 'direction' and m.is_active()
                ]
                if self.eth_markets:
                    logger.info(f"📍 No ETH markets — using {len(self.eth_markets)} other direction markets as fallback")
            
            logger.info(f"📍 Found {len(self.eth_markets)} active ETH direction markets")

            # Update ETH momentum from Kraken (15-min candles, last ~1h)
            try:
                async with KrakenClient() as kraken:
                    df_eth = await asyncio.wait_for(
                        kraken.get_dataframe(pair="XETHZUSD", interval=15, lookback_hours=4),
                        timeout=8.0
                    )
                if df_eth is not None and len(df_eth) >= 6:
                    raw_mom = (df_eth['close'].iloc[-1] - df_eth['close'].iloc[-6]) / df_eth['close'].iloc[-6]
                    self._kraken_momentum = max(-1.0, min(1.0, raw_mom / 0.02))
                    # Store real ETH spot and 30d realised vol for BS directional filter
                    self._eth_spot = float(df_eth['close'].iloc[-1])
                    if len(df_eth) >= 20:
                        import numpy as np
                        log_returns = np.log(df_eth['close'] / df_eth['close'].shift(1)).dropna()
                        # 15-min candles → annualise: sqrt(365 * 24 * 4)
                        self._eth_vol = float(log_returns.std() * (365 * 24 * 4) ** 0.5)
                    else:
                        self._eth_vol = FALLBACK_VOL
                    logger.debug(
                        f"ETH 15m momentum: {self._kraken_momentum:+.2f} | "
                        f"spot=${self._eth_spot:,.0f} | vol={self._eth_vol:.0%}"
                    )
            except Exception:
                pass  # Keep prior momentum if Kraken fetch fails

        except Exception as e:
            logger.warning(f"Error refreshing markets: {e}")
    
    async def quote_loop(self):
        """Main market maker loop: generate and manage quotes."""
        try:
            if not self.eth_markets:
                logger.debug("No ETH markets available")
                return

            # Position limit: avoid piling up correlated positions on one market
            MAX_OPEN_POSITIONS = 5
            if len(self.strategy.open_trades) >= MAX_OPEN_POSITIONS:
                logger.debug(f"Position cap reached ({MAX_OPEN_POSITIONS} open), skipping quotes")
                return
            
            # Only quote markets with REAL liquidity and a mid-price in a
            # tradeable range. Synthetic fills on markets with no orderbook
            # (best_bid=0) are not real — a live order would never fill.
            # Price range 0.20-0.80 avoids near-zero/near-certain contracts
            # where the spread is pure noise and gas costs dominate.
            PRICE_RANGE_MIN = 0.15
            PRICE_RANGE_MAX = 0.85
            markets_for_quotes = [
                {
                    'id': m.market_id,
                    'title': m.title,
                    'best_bid': m.best_bid,
                    'best_ask': m.best_ask,
                }
                for m in self.eth_markets
                if m.best_bid > 0
                and m.best_ask > 0
                and PRICE_RANGE_MIN <= m.probability_yes <= PRICE_RANGE_MAX
                and _days_to_expiry(m.closes_at) <= 2.0  # high-gamma only: near-expiry markets react to spot
                and _days_to_expiry(m.closes_at) >= 0.05  # avoid resolution-second whiplash
            ]
            if not markets_for_quotes:
                logger.info("No liquid in-range ETH markets — skipping quotes")
                return
            
            quotes = self.strategy.generate_quotes(markets_for_quotes)
            
            if not quotes:
                logger.debug("No quotes generated")
                return

            # Build a market-id → DiscoveredMarket lookup for the filter
            market_map = {m.market_id: m for m in self.eth_markets}

            # Execute trades on both sides of each quote
            executed_quotes = 0
            for quote in quotes:
                try:
                    market = market_map.get(quote.market_id)

                    # ── Directional Filter (Gemini / BS alpha) ──────────────────
                    # Compute fair probability using the same Black-Scholes model
                    # that's already proven in KrakenPolyArb (88% WR).
                    # Only quote on the side where we have a theoretical edge:
                    #   BUY YES  → fair_prob > market mid  (market underpricing YES)
                    #   SELL YES → fair_prob < market mid  (market overpricing YES)
                    fair_prob = None
                    if market and self._kraken_momentum != 0.0:
                        strike = _extract_strike(market.title, "ETH")
                        if strike:
                            try:
                                spot = market.probability_yes  # proxy: re-use momentum ETH price
                                # We stored the real ETH spot in _kraken_momentum context;
                                # pull it from the latest Kraken candle via momentum calc.
                                # Use probability_yes as fallback if no strike context.
                                dte = _days_to_expiry(market.closes_at)
                                _t = market.title.lower()
                                _down = ("below", "under", "less than", "dip", "drop", "fall",
                                         "crash", "decline", "sink", "plunge")
                                direction = "below" if any(w in _t for w in _down) else "above"
                                # We need the real ETH spot — stored during refresh_markets
                                eth_spot = getattr(self, '_eth_spot', None)
                                if eth_spot and eth_spot > 0:
                                    fair_prob = fair_probability(
                                        spot=eth_spot,
                                        strike=strike,
                                        days_to_expiry=dte,
                                        annualised_vol=getattr(self, '_eth_vol', FALLBACK_VOL),
                                        direction=direction,
                                    )
                            except Exception:
                                pass

                    mid = quote.mid_price

                    # ── Inventory Skew (Stoikov-lite) ───────────────────────────
                    # Skew our quotes based on current YES inventory to avoid
                    # one-sided exposure. Each unit of inventory biases by 0.5¢.
                    SKEW_PER_UNIT = 0.005
                    inv = self.strategy.inventory.get(quote.market_id, {})
                    yes_inv = inv.get('YES', 0.0)
                    # Positive yes_inv → we're long YES → lower both prices to sell
                    # Negative yes_inv → we're short YES → raise both prices to buy
                    inv_skew = -yes_inv * SKEW_PER_UNIT
                    skewed_bid = max(0.01, min(0.99, quote.bid_price + inv_skew))
                    skewed_ask = max(0.01, min(0.99, quote.ask_price + inv_skew))

                    # Filter: require meaningful BS model edge before entering.
                    # Need 3%+ mispricing to clear fees (2c round-trip) and earn profit.
                    # If fair_prob unavailable, skip the trade — blind quoting has no edge.
                    MIN_EDGE = 0.03  # 3 cent minimum edge vs fair value
                    if fair_prob is None:
                        can_bid = False
                        can_ask = False
                    else:
                        can_bid = fair_prob >= (mid + MIN_EDGE)   # market underpricing YES
                        can_ask = fair_prob <= (mid - MIN_EDGE)   # market overpricing YES

                    if fair_prob is not None:
                        logger.debug(
                            f"  BS filter: {quote.market_title[:40]} | "
                            f"fair={fair_prob:.3f} mid={mid:.3f} | "
                            f"can_bid={can_bid} can_ask={can_ask}"
                        )

                    features_base = {
                        'market_spread': round(quote.market_ask - quote.market_bid, 4),
                        'mid_price': round(mid, 4),
                        'fair_prob': round(fair_prob, 4) if fair_prob is not None else None,
                        'our_spread': round(quote.spread, 4),
                        'inventory_yes': round(yes_inv, 2),
                        'inventory_no': round(inv.get('NO', 0.0), 2),
                        'inv_skew': round(inv_skew, 4),
                        'open_positions_count': len(self.strategy.open_trades),
                        'session_win_rate': round(self._session_wins / max(1, self._session_wins + self._session_losses), 4),
                        'market_title_short': quote.market_title[:40],
                        'confidence': round(quote.confidence, 4),
                    }

                    # BID side: we buy YES at (skewed) bid price
                    if can_bid and quote.bid_size > 0:
                        trade = self.strategy.execute_trade(
                            market_id=quote.market_id,
                            market_title=quote.market_title,
                            side='YES',
                            action='BUY',
                            price=skewed_bid,
                            size=quote.bid_size,
                        )
                        if trade:
                            self._position_prices[trade.trade_id] = trade.execution_price
                            self._position_steps[trade.trade_id] = 0
                            self._position_features[trade.trade_id] = {**features_base, 'side': 'BUY',
                                'market_bid': round(quote.market_bid, 4), 'market_ask': round(quote.market_ask, 4)}
                        executed_quotes += 1

                    # ASK side: we sell YES at (skewed) ask price
                    if can_ask and quote.ask_size > 0:
                        trade = self.strategy.execute_trade(
                            market_id=quote.market_id,
                            market_title=quote.market_title,
                            side='YES',
                            action='SELL',
                            price=skewed_ask,
                            size=quote.ask_size,
                        )
                        if trade:
                            self._position_prices[trade.trade_id] = trade.execution_price
                            self._position_steps[trade.trade_id] = 0
                            self._position_features[trade.trade_id] = {**features_base, 'side': 'SELL',
                                'market_bid': round(quote.market_bid, 4), 'market_ask': round(quote.market_ask, 4)}
                        executed_quotes += 1
                
                except Exception as quote_err:
                    logger.warning(f"Error executing quote: {quote_err}")
                    continue
            
            if executed_quotes > 0:
                logger.info(f"💰 Executed {executed_quotes} market maker trades this cycle")
        
        except Exception as e:
            logger.error(f"❌ Error in quote loop: {type(e).__name__}: {e}", exc_info=False)
    
    async def close_profitable_positions(self):
        """Track open positions using real Polymarket market prices (paper trading).
        
        Each 20-second tick we use the real current mid-price from Polymarket
        (refreshed every 5 minutes via market discovery). We close when:
          - Price moves +3.0 cents in our favour -> WIN
          - Price moves -3.0 cents against us    -> LOSS
          - Position held > 20 ticks (~6.5 min)  -> close at current real price
        
        P&L reflects actual Polymarket market movement, not a random walk.
        """
        # Fixed R/R: 2.5:1 — need positive EV even at 50% win rate.
        # Old: 0.8c target / 1.5c stop = 0.53 R/R → negative EV. Fixed:
        PROFIT_TARGET = 0.030   # +3.0 cents  — clears 2c fee drag + 1c real profit
        STOP_LOSS     = 0.012   # -1.2 cents  — tight stop (2.5:1 R/R ratio)
        MAX_STEPS     = 25      # Force-close after ~8 minutes

        try:
            if not self.strategy.open_trades:
                return
            
            # Build a real-time price map.
            # For liquid markets (real bid/ask), close BUY against bid and SELL against ask.
            # For illiquid markets (bid=0, ask=0), use probability_yes as the mark price —
            # this is the correct "fair value" for paper trading: a MM who bought below mid
            # should mark against mid, not the synthetic bid (which would guarantee a loss).
            live_price_map = {
                m.market_id: {
                    'bid': m.best_bid,
                    'ask': m.best_ask,
                    'prob': m.probability_yes,
                    'is_liquid': m.best_bid > 0 and m.best_ask > 0,
                }
                for m in self.eth_markets
            }

            closed_count = 0
            for trade_id, trade in list(self.strategy.open_trades.items()):
                try:
                    # Initialise price tracking on first tick
                    if trade_id not in self._position_prices:
                        self._position_prices[trade_id] = trade.execution_price
                        self._position_steps[trade_id] = 0

                    # Update mark price.
                    # Liquid: close BUY at current bid (sell side), SELL at current ask (buy side).
                    # Illiquid: use probability_yes as the fair-value mark — avoids the structural
                    # loss caused by entering at (prob - entry_spread) but closing at (prob - synth_spread).
                    price_data = live_price_map.get(trade.market_id)
                    if price_data:
                        if price_data['is_liquid']:
                            live_price = price_data['bid'] if trade.action == 'BUY' else price_data['ask']
                        else:
                            live_price = price_data['prob']  # Mark to mid in illiquid markets
                        if live_price and live_price > 0:
                            self._position_prices[trade_id] = live_price
                    self._position_steps[trade_id] += 1

                    current_price = self._position_prices[trade_id]
                    entry_price   = trade.execution_price
                    steps_held    = self._position_steps[trade_id]

                    # P&L from our perspective
                    if trade.action == 'BUY':
                        move = current_price - entry_price
                    else:  # SELL
                        move = entry_price - current_price

                    # Decide whether to close
                    hit_target  = move >= PROFIT_TARGET
                    hit_stop    = move <= -STOP_LOSS
                    timed_out   = steps_held >= MAX_STEPS

                    if not (hit_target or hit_stop or timed_out):
                        continue  # Position still running

                    # Cap close price at exact barrier to prevent random-walk overshoot
                    if hit_target:
                        close_price = (entry_price + PROFIT_TARGET) if trade.action == 'BUY' else (entry_price - PROFIT_TARGET)
                    elif hit_stop:
                        close_price = (entry_price - STOP_LOSS) if trade.action == 'BUY' else (entry_price + STOP_LOSS)
                    else:
                        # Timed out: close at wherever random walk ended
                        close_price = current_price
                    close_price = max(0.01, min(0.99, close_price))

                    # Clean up price tracking
                    self._position_prices.pop(trade_id, None)
                    self._position_steps.pop(trade_id, None)
                    entry_features = self._position_features.pop(trade_id, None)
                    # Enrich features with outcome context
                    if entry_features is not None:
                        entry_features['steps_held'] = steps_held
                        entry_features['hit_target'] = hit_target
                        entry_features['hit_stop'] = hit_stop
                        entry_features['timed_out'] = timed_out
                    
                    # Close the trade
                    closed_trade = self.strategy.close_trade(trade_id, close_price)
                    
                    if closed_trade and closed_trade.pnl is not None:
                        # Deduct real Polymarket taker fee: 2% on entry + 2% on exit.
                        # fee_pct as a fraction of entry notional:
                        #   entry leg: 2% × entry_price / entry_price = 0.02
                        #   exit leg:  2% × close_price / entry_price
                        # Use shared live_rules for correct taker fee (1% per leg)
                        fee_pct = live_rules.fee_drag(entry_price, close_price)
                        net_pnl_pct = closed_trade.pnl_pct - fee_pct
                        trade_size = (self.trade_size_pct * self._balance) if self.trade_size_pct else self.TRADE_SIZE_USD
                        dollar_pnl = net_pnl_pct * trade_size
                        is_win = dollar_pnl > 0.01  # must net at least 1 cent to count as win

                        # Update session counters
                        self._balance += dollar_pnl
                        self._session_dollar_pnl += dollar_pnl
                        if is_win:
                            self._session_wins += 1
                            self._current_streak = self._current_streak + 1 if self._current_streak >= 0 else 1
                        else:
                            self._session_losses += 1
                            self._current_streak = self._current_streak - 1 if self._current_streak <= 0 else -1
                        self._session_pnl += net_pnl_pct

                        all_time_wins = self._session_wins
                        all_time_losses = self._session_losses

                        # Send Discord alert for every trade
                        try:
                            await BotAlert.trade_result(
                                bot_name=self.bot_name,
                                asset="ETH",
                                signal_type="MARKET_MAKE",
                                pnl_pct=closed_trade.pnl_pct,
                                is_win=is_win,
                                entry_price=closed_trade.execution_price,
                                exit_price=closed_trade.counter_price,
                                session_wins=self._session_wins,
                                session_losses=self._session_losses,
                                session_pnl=self._session_pnl,
                                all_time_wins=all_time_wins,
                                all_time_losses=all_time_losses,
                                streak=abs(self._current_streak),
                                dollar_pnl=dollar_pnl,
                                account_balance=self._balance,
                                starting_capital=self.STARTING_CAPITAL,
                            )
                        except Exception as discord_err:
                            logger.debug(f"Discord error: {discord_err}")

                        # Persist to unified trade log (Supabase → JSONL fallback)
                        try:
                            from src.tracking.trade_logger import log_trade
                            log_trade(
                                bot="eth_mm",
                                market=f"ETH/{closed_trade.market_id}",
                                outcome=closed_trade.action,
                                entry_price=closed_trade.execution_price,
                                exit_price=closed_trade.counter_price,
                                size_usd=trade_size,
                                is_win=is_win,
                                resolved_via="price_target" if hit_target else ("stop_loss" if hit_stop else "timeout"),
                            )
                        except Exception:
                            pass

                        closed_count += 1
                
                except Exception as pos_err:
                    logger.debug(f"Error closing position: {pos_err}")
                    continue
            
            if closed_count > 0:
                logger.info(f"✅ Closed {closed_count} profitable positions")
                
                # Log session stats every 10 closed trades
                if closed_count % 10 == 0:
                    stats = self.strategy.get_session_stats()
                    logger.warning(f"📊 MM Stats: {stats['closed_trades']} closed | "
                                 f"WR: {stats['win_rate']:.1%} | "
                                 f"P&L: {stats['total_pnl']:+.2f} | "
                                 f"Avg: {stats['avg_pnl_per_trade']:+.4f}/trade")
        
        except Exception as e:
            logger.error(f"❌ Error closing positions: {type(e).__name__}: {e}", exc_info=False)
    
    async def send_heartbeat(self):
        """Send hourly Discord status update."""
        try:
            discord = get_discord_client()
            if not discord.enabled:
                return
            stats = self.strategy.get_session_stats()
            wr = stats['win_rate']
            total = stats['closed_trades']
            wins = stats['wins']
            losses = stats['losses']
            pnl_pct = stats['total_pnl']
            balance = self._balance
            bal_change = balance - self.STARTING_CAPITAL
            streak = self._current_streak

            streak_label = f"🔥 {streak}W streak" if streak > 0 else (f"❄️ {abs(streak)}L streak" if streak < 0 else "—")
            color = 0x00C853 if bal_change >= 0 else 0xFF1744

            embed = {
                "title": "Ξ ETH Market Maker — Hourly Update",
                "color": color,
                "fields": [
                    {"name": "Balance", "value": f"${balance:,.2f}  ({bal_change:+.2f})", "inline": True},
                    {"name": "Session P&L", "value": f"{pnl_pct:+.4f} pts", "inline": True},
                    {"name": "Win Rate", "value": f"{wr:.1%}  ({wins}W/{losses}L)", "inline": True},
                    {"name": "Total Trades", "value": str(total), "inline": True},
                    {"name": "Open Positions", "value": str(stats['open_positions']), "inline": True},
                    {"name": "Streak", "value": streak_label, "inline": True},
                ],
                "footer": {"text": f"Paper Trading | ETH Market Maker | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                "timestamp": datetime.utcnow().isoformat(),
            }
            await discord.send_message(embed=embed)
            logger.info("📨 Hourly heartbeat sent to Discord")
        except Exception as e:
            logger.debug(f"Heartbeat error: {e}")

    async def run_forever(self):
        """Run the market maker indefinitely."""
        try:
            # Manually refresh markets first
            await self.refresh_markets()
            logger.info(f"🚀 Starting ETH Market Maker strategy...")
            self.start()
            
        except Exception as start_err:
            logger.error(f"❌ Failed to start market maker: {start_err}", exc_info=True)
            raise
        
        # Main loop - manually trigger the scheduled jobs
        consecutive_errors = 0
        max_consecutive_errors = 5

        try:
            refresh_counter = 0
            quote_counter = 0
            close_counter = 0
            heartbeat_counter = 0

            while True:
                try:
                    # Manually call jobs on schedule
                    refresh_counter += 1
                    quote_counter += 1
                    close_counter += 1
                    heartbeat_counter += 1

                    # Hourly Discord heartbeat (3600 seconds)
                    if heartbeat_counter >= 3600:
                        try:
                            await self.send_heartbeat()
                        except Exception as h_err:
                            logger.debug(f"Heartbeat error: {h_err}")
                        heartbeat_counter = 0

                    # Refresh markets every 300 sleep cycles (300 seconds = 5 min interval)
                    if refresh_counter >= 300:
                        try:
                            await self.refresh_markets()
                        except Exception as r_err:
                            logger.debug(f"Refresh error: {r_err}")
                        refresh_counter = 0

                    # Quote loop every 10 sleep cycles (~10 seconds)
                    if quote_counter >= 10:
                        try:
                            await self.quote_loop()
                        except Exception as q_err:
                            logger.warning(f"Quote loop error: {type(q_err).__name__}: {q_err}")
                        quote_counter = 0

                    # Close positions every 20 sleep cycles (~20 seconds)
                    if close_counter >= 20:
                        try:
                            await self.close_profitable_positions()
                        except Exception as c_err:
                            logger.warning(f"Close positions error: {type(c_err).__name__}: {c_err}")
                        close_counter = 0
                    
                    await asyncio.sleep(1)
                    consecutive_errors = 0
                    
                except asyncio.CancelledError:
                    logger.info("Market maker cancelled")
                    raise
                except Exception as inner_err:
                    consecutive_errors += 1
                    logger.error(
                        f"❌ Error in market maker loop ({consecutive_errors}/{max_consecutive_errors}): "
                        f"{type(inner_err).__name__}",
                        exc_info=False
                    )
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("Too many errors. Stopping market maker.")
                        raise
                    
                    backoff_time = min(2 ** (consecutive_errors - 1), 30)
                    await asyncio.sleep(backoff_time)
        
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("⏹️  Market maker shutting down...")
        finally:
            try:
                self.stop()
            except Exception:
                pass
            
            # Log final stats
            stats = self.strategy.get_session_stats()
            logger.warning(f"📊 Final Stats: {stats['closed_trades']} trades | "
                         f"{stats['wins']}W / {stats['losses']}L | "
                         f"Total P&L: {stats['total_pnl']:+.2f}")
            logger.info("Market maker stopped")
