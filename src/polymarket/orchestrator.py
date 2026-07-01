"""
Polymarket Trading Orchestrator - Integration of all trading components
Coordinates market discovery, signal matching, order placement, wallet management, and exits
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json
from loguru import logger

from src.polymarket.market_discovery import MarketDiscovery, DiscoveredMarket
from src.polymarket.signal_matcher import SignalMatcher, SignalMatch
from src.polymarket.order_placer import OrderPlacer, PolymarketOrder, OrderStatus
from src.polymarket.wallet_manager import WalletManager, Wallet
from src.polymarket.exit_manager import ExitManager, ExitTarget, PositionExit, ExitReason


class TradeWorkflowStage(Enum):
    """Stages in the trade execution workflow"""
    SIGNAL_RECEIVED = "signal_received"
    MARKET_FOUND = "market_found"
    WALLET_VALIDATED = "wallet_validated"
    ORDER_CREATED = "order_created"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_FILLED = "order_filled"
    EXITS_CONFIGURED = "exits_configured"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TradeWorkflowError(Exception):
    """Error during trade workflow execution"""
    pass


@dataclass
class TradeSignal:
    """Signal from trading bot to execute a trade"""
    bot_id: str
    signal_type: str  # BUY_YES, BUY_NO
    asset: str  # BTC, ETH
    confidence: float  # 0-100
    target_size: float  # USD amount
    risk_reward_ratio: float = 2.0
    comment: str = ""


@dataclass
class TradeWorkflow:
    """Complete workflow for a single trade from signal to position tracking"""
    workflow_id: str
    signal: TradeSignal
    stage: TradeWorkflowStage
    
    discovered_market: Optional[DiscoveredMarket] = None
    signal_match: Optional[SignalMatch] = None
    order: Optional[PolymarketOrder] = None
    wallet: Optional[Wallet] = None
    exit_targets: List[ExitTarget] = field(default_factory=list)
    position_exits: List[PositionExit] = field(default_factory=list)
    
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    error_message: str = ""
    
    def __post_init__(self):
        self.workflow_id = f"{self.signal.bot_id}-{self.signal.asset}-{self.created_at.timestamp()}"
    
    def to_dict(self) -> dict:
        """Convert workflow to dictionary for JSON serialization"""
        return {
            "workflow_id": self.workflow_id,
            "bot_id": self.signal.bot_id,
            "asset": self.signal.asset,
            "signal_type": self.signal.signal_type,
            "confidence": self.signal.confidence,
            "stage": self.stage.value,
            "market_id": self.discovered_market.market_id if self.discovered_market else None,
            "order_id": self.order.order_id if self.order else None,
            "wallet": self.wallet.wallet_id if self.wallet else None,
            "exit_count": len(self.exit_targets),
            "position_exit_count": len(self.position_exits),
            "created_at": self.created_at.isoformat(),
            "status": "success" if self.stage == TradeWorkflowStage.COMPLETE else "pending" if self.stage == TradeWorkflowStage.FAILED else "in-progress"
        }


class PolymarketOrchestrator:
    """
    Master orchestrator for Polymarket trading
    Coordinates all components: discovery, matching, placement, wallets, exits
    """
    
    def __init__(self, order_placer: OrderPlacer, wallet_mgr: WalletManager):
        """
        Initialize orchestrator with core components
        
        Args:
            order_placer: OrderPlacer instance for managing orders
            wallet_mgr: WalletManager instance for credential/balance management
        """
        self.order_placer = order_placer
        self.wallet_mgr = wallet_mgr
        
        # Initialize sub-components
        self.market_discovery = MarketDiscovery(order_placer.client)
        self.signal_matcher = SignalMatcher(self.market_discovery)
        self.exit_manager = ExitManager(order_placer)
        
        # Workflow tracking
        self.workflows: Dict[str, TradeWorkflow] = {}
        self.completed_workflows: List[TradeWorkflow] = []
        
        logger.info(f"✅ PolymarketOrchestrator initialized (discovery, matching, placement, wallets, exits)")
    
    def execute_trade_from_signal(self, signal: TradeSignal) -> TradeWorkflow:
        """
        Execute complete trade workflow from bot signal to position tracking
        
        Args:
            signal: TradeSignal from trading bot
            
        Returns:
            TradeWorkflow with complete execution details
            
        Raises:
            TradeWorkflowError: If any stage fails
        """
        workflow = TradeWorkflow(
            workflow_id="",
            signal=signal,
            stage=TradeWorkflowStage.SIGNAL_RECEIVED
        )
        
        try:
            logger.info(f"📊 Trade workflow started: {signal.bot_id} {signal.signal_type} {signal.asset}")
            
            # Stage 1: Discover best market
            workflow.discovered_market = self._discover_market(signal)
            workflow.stage = TradeWorkflowStage.MARKET_FOUND
            
            # Stage 2: Match signal to market
            workflow.signal_match = self._match_signal_to_market(signal, workflow.discovered_market)
            
            # Stage 3: Validate wallet capability
            workflow.wallet = self._validate_wallet(signal)
            workflow.stage = TradeWorkflowStage.WALLET_VALIDATED
            
            # Stage 4: Create order
            order = self._create_order(signal, workflow.signal_match, workflow.wallet)
            workflow.order = order
            workflow.stage = TradeWorkflowStage.ORDER_CREATED
            
            # Stage 5: Submit order
            submitted_order = self._submit_order(order)
            workflow.order = submitted_order
            workflow.stage = TradeWorkflowStage.ORDER_SUBMITTED
            
            # Stage 6: (In real system, wait for fill here)
            # For demo, mark as filled
            workflow.order.status = OrderStatus.FILLED
            workflow.order.filled_amount = workflow.order.size
            workflow.stage = TradeWorkflowStage.ORDER_FILLED
            
            # Stage 7: Configure exit targets
            workflow.exit_targets = self._create_exit_targets(
                order=submitted_order,
                entry_price=submitted_order.average_fill_price,
                signal=signal
            )
            workflow.stage = TradeWorkflowStage.EXITS_CONFIGURED
            
            # Stage 8: Mark complete
            workflow.stage = TradeWorkflowStage.COMPLETE
            
            # Record trade in wallet for accounting
            if workflow.wallet:
                self.wallet_mgr.record_trade(
                    wallet=workflow.wallet,
                    order_id=workflow.order.order_id,
                    size=workflow.order.size,
                    price=workflow.order.average_fill_price,
                    pnl=0.0
                )
            
            logger.info(f"✅ Trade workflow COMPLETE: {workflow.workflow_id}")
            
        except Exception as e:
            workflow.stage = TradeWorkflowStage.FAILED
            workflow.error_message = str(e)
            logger.error(f"❌ Trade workflow FAILED: {str(e)}")
            raise TradeWorkflowError(f"Workflow {workflow.workflow_id} failed: {str(e)}")
        
        finally:
            workflow.updated_at = datetime.utcnow()
            self.workflows[workflow.workflow_id] = workflow
        
        return workflow
    
    def _discover_market(self, signal: TradeSignal) -> DiscoveredMarket:
        """Discover the best market for a signal"""
        try:
            # Determine market type from signal (direction vs price_target)
            market_type = "direction"  # Default to direction for BUY_YES/BUY_NO
            
            market = self.market_discovery.get_best_market_for_signal(
                asset=signal.asset,
                signal_confidence=signal.confidence,
                market_type=market_type
            )
            if not market:
                raise TradeWorkflowError(f"No market found for {signal.asset} {signal.signal_type}")
            logger.info(f"🎯 Market discovered: {market.market_id} ({market.title})")
            return market
        except Exception as e:
            raise TradeWorkflowError(f"Market discovery failed: {str(e)}")
    
    def _match_signal_to_market(self, signal: TradeSignal, market: DiscoveredMarket) -> SignalMatch:
        """Match signal to discovered market"""
        try:
            signal_match = self.signal_matcher.match_signal(
                bot_id=signal.bot_id,
                signal=signal.signal_type,  # BUY_YES or BUY_NO
                asset=signal.asset,
                confidence=signal.confidence,
                entry_price=market.current_price,
                position_size=signal.target_size
            )
            
            if not signal_match:
                raise TradeWorkflowError(f"Signal matching failed for {signal.asset}")
            
            logger.info(f"✨ Signal matched: edge {signal_match.market_edge:.1f}%, "
                       f"size ${signal_match.recommended_size:.0f}")
            return signal_match
        except Exception as e:
            raise TradeWorkflowError(f"Signal matching failed: {str(e)}")
    
    def _validate_wallet(self, signal: TradeSignal) -> Wallet:
        """Validate wallet capability for trade"""
        try:
            # Get primary trading wallet
            wallets = self.wallet_mgr.list_wallets()
            if not wallets:
                raise TradeWorkflowError("No wallets available")
            
            wallet = wallets[0]
            
            # Validate trade capability
            can_trade, warning = self.wallet_mgr.can_trade(
                wallet=wallet,
                order_size=signal.target_size
            )
            
            if not can_trade:
                raise TradeWorkflowError(f"Wallet validation failed: {warning}")
            
            logger.info(f"💳 Wallet validated: {wallet.wallet_id}")
            return wallet
        except Exception as e:
            raise TradeWorkflowError(f"Wallet validation failed: {str(e)}")
    
    def _create_order(
        self,
        signal: TradeSignal,
        signal_match: SignalMatch,
        wallet: Wallet
    ) -> PolymarketOrder:
        """Create order from signal and market match"""
        try:
            from src.polymarket.order_placer import OrderType
            
            # Validate first
            validation = self.order_placer.validate_order(signal_match, signal_match.recommended_size)
            if not validation.is_valid:
                raise TradeWorkflowError(f"Order validation failed: {', '.join(validation.errors)}")
            
            if validation.warnings:
                logger.warning(f"Order warnings: {', '.join(validation.warnings)}")
            
            # Create order
            order = self.order_placer.create_order(
                signal_match=signal_match,
                size=signal_match.recommended_size,
                order_type=OrderType.LIMIT,
                limit_price=signal_match.matched_market.best_bid
            )
            
            if not order:
                raise TradeWorkflowError(f"Order creation returned None")
            
            logger.info(f"📝 Order created: {order.order_id} ({order.size} @ ${order.price})")
            return order
        except Exception as e:
            raise TradeWorkflowError(f"Order creation failed: {str(e)}")
    
    def _submit_order(self, order: PolymarketOrder) -> PolymarketOrder:
        """Submit order to exchange"""
        try:
            # In production, this would submit to real API
            # For now, mark as submitted
            order.status = OrderStatus.SUBMITTED
            logger.info(f"📤 Order submitted: {order.order_id}")
            
            # Simulate fill (in production, would track via API)
            order.status = OrderStatus.FILLED
            order.filled_amount = order.size
            order.average_fill_price = order.price
            
            logger.info(f"✅ Order filled: {order.order_id} @ ${order.average_fill_price}")
            return order
        except Exception as e:
            raise TradeWorkflowError(f"Order submission failed: {str(e)}")
    
    def _create_exit_targets(
        self,
        order: PolymarketOrder,
        entry_price: float,
        signal: TradeSignal
    ) -> List[ExitTarget]:
        """Create exit targets (TP/SL) for position"""
        try:
            targets = self.exit_manager.create_exit_targets(
                order_id=order.order_id,
                entry_price=entry_price,
                take_profit_percent=0.20,  # 20% gain
                stop_loss_percent=-0.10,   # 10% loss
                use_tiered_exits=True  # 3-level exits
            )
            
            logger.info(f"🎯 Exit targets created: {len(targets)} targets for {order.order_id}")
            return targets
        except Exception as e:
            raise TradeWorkflowError(f"Exit target creation failed: {str(e)}")
    
    def check_and_execute_exits(self, order_id: str, current_price: float) -> List[PositionExit]:
        """Check if any exits should trigger and execute them"""
        try:
            # Check if targets are triggered
            triggered = self.exit_manager.check_exit_triggers(order_id, current_price)
            
            executed_exits = []
            for target in triggered:
                exit_record = self.exit_manager.execute_exit(
                    order_id=order_id,
                    exit_target=target,
                    exit_reason=ExitReason.TAKE_PROFIT if "TP" in target.name else ExitReason.STOP_LOSS,
                    current_price=current_price,
                    position_size=100.0  # From order if available
                )
                if exit_record:
                    executed_exits.append(exit_record)
                    # Update workflow with exit
                    for wf in self.workflows.values():
                        if wf.order and wf.order.order_id == order_id:
                            wf.position_exits.append(exit_record)
            
            if executed_exits:
                logger.info(f"💸 Exits executed: {len(executed_exits)} position(s) closed")
            
            return executed_exits
        except Exception as e:
            logger.error(f"Exit execution failed: {str(e)}")
            return []
    
    def get_workflow_summary(self, workflow_id: str) -> dict:
        """Get summary of a trade workflow"""
        if workflow_id not in self.workflows:
            return {"error": "Workflow not found"}
        
        workflow = self.workflows[workflow_id]
        return workflow.to_dict()
    
    def get_all_workflows(self, status: Optional[str] = None) -> List[dict]:
        """Get all workflows, optionally filtered by status"""
        workflows = list(self.workflows.values())
        
        if status:
            workflows = [w for w in workflows if w.stage.value == status]
        
        return [w.to_dict() for w in workflows]
    
    def get_active_positions(self) -> List[dict]:
        """Get list of active trading positions"""
        active = []
        for workflow in self.workflows.values():
            if workflow.stage == TradeWorkflowStage.COMPLETE and workflow.order:
                active.append({
                    "order_id": workflow.order.order_id,
                    "market": workflow.discovered_market.title if workflow.discovered_market else "N/A",
                    "entry_price": workflow.order.average_fill_price,
                    "size": workflow.order.filled_amount,
                    "cost": workflow.order.total_cost,
                    "exits_set": len(workflow.exit_targets),
                    "exits_executed": len(workflow.position_exits),
                    "created": workflow.created_at.isoformat()
                })
        
        return active
    
    def get_portfolio_performance(self) -> dict:
        """Get overall portfolio performance"""
        total_pnl = 0.0
        total_trades = 0
        winning_trades = 0
        
        for workflow in self.workflows.values():
            if workflow.stage == TradeWorkflowStage.COMPLETE:
                total_trades += 1
                for exit in workflow.position_exits:
                    total_pnl += exit.realized_pnl
                    if exit.realized_pnl > 0:
                        winning_trades += 1
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        return {
            "total_trades": total_trades,
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "avg_pnl": total_pnl / total_trades if total_trades > 0 else 0
        }
    
    def reset_workflows(self):
        """Clear workflow history (for testing/reset)"""
        self.completed_workflows.extend(self.workflows.values())
        self.workflows.clear()
        logger.info(f"✅ Workflows reset ({len(self.completed_workflows)} archived)")
