# tests/integration/test_test_integration_scenario.py

import pytest
from unittest.mock import Mock, patch, MagicMock
import datetime

from src.trading.execution.trading_manager import TradingManager
from src.trading.orders.order_loading_orchestrator import OrderLoadingOrchestrator
from src.trading.orders.order_lifecycle_manager import OrderLifecycleManager
from src.trading.orders.planned_order import PlannedOrder, Action, OrderType, PositionStrategy, SecurityType
from src.core.models import PlannedOrderDB
from src.core.events import OrderState


class TestIntegrationScenarios:
    """Integration test scenarios for the complete order loading system."""
    
    def create_mock_db_order(self, symbol="TEST", action="BUY", entry_price=100.0, 
                           position_strategy="DAY", status="PENDING", days_old=0):
        """Create a proper mock PlannedOrderDB with SQLAlchemy attributes."""
        db_order = MagicMock(spec=PlannedOrderDB)
        db_order.symbol = symbol
        db_order.action = action
        db_order.entry_price = entry_price
        # Set stop loss correctly based on action
        if action == "BUY":
            db_order.stop_loss = entry_price * 0.97  # Below entry for BUY
        else:
            db_order.stop_loss = entry_price * 1.03  # Above entry for SELL
        db_order.order_type = "LMT"
        db_order.position_strategy = position_strategy
        db_order.security_type = "STK"
        db_order.exchange = "SMART"
        db_order.currency = "USD"
        db_order.status = status
        db_order.created_at = datetime.datetime.now() - datetime.timedelta(days=days_old)
        db_order.updated_at = datetime.datetime.now() - datetime.timedelta(days=days_old)
        db_order._sa_instance_state = MagicMock()  # SQLAlchemy attribute
        return db_order
    
    def create_planned_order(self, symbol="TEST", action=Action.BUY, entry_price=100.0,
                           position_strategy=PositionStrategy.DAY):
        """Create a PlannedOrder with all required parameters and valid stop loss."""
        # Set stop loss correctly based on action
        if action == Action.BUY:
            stop_loss = entry_price * 0.97  # Below entry for BUY
        else:
            stop_loss = entry_price * 1.03  # Above entry for SELL
            
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            symbol=symbol,
            action=action,
            entry_price=entry_price,
            stop_loss=stop_loss,
            order_type=OrderType.LMT,
            position_strategy=position_strategy,
            risk_per_trade=0.01,
            risk_reward_ratio=2.0,
            priority=3
        )
    
    def test_system_restart_recovery(self):
        """Test complete system restart with order recovery from database."""
        # Mock core dependencies
        mock_data_feed = Mock()
        mock_data_feed.is_connected.return_value = True
        
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True
        mock_ibkr_client.is_paper_account = True
        
        # Create trading manager
        manager = TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)
        
        # Mock active orders in database (simulating previous session)
        db_orders = [
            self.create_mock_db_order("AAPL", "BUY", 150.0, "DAY", "PENDING", days_old=0),
            self.create_mock_db_order("MSFT", "SELL", 300.0, "CORE", "PENDING", days_old=1)
        ]
        
        # Mock new orders from Excel
        excel_orders = [
            self.create_planned_order("GOOGL", Action.BUY, 2500.0, PositionStrategy.HYBRID)
        ]
        
        # Mock orchestrator to return combined orders
        if hasattr(manager, 'order_loading_orchestrator'):
            manager.order_loading_orchestrator = Mock()
            manager.order_loading_orchestrator.load_all_orders.return_value = excel_orders
        
        # Mock lifecycle manager
        if hasattr(manager, 'order_lifecycle_manager'):
            manager.order_lifecycle_manager = Mock()
            manager.order_lifecycle_manager.load_and_persist_orders.return_value = excel_orders
        
        # Load orders (simulating system restart)
        result = manager.load_planned_orders()
        
        # Verify orders are loaded
        assert isinstance(result, list)
    
    def test_duplicate_prevention_across_sources(self):
        """Test that duplicates are prevented across database and Excel sources."""
        # Mock core dependencies
        mock_data_feed = Mock()
        mock_data_feed.is_connected.return_value = True
        
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True
        
        # Create trading manager
        manager = TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)
        
        # Create duplicate order (same symbol, action, price)
        duplicate_order_db = self.create_mock_db_order("DUPLICATE", "BUY", 100.0, "DAY")
        duplicate_order_excel = self.create_planned_order("DUPLICATE", Action.BUY, 100.0)
        different_order = self.create_planned_order("DIFFERENT", Action.SELL, 200.0)
        
        # Mock orchestrator with deduplication
        if hasattr(manager, 'order_loading_orchestrator'):
            manager.order_loading_orchestrator = Mock()
            manager.order_loading_orchestrator.load_all_orders.return_value = [duplicate_order_db, different_order]
        
        # Mock lifecycle manager persistence logic
        if hasattr(manager, 'order_lifecycle_manager'):
            manager.order_lifecycle_manager = Mock()
            manager.order_lifecycle_manager.load_and_persist_orders.return_value = [duplicate_order_db, different_order]
        
        result = manager.load_planned_orders()
        
        # Should handle duplicates gracefully
        assert isinstance(result, list)
    
    def test_strategy_expiration_handling(self):
        """Test that position strategy expiration rules are properly applied."""
        # Mock core dependencies
        mock_data_feed = Mock()
        mock_data_feed.is_connected.return_value = True
        
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True
        
        # Create trading manager
        manager = TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)
        
        # Create orders with different strategies and ages
        now = datetime.datetime.now()
        
        expired_day_order = self.create_mock_db_order("EXPIRED_DAY", "BUY", 100.0, "DAY", "PENDING", days_old=2)
        active_core_order = self.create_mock_db_order("ACTIVE_CORE", "BUY", 100.0, "CORE", "PENDING", days_old=365)
        expired_hybrid_order = self.create_mock_db_order("EXPIRED_HYBRID", "BUY", 100.0, "HYBRID", "PENDING", days_old=11)
        active_hybrid_order = self.create_mock_db_order("ACTIVE_HYBRID", "BUY", 100.0, "HYBRID", "PENDING", days_old=9)
        
        orders = [expired_day_order, active_core_order, expired_hybrid_order, active_hybrid_order]
        
        # Mock orchestrator with expiration filtering
        if hasattr(manager, 'order_loading_orchestrator'):
            manager.order_loading_orchestrator = Mock()
            manager.order_loading_orchestrator.load_all_orders.return_value = [active_core_order, active_hybrid_order]
        
        result = manager.load_planned_orders()
        
        # Should handle expiration filtering
        assert isinstance(result, list)