"""
Unit tests for the PrioritizationService Phase B deterministic scoring and capital allocation.
"""

import pytest
from unittest.mock import Mock, MagicMock
from src.services.prioritization_service import PrioritizationService
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy


class MockSizingService:
    """Mock sizing service for testing prioritization in isolation."""
    
    def calculate_order_quantity(self, order, total_capital):
        """Mock quantity calculation based on simple risk management."""
        if order.entry_price is None or order.stop_loss is None:
            raise ValueError("Missing price data")
        
        risk_per_share = abs(order.entry_price - order.stop_loss)
        risk_amount = total_capital * order.risk_per_trade
        return max(1, int(risk_amount / risk_per_share))


class TestPrioritizationService:
    """Test suite for PrioritizationService Phase B features."""
    
    @pytest.fixture
    def mock_sizing_service(self):
        """Provide a mock sizing service for testing."""
        return MockSizingService()
    
    @pytest.fixture
    def prioritization_service(self, mock_sizing_service):
        """Provide a prioritization service instance for testing."""
        return PrioritizationService(mock_sizing_service)
    
    @pytest.fixture
    def sample_buy_order(self):
        """Create a sample BUY order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=150.0,
            stop_loss=145.0,
            risk_per_trade=0.01,
            risk_reward_ratio=2.0,
            position_strategy=PositionStrategy.CORE,
            priority=3,
            trading_setup="Breakout",
            core_timeframe="15min"
        )
    
    @pytest.fixture
    def sample_sell_order(self):
        """Create a sample SELL order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.SELL,
            symbol="MSFT",
            order_type=OrderType.LMT,
            entry_price=300.0,
            stop_loss=310.0,
            risk_per_trade=0.005,
            risk_reward_ratio=2.5,
            position_strategy=PositionStrategy.DAY,
            priority=2,
            trading_setup="Reversal",
            core_timeframe="1H"
        )
    
    def test_default_configuration(self, prioritization_service):
        """Test that default configuration is loaded correctly."""
        config = prioritization_service.config
        
        # Test weights sum to approximately 1.0
        weights_sum = sum(config['weights'].values())
        assert abs(weights_sum - 1.0) < 0.001
        
        # Test conservative weights as specified in Phase B
        assert config['weights']['fill_prob'] == 0.45
        assert config['weights']['manual_priority'] == 0.20
        assert config['weights']['efficiency'] == 0.15
        
        # Test operational parameters
        assert config['max_open_orders'] == 5
        assert config['max_capital_utilization'] == 0.8
    
    def test_calculate_efficiency_buy_order(self, prioritization_service, sample_buy_order):
        """Test capital efficiency calculation for BUY orders."""
        total_capital = 100000
        efficiency = prioritization_service.calculate_efficiency(sample_buy_order, total_capital)
        
        # Should be positive efficiency for profitable order
        assert efficiency > 0
        assert isinstance(efficiency, float)
    
    def test_calculate_efficiency_sell_order(self, prioritization_service, sample_sell_order):
        """Test capital efficiency calculation for SELL orders."""
        total_capital = 100000
        efficiency = prioritization_service.calculate_efficiency(sample_sell_order, total_capital)
        
        # Should be positive efficiency for profitable order
        assert efficiency > 0
        assert isinstance(efficiency, float)
    
    def test_calculate_efficiency_invalid_order(self, prioritization_service):
        """Test efficiency calculation with invalid order data."""
        invalid_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="TEST",
            order_type=OrderType.LMT,
            entry_price=None,  # Missing entry price
            stop_loss=95.0,
            risk_per_trade=0.01
        )
        
        efficiency = prioritization_service.calculate_efficiency(invalid_order, 100000)
        assert efficiency == 0.0  # Should return 0 for invalid orders
    
    def test_deterministic_score_calculation(self, prioritization_service, sample_buy_order):
        """Test comprehensive deterministic score calculation."""
        fill_prob = 0.85
        total_capital = 100000
        
        score_result = prioritization_service.calculate_deterministic_score(
            sample_buy_order, fill_prob, total_capital
        )
        
        # Test result structure
        assert 'final_score' in score_result
        assert 'components' in score_result
        assert 'weights' in score_result
        assert 'capital_commitment' in score_result
        
        # Test score range
        assert 0.0 <= score_result['final_score'] <= 1.0
        
        # Test components exist
        components = score_result['components']
        assert 'fill_prob' in components
        assert 'priority_norm' in components
        assert 'efficiency' in components
        assert 'efficiency_norm' in components
        assert 'size_pref' in components
        assert 'timeframe_match' in components
        assert 'setup_bias' in components
        
        # Test priority normalization (1-5 → 0-1, where 5 is best → 1.0)
        # priority=3 should normalize to (6-3)/5 = 0.6
        assert abs(components['priority_norm'] - 0.6) < 0.001
    
    def test_prioritize_orders_basic(self, prioritization_service, sample_buy_order, sample_sell_order):
        """Test basic order prioritization without constraints."""
        executable_orders = [
            {
                'order': sample_buy_order,
                'fill_probability': 0.9,
                'priority': sample_buy_order.priority,
                'timestamp': None
            },
            {
                'order': sample_sell_order, 
                'fill_probability': 0.7,
                'priority': sample_sell_order.priority,
                'timestamp': None
            }
        ]
        
        total_capital = 100000
        prioritized_orders = prioritization_service.prioritize_orders(executable_orders, total_capital)
        
        assert len(prioritized_orders) == 2
        
        # Should have deterministic scores
        for order in prioritized_orders:
            assert 'deterministic_score' in order
            assert 'score_components' in order
            assert 'quantity' in order
            assert 'capital_commitment' in order
            assert 'allocated' in order
            assert 'allocation_reason' in order
        
        # Orders should be sorted by score descending
        scores = [o['deterministic_score'] for o in prioritized_orders]
        assert scores == sorted(scores, reverse=True)

    def test_prioritize_orders_with_capital_constraint_returns_all_orders_but_unallocated(self, prioritization_service, sample_buy_order):
        """Test that ALL orders are returned but marked as unallocated when capital is insufficient."""
        executable_orders = [
            {
                'order': sample_buy_order,
                'fill_probability': 0.9,
                'priority': sample_buy_order.priority,
                'timestamp': None
            }
        ]

        # Mock the sizing service to return a LARGE position size that exceeds available capital
        def mock_calculate_quantity(order, total_capital):
            # Return a quantity that would require more than 1000 capital
            return 100  # This will make capital_commitment = 150.0 * 100 = 15000
        
        prioritization_service.sizing_service.calculate_order_quantity = mock_calculate_quantity

        # Very small capital - should not be able to allocate
        small_capital = 1000  # Too small for the order
        prioritized_orders = prioritization_service.prioritize_orders(executable_orders, small_capital)

        # Should return ALL orders (1 in this case) but marked as not allocated
        assert len(prioritized_orders) == 1
        assert not prioritized_orders[0]['allocated']  # Should be False due to insufficient capital
        assert prioritized_orders[0]['allocation_reason'] == 'Insufficient capital'

    def test_prioritize_orders_with_order_limit(self, prioritization_service, sample_buy_order):
        """Test prioritization with open order limit constraints."""
        executable_orders = [
            {
                'order': sample_buy_order,
                'fill_probability': 0.9,
                'priority': sample_buy_order.priority,
                'timestamp': None
            }
        ]
        
        # Mock working orders that already hit the limit
        working_orders = [{'capital_commitment': 10000}] * 5  # Max open orders
        
        total_capital = 100000
        prioritized_orders = prioritization_service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )
        
        assert len(prioritized_orders) == 1
        assert not prioritized_orders[0]['allocated']
        assert prioritized_orders[0]['allocation_reason'] == 'Max open orders reached'
    
    def test_get_prioritization_summary(self, prioritization_service, sample_buy_order, sample_sell_order):
        """Test prioritization summary generation."""
        executable_orders = [
            {
                'order': sample_buy_order,
                'fill_probability': 0.9,
                'priority': sample_buy_order.priority,
                'timestamp': None
            },
            {
                'order': sample_sell_order,
                'fill_probability': 0.7, 
                'priority': sample_sell_order.priority,
                'timestamp': None
            }
        ]
        
        total_capital = 100000
        prioritized_orders = prioritization_service.prioritize_orders(executable_orders, total_capital)
        
        # Mark one as not allocated for testing
        prioritized_orders[1]['allocated'] = False
        prioritized_orders[1]['allocation_reason'] = 'Test reason'
        
        summary = prioritization_service.get_prioritization_summary(prioritized_orders)
        
        # Test summary structure
        assert 'total_allocated' in summary
        assert 'total_rejected' in summary
        assert 'total_capital_commitment' in summary
        assert 'average_score' in summary
        assert 'allocation_reasons' in summary
        
        # Should have 1 allocated, 1 rejected
        assert summary['total_allocated'] == 1
        assert summary['total_rejected'] == 1
        assert 'Test reason' in summary['allocation_reasons']
    
    def test_priority_normalization_extremes(self, prioritization_service, sample_buy_order):
        """Test priority normalization at extreme values."""
        total_capital = 100000
        fill_prob = 0.8
        
        # Test priority 1 (highest manual priority)
        sample_buy_order.priority = 1
        score_result = prioritization_service.calculate_deterministic_score(
            sample_buy_order, fill_prob, total_capital
        )
        # priority=1 should normalize to (6-1)/5 = 1.0
        assert abs(score_result['components']['priority_norm'] - 1.0) < 0.001
        
        # Test priority 5 (lowest manual priority)
        sample_buy_order.priority = 5
        score_result = prioritization_service.calculate_deterministic_score(
            sample_buy_order, fill_prob, total_capital
        )
        # priority=5 should normalize to (6-5)/5 = 0.2
        assert abs(score_result['components']['priority_norm'] - 0.2) < 0.001
    
    def test_custom_configuration(self, mock_sizing_service):
        """Test prioritization with custom configuration."""
        custom_config = {
            'weights': {
                'fill_prob': 0.6,
                'manual_priority': 0.2,
                'efficiency': 0.1,
                'size_pref': 0.05,
                'timeframe_match': 0.03,
                'setup_bias': 0.02
            },
            'max_open_orders': 3,
            'max_capital_utilization': 0.9
        }
        
        service = PrioritizationService(mock_sizing_service, custom_config)
        
        # Test custom config is used
        assert service.config['weights']['fill_prob'] == 0.6
        assert service.config['max_open_orders'] == 3
        assert service.config['max_capital_utilization'] == 0.9

    def test_empty_executable_orders(self, prioritization_service):
        """Test prioritization with empty executable orders list."""
        prioritized_orders = prioritization_service.prioritize_orders([], 100000)
        assert prioritized_orders == []

    def test_single_order_prioritization(self, prioritization_service, sample_buy_order):
        """Test prioritization with only one executable order."""
        executable_orders = [
            {
                'order': sample_buy_order,
                'fill_probability': 0.9,
                'priority': sample_buy_order.priority,
                'timestamp': None
            }
        ]
        
        prioritized_orders = prioritization_service.prioritize_orders(executable_orders, 100000)
        
        assert len(prioritized_orders) == 1
        assert prioritized_orders[0]['allocated'] == True
        assert prioritized_orders[0]['allocation_reason'] == 'Allocated'