"""
Unit tests for the OutcomeLabelingService Phase B ML label generation.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from src.services.outcome_labeling_service import OutcomeLabelingService, label_recent_orders
from src.core.models import OrderLabelDB, ExecutedOrderDB, PlannedOrderDB, ProbabilityScoreDB, PositionStrategy


class TestOutcomeLabelingService:
    """Test suite for OutcomeLabelingService Phase B features."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Provide a mock database session for testing."""
        session = Mock()
        session.query = Mock()
        session.add = Mock()
        session.commit = Mock()
        return session
    
    @pytest.fixture
    def labeling_service(self, mock_db_session):
        """Provide a labeling service instance for testing."""
        return OutcomeLabelingService(mock_db_session)
    
    @pytest.fixture
    def sample_executed_order(self):
        """Create a sample executed order for testing."""
        order = Mock(spec=ExecutedOrderDB)
        order.id = 1
        order.planned_order_id = 100
        order.status = 'FILLED'
        order.executed_at = datetime.now() - timedelta(hours=1)
        order.filled_price = 150.25
        order.filled_quantity = 100
        order.commission = 1.50
        order.pnl = 250.0
        order.closed_at = datetime.now() - timedelta(minutes=30)
        return order
    
    @pytest.fixture
    def sample_planned_order(self):
        """Create a sample planned order for testing."""
        order = Mock(spec=PlannedOrderDB)
        order.id = 100
        order.symbol = 'AAPL'
        order.action = 'BUY'
        order.entry_price = 150.00
        order.stop_loss = 145.00
        order.created_at = datetime.now() - timedelta(hours=2)
        return order
    
    @pytest.fixture
    def sample_probability_score(self):
        """Create a sample probability score for testing."""
        score = Mock(spec=ProbabilityScoreDB)
        score.planned_order_id = 100
        score.fill_probability = 0.85
        score.timestamp = datetime.now() - timedelta(hours=3)
        score.features = {
            'current_price': 150.10,
            'spread_absolute': 0.15,
            'time_of_day_seconds': 34200,
            'priority_manual': 3
        }
        return score
    
    def test_label_completed_orders_no_orders(self, labeling_service, mock_db_session):
        """Test labeling when no completed orders are found."""
        # Mock empty result
        mock_db_session.query.return_value.filter.return_value.all.return_value = []
        
        summary = labeling_service.label_completed_orders(hours_back=24)
        
        assert summary['total_orders'] == 0
        assert summary['labeled_orders'] == 0
        assert summary['labels_created'] == 0
        assert summary['errors'] == 0
    
    def test_label_completed_orders_with_orders(self, labeling_service, mock_db_session, 
                                              sample_executed_order, sample_planned_order):
        """Test labeling with completed orders."""
        # Mock database responses
        mock_db_session.query.return_value.filter.return_value.all.return_value = [sample_executed_order]
        mock_db_session.query.return_value.filter_by.return_value.first.return_value = sample_planned_order
        mock_db_session.query.return_value.order_by.return_value.first.return_value = None  # No probability score
        
        summary = labeling_service.label_completed_orders(hours_back=24)
        
        assert summary['total_orders'] == 1
        assert summary['labeled_orders'] == 1
        assert summary['labels_created'] > 0  # Should create at least binary fill label
        assert summary['errors'] == 0
        
        # Should have called add for each label created
        assert mock_db_session.add.call_count == summary['labels_created']
        mock_db_session.commit.assert_called_once()
    
    def test_label_single_order_all_labels(self, labeling_service, mock_db_session,
                                         sample_executed_order, sample_planned_order, sample_probability_score):
        """Test labeling a single order with all possible labels."""
        # Mock database responses
        mock_db_session.query.return_value.filter_by.return_value.first.return_value = sample_planned_order
        mock_db_session.query.return_value.order_by.return_value.first.return_value = sample_probability_score
        
        labels_created = labeling_service._label_single_order(sample_executed_order)
        
        # Should create multiple labels: binary fill, time-to-fill, slippage, profitability, accuracy
        assert labels_created >= 4
        assert mock_db_session.add.call_count == labels_created
        
        # Verify all expected label types were created
        added_labels = [call[0][0] for call in mock_db_session.add.call_args_list]
        label_types = [label.label_type for label in added_labels if hasattr(label, 'label_type')]
        
        assert 'filled_binary' in label_types
        assert 'time_to_fill' in label_types
        assert 'slippage' in label_types
        assert 'profitability' in label_types
    
    def test_label_single_order_no_planned_order(self, labeling_service, mock_db_session, sample_executed_order):
        """Test labeling when no planned order is found."""
        # Mock no planned order found
        mock_db_session.query.return_value.filter_by.return_value.first.return_value = None
        
        labels_created = labeling_service._label_single_order(sample_executed_order)
        
        assert labels_created == 0
        mock_db_session.add.assert_not_called()
    
    def test_calculate_slippage_buy_order(self, labeling_service):
        """Test slippage calculation for BUY orders."""
        planned_order = Mock()
        planned_order.action = 'BUY'
        planned_order.entry_price = 150.00
        
        executed_order = Mock()
        executed_order.filled_price = 150.25  # Paid more than expected
        
        slippage = labeling_service._calculate_slippage(planned_order, executed_order)
        
        # For BUY: positive slippage = paid more than expected (unfavorable)
        assert slippage == 0.25
    
    def test_calculate_slippage_sell_order(self, labeling_service):
        """Test slippage calculation for SELL orders."""
        planned_order = Mock()
        planned_order.action = 'SELL'
        planned_order.entry_price = 150.00
        
        executed_order = Mock()
        executed_order.filled_price = 149.75  # Received less than expected
        
        slippage = labeling_service._calculate_slippage(planned_order, executed_order)
        
        # For SELL: positive slippage = received less than expected (unfavorable)
        assert slippage == 0.25
    
    def test_create_label_success(self, labeling_service, mock_db_session):
        """Test successful label creation."""
        label = labeling_service._create_label(100, 'filled_binary', 1.0, "Test label")
        
        assert label is not None
        mock_db_session.add.assert_called_once()
        assert label.planned_order_id == 100
        assert label.label_type == 'filled_binary'
        assert label.label_value == 1.0
    
    def test_create_label_duplicate(self, labeling_service, mock_db_session):
        """Test label creation when duplicate exists."""
        # Mock existing label
        existing_label = Mock(spec=OrderLabelDB)
        mock_db_session.query.return_value.filter_by.return_value.first.return_value = existing_label
        
        label = labeling_service._create_label(100, 'filled_binary', 1.0, "Test label")
        
        assert label is None  # Should not create duplicate
        mock_db_session.add.assert_not_called()
    
    def test_get_labeled_data_no_labels(self, labeling_service, mock_db_session):
        """Test getting labeled data when no labels exist."""
        mock_db_session.query.return_value.filter.return_value.all.return_value = []
        
        labeled_data = labeling_service.get_labeled_data('filled_binary', hours_back=24)
        
        assert len(labeled_data) == 0
    
    def test_get_labeled_data_with_labels(self, labeling_service, mock_db_session, sample_probability_score):
        """Test getting labeled data with existing labels."""
        # Mock label and probability score
        label = Mock(spec=OrderLabelDB)
        label.label_type = 'filled_binary'
        label.label_value = 1.0
        label.planned_order_id = 100
        label.computed_at = datetime.now()
        label.notes = "Test label"
        
        mock_db_session.query.return_value.filter.return_value.all.return_value = [label]
        mock_db_session.query.return_value.order_by.return_value.first.return_value = sample_probability_score
        
        labeled_data = labeling_service.get_labeled_data('filled_binary', hours_back=24)
        
        assert len(labeled_data) == 1
        assert labeled_data[0]['label_value'] == 1.0
        assert 'features' in labeled_data[0]
        assert labeled_data[0]['features']['current_price'] == 150.10
    
    def test_export_training_data_success(self, labeling_service, mock_db_session):
        """Test successful training data export."""
        # Mock labeled data
        labeled_data = [{
            'label_type': 'filled_binary',
            'label_value': 1.0,
            'planned_order_id': 100,
            'computed_at': datetime.now(),
            'features': {'current_price': 150.10, 'spread_absolute': 0.15},
            'notes': 'Test data'
        }]
        
        with patch.object(labeling_service, 'get_labeled_data', return_value=labeled_data):
            with patch('builtins.open', Mock()) as mock_open:
                with patch('csv.DictWriter') as mock_writer:
                    mock_writer_instance = Mock()
                    mock_writer.return_value = mock_writer_instance
                    
                    success = labeling_service.export_training_data('/tmp/test_data.csv')
                    
                    assert success is True
                    mock_writer.assert_called_once()
                    mock_writer_instance.writeheader.assert_called_once()
                    mock_writer_instance.writerow.assert_called_once()
    
    def test_export_training_data_no_data(self, labeling_service):
        """Test training data export when no data exists."""
        with patch.object(labeling_service, 'get_labeled_data', return_value=[]):
            success = labeling_service.export_training_data('/tmp/test_data.csv')
            
            assert success is False
    
    def test_export_training_data_error(self, labeling_service):
        """Test training data export with error."""
        with patch.object(labeling_service, 'get_labeled_data', side_effect=Exception("Test error")):
            success = labeling_service.export_training_data('/tmp/test_data.csv')
            
            assert success is False


def test_label_recent_orders_convenience_function(mock_db_session):
    """Test the convenience function for labeling recent orders."""
    with patch('src.services.outcome_labeling_service.OutcomeLabelingService') as mock_service:
        mock_instance = Mock()
        mock_instance.label_completed_orders.return_value = {'total_orders': 5, 'labeled_orders': 5}
        mock_service.return_value = mock_instance
        
        result = label_recent_orders(mock_db_session, hours_back=12)
        
        assert result['total_orders'] == 5
        mock_service.assert_called_once_with(mock_db_session)
        mock_instance.label_completed_orders.assert_called_once_with(12)


def test_order_label_db_model_validation():
    """Test that OrderLabelDB model validates label types correctly."""
    # Test valid label types
    valid_labels = ['filled_binary', 'time_to_fill', 'profitability', 'slippage', 'probability_accuracy']
    
    for label_type in valid_labels:
        label = OrderLabelDB(
            planned_order_id=1,
            label_type=label_type,
            label_value=1.0,
            computed_at=datetime.now()
        )
        # Should not raise validation errors
        assert label.label_type == label_type


def test_labeling_error_handling(labeling_service, mock_db_session, sample_executed_order):
    """Test error handling during labeling."""
    # Mock database error during commit
    mock_db_session.commit.side_effect = Exception("Database error")
    mock_db_session.query.return_value.filter_by.return_value.first.return_value = Mock()
    
    labels_created = labeling_service._label_single_order(sample_executed_order)
    
    # Should handle error gracefully and return 0 labels created
    assert labels_created == 0

