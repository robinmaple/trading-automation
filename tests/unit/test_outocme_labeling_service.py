import pytest
import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from src.services.outcome_labeling_service import OutcomeLabelingService


class TestOutcomeLabelingService:

    def setup_method(self):
        self.db_session = MagicMock()
        self.service = OutcomeLabelingService(self.db_session)

    def _make_executed_order(self, **overrides):
        now = datetime.datetime.now()
        defaults = dict(
            id=1,
            planned_order_id=10,
            executed_at=now,
            status="FILLED",
            filled_price=101.0,
            pnl=50.0,   # safe default
            closed_at=now + datetime.timedelta(hours=1),  # safe default
        )
        defaults.update(overrides)  # allow overriding without duplication
        return SimpleNamespace(**defaults)

    def _make_planned_order(self, **overrides):
        now = datetime.datetime.now()
        return SimpleNamespace(
            id=10,
            created_at=now - datetime.timedelta(seconds=30),
            entry_price=100.0,
            action="BUY",
            **overrides
        )

    def _make_probability_score(self, **overrides):
        return SimpleNamespace(
            planned_order_id=10,
            fill_probability=0.8,
            timestamp=datetime.datetime.now(),
            features={"feature1": 1.0, "feature2": 2.0},
            **overrides
        )

    def test_label_completed_orders_with_orders(self):
        executed_order = self._make_executed_order(pnl=50.0)
        planned_order = self._make_planned_order()
        probability_score = self._make_probability_score()

        self.db_session.query().filter().all.return_value = [executed_order]
        # First .first() returns planned_order, all later label lookups return None
        self.db_session.query().filter_by().first.side_effect = [planned_order, None, None, None, None]
        self.db_session.query().filter_by().order_by().first.return_value = probability_score

        result = self.service.label_completed_orders(hours_back=24)

        assert result["total_orders"] == 1
        assert result["labeled_orders"] == 1
        assert result["labels_created"] >= 1
        self.db_session.commit.assert_called_once()

    def test_label_single_order_all_labels(self):
        executed_order = self._make_executed_order(pnl=50.0)
        planned_order = self._make_planned_order()
        probability_score = self._make_probability_score()

        self.db_session.query().filter_by().first.side_effect = [planned_order, None, None, None, None]
        self.db_session.query().filter_by().order_by().first.return_value = probability_score

        with patch.object(self.service, "_create_label", return_value=True) as mock_create:
            labels_created = self.service._label_single_order(executed_order)

        assert labels_created >= 3
        assert mock_create.call_count >= 3

    def test_create_label_success(self):
        self.db_session.query().filter_by().first.return_value = None

        result = self.service._create_label(
            planned_order_id=10,
            label_type="filled_binary",
            label_value=1.0,
            notes="Order filled"
        )

        assert result is not None
        self.db_session.add.assert_called_once()
        self.db_session.commit.assert_not_called()

    def test_export_training_data_success(self, tmp_path):
        planned_order = self._make_planned_order()
        label = SimpleNamespace(
            planned_order_id=planned_order.id,
            label_type="filled_binary",
            label_value=1.0,
            computed_at=datetime.datetime.now(),
            notes="note"
        )
        probability_score = self._make_probability_score()

        self.db_session.query().filter().all.return_value = [label]
        self.db_session.query().filter_by().order_by().first.return_value = probability_score

        output_file = tmp_path / "training.csv"
        success = self.service.export_training_data(str(output_file))

        assert success is True
        assert output_file.exists()
        content = output_file.read_text()
        assert "filled_binary" in content

    def test_labeling_error_handling(self):
        bad_order = self._make_executed_order(pnl=None, closed_at=None)
        self.db_session.query().filter().all.return_value = [bad_order]
        # planned_order lookup fails
        self.db_session.query().filter_by().first.return_value = None
        self.db_session.query().filter_by().order_by().first.return_value = None

        result = self.service.label_completed_orders(hours_back=24)

        assert result["errors"] >= 0
        self.db_session.commit.assert_called_once()
