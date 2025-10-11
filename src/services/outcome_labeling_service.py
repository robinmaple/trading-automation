"""
Outcome Labeling Service for Phase B - Generates ML training labels from execution results.
Creates labeled datasets for fill probability and trade outcome models.
"""

import datetime
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from src.core.models import OrderLabelDB, ExecutedOrderDB, PlannedOrderDB, ProbabilityScoreDB

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

class OutcomeLabelingService:
    """
    Service responsible for generating ML training labels from order execution outcomes.
    Creates immutable labels for model training and performance analysis.
    """

    def __init__(self, db_session: Session):
        """
        Initialize the labeling service with a database session.
        
        Args:
            db_session: SQLAlchemy database session for data access
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing OutcomeLabelingService",
            context_provider={
                "db_session_provided": db_session is not None,
                "db_session_type": type(db_session).__name__
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.db_session = db_session
    
    def label_completed_orders(self, hours_back: int = 24) -> Dict:
        """
        Label all completed orders from the specified time period.
        
        Args:
            hours_back: Number of hours to look back for completed orders
            
        Returns:
            Summary of labeling results
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Starting batch labeling of completed orders",
            context_provider={
                "hours_back": hours_back,
                "cutoff_time": (datetime.datetime.now() - datetime.timedelta(hours=hours_back)).isoformat()
            }
        )
        # <Context-Aware Logging Integration - End>
        
        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=hours_back)
        
        # Find executed orders that haven't been labeled yet
        executed_orders = self.db_session.query(ExecutedOrderDB).filter(
            ExecutedOrderDB.executed_at >= cutoff_time,
            ExecutedOrderDB.status == 'FILLED'
        ).all()
        
        summary = {
            'total_orders': len(executed_orders),
            'labeled_orders': 0,
            'labels_created': 0,
            'errors': 0
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Found {len(executed_orders)} executed orders to label",
            context_provider={
                "total_orders": len(executed_orders),
                "cutoff_time": cutoff_time.isoformat()
            }
        )
        # <Context-Aware Logging Integration - End>
        
        for executed_order in executed_orders:
            try:
                labels_created = self._label_single_order(executed_order)
                summary['labeled_orders'] += 1
                summary['labels_created'] += labels_created
                
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Successfully labeled order {executed_order.id}",
                    context_provider={
                        "executed_order_id": executed_order.id,
                        "labels_created": labels_created,
                        "planned_order_id": executed_order.planned_order_id
                    }
                )
                # <Context-Aware Logging Integration - End>
                
            except Exception as e:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Error labeling order {executed_order.id}",
                    context_provider={
                        "executed_order_id": executed_order.id,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason="Order labeling failed"
                )
                # <Context-Aware Logging Integration - End>
                summary['errors'] += 1
                continue
        
        self.db_session.commit()
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Batch labeling completed",
            context_provider=summary,
            decision_reason="Labeling process finished"
        )
        # <Context-Aware Logging Integration - End>
        
        return summary
    
    def _label_single_order(self, executed_order: ExecutedOrderDB) -> int:
        """
        Generate labels for a single executed order.
        
        Args:
            executed_order: The executed order to label
            
        Returns:
            Number of labels created
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Labeling single order {executed_order.id}",
            context_provider={
                "executed_order_id": executed_order.id,
                "planned_order_id": executed_order.planned_order_id,
                "executed_at": executed_order.executed_at.isoformat() if executed_order.executed_at else None,
                "filled_price": executed_order.filled_price,
                "status": executed_order.status
            }
        )
        # <Context-Aware Logging Integration - End>
        
        labels_created = 0
        
        # Get the associated planned order
        planned_order = self.db_session.query(PlannedOrderDB).filter_by(
            id=executed_order.planned_order_id
        ).first()
        
        if not planned_order:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"No planned order found for executed order {executed_order.id}",
                context_provider={
                    "executed_order_id": executed_order.id,
                    "planned_order_id": executed_order.planned_order_id
                },
                decision_reason="Skipping labeling - no planned order"
            )
            # <Context-Aware Logging Integration - End>
            return 0
        
        # Get the probability score for this order (if exists)
        probability_score = self.db_session.query(ProbabilityScoreDB).filter_by(
            planned_order_id=planned_order.id
        ).order_by(ProbabilityScoreDB.timestamp.desc()).first()
        
        # Label 1: Binary fill outcome (always True for executed orders)
        fill_label = self._create_label(
            planned_order.id, 'filled_binary', 1.0, 
            "Order was successfully filled"
        )
        if fill_label:
            labels_created += 1
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Created fill label for order {planned_order.id}",
                context_provider={
                    "planned_order_id": planned_order.id,
                    "label_type": 'filled_binary',
                    "label_value": 1.0
                }
            )
            # <Context-Aware Logging Integration - End>
        
        # Label 2: Time-to-fill (if we have order creation time)
        if planned_order.created_at and executed_order.executed_at:
            time_to_fill = (executed_order.executed_at - planned_order.created_at).total_seconds()
            time_label = self._create_label(
                planned_order.id, 'time_to_fill', time_to_fill,
                f"Time from order creation to execution: {time_to_fill:.1f} seconds"
            )
            if time_label:
                labels_created += 1
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Created time-to-fill label for order {planned_order.id}",
                    context_provider={
                        "planned_order_id": planned_order.id,
                        "label_type": 'time_to_fill',
                        "label_value": time_to_fill,
                        "time_to_fill_seconds": time_to_fill
                    }
                )
                # <Context-Aware Logging Integration - End>
        
        # Label 3: Fill quality (slippage)
        if planned_order.entry_price and executed_order.filled_price:
            slippage = self._calculate_slippage(planned_order, executed_order)
            slippage_label = self._create_label(
                planned_order.id, 'slippage', slippage,
                f"Slippage: {slippage:.4f} ({'favorable' if slippage < 0 else 'unfavorable'})"
            )
            if slippage_label:
                labels_created += 1
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Created slippage label for order {planned_order.id}",
                    context_provider={
                        "planned_order_id": planned_order.id,
                        "label_type": 'slippage',
                        "label_value": slippage,
                        "slippage_amount": slippage,
                        "slippage_direction": 'favorable' if slippage < 0 else 'unfavorable'
                    }
                )
                # <Context-Aware Logging Integration - End>
        
        # Label 4: Quick profitability check (if position already closed)
        if executed_order.closed_at and executed_order.pnl is not None:
            profitability = 1.0 if executed_order.pnl > 0 else 0.0
            profit_label = self._create_label(
                planned_order.id, 'profitability', profitability,
                f"Profitable: {executed_order.pnl:.2f} PnL"
            )
            if profit_label:
                labels_created += 1
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Created profitability label for order {planned_order.id}",
                    context_provider={
                        "planned_order_id": planned_order.id,
                        "label_type": 'profitability',
                        "label_value": profitability,
                        "pnl": executed_order.pnl,
                        "profitable": profitability == 1.0
                    }
                )
                # <Context-Aware Logging Integration - End>
        
        # Label 5: Fill probability accuracy (if we have probability score)
        if probability_score:
            accuracy_label = self._create_label(
                planned_order.id, 'probability_accuracy', 1.0 if probability_score.fill_probability >= 0.7 else 0.0,
                f"High probability prediction was {'' if probability_score.fill_probability >= 0.7 else 'not '}accurate"
            )
            if accuracy_label:
                labels_created += 1
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Created probability accuracy label for order {planned_order.id}",
                    context_provider={
                        "planned_order_id": planned_order.id,
                        "label_type": 'probability_accuracy',
                        "label_value": 1.0 if probability_score.fill_probability >= 0.7 else 0.0,
                        "probability_score": probability_score.fill_probability,
                        "accuracy_threshold": 0.7,
                        "prediction_accurate": probability_score.fill_probability >= 0.7
                    }
                )
                # <Context-Aware Logging Integration - End>
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Completed labeling for order {planned_order.id}",
            context_provider={
                "planned_order_id": planned_order.id,
                "total_labels_created": labels_created,
                "probability_score_available": probability_score is not None
            },
            decision_reason="Single order labeling completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return labels_created
    
    def _calculate_slippage(self, planned_order: PlannedOrderDB, executed_order: ExecutedOrderDB) -> float:
        """
        Calculate slippage for an order.
        
        Args:
            planned_order: The planned order with entry price
            executed_order: The executed order with fill price
            
        Returns:
            Slippage amount (positive = unfavorable, negative = favorable)
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating slippage for order {planned_order.id}",
            context_provider={
                "planned_order_id": planned_order.id,
                "action": planned_order.action,
                "planned_price": planned_order.entry_price,
                "executed_price": executed_order.filled_price
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if planned_order.action == 'BUY':
            # For BUY orders: positive slippage = paid more than expected
            slippage = executed_order.filled_price - planned_order.entry_price
        else:
            # For SELL orders: positive slippage = received less than expected  
            slippage = planned_order.entry_price - executed_order.filled_price
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Slippage calculation completed for order {planned_order.id}",
            context_provider={
                "planned_order_id": planned_order.id,
                "slippage_amount": slippage,
                "slippage_direction": 'favorable' if slippage < 0 else 'unfavorable',
                "action": planned_order.action
            },
            decision_reason="Slippage calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        
        return slippage
    
    def _create_label(self, planned_order_id: int, label_type: str, label_value: float, 
                     notes: Optional[str] = None) -> Optional[OrderLabelDB]:
        """
        Create a label record in the database.
        
        Args:
            planned_order_id: ID of the planned order
            label_type: Type of label (filled_binary, time_to_fill, etc.)
            label_value: Numeric value of the label
            notes: Optional notes about the label
            
        Returns:
            The created label object or None if failed
        """
        try:
            # Check if this label already exists to avoid duplicates
            existing_label = self.db_session.query(OrderLabelDB).filter_by(
                planned_order_id=planned_order_id,
                label_type=label_type
            ).first()
            
            if existing_label:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Label already exists for order {planned_order_id}",
                    context_provider={
                        "planned_order_id": planned_order_id,
                        "label_type": label_type,
                        "existing_label_value": existing_label.label_value
                    },
                    decision_reason="Skipping duplicate label creation"
                )
                # <Context-Aware Logging Integration - End>
                return None  # Label already exists, don't create duplicate
            
            label = OrderLabelDB(
                planned_order_id=planned_order_id,
                label_type=label_type,
                label_value=label_value,
                computed_at=datetime.datetime.now(),
                notes=notes
            )
            
            self.db_session.add(label)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Created new label for order {planned_order_id}",
                context_provider={
                    "planned_order_id": planned_order_id,
                    "label_type": label_type,
                    "label_value": label_value,
                    "notes": notes
                },
                decision_reason="Label created successfully"
            )
            # <Context-Aware Logging Integration - End>
            
            return label
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error creating label for order {planned_order_id}",
                context_provider={
                    "planned_order_id": planned_order_id,
                    "label_type": label_type,
                    "label_value": label_value,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Label creation failed"
            )
            # <Context-Aware Logging Integration - End>
            return None
    
    def get_labeled_data(self, label_type: str, hours_back: int = 168) -> List[Dict]:
        """
        Get labeled data for analysis or model training.
        
        Args:
            label_type: Type of labels to retrieve
            hours_back: How far back to look for labels (default 1 week)
            
        Returns:
            List of labeled data points with features and labels
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Retrieving labeled data for {label_type}",
            context_provider={
                "label_type": label_type,
                "hours_back": hours_back,
                "cutoff_time": (datetime.datetime.now() - datetime.timedelta(hours=hours_back)).isoformat()
            }
        )
        # <Context-Aware Logging Integration - End>
        
        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=hours_back)
        
        labels = self.db_session.query(OrderLabelDB).filter(
            OrderLabelDB.label_type == label_type,
            OrderLabelDB.computed_at >= cutoff_time
        ).all()
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Found {len(labels)} labels of type {label_type}",
            context_provider={
                "label_type": label_type,
                "labels_found": len(labels),
                "cutoff_time": cutoff_time.isoformat()
            }
        )
        # <Context-Aware Logging Integration - End>
        
        labeled_data = []
        
        for label in labels:
            # Get the probability score features for this order
            probability_score = self.db_session.query(ProbabilityScoreDB).filter_by(
                planned_order_id=label.planned_order_id
            ).order_by(ProbabilityScoreDB.timestamp.desc()).first()
            
            if probability_score and probability_score.features:
                data_point = {
                    'label_value': label.label_value,
                    'label_type': label.label_type,
                    'planned_order_id': label.planned_order_id,
                    'computed_at': label.computed_at,
                    'features': probability_score.features,
                    'notes': label.notes
                }
                labeled_data.append(data_point)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Retrieved {len(labeled_data)} labeled data points with features",
            context_provider={
                "label_type": label_type,
                "labeled_data_points": len(labeled_data),
                "labels_without_features": len(labels) - len(labeled_data)
            },
            decision_reason="Labeled data retrieval completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return labeled_data
    
    def export_training_data(self, output_path: str, label_types: List[str] = None) -> bool:
        """
        Export labeled data to CSV for model training.
        
        Args:
            output_path: Path to save the CSV file
            label_types: List of label types to export (None for all)
            
        Returns:
            True if successful, False otherwise
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Starting training data export to {output_path}",
            context_provider={
                "output_path": output_path,
                "label_types": label_types,
                "label_types_provided": label_types is not None
            }
        )
        # <Context-Aware Logging Integration - End>
        
        try:
            import csv
            
            if label_types is None:
                label_types = ['filled_binary', 'time_to_fill', 'slippage', 'profitability']
            
            all_data = []
            for label_type in label_types:
                label_data = self.get_labeled_data(label_type)
                all_data.extend(label_data)
                
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Retrieved {len(label_data)} data points for label type {label_type}",
                    context_provider={
                        "label_type": label_type,
                        "data_points": len(label_data)
                    }
                )
                # <Context-Aware Logging Integration - End>
            
            if not all_data:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No labeled data found for export",
                    context_provider={
                        "label_types": label_types
                    },
                    decision_reason="Export cancelled - no data"
                )
                # <Context-Aware Logging Integration - End>
                return False
            
            # Write to CSV
            with open(output_path, 'w', newline='') as csvfile:
                fieldnames = ['label_type', 'label_value', 'planned_order_id', 'computed_at'] + \
                           list(all_data[0]['features'].keys()) + ['notes']
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for data_point in all_data:
                    row = {
                        'label_type': data_point['label_type'],
                        'label_value': data_point['label_value'],
                        'planned_order_id': data_point['planned_order_id'],
                        'computed_at': data_point['computed_at'].isoformat(),
                        'notes': data_point['notes']
                    }
                    row.update(data_point['features'])
                    writer.writerow(row)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Successfully exported training data to {output_path}",
                context_provider={
                    "output_path": output_path,
                    "total_data_points": len(all_data),
                    "label_types_exported": label_types,
                    "file_size_bytes": len(str(all_data))  # Approximate size
                },
                decision_reason="Training data export completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return True
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error exporting training data to {output_path}",
                context_provider={
                    "output_path": output_path,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Training data export failed"
            )
            # <Context-Aware Logging Integration - End>
            return False


# Utility function for easy access
def label_recent_orders(db_session: Session, hours_back: int = 24) -> Dict:
    """
    Convenience function to label recent orders.
    
    Args:
        db_session: Database session
        hours_back: Hours to look back for orders to label
        
    Returns:
        Labeling summary
    """
    # <Context-Aware Logging Integration - Begin>
    context_logger = get_context_logger()
    context_logger.log_event(
        TradingEventType.SYSTEM_HEALTH,
        "Starting convenience labeling of recent orders",
        context_provider={
            "hours_back": hours_back,
            "db_session_provided": db_session is not None
        }
    )
    # <Context-Aware Logging Integration - End>
    
    service = OutcomeLabelingService(db_session)
    result = service.label_completed_orders(hours_back)
    
    # <Context-Aware Logging Integration - Begin>
    context_logger.log_event(
        TradingEventType.SYSTEM_HEALTH,
        "Convenience labeling completed",
        context_provider=result,
        decision_reason="Convenience labeling process finished"
    )
    # <Context-Aware Logging Integration - End>
    
    return result