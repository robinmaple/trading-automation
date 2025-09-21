# src/core/advanced_feature_coordinator.py
"""
Coordinates advanced Phase B features including:
- Two-layer prioritization with market context integration
- Historical performance analysis and ML feature engineering  
- Outcome labeling for machine learning training
- Advanced order scoring and viability assessment
"""

from typing import Optional, Dict, Any, List, Tuple
import datetime
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
from src.services.prioritization_service import PrioritizationService
from src.services.outcome_labeling_service import OutcomeLabelingService
from src.services.position_sizing_service import PositionSizingService
from src.core.abstract_data_feed import AbstractDataFeed
from src.core.planned_order import PlannedOrder
from sqlalchemy.orm import Session


class AdvancedFeatureCoordinator:
    """Coordinates advanced Phase B features and services."""
    
    def __init__(self, enabled: bool = False):
        """Initialize the advanced feature coordinator."""
        self.enabled = enabled
        self.market_context_service: Optional[MarketContextService] = None
        self.historical_performance_service: Optional[HistoricalPerformanceService] = None
        self.prioritization_service: Optional[PrioritizationService] = None
        self.outcome_labeling_service: Optional[OutcomeLabelingService] = None
        self.initialized = False
        
    def initialize_services(self, data_feed: AbstractDataFeed, 
                          sizing_service: PositionSizingService,
                          db_session: Session,
                          prioritization_config: Dict[str, Any]) -> bool:
        """Initialize advanced services if enabled."""
        if not self.enabled:
            print("ℹ️  Advanced features disabled - skipping initialization")
            return False
            
        if self.initialized:
            print("ℹ️  Advanced services already initialized")
            return True
            
        try:
            print("🚀 Initializing advanced Phase B services...")
            
            # Initialize Market Context Service
            self.market_context_service = MarketContextService(data_feed)
            print("✅ Market context service initialized")
            
            # Initialize Historical Performance Service
            self.historical_performance_service = HistoricalPerformanceService()
            print("✅ Historical performance service initialized")
            
            # Initialize Outcome Labeling Service
            self.outcome_labeling_service = OutcomeLabelingService(db_session)
            print("✅ Outcome labeling service initialized")
            
            # Initialize Prioritization Service with advanced features
            self.prioritization_service = PrioritizationService(
                sizing_service=sizing_service,
                config=prioritization_config,
                market_context_service=self.market_context_service,
                historical_performance_service=self.historical_performance_service
            )
            print("✅ Advanced prioritization service initialized")
            
            self.initialized = True
            print("🎉 All advanced services initialized successfully")
            return True
            
        except ImportError as e:
            print(f"❌ Advanced service import failed: {e}")
            self._disable_advanced_features()
            return False
        except Exception as e:
            print(f"❌ Advanced services initialization failed: {e}")
            self._disable_advanced_features()
            return False
            
    def _disable_advanced_features(self) -> None:
        """Disable advanced features due to initialization failure."""
        self.enabled = False
        self.initialized = False
        self.market_context_service = None
        self.historical_performance_service = None
        self.prioritization_service = None
        self.outcome_labeling_service = None
        print("⚠️  Advanced features disabled due to initialization errors")
        
    def label_completed_orders(self, hours_back: int = 24) -> Dict[str, int]:
        """Label completed orders for ML training with comprehensive reporting."""
        if not self._check_labeling_ready():
            return {'total_orders': 0, 'labeled_orders': 0, 'labels_created': 0, 'errors': 0}
            
        try:
            print(f"🏷️  Labeling completed orders from last {hours_back} hours...")
            start_time = datetime.datetime.now()
            
            summary = self.outcome_labeling_service.label_completed_orders(hours_back)
            duration = (datetime.datetime.now() - start_time).total_seconds()
            
            print(f"✅ Labeling completed in {duration:.1f}s")
            print(f"   Total orders processed: {summary['total_orders']}")
            print(f"   Orders labeled: {summary['labeled_orders']}")
            print(f"   New labels created: {summary['labels_created']}")
            if summary['errors'] > 0:
                print(f"   ⚠️  Errors encountered: {summary['errors']}")
                
            return summary
            
        except Exception as e:
            print(f"❌ Order labeling failed: {e}")
            return {'total_orders': 0, 'labeled_orders': 0, 'labels_created': 0, 'errors': 1}
            
    def generate_training_data(self, output_path: str = "training_data.csv") -> bool:
        """Generate and export ML training data with feature engineering."""
        if not self._check_labeling_ready():
            return False
            
        try:
            print(f"📊 Generating training data for ML model...")
            start_time = datetime.datetime.now()
            
            success = self.outcome_labeling_service.export_training_data(output_path)
            duration = (datetime.datetime.now() - start_time).total_seconds()
            
            if success:
                print(f"✅ Training data exported to {output_path} in {duration:.1f}s")
            else:
                print(f"❌ Training data export failed after {duration:.1f}s")
                
            return success
            
        except Exception as e:
            print(f"❌ Training data generation failed: {e}")
            return False
            
    def enhance_order_prioritization(self, executable_orders: List[Dict], 
                                  total_capital: float, 
                                  working_orders: List[Dict]) -> List[Dict]:
        """Apply advanced prioritization with market context and historical analysis."""
        if not self._check_prioritization_ready():
            return executable_orders  # Fall back to basic prioritization
            
        try:
            enhanced_orders = self.prioritization_service.prioritize_orders(
                executable_orders, total_capital, working_orders
            )
            
            # Add advanced analytics metadata
            for order_data in enhanced_orders:
                order_data['advanced_features'] = self._get_advanced_features_metadata()
                
            return enhanced_orders
            
        except Exception as e:
            print(f"❌ Advanced prioritization failed, falling back to basic: {e}")
            return executable_orders
            
    def get_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict[str, Any]:
        """Get detailed summary of prioritization results with advanced metrics."""
        if not self._check_prioritization_ready():
            return self._get_basic_prioritization_summary(prioritized_orders)
            
        try:
            summary = self.prioritization_service.get_prioritization_summary(prioritized_orders)
            summary['advanced_features_enabled'] = True
            summary['market_context_available'] = self.market_context_service is not None
            summary['historical_data_available'] = self.historical_performance_service is not None
            return summary
            
        except Exception as e:
            print(f"❌ Advanced prioritization summary failed: {e}")
            return self._get_basic_prioritization_summary(prioritized_orders)
            
    def _get_basic_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict[str, Any]:
        """Fallback basic prioritization summary."""
        viable_orders = [o for o in prioritized_orders if o.get('viable', False)]
        allocated_orders = [o for o in viable_orders if o.get('allocated', False)]
        
        return {
            'total_orders': len(prioritized_orders),
            'total_viable': len(viable_orders),
            'total_allocated': len(allocated_orders),
            'total_non_viable': len(prioritized_orders) - len(viable_orders),
            'advanced_features_enabled': False,
            'market_context_available': False,
            'historical_data_available': False
        }
        
    def analyze_market_context(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get advanced market context analysis for a symbol."""
        if not self._check_market_context_ready():
            return None
            
        try:
            return self.market_context_service.analyze_symbol(symbol)
        except Exception as e:
            print(f"❌ Market context analysis failed for {symbol}: {e}")
            return None
            
    def get_historical_performance(self, symbol: str, lookback_days: int = 30) -> Optional[Dict[str, Any]]:
        """Get historical performance analysis for a symbol."""
        if not self._check_historical_performance_ready():
            return None
            
        try:
            return self.historical_performance_service.analyze_symbol_performance(symbol, lookback_days)
        except Exception as e:
            print(f"❌ Historical performance analysis failed for {symbol}: {e}")
            return None
            
    def get_feature_engineering_report(self) -> Dict[str, Any]:
        """Get report on available advanced features and their status."""
        return {
            'advanced_features_enabled': self.enabled,
            'services_initialized': self.initialized,
            'market_context_available': self.market_context_service is not None,
            'historical_performance_available': self.historical_performance_service is not None,
            'outcome_labeling_available': self.outcome_labeling_service is not None,
            'advanced_prioritization_available': self.prioritization_service is not None,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
    def _check_labeling_ready(self) -> bool:
        """Check if outcome labeling services are ready."""
        if not self.enabled or not self.initialized:
            return False
        if self.outcome_labeling_service is None:
            print("⚠️  Outcome labeling service not available")
            return False
        return True
        
    def _check_prioritization_ready(self) -> bool:
        """Check if advanced prioritization services are ready."""
        if not self.enabled or not self.initialized:
            return False
        if (self.prioritization_service is None or 
            self.market_context_service is None or 
            self.historical_performance_service is None):
            print("⚠️  Advanced prioritization services not fully available")
            return False
        return True
        
    def _check_market_context_ready(self) -> bool:
        """Check if market context services are ready."""
        if not self.enabled or not self.initialized:
            return False
        if self.market_context_service is None:
            print("⚠️  Market context service not available")
            return False
        return True
        
    def _check_historical_performance_ready(self) -> bool:
        """Check if historical performance services are ready."""
        if not self.enabled or not self.initialized:
            return False
        if self.historical_performance_service is None:
            print("⚠️  Historical performance service not available")
            return False
        return True
        
    def _get_advanced_features_metadata(self) -> Dict[str, Any]:
        """Get metadata about advanced features applied to orders."""
        return {
            'market_context_integration': self.market_context_service is not None,
            'historical_analysis': self.historical_performance_service is not None,
            'ml_ready_features': self.outcome_labeling_service is not None,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
    def precompute_features(self, orders: List[PlannedOrder]) -> List[Dict[str, Any]]:
        """Precompute advanced features for a batch of orders."""
        if not self._check_prioritization_ready():
            return []
            
        enhanced_orders = []
        for order in orders:
            try:
                features = {
                    'symbol': order.symbol,
                    'basic_features': {
                        'priority': order.priority,
                        'risk_reward': order.risk_reward_ratio,
                        'risk_per_trade': order.risk_per_trade,
                        'overall_trend_human': getattr(order, 'overall_trend', None),
                        'system_trend_score': getattr(order, 'system_trend_score', None),
                        'brief_analysis': getattr(order, 'brief_analysis', None),
                    },
                    'market_context': self.analyze_market_context(order.symbol),
                    'historical_performance': self.get_historical_performance(order.symbol),
                    'computed_at': datetime.datetime.now().isoformat()
                }
                enhanced_orders.append(features)
            except Exception as e:
                print(f"❌ Feature computation failed for {order.symbol}: {e}")
                enhanced_orders.append({
                    'symbol': order.symbol,
                    'error': str(e),
                    'computed_at': datetime.datetime.now().isoformat()
                })
                
        return enhanced_orders
        
    def validate_advanced_configuration(self) -> Tuple[bool, List[str]]:
        """Validate that advanced features are properly configured."""
        issues = []
        
        if not self.enabled:
            return True, ["Advanced features are disabled"]
            
        if not self.initialized:
            issues.append("Advanced services not initialized")
            
        if self.market_context_service is None:
            issues.append("Market context service not available")
            
        if self.historical_performance_service is None:
            issues.append("Historical performance service not available")
            
        if self.outcome_labeling_service is None:
            issues.append("Outcome labeling service not available")
            
        if self.prioritization_service is None:
            issues.append("Prioritization service not available")
            
        return len(issues) == 0, issues
        
    def shutdown(self) -> None:
        """Cleanup and shutdown advanced services."""
        if self.market_context_service:
            try:
                self.market_context_service.cleanup()
            except Exception:
                pass
                
        if self.historical_performance_service:
            try:
                self.historical_performance_service.cleanup()
            except Exception:
                pass
                
        print("✅ Advanced features coordinator shutdown complete")