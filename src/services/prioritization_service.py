"""
Prioritization Service for Phase B - Implements deterministic scoring and capital allocation.
Combines fill probability, manual priority, capital efficiency, and other factors to rank orders.
"""

from typing import List, Dict, Optional, Tuple
import datetime
from src.core.planned_order import PlannedOrder
from src.services.position_sizing_service import PositionSizingService

# <Advanced Feature Integration - Begin>
# New imports for advanced features
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
# <Advanced Feature Integration - End>


class PrioritizationService:
    """
    Service responsible for ranking and allocating capital to executable orders.
    Implements Phase B deterministic scoring algorithm with configurable weights.
    """

    def __init__(self, sizing_service: PositionSizingService, config: Optional[Dict] = None,
                 market_context_service: Optional[MarketContextService] = None,
                 historical_performance_service: Optional[HistoricalPerformanceService] = None):
        """
        Initialize the prioritization service with a sizing service and configuration.
        
        Args:
            sizing_service: Service for calculating position sizes and capital commitment
            config: Configuration dictionary with weights and parameters
            market_context_service: Service for market context analysis (advanced feature)
            historical_performance_service: Service for historical performance data (advanced feature)
        """
        self.sizing_service = sizing_service
        self.config = config or self._get_default_config()
        
        # <Advanced Feature Integration - Begin>
        # Store advanced feature services
        self.market_context_service = market_context_service
        self.historical_performance_service = historical_performance_service
        # <Advanced Feature Integration - End>
        
    def _get_default_config(self) -> Dict:
        """Get conservative default weights as specified in Phase B requirements."""
        return {
            'weights': {
                'fill_prob': 0.45,      # Fill probability importance
                'manual_priority': 0.20, # Manual priority importance  
                'efficiency': 0.15,      # Capital efficiency importance
                'size_pref': 0.10,       # Size preference (smaller positions)
                'timeframe_match': 0.08, # Timeframe matching
                'setup_bias': 0.02       # Setup bias
            },
            'max_open_orders': 5,        # Maximum number of open orders
            'max_capital_utilization': .8,  # Maximum fraction of capital to commit
            # <Advanced Feature Integration - Begin>
            'enable_advanced_features': False,  # Toggle for advanced features
            'setup_performance_thresholds': {
                'min_trades_for_bias': 10,
                'min_win_rate': 0.4,
                'min_profit_factor': 1.2
            }
            # <Advanced Feature Integration - End>
        }
    
    def calculate_efficiency(self, order: PlannedOrder, total_capital: float) -> float:
        """
        Calculate capital efficiency (reward per committed dollar).
        
        Args:
            order: The planned order to evaluate
            total_capital: Total available capital for sizing
            
        Returns:
            Efficiency score (higher is better)
        """
        if order.entry_price is None or order.stop_loss is None:
            return 0.0
            
        try:
            # Calculate position size and capital commitment
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            
            if capital_commitment <= 0:
                return 0.0
                
            # Calculate expected profit
            if order.action.value == 'BUY':
                profit_target = order.entry_price + (order.entry_price - order.stop_loss) * order.risk_reward_ratio
                expected_profit_per_share = profit_target - order.entry_price
            else:  # SELL
                profit_target = order.entry_price - (order.stop_loss - order.entry_price) * order.risk_reward_ratio
                expected_profit_per_share = order.entry_price - profit_target
                
            expected_profit_total = expected_profit_per_share * quantity
            
            # Capital efficiency = expected profit / capital commitment
            efficiency = expected_profit_total / capital_commitment
            
            return max(0.0, efficiency)
            
        except (ValueError, ZeroDivisionError):
            return 0.0
    
    # <Advanced Feature Integration - Begin>
    def calculate_timeframe_match_score(self, order: PlannedOrder) -> float:
        """
        Calculate how well the order's timeframe matches current market conditions.
        
        Args:
            order: The planned order to evaluate
            
        Returns:
            Timeframe match score (0-1, higher is better)
        """
        if not self.config.get('enable_advanced_features', False) or not self.market_context_service:
            return 0.5  # Default neutral score if advanced features disabled or service unavailable
            
        try:
            dominant_timeframe = self.market_context_service.get_dominant_timeframe(order.symbol)
            order_timeframe = order.core_timeframe
            
            if order_timeframe == dominant_timeframe:
                return 1.0  # Perfect match
            
            # Check timeframe compatibility from config
            compatible_timeframes = self.config.get('timeframe_compatibility_map', {}).get(
                dominant_timeframe, []
            )
            if order_timeframe in compatible_timeframes:
                return 0.7  # Compatible timeframes
                
            return 0.3  # Mismatched timeframes
            
        except Exception as e:
            print(f"Error calculating timeframe match for {order.symbol}: {e}")
            return 0.5  # Fallback value
    
    def calculate_setup_bias_score(self, order: PlannedOrder) -> float:
        """
        Calculate bias score based on historical performance of this trading setup.
        
        Args:
            order: The planned order to evaluate
            
        Returns:
            Setup bias score (0-1, higher is better)
        """
        if not self.config.get('enable_advanced_features', False) or not self.historical_performance_service:
            return 0.5  # Default neutral score if advanced features disabled or service unavailable
            
        try:
            setup_name = order.trading_setup
            if not setup_name:
                return 0.5
                
            performance = self.historical_performance_service.get_setup_performance(setup_name)
            
            if not performance:
                return 0.5  # No historical data
                
            # Check minimum thresholds
            thresholds = self.config.get('setup_performance_thresholds', {})
            min_trades = thresholds.get('min_trades_for_bias', 10)
            min_win_rate = thresholds.get('min_win_rate', 0.4)
            min_profit_factor = thresholds.get('min_profit_factor', 1.2)
            
            if (performance.get('total_trades', 0) < min_trades or
                performance.get('win_rate', 0) < min_win_rate or
                performance.get('profit_factor', 0) < min_profit_factor):
                return 0.3  # Below minimum thresholds
                
            # Composite score based on performance metrics
            win_rate = performance.get('win_rate', 0.5)
            profit_factor = min(performance.get('profit_factor', 1.0), 5.0)  # Cap at 5 for stability
            
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            print(f"Error calculating setup bias for {order.trading_setup}: {e}")
            return 0.5  # Fallback value
    # <Advanced Feature Integration - End>
    
    def calculate_deterministic_score(self, order: PlannedOrder, fill_prob: float, 
                                   total_capital: float, current_scores: Optional[List[float]] = None) -> Dict:
        """
        Calculate the deterministic score for an order using Phase B formula.
        
        Args:
            order: The planned order to score
            fill_prob: Fill probability from probability engine (0-1)
            total_capital: Total available capital for sizing and efficiency
            current_scores: Scores of other orders for normalization (optional)
            
        Returns:
            Dictionary with score components and final score
        """
        weights = self.config['weights']
        
        # 1. Normalize manual priority (1-5 → 0-1, where 5 is best → 1.0)
        priority_norm = (6 - order.priority) / 5.0  # 1→1.0, 5→0.2
        
        # 2. Calculate capital efficiency
        efficiency = self.calculate_efficiency(order, total_capital)
        
        # 3. Normalize efficiency if other scores are provided for context
        efficiency_norm = efficiency
        if current_scores:
            # Simple min-max normalization for efficiency across current batch
            max_eff = max([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            min_eff = min([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            if max_eff > min_eff:
                efficiency_norm = (efficiency - min_eff) / (max_eff - min_eff)
        
        # 4. Size preference (prefer smaller capital commitments)
        try:
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            size_pref = 1.0 - min(capital_commitment / total_capital, 1.0)
        except (ValueError, ZeroDivisionError):
            size_pref = 0.5  # Neutral preference
            
        # <Advanced Feature Integration - Begin>
        # 5. Timeframe matching (replaced placeholder with real implementation)
        timeframe_match = self.calculate_timeframe_match_score(order)
        
        # 6. Setup bias (replaced placeholder with real implementation)
        setup_bias = self.calculate_setup_bias_score(order)
        # <Advanced Feature Integration - End>
        
        # Combine all components using configured weights
        score = (
            weights['fill_prob'] * fill_prob +
            weights['manual_priority'] * priority_norm +
            weights['efficiency'] * efficiency_norm +
            weights['size_pref'] * size_pref +
            weights['timeframe_match'] * timeframe_match +
            weights['setup_bias'] * setup_bias
        )
        
        return {
            'final_score': score,
            'components': {
                'fill_prob': fill_prob,
                'priority_norm': priority_norm,
                'efficiency': efficiency,
                'efficiency_norm': efficiency_norm,
                'size_pref': size_pref,
                'timeframe_match': timeframe_match,
                'setup_bias': setup_bias
            },
            'weights': weights,
            'capital_commitment': order.entry_price * self.sizing_service.calculate_order_quantity(order, total_capital) 
                                  if order.entry_price else 0
        }
    
    def prioritize_orders(self, executable_orders: List[Dict], total_capital: float, 
                        current_working_orders: Optional[List] = None) -> List[Dict]:
        """
        Prioritize executable orders and allocate capital based on scoring.
        
        Args:
            executable_orders: List of orders with fill probabilities from eligibility service
            total_capital: Total available capital for allocation
            current_working_orders: Currently active orders (for capital accounting)
            
        Returns:
            List of prioritized orders with allocation decisions and scores
        """
        if not executable_orders:
            return []
            
        # Calculate committed capital from working orders
        committed_capital = 0.0
        working_order_count = 0
        if current_working_orders:
            committed_capital = sum(order.get('capital_commitment', 0) 
                                  for order in current_working_orders)
            working_order_count = len(current_working_orders)
        
        available_capital = total_capital * self.config['max_capital_utilization'] - committed_capital
        available_capital = max(0, available_capital)
        
        available_slots = self.config['max_open_orders'] - working_order_count
        available_slots = max(0, available_slots)
        
        # First pass: calculate scores for all orders
        scored_orders = []
        all_scores = []
        
        for order_data in executable_orders:
            order = order_data['order']
            fill_prob = order_data['fill_probability']
            
            # Calculate score for this order
            score_result = self.calculate_deterministic_score(
                order, fill_prob, total_capital, all_scores
            )
            
            # Calculate actual quantity and capital commitment
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity if order.entry_price else 0
            
            scored_order = {
                **order_data,
                'deterministic_score': score_result['final_score'],
                'score_components': score_result['components'],
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'allocated': False,
                'allocation_reason': None
            }
            
            scored_orders.append(scored_order)
            all_scores.append(score_result['components'])
        
        # Sort by score descending (highest score first)
        scored_orders.sort(key=lambda x: x['deterministic_score'], reverse=True)
        
        # Second pass: allocate capital to top orders
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in scored_orders:
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                continue
                
            if total_allocated_capital + order['capital_commitment'] > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                continue
                
            # Allocate this order
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += order['capital_commitment']
            allocated_count += 1
            allocated_orders.append(order)
        
        # Return both allocated and non-allocated orders with reasons
        return scored_orders
    
    def get_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict:
        """
        Generate a summary of the prioritization results.
        
        Args:
            prioritized_orders: Output from prioritize_orders method
            
        Returns:
            Summary statistics and metrics
        """
        allocated = [o for o in prioritized_orders if o['allocated']]
        not_allocated = [o for o in prioritized_orders if not o['allocated']]
        
        total_commitment = sum(o['capital_commitment'] for o in allocated)
        avg_score = sum(o['deterministic_score'] for o in allocated) / len(allocated) if allocated else 0
        
        return {
            'total_allocated': len(allocated),
            'total_rejected': len(not_allocated),
            'total_capital_commitment': total_commitment,
            'average_score': avg_score,
            'allocation_reasons': {
                reason: sum(1 for o in not_allocated if o['allocation_reason'] == reason)
                for reason in set(o['allocation_reason'] for o in not_allocated)
            }
        }