"""
TradingOrchestrator - Handles core trading orchestration logic.
Manages order execution, prioritization, position management, and market operations.
"""

import datetime
from typing import List, Dict, Any, Optional, Set
import time
from decimal import Decimal

from src.trading.orders.planned_order import PlannedOrder, ActiveOrder, PositionStrategy
from src.core.context_aware_logger import TradingEventType


class TradingOrchestrator:
    """Handles core trading orchestration and execution coordination."""
    
    def __init__(self, trading_manager):
        self.tm = trading_manager
        self.context_logger = trading_manager.context_logger
        
    def execute_prioritized_orders(self, executable_orders: List[Dict]) -> None:
        """Execute orders using two-layer prioritization with duplicate prevention."""
        total_capital = self.tm._get_total_capital()
        working_orders = self.tm._get_working_orders()

        # <Context-Aware Logging - Prioritization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order prioritization with price adjustment initiation",
            context_provider={
                'executable_orders_count': len(executable_orders),
                'total_capital': total_capital,
                'working_orders_count': len(working_orders),
                'max_open_orders': self.tm.max_open_orders,
                'orders_in_progress_count': len(self.tm._orders_in_progress),
                'bracket_validation_level': 'trading_manager',
                'price_adjustment_initiation': 'enabled',
                'adjustment_coordination_chain': 'trading_manager->orchestrator->execution_service->ibkr_client'
            }
        )
        # <Context-Aware Logging - Prioritization Start - End>

        prioritized_orders = self.tm.prioritization_service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )

        executed_count = 0
        skipped_reasons = {}
        
        # Update execution symbols with symbols from executable orders
        current_execution_symbols = set()
        for order_data in executable_orders:
            if isinstance(order_data, dict) and 'order' in order_data:
                current_execution_symbols.add(order_data['order'].symbol)
            
        self.tm._execution_symbols = current_execution_symbols
        
        for order_data in prioritized_orders:
            order = order_data['order']
            fill_prob = order_data['fill_probability']
            symbol = order.symbol

            # Only check allocation for system capacity management
            if not order_data.get('allocated', False):
                skipped_reasons[symbol] = f"Not allocated due to capacity limits"
                continue

            # Bracket order parameter validation with price adjustment awareness
            is_bracket_order = hasattr(order, 'order_type') and getattr(order, 'order_type') is not None
            current_market_price = self.tm._get_current_market_price(symbol) if is_bracket_order else None
            
            if is_bracket_order:
                bracket_valid, bracket_message = self.validate_bracket_order_at_source(order)
                if not bracket_valid:
                    skipped_reasons[symbol] = f"Bracket order validation failed: {bracket_message}"
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Bracket order rejected at trading manager level",
                        symbol=symbol,
                        context_provider={
                            'reason': bracket_message,
                            'risk_reward_ratio': getattr(order, 'risk_reward_ratio', 'MISSING'),
                            'entry_price': getattr(order, 'entry_price', 'MISSING'),
                            'stop_loss': getattr(order, 'stop_loss', 'MISSING'),
                            'validation_level': 'trading_manager',
                            'price_adjustment_initiation': 'blocked',
                            'adjustment_block_reason': 'parameter_validation_failed'
                        },
                        decision_reason=f"Bracket order parameter validation failed: {bracket_message}"
                    )
                    continue

                # Enhanced diagnostic for price adjustment opportunity at initiation level
                if current_market_price:
                    price_diff_pct = abs(current_market_price - order.entry_price) / order.entry_price * 100
                    adjustment_opportunity = (
                        (order.action.value.upper() == "BUY" and current_market_price < order.entry_price) or
                        (order.action.value.upper() == "SELL" and current_market_price > order.entry_price)
                    ) and price_diff_pct >= 0.5  # 0.5% threshold
                    
                    if adjustment_opportunity:
                        self.context_logger.log_event(
                            TradingEventType.EXECUTION_DECISION,
                            "Price adjustment opportunity identified at trading manager level",
                            symbol=symbol,
                            context_provider={
                                'current_market_price': current_market_price,
                                'planned_entry_price': order.entry_price,
                                'price_difference_percent': price_diff_pct,
                                'adjustment_threshold_met': True,
                                'potential_improvement': order.entry_price - current_market_price if order.action.value.upper() == "BUY" else current_market_price - order.entry_price,
                                'risk_amount_maintainable': True,
                                'price_adjustment_initiation': 'opportunity_identified',
                                'adjustment_coordination_initiated': True
                            },
                            decision_reason=f"Price adjustment opportunity identified: {price_diff_pct:.2f}% difference"
                        )

            # Duplicate execution prevention
            can_execute, reason = self.can_execute_order(order)
            if not can_execute:
                skipped_reasons[symbol] = reason
                continue

            # Mark order as starting execution to prevent duplicates
            self.mark_order_execution_start(order)

            # Calculate all required parameters for bracket order with price adjustment context
            effective_priority = order.priority * fill_prob
            account_number = self.tm._get_current_account_number()
            
            # Calculate position details for bracket order
            try:
                quantity = self.tm.sizing_service.calculate_order_quantity(order, total_capital)
                capital_commitment = order.entry_price * quantity
                is_live_trading = self.tm._get_trading_mode()
            except Exception as e:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Failed to calculate bracket order parameters for {symbol}",
                    symbol=symbol,
                    context_provider={
                        'error': str(e),
                        'entry_price': order.entry_price,
                        'total_capital': total_capital,
                        'calculation_stage': 'position_sizing',
                        'price_adjustment_initiation': 'failed',
                        'adjustment_block_reason': 'parameter_calculation_error'
                    },
                    decision_reason=f"Parameter calculation failed: {e}"
                )
                self.mark_order_execution_complete(order, False)
                skipped_reasons[symbol] = f"Parameter calculation failed: {e}"
                continue
            
            # Enhanced diagnostic logging for bracket orders with price adjustment initiation
            if is_bracket_order:
                adjustment_context = {
                    'entry_price': order.entry_price,
                    'stop_loss': order.stop_loss,
                    'action': order.action.value,
                    'fill_probability': fill_prob,
                    'effective_priority': effective_priority,
                    'quantity': quantity,
                    'capital_commitment': capital_commitment,
                    'total_capital': total_capital,
                    'is_live_trading': is_live_trading,
                    'account_number': account_number,
                    'symbol_in_execution_set': symbol in self.tm._execution_symbols,
                    'duplicate_prevention_active': True,
                    'risk_reward_ratio': getattr(order, 'risk_reward_ratio', 'MISSING'),
                    'bracket_parameters_validated': True,
                    'validation_chain_complete': True,
                    'price_adjustment_initiation': 'complete',
                    'adjustment_coordination_ready': True,
                    'current_market_price_available': current_market_price is not None
                }
                
                if current_market_price:
                    adjustment_context.update({
                        'current_market_price': current_market_price,
                        'price_difference_percent': abs(current_market_price - order.entry_price) / order.entry_price * 100,
                        'adjustment_opportunity_present': (
                            (order.action.value.upper() == "BUY" and current_market_price < order.entry_price) or
                            (order.action.value.upper() == "SELL" and current_market_price > order.entry_price)
                        )
                    })

                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Attempting bracket order execution with price adjustment initiation",
                    symbol=symbol,
                    context_provider=adjustment_context,
                    decision_reason=f"Bracket order meets all criteria with price adjustment initiation"
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Attempting order execution with complete parameters",
                    symbol=symbol,
                    context_provider={
                        'entry_price': order.entry_price,
                        'stop_loss': order.stop_loss,
                        'action': order.action.value,
                        'fill_probability': fill_prob,
                        'effective_priority': effective_priority,
                        'quantity': quantity,
                        'capital_commitment': capital_commitment,
                        'total_capital': total_capital,
                        'is_live_trading': is_live_trading,
                        'account_number': account_number,
                        'symbol_in_execution_set': symbol in self.tm._execution_symbols,
                        'duplicate_prevention_active': True,
                        'price_adjustment_initiation': 'not_applicable'
                    },
                    decision_reason=f"Order meets execution criteria with complete parameters"
                )
            
            # Pass ALL parameters to execution orchestrator
            success = self.tm.execution_orchestrator.execute_single_order(
                order, 
                fill_probability=fill_prob,
                effective_priority=effective_priority,
                total_capital=total_capital,
                quantity=quantity,
                capital_commitment=capital_commitment,
                is_live_trading=is_live_trading,
                account_number=account_number
            )
            
            # Mark order execution as complete (regardless of success)
            self.mark_order_execution_complete(order, success)
            
            if success:
                executed_count += 1
                # Ensure symbol remains in execution set after successful execution
                self.tm._execution_symbols.add(symbol)
                
                # Enhanced success logging for bracket orders with price adjustment context
                if is_bracket_order:
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        f"Bracket order execution successful with price adjustment coordination",
                        symbol=symbol,
                        context_provider={
                            'entry_price': order.entry_price,
                            'order_type': order.order_type.value,
                            'quantity': quantity,
                            'execution_symbols_count': len(self.tm._execution_symbols),
                            'duplicate_prevention_active': True,
                            'risk_reward_ratio_used': getattr(order, 'risk_reward_ratio', 'MISSING'),
                            'expected_components': 3,
                            'bracket_success': True,
                            'price_adjustment_initiation': 'successful',
                            'adjustment_coordination_result': 'orchestrator_accepted',
                            'current_market_price_at_initiation': current_market_price
                        },
                        decision_reason="Bracket order execution successful with price adjustment initiation"
                    )
                else:
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        f"Order execution successful for {symbol}",
                        symbol=symbol,
                        context_provider={
                            'entry_price': order.entry_price,
                            'order_type': order.order_type.value,
                            'quantity': quantity,
                            'execution_symbols_count': len(self.tm._execution_symbols),
                            'duplicate_prevention_active': True,
                            'price_adjustment_initiation': 'not_applicable'
                        },
                        decision_reason="Execution orchestrator returned success with complete parameters"
                    )
            else:
                # Enhanced failure logging for bracket orders with price adjustment context
                if is_bracket_order:
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        f"Bracket order execution failed despite price adjustment initiation",
                        symbol=symbol,
                        context_provider={
                            'entry_price': order.entry_price,
                            'quantity': quantity,
                            'duplicate_prevention_active': True,
                            'risk_reward_ratio': getattr(order, 'risk_reward_ratio', 'MISSING'),
                            'bracket_failure': True,
                            'likely_issue': 'orchestrator_or_below',
                            'price_adjustment_initiation': 'failed',
                            'adjustment_coordination_result': 'execution_failed',
                            'current_market_price_at_initiation': current_market_price
                        },
                        decision_reason="Bracket order execution failed despite price adjustment initiation"
                    )
                else:
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        f"Order execution failed for {symbol}",
                        symbol=symbol,
                        context_provider={
                            'entry_price': order.entry_price,
                            'quantity': quantity,
                            'duplicate_prevention_active': True,
                            'price_adjustment_initiation': 'not_applicable'
                        },
                        decision_reason="Execution orchestrator returned failure despite complete parameters"
                    )

        # Enhanced execution summary with price adjustment initiation context
        bracket_orders_attempted = sum(1 for order_data in prioritized_orders 
                                    if hasattr(order_data['order'], 'order_type') and 
                                    getattr(order_data['order'], 'order_type') is not None)
        bracket_orders_executed = sum(1 for order_data in prioritized_orders 
                                    if hasattr(order_data['order'], 'order_type') and 
                                    getattr(order_data['order'], 'order_type') is not None and
                                    order_data['order'].symbol in [o.symbol for o in self.tm.planned_orders if hasattr(o, 'order_type')])
        
        # Calculate price adjustment opportunities
        adjustment_opportunities = 0
        for order_data in prioritized_orders:
            order = order_data['order']
            if (hasattr(order, 'order_type') and getattr(order, 'order_type') is not None and
                hasattr(order, 'entry_price')):
                current_price = self.tm._get_current_market_price(order.symbol)
                if current_price:
                    price_diff_pct = abs(current_price - order.entry_price) / order.entry_price * 100
                    if ((order.action.value.upper() == "BUY" and current_price < order.entry_price) or
                        (order.action.value.upper() == "SELL" and current_price > order.entry_price)) and price_diff_pct >= 0.5:
                        adjustment_opportunities += 1
        
        # <Context-Aware Logging - Execution Summary - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Order execution cycle completed: {executed_count} executed, {len(skipped_reasons)} skipped",
            context_provider={
                'executed_count': executed_count,
                'skipped_count': len(skipped_reasons),
                'skipped_reasons': skipped_reasons,
                'total_considered': len(prioritized_orders),
                'execution_symbols_count': len(self.tm._execution_symbols),
                'orders_in_progress_count': len(self.tm._orders_in_progress),
                'duplicate_prevention_active': True,
                'parameter_flow_fixed': True,
                'bracket_orders_attempted': bracket_orders_attempted,
                'bracket_orders_executed': bracket_orders_executed,
                'bracket_validation_chain': 'complete',
                'validation_levels': ['trading_manager', 'orchestrator', 'execution_service', 'ibkr_client'],
                'price_adjustment_initiation': 'complete',
                'adjustment_opportunities_identified': adjustment_opportunities,
                'adjustment_coordination_chain': 'trading_manager->orchestrator->execution_service->ibkr_client',
                'feature_status': 'fully_implemented'
            },
            decision_reason=f"Execution summary with price adjustment initiation: {executed_count} executed, {adjustment_opportunities} adjustment opportunities"
        )
        # <Context-Aware Logging - Execution Summary - End>

        # DEBUG: Verify execution symbols propagation and price adjustment initiation
        print(f"🔧 DEBUG: Execution symbols: {self.tm._execution_symbols}")
        print(f"🔧 DEBUG: Orders in progress: {self.tm._orders_in_progress}")
        print(f"🔧 DEBUG: Price adjustment initiation complete - coordination chain established")
        print(f"🔧 DEBUG: Bracket orders attempted/executed: {bracket_orders_attempted}/{bracket_orders_executed}")
        print(f"🔧 DEBUG: Price adjustment opportunities: {adjustment_opportunities}")

    def check_and_execute_orders(self) -> None:
        """Check market conditions and execute orders that meet the criteria."""
        if not self.tm.planned_orders:
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "No planned orders available for execution check",
                context_provider={
                    'planned_orders_count': 0
                },
                decision_reason="Skipping execution cycle - no planned orders"
            )
            return

        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order execution cycle",
            context_provider={
                'planned_orders_count': len(self.tm.planned_orders),
                'active_orders_count': len(self.tm.active_orders),
                'market_open': self.tm.market_hours.is_market_open()
            }
        )

        # Fix eligibility service call to pass planned_orders parameter
        executable_orders = self.tm.eligibility_service.find_executable_orders(self.tm.planned_orders)
        
        if not executable_orders:
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "No executable orders found",
                context_provider={
                    'planned_orders_count': len(self.tm.planned_orders),
                    'executable_orders_count': 0
                },
                decision_reason="Eligibility service returned no executable orders"
            )
            return

        self.execute_prioritized_orders(executable_orders)

    def check_market_close_actions(self) -> None:
        """Check if any DAY positions need to be closed before market close."""
        # Safely get buffer_minutes from config with fallback
        market_close_config = self.tm.trading_config.get('market_close', {})
        buffer_minutes = market_close_config.get('buffer_minutes', 10)
        
        # Log market close check with context
        should_close = self.tm.market_hours.should_close_positions(buffer_minutes=buffer_minutes)
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Market close position check",
            context_provider={
                'buffer_minutes': buffer_minutes,
                'should_close': should_close,
                'current_time': datetime.datetime.now().isoformat(),
                'minutes_until_close': self.tm.market_hours.minutes_until_close(),
                'market_status': self.tm.market_hours.get_market_status()
            },
            decision_reason=f"Market close check: should_close={should_close}"
        )
        
        if should_close:
            # Close all DAY strategy positions
            day_positions = self.tm.state_service.get_positions_by_strategy(PositionStrategy.DAY)
            
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Found {len(day_positions)} DAY positions to close",
                context_provider={
                    'day_positions_count': len(day_positions),
                    'position_symbols': [p.symbol for p in day_positions] if day_positions else []
                },
                decision_reason=f"Market close: closing {len(day_positions)} DAY positions"
            )
            
            for position in day_positions:
                self.close_single_position(position)

    def close_single_position(self, position) -> None:
        """Orchestrate the closing of a single position through the execution service."""
        try:
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Closing position: {position.symbol}",
                symbol=position.symbol,
                context_provider={
                    'position_action': position.action,
                    'position_quantity': position.quantity,
                    'position_strategy': getattr(position, 'position_strategy', 'UNKNOWN'),
                    'reason': 'market_close' if hasattr(self.tm, '_check_market_close_actions') else 'manual'
                },
                decision_reason="Closing position"
            )
            
            cancel_success = self.tm.execution_service.cancel_orders_for_symbol(position.symbol)
            if not cancel_success:
                pass

            close_action = 'SELL' if position.action == 'BUY' else 'BUY'

            # Pass account number to execution service
            account_number = self.tm._get_current_account_number()
            order_id = self.tm.execution_service.close_position({
                'symbol': position.symbol,
                'action': close_action,
                'quantity': position.quantity,
                'security_type': position.security_type,
                'exchange': position.exchange,
                'currency': position.currency
            }, account_number)

            if order_id is not None:
                position.status = 'CLOSING'
                self.tm.db_session.commit()
                
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Position closing initiated for {position.symbol}",
                    symbol=position.symbol,
                    context_provider={
                        'close_order_id': order_id,
                        'close_action': close_action,
                        'quantity': position.quantity
                    },
                    decision_reason="Position close order placed successfully"
                )
                
                # Risk Management - Record P&L on position close
                try:
                    for active_order in self.tm.active_orders.values():
                        if active_order.symbol == position.symbol and active_order.is_working():
                            self.tm.risk_service.record_trade_outcome(active_order, None)
                            break
                except Exception as e:
                    pass
                
            else:
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Position close simulated for {position.symbol}",
                    symbol=position.symbol,
                    context_provider={
                        'close_action': close_action,
                        'quantity': position.quantity
                    },
                    decision_reason="Simulation mode - no actual order placed"
                )

        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Failed to close position {position.symbol}",
                symbol=position.symbol,
                context_provider={
                    'error': str(e)
                },
                decision_reason=f"Position close failed: {e}"
            )

    def replace_active_order(self, old_order: ActiveOrder, new_planned_order: PlannedOrder,
                           new_fill_probability: float) -> bool:
        """Replace a stale active order with a new order."""
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting order replacement process",
            symbol=old_order.symbol,
            context_provider={
                'old_order_symbol': old_order.symbol,
                'new_order_symbol': new_planned_order.symbol,
                'new_fill_probability': new_fill_probability,
                'old_order_ids': old_order.order_ids
            }
        )
        
        if not self.tm.cancel_active_order(old_order):
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order replacement failed - could not cancel old order",
                symbol=old_order.symbol,
                context_provider={
                    'old_order_symbol': old_order.symbol,
                    'new_order_symbol': new_planned_order.symbol
                },
                decision_reason="Old order cancellation failed, replacement aborted"
            )
            return False

        # Calculate all required parameters for bracket order replacement
        effective_priority = new_planned_order.priority * new_fill_probability
        account_number = self.tm._get_current_account_number()
        total_capital = self.tm._get_total_capital()
        is_live_trading = self.tm._get_trading_mode()
        
        try:
            quantity = self.tm.sizing_service.calculate_order_quantity(new_planned_order, total_capital)
            capital_commitment = new_planned_order.entry_price * quantity
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to calculate replacement order parameters",
                symbol=new_planned_order.symbol,
                context_provider={
                    'error': str(e),
                    'entry_price': new_planned_order.entry_price,
                    'total_capital': total_capital
                },
                decision_reason=f"Replacement parameter calculation failed: {e}"
            )
            return False
        
        # Pass ALL required parameters for bracket order replacement
        success = self.tm.execution_orchestrator.execute_single_order(
            new_planned_order, 
            fill_probability=new_fill_probability, 
            effective_priority=effective_priority,
            total_capital=total_capital,
            quantity=quantity,
            capital_commitment=capital_commitment,
            is_live_trading=is_live_trading,
            account_number=account_number
        )
        
        if success:
            old_order.update_status('REPLACED')
            
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order successfully replaced with complete parameters",
                symbol=new_planned_order.symbol,
                context_provider={
                    'old_order_symbol': old_order.symbol,
                    'new_order_symbol': new_planned_order.symbol,
                    'effective_priority': effective_priority,
                    'quantity': quantity,
                    'capital_commitment': capital_commitment,
                    'account_number': account_number
                },
                decision_reason="Order replacement completed with complete bracket parameters"
            )
        else:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order replacement failed - new order execution failed",
                symbol=new_planned_order.symbol,
                context_provider={
                    'old_order_symbol': old_order.symbol,
                    'new_order_symbol': new_planned_order.symbol,
                    'effective_priority': effective_priority,
                    'quantity': quantity
                },
                decision_reason="New order execution failed after old order cancellation"
            )
        
        return success

    def validate_bracket_order_at_source(self, order) -> tuple[bool, str]:
        """
        Validate bracket order parameters at the trading manager level (source validation).
        """
        try:
            # Check for required bracket order parameters
            required_parameters = [
                ('risk_reward_ratio', 'Risk reward ratio'),
                ('risk_per_trade', 'Risk per trade'),
                ('entry_price', 'Entry price'),
                ('stop_loss', 'Stop loss'),
                ('order_type', 'Order type'),
                ('action', 'Action'),
                ('security_type', 'Security type')
            ]
            
            missing_parameters = []
            for param_name, param_description in required_parameters:
                if not hasattr(order, param_name) or getattr(order, param_name) is None:
                    missing_parameters.append(param_description)
            
            if missing_parameters:
                return False, f"Missing required parameters: {', '.join(missing_parameters)}"
            
            # Validate specific parameter values for price adjustment
            risk_reward_ratio = getattr(order, 'risk_reward_ratio')
            if not isinstance(risk_reward_ratio, (int, float, Decimal)):
                return False, f"Invalid risk_reward_ratio type: {type(risk_reward_ratio)}"
            
            if risk_reward_ratio <= 0:
                return False, f"Invalid risk_reward_ratio value: {risk_reward_ratio}"
            
            # Enhanced validation for price adjustment scenarios
            order_type = getattr(order, 'order_type').value.upper()
            if order_type == 'LMT':
                # For LIMIT orders, validate that parameters support price adjustment
                entry_price = getattr(order, 'entry_price')
                stop_loss = getattr(order, 'stop_loss')
                
                if entry_price is None or entry_price <= 0:
                    return False, f"Invalid entry_price: {entry_price}"
                
                if stop_loss is None or stop_loss <= 0:
                    return False, f"Invalid stop_loss: {stop_loss}"
                
                # Validate price relationship for meaningful adjustment
                price_difference = abs(entry_price - stop_loss)
                if price_difference == 0:
                    return False, "Entry price and stop loss cannot be the same"
                
                # Validate meaningful price difference for adjustment (at least 0.5%)
                if price_difference / entry_price < 0.005:
                    return False, f"Risk amount too small for meaningful price adjustment: {price_difference:.4f} ({price_difference/entry_price:.2%})"
            
            # Test profit target calculation with adjustment awareness
            entry_price = getattr(order, 'entry_price')
            stop_loss = getattr(order, 'stop_loss')
            
            try:
                if order.action.value == "BUY":
                    profit_target = entry_price + (abs(entry_price - stop_loss) * risk_reward_ratio)
                else:
                    profit_target = entry_price - (abs(entry_price - stop_loss) * risk_reward_ratio)
                
                # Validate profit target is reasonable for adjustment scenarios
                if profit_target <= 0:
                    return False, f"Invalid profit target calculated: {profit_target}"
                
                # Validate profit target has room for adjustment
                if abs(profit_target - entry_price) / entry_price < 0.005:
                    return False, f"Profit target too close to entry price for adjustment: {profit_target}"
                    
            except Exception as calc_error:
                return False, f"Profit target calculation test failed: {calc_error}"
            
            # All validations passed with price adjustment support
            return True, "All bracket order parameters validated successfully including price adjustment support"
            
        except Exception as e:
            return False, f"Bracket order source validation error: {e}"

    def can_execute_order(self, order) -> tuple[bool, str]:
        """Check if an order can be executed without duplication."""
        symbol = order.symbol
        order_key = f"{symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        # Check if order is already in progress
        if order_key in self.tm._orders_in_progress:
            return False, f"Order execution already in progress for {symbol}"
        
        # Check execution cooldown
        current_time = time.time()
        last_execution = self.tm._last_execution_time.get(order_key, 0)
        time_since_last = current_time - last_execution
        
        if time_since_last < self.tm._execution_cooldown_seconds:
            return False, f"Execution cooldown active for {symbol} ({time_since_last:.1f}s < {self.tm._execution_cooldown_seconds}s)"
        
        # Check if order already has active working orders
        if self.tm.state_service.has_open_position(symbol):
            return False, f"Open position exists for {symbol}"
            
        # Check database for active duplicates
        db_order = self.tm.order_lifecycle_manager.find_existing_order(order)
        if db_order and db_order.status in ['LIVE', 'LIVE_WORKING', 'FILLED']:
            same_action = db_order.action == order.action.value
            same_entry = abs(db_order.entry_price - order.entry_price) < 0.0001
            same_stop = abs(db_order.stop_loss - order.stop_loss) < 0.0001
            if same_action and same_entry and same_stop:
                return False, f"Duplicate active order in database (status: {db_order.status})"
        
        return True, "Order can be executed"

    def mark_order_execution_start(self, order) -> None:
        """Mark an order as starting execution to prevent duplicates."""
        symbol = order.symbol
        order_key = f"{symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        self.tm._orders_in_progress.add(order_key)
        
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Order execution tracking started",
            symbol=symbol,
            context_provider={
                'order_key': order_key,
                'orders_in_progress_count': len(self.tm._orders_in_progress)
            },
            decision_reason="Order marked as in-progress to prevent duplicate execution"
        )

    def mark_order_execution_complete(self, order, success: bool) -> None:
        """Mark an order as completed execution and update tracking."""
        symbol = order.symbol
        order_key = f"{symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        # Remove from in-progress tracking
        if order_key in self.tm._orders_in_progress:
            self.tm._orders_in_progress.remove(order_key)
        
        # Update last execution time (regardless of success)
        self.tm._last_execution_time[order_key] = time.time()
        
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Order execution tracking completed",
            symbol=symbol,
            context_provider={
                'order_key': order_key,
                'success': success,
                'orders_in_progress_count': len(self.tm._orders_in_progress),
                'total_tracked_executions': len(self.tm._last_execution_time)
            },
            decision_reason="Order execution tracking updated"
        )