# src/brokers/ibkr/core/account_manager.py

"""
Handles all account-related operations for IBKR including account value retrieval,
position tracking, and account information management.
"""

import time
import datetime
import threading
from typing import Optional, Dict, Any, List
from decimal import Decimal

from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.brokers.ibkr.types.ibkr_types import IbkrPosition


class AccountManager:
    """Manages account information, values, positions, and capital calculations."""
    
    # Valid numeric account value fields
    VALID_NUMERIC_ACCOUNT_FIELDS = {
        "NetLiquidation", "BuyingPower", "AvailableFunds", "TotalCashValue", 
        "CashBalance", "EquityWithLoanValue", "GrossPositionValue", 
        "MaintMarginReq", "FullInitMarginReq", "FullAvailableFunds",
        "FullExcessLiquidity", "Cushion", "LookAheadNextChange"
    }
    
    def __init__(self, connection_manager):
        """Initialize account manager with connection reference."""
        self.context_logger = get_context_logger()
        self.connection_manager = connection_manager
        
        # Account data storage
        self.account_values = {}
        self.positions: List[IbkrPosition] = []
        
        # Synchronization events
        self.account_value_received = threading.Event()
        self.positions_received_event = threading.Event()
        self.positions_end_received = False
        
        # Tracking for debugging
        self._ignored_metadata_warned = set()
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "AccountManager initialized",
            context_provider={
                "connection_manager_available": connection_manager is not None,
                "valid_numeric_fields_count": len(self.VALID_NUMERIC_ACCOUNT_FIELDS)
            }
        )
    
    def get_account_value(self) -> float:
        """
        Request and return the Net Liquidation value using proven strategy.
        Raises ValueError if no valid numeric account values are found.
        """
        if not self.connection_manager.connected:
            raise ValueError("Not connected to IBKR - cannot retrieve account value")

        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting account value retrieval",
            context_provider={
                'account_number': self.connection_manager.account_number,
                'timeout_seconds': 15.0,
                'strategy': 'direct_account_updates_empty_string'
            }
        )
        
        # Use proven strategy: Direct Account Updates with empty string
        capital = self._get_account_value_direct_updates()
        if capital is not None and capital > 0:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Capital determined successfully",
                context_provider={
                    'final_capital': capital,
                    'strategy': 'direct_account_updates_empty_string'
                }
            )
            return capital

        # No valid account values found - stop trading for safety
        error_msg = "CRITICAL: No valid numeric account values found - TRADING HALTED for safety"
        
        # Log detailed diagnostic information
        received_keys = list(self.account_values.keys()) if self.account_values else []
        numeric_values_found = []
        
        for key, value in self.account_values.items():
            if isinstance(value, (int, float)) and value > 0:
                numeric_values_found.append((key, value))
            elif isinstance(value, dict) and 'value' in value:
                if isinstance(value['value'], (int, float)) and value['value'] > 0:
                    numeric_values_found.append((key, value['value']))

        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TRADING HALTED - No valid account capital available",
            context_provider={
                'account_values_total_received': len(self.account_values),
                'received_account_keys': received_keys,
                'numeric_values_found': numeric_values_found,
                'valid_numeric_fields_checked': list(self.VALID_NUMERIC_ACCOUNT_FIELDS),
                'strategy_used': 'direct_account_updates_empty_string',
                'safety_action': 'trading_halted'
            }
        )
        
        raise ValueError(error_msg)
    
    def _get_account_value_direct_updates(self) -> Optional[float]:
        """
        ROBUST STRATEGY: Wait specifically for numeric account values to arrive.
        """
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            self.connection_manager.reqAccountUpdates(True, "")
            
            # Wait for ANY data first
            if not self.account_value_received.wait(10.0):
                self.connection_manager.reqAccountUpdates(False, "")
                return None

            # Now wait specifically for numeric values
            start_time = time.time()
            while time.time() - start_time < 10.0:  # Wait up to 10 more seconds
                # Check if we have any numeric values
                has_numeric = any(
                    any(field in key for field in self.VALID_NUMERIC_ACCOUNT_FIELDS)
                    for key in self.account_values.keys()
                )
                
                if has_numeric:
                    break
                    
                time.sleep(0.5)  # Check every 500ms
            
            # Cleanup
            self.connection_manager.reqAccountUpdates(False, "")
            
            # Final extraction attempt
            capital = self._extract_capital_from_values()
            
            if capital is not None and capital > 0:
                return capital
            else:
                return None
                
        except Exception as e:
            try:
                self.connection_manager.reqAccountUpdates(False, "")
            except:
                pass
            return None
    
    def _extract_capital_from_values(self) -> Optional[float]:
        """Extract capital from account_values with enhanced nested structure handling."""
        capital_priority = [
            "NetLiquidation_CAD_PRIMARY", "NetLiquidation_CAD", 
            "AvailableFunds_CAD_PRIMARY", "AvailableFunds_CAD",
            "BuyingPower_CAD_PRIMARY", "BuyingPower_CAD",
            "TotalCashValue_CAD_PRIMARY", "TotalCashValue_CAD",
            "NetLiquidation", "AvailableFunds", "BuyingPower", "TotalCashValue"
        ]
        
        for field in capital_priority:
            if field in self.account_values:
                value = self.account_values[field]
                
                # Handle nested dictionary structure
                if isinstance(value, dict) and 'value' in value:
                    capital = value['value']
                    if isinstance(capital, (int, float)) and capital > 0:
                        return capital
                # Handle direct numeric value
                elif isinstance(value, (int, float)) and value > 0:
                    return value
                # Handle string values that can be converted
                elif isinstance(value, str):
                    try:
                        capital = float(value)
                        if capital > 0:
                            return capital
                    except (ValueError, TypeError):
                        continue
                        
        return None
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """Callback: Received account value updates with comprehensive debugging and filtering."""
        # Store all raw values for debugging with currency context
        if key not in self.account_values:
            self.account_values[key] = {}
        
        # Store the raw string value with currency context (for debugging)
        self.account_values[key][currency] = val

        # ONLY process known numeric financial fields - ignore metadata
        if key in self.VALID_NUMERIC_ACCOUNT_FIELDS:
            # Store currency-specific numeric values SAFELY
            if currency in ["CAD", "USD", "BASE"]:
                currency_key = f"{key}_{currency}"
                try:
                    # Convert string to float
                    numeric_value = float(val) if val and val.strip() else 0.0
                    
                    # Store as dictionary with numeric value
                    self.account_values[currency_key] = {
                        'value': numeric_value,
                        'currency': currency,
                        'key': key,
                        'timestamp': time.time()
                    }
                    
                except (ValueError, TypeError) as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Failed to convert valid numeric account value",
                        context_provider={
                            'key': key,
                            'value': val,
                            'currency': currency,
                            'error': str(e)
                        }
                    )

            # Store important CAD values safely for easy retrieval
            important_keys = ["NetLiquidation", "BuyingPower", "AvailableFunds", "TotalCashValue", "CashBalance"]
            if key in important_keys and currency == "CAD":
                try:
                    # Create dedicated key for important CAD values
                    important_cad_key = f"{key}_CAD_PRIMARY"
                    numeric_value = float(val) if val and val.strip() else 0.0
                    
                    self.account_values[important_cad_key] = {
                        'value': numeric_value,
                        'currency': currency,
                        'key': key,
                        'timestamp': time.time(),
                        'priority': 'high'
                    }
                    
                except (ValueError, TypeError) as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Failed to store important numeric account value",
                        context_provider={
                            'key': key,
                            'value': val,
                            'currency': currency,
                            'error': str(e)
                        }
                    )
        else:
            # Log ignored metadata fields (first occurrence only to avoid spam)
            if key not in self._ignored_metadata_warned:
                self._ignored_metadata_warned.add(key)
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Ignoring non-numeric metadata field",
                    context_provider={
                        'key': key,
                        'value': val,
                        'currency': currency,
                        'reason': 'not_a_valid_numeric_field'
                    }
                )

        self.account_value_received.set()
    
    def get_simple_account_value(self, key: str) -> Optional[float]:
        """
        Safe method to extract numeric account values from complex nested structure.
        Returns: Numeric value or None if not found/invalid.
        """
        if key not in self.account_values:
            return None
            
        value = self.account_values[key]
        
        # Handle nested dictionary structure
        if isinstance(value, dict) and 'value' in value:
            numeric_val = value['value']
            if isinstance(numeric_val, (int, float)) and numeric_val > 0:
                return numeric_val
        
        # Handle direct numeric value  
        elif isinstance(value, (int, float)) and value > 0:
            return value
            
        return None
    
    def get_positions(self) -> List[IbkrPosition]:
        """Fetch all positions from IBKR API synchronously."""
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Positions request failed - not connected to IBKR"
            )
            return []

        try:
            self.positions.clear()
            self.positions_end_received = False
            self.positions_received_event.clear()

            self.connection_manager.reqPositions()

            if self.positions_received_event.wait(10.0):
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Positions retrieved successfully",
                    context_provider={'positions_count': len(self.positions)}
                )
                return self.positions.copy()
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Timeout waiting for positions data"
                )
                return []

        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Positions request failed with exception",
                context_provider={'error': str(e)}
            )
            return []
    
    # Position callbacks
    def position(self, account: str, contract, position: float, avgCost: float) -> None:
        """Callback: Received position data."""
        try:
            ibkr_position = IbkrPosition(
                account=account,
                contract_id=contract.conId,
                symbol=contract.symbol,
                security_type=contract.secType,
                currency=contract.currency,
                position=position,
                avg_cost=avgCost
            )
            self.positions.append(ibkr_position)
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error processing position data",
                context_provider={
                    'symbol': getattr(contract, 'symbol', 'UNKNOWN'),
                    'error': str(e)
                }
            )

    def positionEnd(self) -> None:
        """Callback: Finished receiving positions."""
        self.positions_end_received = True
        self.positions_received_event.set()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Positions request completed",
            context_provider={'positions_count': len(self.positions)}
        )
    
    def get_account_summary(self) -> Dict[str, Any]:
        """Get comprehensive account summary information."""
        capital = self.get_simple_account_value("NetLiquidation_CAD_PRIMARY")
        buying_power = self.get_simple_account_value("BuyingPower_CAD_PRIMARY")
        available_funds = self.get_simple_account_value("AvailableFunds_CAD_PRIMARY")
        
        return {
            'account_number': self.connection_manager.account_number,
            'account_name': self.connection_manager.account_name,
            'is_paper_account': self.connection_manager.is_paper_account,
            'net_liquidation': capital,
            'buying_power': buying_power,
            'available_funds': available_funds,
            'positions_count': len(self.positions),
            'open_orders_count': 0,  # This would come from OrderManager
            'connection_status': 'Connected' if self.connection_manager.connected else 'Disconnected'
        }