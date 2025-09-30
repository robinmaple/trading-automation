"""
Orchestrates order loading from multiple sources including database resumption and Excel input.
Handles multi-source order loading with proper deduplication and error resilience.
Provides comprehensive order loading capabilities for production trading systems.
"""

import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from src.core.planned_order import PlannedOrder
from src.core.models import PlannedOrderDB
from src.services.order_loading_service import OrderLoadingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService

# <IBKR Integration - Begin>
from src.core.ibkr_client import IbkrClient
from src.core.ibkr_types import IbkrOrder
# <IBKR Integration - End>


class OrderLoadingOrchestrator:
    """
    Orchestrates order loading from multiple sources with proper deduplication
    and error handling. Supports database order resumption and Excel order loading.
    """
    
    def __init__(self, 
                 loading_service: OrderLoadingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 db_session: Session,
                 # <IBKR Integration - Begin>
                 ibkr_client: Optional[IbkrClient] = None
                 # <IBKR Integration - End>
                 ):
        """
        Initialize the order loading orchestrator with required services.
        
        Args:
            loading_service: Service for loading orders from Excel
            persistence_service: Service for order persistence operations
            state_service: Service for checking system state
            db_session: Database session for querying orders
            ibkr_client: IBKR client for order discovery (optional)
        """
        self.loading_service = loading_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.db_session = db_session
        # <IBKR Integration - Begin>
        self.ibkr_client = ibkr_client
        # <IBKR Integration - End>
        
    def load_all_orders(self, excel_path: str) -> List[PlannedOrder]:
        """
        Load orders from all available sources and merge results.
        
        Args:
            excel_path: Path to Excel file containing new orders
            
        Returns:
            List of PlannedOrder objects from all sources after deduplication
        """
        print("🔄 OrderLoadingOrchestrator: Loading orders from all sources...")
        
        all_orders = []
        sources_loaded = 0
        
        try:
            # Load from database (order resumption)
            db_orders = self._load_from_database()
            if db_orders:
                all_orders.extend(db_orders)
                sources_loaded += 1
                print(f"   ✅ Database: {len(db_orders)} active orders resumed")
            else:
                print("   ℹ️  Database: No active orders to resume")
                
        except Exception as e:
            print(f"   ❌ Database loading failed: {e}")
            # Continue with other sources despite database failure
            
        try:
            # Load from Excel (new orders)
            excel_orders = self._load_from_excel(excel_path)
            if excel_orders:
                all_orders.extend(excel_orders)
                sources_loaded += 1
                print(f"   ✅ Excel: {len(excel_orders)} new orders loaded")
            else:
                print("   ℹ️  Excel: No new orders found")
                
        except Exception as e:
            print(f"   ❌ Excel loading failed: {e}")
            # Continue with available orders despite Excel failure

        # <IBKR Order Discovery - Begin>
        try:
            # Load from IBKR (working orders discovery)
            ibkr_orders = self._discover_ibkr_orders()
            if ibkr_orders:
                all_orders.extend(ibkr_orders)
                sources_loaded += 1
                print(f"   ✅ IBKR: {len(ibkr_orders)} working orders discovered")
            else:
                print("   ℹ️  IBKR: No working orders to discover")
                
        except Exception as e:
            print(f"   ❌ IBKR discovery failed: {e}")
            print("   📋 Continuing with Database + Excel orders only")
            # Graceful degradation: continue without IBKR orders
        # <IBKR Order Discovery - End>
            
        # Merge and deduplicate orders from all sources
        merged_orders = self._merge_orders(all_orders)
        
        # <Conflict Logging - Begin>
        # Log any conflicts between sources for manual review
        self._log_order_conflicts(all_orders, merged_orders)
        # <Conflict Logging - End>
        
        print(f"📊 OrderLoadingOrchestrator: {sources_loaded}/3 sources loaded, "
              f"{len(merged_orders)} total orders after deduplication")
              
        return merged_orders

    # <IBKR Order Discovery Methods - Begin>
    def _discover_ibkr_orders(self) -> List[PlannedOrder]:
        """
        Discover working orders from IBKR that should be tracked in our system.
        
        Returns:
            List of PlannedOrder objects representing working IBKR orders
        """
        if not self.ibkr_client or not self.ibkr_client.connected:
            print("   ⚠️  IBKR: Client not connected, skipping discovery")
            return []
            
        try:
            # Get working orders from IBKR
            ibkr_orders = self.ibkr_client.get_open_orders()
            if not ibkr_orders:
                return []
                
            # Convert and filter IBKR orders
            planned_orders = []
            for ibkr_order in ibkr_orders:
                try:
                    planned_order = self._convert_ibkr_order(ibkr_order)
                    if planned_order and self._is_ibkr_order_resumable(planned_order, ibkr_order):
                        planned_orders.append(planned_order)
                except Exception as e:
                    print(f"   ❌ Failed to convert IBKR order {ibkr_order.order_id}: {e}")
                    continue
                    
            return planned_orders
            
        except Exception as e:
            print(f"❌ IBKR order discovery failed: {e}")
            return []
            
    def _convert_ibkr_order(self, ibkr_order: IbkrOrder) -> Optional[PlannedOrder]:
        """
        Convert IBKR order format to PlannedOrder object.
        
        Args:
            ibkr_order: The IBKR order to convert
            
        Returns:
            PlannedOrder object or None if conversion fails
        """
        try:
            # Extract basic order information from IBKR order
            # Note: This is a simplified conversion - you may need to adjust based on your IBKR order structure
            symbol = ibkr_order.symbol
            action = ibkr_order.action  # BUY/SELL
            order_type = ibkr_order.order_type  # LMT/MKT/STP
            
            # Use limit price for LMT orders, otherwise use current market data
            entry_price = ibkr_order.lmt_price if ibkr_order.lmt_price else 0.0
            
            # For stop orders, use aux price as stop loss
            stop_loss = ibkr_order.aux_price if ibkr_order.aux_price else 0.0
            
            # Create a basic PlannedOrder - you may need to adjust based on your requirements
            # This is a placeholder - you'll need to implement proper conversion based on your IBKR order structure
            planned_order = PlannedOrder(
                symbol=symbol,
                action=action,  # This should be an OrderAction enum value
                entry_price=entry_price,
                stop_loss=stop_loss,
                # Add other required fields with default values
                order_type=order_type,  # This should be an OrderType enum value
                security_type="STK",  # Default - adjust based on IBKR contract
                exchange="SMART",     # Default exchange
                currency="USD",       # Default currency
                risk_per_trade=0.01,  # Default risk
                risk_reward_ratio=2.0, # Default R:R
                priority=3,           # Default priority
                position_strategy="CORE"  # Default to CORE for IBKR orders
            )
            
            return planned_order
            
        except Exception as e:
            print(f"   ❌ Failed to convert IBKR order {ibkr_order.order_id}: {e}")
            return None
            
    def _is_ibkr_order_resumable(self, planned_order: PlannedOrder, ibkr_order: IbkrOrder) -> bool:
        """
        Check if an IBKR order should be resumed to our system.
        
        Args:
            planned_order: The converted PlannedOrder
            ibkr_order: The original IBKR order
            
        Returns:
            True if order should be resumed, False otherwise
        """
        # Only resume CORE and HYBRID strategy orders
        # DAY orders are auto-expired and shouldn't be resumed
        strategy = planned_order.position_strategy.upper() if planned_order.position_strategy else "CORE"
        
        if strategy not in ['CORE', 'HYBRID']:
            print(f"   ⏰ IBKR order skipped (strategy: {strategy}): {planned_order.symbol}")
            return False
            
        # Check if order is already in our database to avoid duplicates
        existing_db_order = self.db_session.query(PlannedOrderDB).filter_by(
            symbol=planned_order.symbol,
            action=planned_order.action.value,
            entry_price=planned_order.entry_price
        ).first()
        
        if existing_db_order:
            print(f"   ℹ️  IBKR order already in database: {planned_order.symbol}")
            return False
            
        print(f"   ✅ IBKR order resumable: {planned_order.symbol} (strategy: {strategy})")
        return True
    # <IBKR Order Discovery Methods - End>
        
    def _load_from_database(self) -> List[PlannedOrder]:
        """
        Load and resume active orders from database.
        
        Returns:
            List of PlannedOrder objects representing active database orders
        """
        try:
            # Query for active orders (PENDING or LIVE status)
            active_db_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_(['PENDING', 'LIVE'])
            ).all()
            
            if not active_db_orders:
                return []
                
            # Convert to PlannedOrder objects and filter out expired orders
            planned_orders = []
            for db_order in active_db_orders:
                try:
                    planned_order = self.persistence_service.convert_to_planned_order(db_order)
                    if not self._is_order_expired(db_order):
                        planned_orders.append(planned_order)
                    else:
                        print(f"   ⏰ Order expired: {db_order.symbol} (strategy: {db_order.position_strategy})")
                except Exception as e:
                    print(f"   ❌ Failed to convert DB order {db_order.symbol}: {e}")
                    continue
                    
            return planned_orders
            
        except Exception as e:
            print(f"❌ Database order loading failed: {e}")
            return []
            
    def _load_from_excel(self, excel_path: str) -> List[PlannedOrder]:
        """
        Load new orders from Excel file.
        
        Args:
            excel_path: Path to Excel file
            
        Returns:
            List of PlannedOrder objects from Excel
        """
        try:
            return self.loading_service.load_and_validate_orders(excel_path)
        except Exception as e:
            print(f"❌ Excel order loading failed: {e}")
            return []
            
    def _merge_orders(self, orders: List[PlannedOrder]) -> List[PlannedOrder]:
        """
        Merge and deduplicate orders from multiple sources.
        
        Args:
            orders: List of orders from all sources
            
        Returns:
            Deduplicated list of orders
        """
        if not orders:
            return []
            
        # Use a dictionary to track unique orders by key with source priority
        unique_orders = {}
        
        for order in orders:
            order_key = self._get_order_key(order)
            
            # <Enhanced Source Priority - Begin>
            # Priority: IBKR (reality) > Database > Excel
            current_source = self._get_order_source(order)
            
            if order_key not in unique_orders:
                unique_orders[order_key] = (order, current_source)
            else:
                existing_order, existing_source = unique_orders[order_key]
                # Keep the order from the higher priority source
                if self._get_source_priority(current_source) > self._get_source_priority(existing_source):
                    unique_orders[order_key] = (order, current_source)
                    print(f"   🔄 Source priority: {current_source} over {existing_source} for {order.symbol}")
            # <Enhanced Source Priority - End>
                
        # Extract just the orders from the dictionary
        return [order for order, _ in unique_orders.values()]
        
    def _get_order_key(self, order: PlannedOrder) -> str:
        """
        Generate a unique key for an order for deduplication.
        
        Args:
            order: The PlannedOrder object
            
        Returns:
            String key representing order uniqueness
        """
        # Use symbol, action, and prices to identify unique orders
        # This matches the duplicate detection logic in OrderLifecycleManager
        return f"{order.symbol}_{order.action.value}_{order.entry_price:.4f}_{order.stop_loss:.4f}"

    # <Enhanced Order Merging - Begin>
    def _get_order_source(self, order: PlannedOrder) -> str:
        """
        Determine the source of an order for conflict resolution.
        
        Args:
            order: The PlannedOrder object
            
        Returns:
            String representing order source
        """
        # This is a simplified implementation - you may need a more robust way
        # to track order sources in your actual implementation
        if hasattr(order, '_source'):
            return getattr(order, '_source', 'UNKNOWN')
        return 'UNKNOWN'
        
    def _get_source_priority(self, source: str) -> int:
        """
        Get priority level for order sources (higher = more authoritative).
        
        Args:
            source: The order source
            
        Returns:
            Priority integer (higher = better)
        """
        priority_map = {
            'IBKR': 3,    # Highest priority - reality
            'DATABASE': 2, # Medium priority - system state
            'EXCEL': 1,    # Lowest priority - user input
            'UNKNOWN': 0
        }
        return priority_map.get(source.upper(), 0)
        
    def _log_order_conflicts(self, all_orders: List[PlannedOrder], merged_orders: List[PlannedOrder]) -> None:
        """
        Log conflicts between orders from different sources for manual review.
        
        Args:
            all_orders: All orders from all sources before merging
            merged_orders: Orders after deduplication
        """
        if len(all_orders) <= len(merged_orders):
            return  # No conflicts
            
        conflict_count = len(all_orders) - len(merged_orders)
        if conflict_count > 0:
            print(f"   ⚠️  Found {conflict_count} order conflicts (logged for review)")
            # In a real implementation, you might want to log these to a file or database
            # For now, we just print a summary
    # <Enhanced Order Merging - End>
        
    def _is_order_expired(self, db_order: PlannedOrderDB) -> bool:
        """
        Check if a database order should be considered expired.
        
        Args:
            db_order: The database order to check
            
        Returns:
            True if order is expired, False otherwise
        """
        if not db_order.position_strategy:
            return False  # No strategy = never expire (conservative)
            
        strategy = db_order.position_strategy.upper()
        created_date = db_order.created_at.date() if db_order.created_at else datetime.datetime.now().date()
        current_date = datetime.datetime.now().date()
        
        if strategy == 'DAY':
            # DAY orders expire at the end of the trading day
            # For simplicity, consider expired if created before today
            return created_date < current_date
            
        elif strategy == 'CORE':
            # CORE orders never expire
            return False
            
        elif strategy == 'HYBRID':
            # HYBRID orders expire after 10 days
            expiry_date = created_date + datetime.timedelta(days=10)
            return current_date > expiry_date
            
        else:
            # Unknown strategy - default to not expired
            print(f"   ⚠️  Unknown position strategy: {strategy} for {db_order.symbol}")
            return False
            
    def get_loading_statistics(self, excel_path: str) -> Dict[str, Any]:
        """
        Get statistics about order loading from all sources.
        
        Args:
            excel_path: Path to Excel file
            
        Returns:
            Dictionary with loading statistics
        """
        try:
            db_orders = self._load_from_database()
            excel_orders = self._load_from_excel(excel_path)
            # <Enhanced Statistics - Begin>
            ibkr_orders = self._discover_ibkr_orders()
            all_orders = db_orders + excel_orders + ibkr_orders
            merged_orders = self._merge_orders(all_orders)
            
            return {
                'database_orders': len(db_orders),
                'excel_orders': len(excel_orders),
                'ibkr_orders': len(ibkr_orders),
                'merged_orders': len(merged_orders),
                'duplicates_removed': len(all_orders) - len(merged_orders),
                'sources_available': 3,
                'ibkr_connected': self.ibkr_client and self.ibkr_client.connected,
                'timestamp': datetime.datetime.now()
            }
            # <Enhanced Statistics - End>
            
        except Exception as e:
            return {
                'error': str(e),
                'timestamp': datetime.datetime.now()
            }