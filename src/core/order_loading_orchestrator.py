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


class OrderLoadingOrchestrator:
    """
    Orchestrates order loading from multiple sources with proper deduplication
    and error handling. Supports database order resumption and Excel order loading.
    """
    
    def __init__(self, 
                 loading_service: OrderLoadingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 db_session: Session):
        """
        Initialize the order loading orchestrator with required services.
        
        Args:
            loading_service: Service for loading orders from Excel
            persistence_service: Service for order persistence operations
            state_service: Service for checking system state
            db_session: Database session for querying orders
        """
        self.loading_service = loading_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.db_session = db_session
        
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
            
        # Merge and deduplicate orders from all sources
        merged_orders = self._merge_orders(all_orders)
        
        print(f"📊 OrderLoadingOrchestrator: {sources_loaded}/2 sources loaded, "
              f"{len(merged_orders)} total orders after deduplication")
              
        return merged_orders
        
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
            
        # Use a dictionary to track unique orders by key
        unique_orders = {}
        
        for order in orders:
            order_key = self._get_order_key(order)
            
            # If we haven't seen this order, or if this is a "better" version
            # (e.g., from a more authoritative source), keep it
            if order_key not in unique_orders:
                unique_orders[order_key] = order
            else:
                # For now, keep the first occurrence
                # In future, we could implement source priority
                pass
                
        return list(unique_orders.values())
        
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
            merged_orders = self._merge_orders(db_orders + excel_orders)
            
            return {
                'database_orders': len(db_orders),
                'excel_orders': len(excel_orders),
                'merged_orders': len(merged_orders),
                'duplicates_removed': len(db_orders) + len(excel_orders) - len(merged_orders),
                'sources_available': 2,
                'timestamp': datetime.datetime.now()
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'timestamp': datetime.datetime.now()
            }