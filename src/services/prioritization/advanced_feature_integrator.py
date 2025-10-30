"""
Advanced feature integration service for prioritization.
Handles integration with market context and historical performance services.
"""

from typing import Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class AdvancedFeatureIntegrator:
    """Handles integration with advanced features for prioritization."""
    
    def __init__(self, market_context_service=None, historical_performance_service=None):
        self.context_logger = get_context_logger()
        self.market_context_service = market_context_service
        self.historical_performance_service = historical_performance_service

    def get_market_context_service(self) -> Optional[object]:
        """Get the market context service if available."""
        return self.market_context_service

    def get_historical_performance_service(self) -> Optional[object]:
        """Get the historical performance service if available."""
        return self.historical_performance_service

    def are_advanced_features_available(self, config: dict) -> bool:
        """Check if advanced features are enabled and services are available."""
        advanced_enabled = config.get('enable_advanced_features', False)
        market_context_available = self.market_context_service is not None
        historical_performance_available = self.historical_performance_service is not None
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Checking advanced feature availability",
            context_provider={
                "advanced_features_enabled": advanced_enabled,
                "market_context_service_available": market_context_available,
                "historical_performance_service_available": historical_performance_available
            }
        )
        # <Context-Aware Logging Integration - End>
            
        return advanced_enabled and (market_context_available or historical_performance_available)