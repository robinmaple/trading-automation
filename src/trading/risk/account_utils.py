# src/core/account_utils.py
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def detect_trading_environment(account_name: Optional[str]) -> str:
    """
    Detect trading environment based on account name.
    
    Args:
        account_name: IBKR account name/number
        
    Returns:
        'paper' if account starts with "DU", 'live' otherwise
    """
    if not account_name:
        logger.warning("No account name provided, defaulting to paper trading")
        return 'paper'
    
    if account_name.upper().startswith('DU'):
        logger.info(f"Detected paper trading account: {account_name}")
        return 'paper'
    else:
        logger.info(f"Detected live trading account: {account_name}")
        return 'live'

def is_paper_account(account_name: Optional[str]) -> bool:
    """
    Check if account is a paper trading account.
    
    Args:
        account_name: IBKR account name/number
        
    Returns:
        True if paper account, False if live account
    """
    return detect_trading_environment(account_name) == 'paper'

def get_ibkr_port(account_name: str) -> int:
    """Return the IBKR API port based on account type."""
    if is_paper_account(account_name):
        return 7497  # Paper trading
    return 7496  # Live trading