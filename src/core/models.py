from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass

Base = declarative_base()

class PositionStrategy(Base):
    """Lookup table for position management strategies"""
    __tablename__ = 'position_strategies'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)  # DAY, CORE, HYBRID
    
    # Relationship to planned orders
    planned_orders = relationship("PlannedOrderDB", back_populates="position_strategy")
    
    def __repr__(self):
        return f"<PositionStrategy(id={self.id}, name='{self.name}')>"

class TradingSetup(Base):
    """Trading strategies and setups (minimal for Phase 1)"""
    __tablename__ = 'trading_setups'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    
    # Relationship to planned orders
    planned_orders = relationship("PlannedOrderDB", back_populates="trading_setup")
    
    def __repr__(self):
        return f"<TradingSetup(id={self.id}, name='{self.name}')>"

class PlannedOrderDB(Base):
    """Database model for planned orders"""
    __tablename__ = 'planned_orders'
    
    id = Column(Integer, primary_key=True)
    setup_id = Column(Integer, ForeignKey('trading_setups.id'), nullable=True)
    position_strategy_id = Column(Integer, ForeignKey('position_strategies.id'), nullable=False)
    
    # Order details
    symbol = Column(String(20), nullable=False)
    security_type = Column(String(10), nullable=False)  # CASH, STK, OPT, FUT
    action = Column(String(10), nullable=False)  # BUY, SELL
    order_type = Column(String(10), nullable=False)  # LMT, MKT, STP
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    risk_per_trade = Column(Float, nullable=False)  # 0.001 for 0.1%
    risk_reward_ratio = Column(Float, nullable=False)  # 2.0
    # Phase 1 - Priority Field - Begin
    priority = Column(Integer, nullable=False, default=3)  # Priority scale 1-5, default medium
    # Phase 1 - Priority Field - End

    # Status tracking
    status = Column(String(20), default='PENDING')  # PENDING, LIVE, FILLED, CANCELLED
    planned_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    
    # Live vs Paper trading tracking - Begin
    is_live_trading = Column(Boolean, default=False, nullable=False)
    # Live vs Paper trading tracking - End
    
    # Relationships
    trading_setup = relationship("TradingSetup", back_populates="planned_orders")
    position_strategy = relationship("PositionStrategy", back_populates="planned_orders")
    executed_orders = relationship("ExecutedOrderDB", back_populates="planned_order")
    
    def __repr__(self):
        return f"<PlannedOrderDB(id={self.id}, symbol='{self.symbol}', status='{self.status}')>"

class ExecutedOrderDB(Base):
    """Database model for executed orders and P&L"""
    __tablename__ = 'executed_orders'
    
    id = Column(Integer, primary_key=True)
    planned_order_id = Column(Integer, ForeignKey('planned_orders.id'), nullable=False)
    
    # Execution details
    filled_price = Column(Float, nullable=False)
    filled_quantity = Column(Float, nullable=False)
    commission = Column(Float, default=0.0)
    pnl = Column(Float, default=0.0)  # Realized P&L
    
    # Status and timestamps
    status = Column(String(20), default='FILLED')  # FILLED, CANCELLED, REJECTED
    executed_at = Column(DateTime, default=datetime.datetime.now)
    closed_at = Column(DateTime, nullable=True)  # When position fully closed
    
    # Live vs Paper trading tracking - Begin
    is_live_trading = Column(Boolean, default=False, nullable=False)
    # Live vs Paper trading tracking - End
    
    # Relationship
    planned_order = relationship("PlannedOrderDB", back_populates="executed_orders")
    
    def __repr__(self):
        return f"<ExecutedOrderDB(id={self.id}, pnl={self.pnl}, status='{self.status}')>"