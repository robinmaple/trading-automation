"""
SQLAlchemy ORM models defining the database schema for the trading system.
Contains tables for trading strategies, planned orders, executed orders, and their relationships.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean, Enum, JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass

Base = declarative_base()

class PositionStrategy(Base):
    """Lookup table for position management strategies (DAY, CORE, HYBRID)."""
    __tablename__ = 'position_strategies'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)

    planned_orders = relationship("PlannedOrderDB", back_populates="position_strategy")

    def __repr__(self):
        return f"<PositionStrategy(id={self.id}, name='{self.name}')>"

class TradingSetup(Base):
    """Trading strategies and setups that can be associated with planned orders."""
    __tablename__ = 'trading_setups'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text, nullable=True)

    planned_orders = relationship("PlannedOrderDB", back_populates="trading_setup")

    def __repr__(self):
        return f"<TradingSetup(id={self.id}, name='{self.name}')>"

class PlannedOrderDB(Base):
    """Database model for planned trading orders with parameters and status tracking."""
    __tablename__ = 'planned_orders'

    id = Column(Integer, primary_key=True)
    setup_id = Column(Integer, ForeignKey('trading_setups.id'), nullable=True)
    position_strategy_id = Column(Integer, ForeignKey('position_strategies.id'), nullable=False)

    symbol = Column(String(20), nullable=False)
    security_type = Column(String(10), nullable=False)
    action = Column(String(10), nullable=False)
    order_type = Column(String(10), nullable=False)
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    risk_per_trade = Column(Float, nullable=False)
    risk_reward_ratio = Column(Float, nullable=False)
    priority = Column(Integer, nullable=False, default=3)

    status = Column(Enum('PENDING', 'LIVE', 'LIVE_WORKING', 'FILLED', 'CANCELLED', 'EXPIRED',
                        'LIQUIDATED', 'LIQUIDATED_EXTERNALLY', 'REPLACED',
                        name='order_state_enum', native_enum=False),
                   default='PENDING')
    created_at = Column(DateTime, default=datetime.datetime.now)
    planned_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    is_live_trading = Column(Boolean, default=False, nullable=False)

    trading_setup = relationship("TradingSetup", back_populates="planned_orders")
    position_strategy = relationship("PositionStrategy", back_populates="planned_orders")
    executed_orders = relationship("ExecutedOrderDB", back_populates="planned_order")

    def __repr__(self):
        return f"<PlannedOrderDB(id={self.id}, symbol='{self.symbol}', status='{self.status}')>"

class ExecutedOrderDB(Base):
    """Database model for executed orders, tracking fills, commissions, and P&L."""
    __tablename__ = 'executed_orders'

    id = Column(Integer, primary_key=True)
    planned_order_id = Column(Integer, ForeignKey('planned_orders.id'), nullable=False)

    filled_price = Column(Float, nullable=False)
    filled_quantity = Column(Float, nullable=False)
    commission = Column(Float, default=0.0)
    pnl = Column(Float, default=0.0)

    status = Column(String(20), default='FILLED')
    executed_at = Column(DateTime, default=datetime.datetime.now)
    closed_at = Column(DateTime, nullable=True)
    is_live_trading = Column(Boolean, default=False, nullable=False)
    is_open = Column(Boolean, default=True)

    planned_order = relationship("PlannedOrderDB", back_populates="executed_orders")

    def __repr__(self):
        return f"<ExecutedOrderDB(id={self.id}, pnl={self.pnl}, status='{self.status}')>"

class ProbabilityScoreDB(Base):
    """Stores Phase A fill probabilities and feature snapshots for Phase B/ML."""
    __tablename__ = 'probability_scores'

    id = Column(Integer, primary_key=True)
    planned_order_id = Column(Integer, ForeignKey('planned_orders.id'), nullable=True)
    symbol = Column(String(20), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    fill_probability = Column(Float, nullable=False)
    features = Column(JSON, nullable=True)
    score = Column(Float, nullable=True)
    engine_version = Column(String(50), nullable=True)
    source = Column(String(50), nullable=True)

    planned_order = relationship("PlannedOrderDB", backref="probability_scores")

class OrderAttemptDB(Base):
    """Tracks every attempt to place, cancel, or replace an order."""
    __tablename__ = 'order_attempts'

    id = Column(Integer, primary_key=True)
    planned_order_id = Column(Integer, ForeignKey('planned_orders.id'), nullable=False)
    attempt_ts = Column(DateTime, default=datetime.datetime.now)
    attempt_type = Column(Enum('PLACEMENT','CANCELLATION','REPLACEMENT', name='attempt_type_enum', native_enum=False))
    fill_probability = Column(Float, nullable=True)
    effective_priority = Column(Float, nullable=True)
    capital_commitment = Column(Float, nullable=True)
    quantity = Column(Integer, nullable=True)
    status = Column(String(20), nullable=True)
    ib_order_ids = Column(JSON, nullable=True)
    details = Column(JSON, nullable=True)

    planned_order = relationship("PlannedOrderDB", backref="order_attempts")

class OrderLabelDB(Base):
    """Derived labels for ML training (immutable once computed)."""
    __tablename__ = 'order_labels'

    id = Column(Integer, primary_key=True)
    planned_order_id = Column(Integer, ForeignKey('planned_orders.id'), nullable=False)
    label_type = Column(Enum('filled_binary','time_to_fill','profitability','target_hit','stop_hit',
                             name='label_type_enum', native_enum=False))
    label_value = Column(Float, nullable=False)
    computed_at = Column(DateTime, default=datetime.datetime.now)
    notes = Column(Text, nullable=True)

    planned_order = relationship("PlannedOrderDB", backref="order_labels")

class MarketSnapshotDB(Base):
    """Optional low-latency snapshot of market data for ML reconstruction."""
    __tablename__ = 'market_snapshots'

    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    bid = Column(Float, nullable=True)
    ask = Column(Float, nullable=True)
    bid_size = Column(Float, nullable=True)
    ask_size = Column(Float, nullable=True)
    last = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    vwap = Column(Float, nullable=True)
    level2_snapshot = Column(JSON, nullable=True)

# Extend PlannedOrderDB - Begin
PlannedOrderDB.core_timeframe = Column(String(50), nullable=True)
# Extend PlannedOrderDB - End

# Phase B Additions - End
