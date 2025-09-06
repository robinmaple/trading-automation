from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from .models import Base, PositionStrategy
import os

class DatabaseManager:
    """Manage database connections and sessions"""
    
    def __init__(self, db_path="trading_automation.db"):
        self.db_path = db_path
        self.engine = None
        self.Session = None
        
    def init_db(self):
        """Initialize database with tables and default data"""
        # Create SQLite database URL
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(db_url, echo=False)  # Set echo=True for debugging
        
        # Create all tables
        Base.metadata.create_all(self.engine)
        
        # Create session factory
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        
        # Initialize default data
        self._init_default_data()
        
        print(f"✅ Database initialized: {self.db_path}")
        return True
    
    def _init_default_data(self):
        """Initialize default lookup data"""
        session = self.Session()
        
        try:
            # Create default position strategies if they don't exist
            strategies = [
                PositionStrategy(name="DAY"),
                PositionStrategy(name="CORE"), 
                PositionStrategy(name="HYBRID")
            ]
            
            for strategy in strategies:
                if not session.query(PositionStrategy).filter_by(name=strategy.name).first():
                    session.add(strategy)
            
            session.commit()
            print("✅ Default position strategies initialized")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Error initializing default data: {e}")
        finally:
            session.close()
    
    def get_session(self):
        """Get a new database session"""
        if not self.Session:
            raise Exception("Database not initialized. Call init_db() first.")
        return self.Session()
    
    def close(self):
        """Close database connections"""
        if self.Session:
            self.Session.remove()
        if self.engine:
            self.engine.dispose()

# Global database instance
db_manager = DatabaseManager()

def init_database():
    """Initialize the database (call this at application start)"""
    return db_manager.init_db()

def get_db_session():
    """Get a database session"""
    return db_manager.get_session()