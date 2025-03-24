from typing import AsyncGenerator, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData
from app.core.config import settings

# Create engine with modern configuration
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.ENVIRONMENT == 'development',  # Simplified boolean expression
    pool_pre_ping=True,    # Health check connections before using them
    pool_size=20,          # Adjust based on expected concurrent connections
    max_overflow=10,       # Allow temporary additional connections during spikes
    pool_timeout=60,       # Wait time for a connection (seconds)
    pool_recycle=3600      # Recycle connections every hour to prevent stale connections
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# Naming convention for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}


# Create base class for all models
class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models"""
    
    # Apply metadata convention for consistent constraint naming
    metadata = MetaData(naming_convention=convention)
    

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function that creates a new SQLAlchemy session for each request.
    
    Yields:
        AsyncSession: A SQLAlchemy async session for database operations.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
