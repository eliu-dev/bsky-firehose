"""
Database initialization script.

This module provides functions to initialize the database, create tables,
and perform any needed migrations.
"""

import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
from app.core.database import engine
from app.core.config import settings
from app.models.db.base import Base
# These imports are used by SQLAlchemy metadata even though they appear unused
from app.models.db.bluesky import BlueskyUser, BlueskyPost, RawMessage  # noqa: F401
from app.db.migrations import run_migrations

logger = logging.getLogger(__name__)


async def enable_extensions(db_engine: AsyncEngine = engine) -> bool:
    """
    Enable required PostgreSQL extensions before table creation.
    
    Args:
        db_engine: SQLAlchemy async engine to use.
        
    Returns:
        bool: True if pg_trgm extension was successfully enabled, False otherwise
    """
    
    logger.info("Checking PostgreSQL extensions...")
    trgm_enabled = False
    
    async with db_engine.begin() as conn:
        try:
            # Enable pg_trgm extension for GIN text search index
            logger.info("Enabling pg_trgm extension if not already enabled...")
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            
            # Verify the extension was actually created
            result = await conn.execute(text(
                "SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'"
            ))
            trgm_enabled = bool(result.scalar())
            
            if trgm_enabled:
                logger.info("pg_trgm extension is available")
            else:
                logger.warning("pg_trgm extension could not be confirmed as available")
                
        except Exception as e:
            # Log warning but continue - this allows development on DBs without extension privileges
            logger.warning(f"Could not enable pg_trgm extension: {e}")
            logger.warning("GIN text search index will not be available")
            trgm_enabled = False
    
    return trgm_enabled


async def create_tables(db_engine: AsyncEngine = engine) -> None:
    """
    Create all database tables defined in SQLAlchemy models.
    
    Args:
        db_engine: SQLAlchemy async engine to use for creating tables.
    """
    logger.info("Creating database tables...")
    async with db_engine.begin() as conn:
        # Only drop tables if explicitly configured to do so
        if settings.ENVIRONMENT == 'development' and getattr(settings, 'RESET_DB', False):
            logger.warning("DROPPING ALL TABLES in development mode!")
            await conn.run_sync(Base.metadata.drop_all)
        
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created successfully")


async def create_text_search_index(db_engine: AsyncEngine = engine) -> None:
    """
    Create GIN text search index on the bluesky_post table.
    This should only be called after tables are created and
    if pg_trgm extension is available.
    
    Args:
        db_engine: SQLAlchemy async engine to use.
    """
    
    logger.info("Creating text search index...")
    try:
        async with db_engine.begin() as conn:
            await conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_bluesky_post_text_search "
                "ON bluesky_post USING gin (text gin_trgm_ops)"
            ))
            logger.info("Text search index created successfully")
    except Exception as e:
        logger.warning(f"Could not create text search index: {e}")
        logger.warning("Text search functionality will be limited")


async def init_db() -> None:
    """
    Initialize the database with tables and initial data.
    """
    # First run migrations
    run_migrations()
    
    # Then enable extensions
    pg_trgm_available = await enable_extensions()
    
    # Create tables (will use the updated model with BigInteger)
    await create_tables()
    
    # If pg_trgm is available, create the text search index
    if pg_trgm_available:
        await create_text_search_index()
    
    logger.info("Database initialized successfully")


if __name__ == "__main__":
    # Run the initialization script
    asyncio.run(init_db())
