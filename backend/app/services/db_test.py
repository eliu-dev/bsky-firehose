"""
Database connection test utility.

This module provides functions to test the PostgreSQL connection
and perform basic operations to verify database functionality.
"""

import asyncio
import logging
from sqlalchemy import text

from app.core.database import AsyncSessionLocal
from app.db.init_db import init_db

logger = logging.getLogger(__name__)


async def test_database_connection() -> bool:
    """
    Test the connection to the PostgreSQL database.
    
    Executes a simple query to verify that the connection works
    and the database is responsive.
    """
    logger.info("Testing database connection...")
    async with AsyncSessionLocal() as session:
        try:
            # Simple test query
            result = await session.execute(text("SELECT 1"))
            value = result.scalar()
            
            if value == 1:
                logger.info("✅ Database connection successful!")
                return True
            else:
                logger.error("❌ Database connection test failed: unexpected result")
                return False
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            return False


async def test_with_db_init() -> bool:
    """
    Test database connection and initialize the database.
    
    Checks if the database connection works and then creates all tables.
    """
    # Test connection
    connection_ok = await test_database_connection()
    
    if connection_ok:
        # Initialize database tables
        await init_db()
        logger.info("Database initialized successfully")
        return True
    
    return False


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Run the test
    asyncio.run(test_with_db_init())
