"""
Manual test script for PostgreSQL connection.

This script tests the PostgreSQL connection, initializes the database,
and performs basic operations to verify functionality.

"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy.future import select

from app.core.logging import setup_local_logging
from app.core.database import AsyncSessionLocal
from app.db.init_db import init_db
from app.models.db.bluesky import BlueskyUser, BlueskyPost, RawMessage
from app.services.db_test import test_database_connection

# Configure logging
setup_local_logging()
logger = logging.getLogger(__name__)


async def test_create_user():
    """Test creating a BlueskyUser in the database."""
    async with AsyncSessionLocal() as session:
        try:
            # Create a test user
            test_user = BlueskyUser(
                id=str(uuid.uuid4()),
                did="did:plc:test12345",
                handle="test_user",
                seq=1,
                bsky_timestamp=datetime.now(timezone.utc),
                active=True
            )
            
            session.add(test_user)
            await session.commit()
            
            logger.info(f"Created test user: {test_user.handle} (ID: {test_user.id})")
            return test_user.id
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating test user: {e}")
            raise


async def test_create_post(user_id):
    """Test creating a BlueskyPost in the database."""
    async with AsyncSessionLocal() as session:
        try:
            # Create a test post
            test_post = BlueskyPost(
                id=str(uuid.uuid4()),
                cid="test-cid-12345",
                uri="at://did:plc:test12345/app.bsky.feed.post/test",
                text="This is a test post from the PostgreSQL test script",
                langs=["en"],
                record_type="app.bsky.feed.post",
                bsky_created_at=datetime.now(timezone.utc),
                rev="rev1",
                rkey="test",
                collection="app.bsky.feed.post",
                operation="create",
                user_id=user_id
            )
            
            session.add(test_post)
            await session.commit()
            
            logger.info(f"Created test post: {test_post.uri} (ID: {test_post.id})")
            return test_post.id
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating test post: {e}")
            raise


async def test_query_data():
    """Test querying data from the database."""
    async with AsyncSessionLocal() as session:
        try:
            # Query users
            user_query = select(BlueskyUser)
            user_result = await session.execute(user_query)
            users = user_result.scalars().all()
            
            logger.info(f"Found {len(users)} users in the database:")
            for user in users:
                logger.info(f"  - {user.handle} (DID: {user.did})")
            
            # Query posts
            post_query = select(BlueskyPost)
            post_result = await session.execute(post_query)
            posts = post_result.scalars().all()
            
            logger.info(f"Found {len(posts)} posts in the database:")
            for post in posts:
                logger.info(f"  - {post.uri}: '{post.text[:50]}...' (User ID: {post.user_id})")
            
            return len(users), len(posts)
        except Exception as e:
            logger.error(f"Error querying data: {e}")
            raise


async def run_postgres_tests():
    """Run a full suite of PostgreSQL tests."""
    logger.info("Starting PostgreSQL tests...")
    
    # Test connection
    db_ok = await test_database_connection()
    if not db_ok:
        logger.error("Database connection failed. Aborting tests.")
        return
    
    logger.info("Database connection successful.")
    
    # Initialize database
    try:
        await init_db()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return
    
    # Run tests
    try:
        # Create user
        user_id = await test_create_user()
        logger.info(f"User created with ID: {user_id}")
        
        # Create post
        post_id = await test_create_post(user_id)
        logger.info(f"Post created with ID: {post_id}")
        
        # Query data
        user_count, post_count = await test_query_data()
        logger.info(f"Query test complete. Found {user_count} users and {post_count} posts.")
        
        logger.info("All PostgreSQL tests completed successfully.")
    except Exception as e:
        logger.error(f"Test failed: {e}")


if __name__ == "__main__":
    asyncio.run(run_postgres_tests())
