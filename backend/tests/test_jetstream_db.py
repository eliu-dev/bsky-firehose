#!/usr/bin/env python
"""
Test script for Bluesky Jetstream to PostgreSQL data flow.

This script tests that our PostgreSQL code changes work properly by:
1. Connecting to Bluesky Jetstream using JetstreamClient
2. Processing a sample of messages through database repositories

"""

import asyncio
import argparse
import logging
import time
from datetime import datetime
from typing import Any, Optional, List, Callable, Awaitable, Sequence, Tuple

from sqlalchemy import Result, Select

from app.core.config import settings
from app.core.logging import setup_local_logging
from app.core.database import AsyncSessionLocal
from app.db.init_db import create_tables

# Import your existing clients and repositories
from app.models.db.bluesky import RawMessage
from app.services.jetstream_client import JetstreamClient
from app.repositories.bluesky import BlueskyUserRepository, BlueskyPostRepository, RawMessageRepository
from app.models.jetstream_types import Message, Commit, Identity, Account

# Set up logging
setup_local_logging()
logger = logging.getLogger("jetstream_db_test")

class JetstreamDbTest:
    """Test the Jetstream-to-PostgreSQL data flow using existing clients."""
    
    def __init__(self, message_limit: Optional[int] = 100):
        """Initialize the test with a specified message limit."""
        self.message_limit = message_limit
        self.message_count = 0
        self.start_time = None
        self.jetstream_client = None
        
        # Stats for tracking
        self.stats = {
            "messages_processed": 0,
            "users_created": 0,
            "users_updated": 0,
            "posts_created": 0,
            "posts_updated": 0,
            "posts_deleted": 0,
            "errors": 0
        }
    
    async def initialize_db(self):
        """Initialize the database if needed."""
        logger.info("Initializing database...")
        await create_tables()
        logger.info("Database initialized.")
    
    async def print_stats(self):
        """Print current statistics."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        msgs_per_sec = self.stats["messages_processed"] / elapsed if elapsed > 0 else 0
        
        print("\n--- Jetstream DB Test Statistics ---")
        print(f"Running for: {elapsed:.1f} seconds")
        print(f"Messages processed: {self.stats['messages_processed']} ({msgs_per_sec:.1f}/sec)")
        print(f"Users: {self.stats['users_created']} created, {self.stats['users_updated']} updated")
        print(f"Posts: {self.stats['posts_created']} created, {self.stats['posts_updated']} updated, {self.stats['posts_deleted']} deleted")
        print(f"Errors: {self.stats['errors']}")
        print("------------------------------------\n")
    
    async def examine_raw_messages(self, limit: int = 5):
        """Examine a sample of raw messages to see what they contain."""
        async with AsyncSessionLocal() as session:
            try:
                # Get some unprocessed raw messages
                from sqlalchemy import select
                from app.models.db.bluesky import RawMessage
                from app.models.jetstream_types import Message
                
                raw_query: Select[Tuple[RawMessage]] = select(RawMessage).order_by(RawMessage.time_us.desc()).limit(limit)
                raw_result: Result[Tuple[RawMessage]] = await session.execute(raw_query)
                raw_messages: Sequence[RawMessage] = raw_result.scalars().all()
                
                if not raw_messages:
                    logger.info("No raw messages found to examine")
                    return
                
                logger.info(f"Examining {len(raw_messages)} raw messages:")
                
                for raw in raw_messages:
                    logger.info(f"Message {raw.id} - Kind: {raw.kind} - DID: {raw.did}")
                    
                    # Try to parse as a Message
                    try:
                        # Extract useful data
                        if raw.kind == "identity":
                            if 'identity' in raw.raw_data:
                                identity_data = raw.raw_data['identity']
                                logger.info(f"  Identity: handle={identity_data.get('handle')}, seq={identity_data.get('seq')}")
                            else:
                                logger.warning("  No identity data found in identity message")
                                
                        elif raw.kind == "commit":
                            if 'commit' in raw.raw_data:
                                commit_data = raw.raw_data['commit']
                                collection = commit_data.get('collection', '')
                                operation = commit_data.get('operation', '')
                                logger.info(f"  Commit: collection={collection}, operation={operation}")
                                
                                # Check if it has a record
                                if 'record' in commit_data:
                                    record_data = commit_data['record']
                                    if isinstance(record_data, dict):
                                        record_type = record_data.get('$type', record_data.get('record_type', 'unknown'))
                                        text = record_data.get('text', '')[:50]
                                        if text:
                                            text = text + '...' if len(text) >= 50 else text
                                        logger.info(f"  Record: type={record_type}, text={text}")
                            else:
                                logger.warning("  No commit data found in commit message")
                    except Exception as e:
                        logger.error(f"  Error examining raw message: {e}")
            except Exception as e:
                logger.error(f"Error examining raw messages: {e}")
    
    async def check_database_stats(self):
        """Query the database to check record counts."""
        async with AsyncSessionLocal() as session:
            try:
                # Create repositories
                user_repo = BlueskyUserRepository(session)
                post_repo = BlueskyPostRepository(session)
                raw_repo = RawMessageRepository(session)
                
                # Get counts using the ORM
                from sqlalchemy import func, select
                from app.models.db.bluesky import BlueskyUser, BlueskyPost, RawMessage
                
                user_query = select(func.count()).select_from(BlueskyUser)
                post_query = select(func.count()).select_from(BlueskyPost)
                raw_query = select(func.count()).select_from(RawMessage)
                
                user_result = await session.execute(user_query)
                post_result = await session.execute(post_query)
                raw_result = await session.execute(raw_query)
                
                total_users = user_result.scalar() or 0
                total_posts = post_result.scalar() or 0
                total_raw = raw_result.scalar() or 0
                
                print(f"\nDatabase contains {total_users} users, {total_posts} posts, and {total_raw} raw messages")
                
                # Get some recent users
                recent_users_query = (
                    select(BlueskyUser.id, BlueskyUser.did, BlueskyUser.handle)
                    .order_by(BlueskyUser.created_at.desc())
                    .limit(3)
                )
                recent_users_result = await session.execute(recent_users_query)
                recent_users = recent_users_result.fetchall()
                
                # Get some recent posts
                recent_posts_query = (
                    select(BlueskyPost.id, BlueskyPost.uri, BlueskyPost.text)
                    .order_by(BlueskyPost.created_at.desc())
                    .limit(3)
                )
                recent_posts_result = await session.execute(recent_posts_query)
                recent_posts = recent_posts_result.fetchall()
                
                # Display samples
                if recent_users:
                    print("\nRecent users:")
                    for user in recent_users:
                        print(f"  - {user.handle} (DID: {user.did[:15]}...)")
                
                if recent_posts:
                    print("\nRecent posts:")
                    for post in recent_posts:
                        text = post.text[:50] + "..." if post.text and len(post.text) > 50 else post.text
                        print(f"  - {text or '[No text]'}")
            except Exception as e:
                logger.error(f"Error checking database stats: {e}")
                self.stats["errors"] += 1
    
    async def process_message(self, message: Message):
        """Process a single message using the repositories."""
        async with AsyncSessionLocal() as session:
            try:
                # Create repositories
                user_repo = BlueskyUserRepository(session)
                post_repo = BlueskyPostRepository(session)
                raw_repo = RawMessageRepository(session)
                
                # Store raw message with error handling
                try:
                    await raw_repo.store_raw_message(message)
                except Exception as e:
                    logger.error(f"Error storing raw message: {e}")
                    # Continue processing even if raw storage fails
                
                # Process based on message type
                if message.kind == "identity" and message.identity:
                    # Process identity message
                    logger.info(f"Processing identity message for {message.identity.handle} (DID: {message.identity.did})")
                    result = await user_repo.upsert_from_identity(message.identity)
                    if result:
                        logger.info(f"Successfully processed identity: {result.handle}")
                        self.stats["users_created" if not hasattr(result, "id") else "users_updated"] += 1
                    else:
                        logger.warning("Failed to process identity message - no result returned")
                
                elif message.kind == "account" and message.account:
                    # Process account message
                    logger.info(f"Processing account message for DID: {message.account.did}")
                    result = await user_repo.update_from_account(message.account)
                    if result:
                        logger.info(f"Successfully processed account: {result.handle}")
                        self.stats["users_updated"] += 1
                    else:
                        logger.warning("Failed to process account message - no result returned")
                
                elif message.kind == "commit" and message.commit:
                    # For commits, we need to get the user first
                    logger.info(f"Processing commit message: {message.commit.collection}/{message.commit.rkey}")
                    user = await user_repo.get_by_did(message.did)
                    
                    if not user:
                        logger.warning(f"User not found for DID: {message.did}, creating placeholder")
                        # Create a placeholder user if not found
                        user = await user_repo.create({
                            "did": message.did,
                            "handle": f"unknown-{message.did[-8:]}",
                            "seq": 0,
                            "bsky_timestamp": datetime.now(),
                            "active": True
                        })
                        logger.info(f"Created placeholder user: {user.handle}")
                    
                    if user and message.commit.collection and message.commit.collection.startswith("app.bsky.feed."):
                        logger.info(f"Processing post from {user.handle}: {message.commit.operation}")
                        result = await post_repo.process_post_commit(message.commit, user)
                        if result:
                            logger.info(f"Successfully processed post: {result.uri}")
                            if message.commit.operation == "delete":
                                self.stats["posts_deleted"] += 1
                            elif message.commit.operation == "create":
                                self.stats["posts_created"] += 1
                            else:
                                self.stats["posts_updated"] += 1
                        else:
                            logger.warning("Failed to process post - no result returned")
                    else:
                        if not user:
                            logger.warning("Failed to get or create user")
                        elif not message.commit.collection:
                            logger.warning("No collection in commit")
                        elif not message.commit.collection.startswith("app.bsky.feed."):
                            logger.info(f"Skipping non-post collection: {message.commit.collection}")
                
                # Commit the transaction
                await session.commit()
                
                # Update stats
                self.stats["messages_processed"] += 1
                
                # Print stats periodically
                if self.stats["messages_processed"] % 10 == 0:
                    await self.print_stats()
                
                # Check if we've reached the limit
                if self.message_limit and self.stats["messages_processed"] >= self.message_limit:
                    logger.info(f"Reached message limit ({self.message_limit}). Stopping.")
                    return True  # Signal to stop
                
                return False  # Continue processing
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await session.rollback()
                self.stats["errors"] += 1
                return False
    
    async def run(self):
        """Run the test."""
        self.start_time = time.time()
        logger.info("Starting Jetstream DB test...")
        
        # Initialize database
        await self.initialize_db()
        
        # Check initial database stats
        await self.check_database_stats()
        
        # Examine raw messages if there are any
        await self.examine_raw_messages()
        
        # Create Jetstream client
        jetstream = JetstreamClient()
        
        try:
            # Connect to Bluesky Jetstream
            logger.info("Connecting to Bluesky Jetstream...")
            
            # Process messages
            stop_requested = False
            async for message in jetstream.subscribe():
                if stop_requested:
                    break
                
                # Process the message
                stop_requested = await self.process_message(message)
            
        except KeyboardInterrupt:
            logger.info("Test interrupted. Shutting down...")
        except Exception as e:
            logger.error(f"Error in test: {e}")
            self.stats["errors"] += 1
        finally:
            # Clean up
            await jetstream.close()
            
            # Print final statistics
            await self.print_stats()
            await self.check_database_stats()
            await self.examine_raw_messages()
            
            logger.info("Jetstream DB test completed.")

async def main():
    """Main entry point for the script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test Bluesky Jetstream to PostgreSQL data flow")
    parser.add_argument("--limit", type=int, default=200, help="Number of messages to process")
    args = parser.parse_args()
    
    # Run the test
    test = JetstreamDbTest(message_limit=args.limit)
    await test.run()

if __name__ == "__main__":
    asyncio.run(main())
