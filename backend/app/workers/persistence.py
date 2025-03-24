"""
Worker for consuming messages from Kafka and persisting them to PostgreSQL.

This module provides a worker that:
1. Consumes Bluesky Firehose messages from Kafka
2. Processes and transforms the messages 
3. Persists them to PostgreSQL using the repository layer
"""

import json
import logging
import asyncio
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.services.kafka_client import KafkaClient
from app.models.jetstream_types import Message
from app.repositories.bluesky import BlueskyUserRepository, BlueskyPostRepository, RawMessageRepository

logger = logging.getLogger(__name__)

# Constants for Kafka topics
TOPIC_FIREHOSE_RAW = "bluesky-firehose-raw"


class PersistenceWorker:
    """
    Worker for consuming Bluesky Firehose messages from Kafka and persisting to PostgreSQL.
    
    This worker:
    1. Consumes messages from the Kafka topic
    2. Deserializes the messages into Pydantic models
    3. Uses repositories to persist the data to PostgreSQL
    """
    
    def __init__(self):
        """Initialize the worker."""
        self.kafka_client = KafkaClient()
        self.db_session_generator = get_db()
        self.running = False
    
    async def get_db_session(self) -> AsyncSession:
        """Get a database session from the generator."""
        session = None
        async for s in self.db_session_generator:
            session = s
            break
        if session is None:
            raise RuntimeError("Failed to get database session")
        return session
    
    async def deserialize_message(self, raw_data: Any) -> Optional[Message]:
        """
        Deserialize a raw Kafka message into a Message model.
        
        Args:
            raw_data: Raw bytes data from Kafka.
            
        Returns:
            Deserialized Message object or None if deserialization fails.
        """
        try:
            if isinstance(raw_data, bytes):
                data = json.loads(raw_data.decode('utf-8'))
            else:
                data = raw_data
                
            return Message.model_validate(data)
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            return None
    
    async def process_message(self, message: Message, session: AsyncSession) -> None:
        """
        Process a Bluesky Firehose message and persist it to PostgreSQL.
        
        Args:
            message: Deserialized Message object.
            session: Database session for persistence.
        """
        raw_message = None
        raw_repo = None
        
        try:
            # Create repositories with the session
            user_repo = BlueskyUserRepository(session)
            post_repo = BlueskyPostRepository(session)
            raw_repo = RawMessageRepository(session)
            
            # Store the raw message first
            raw_message = await raw_repo.store_raw_message(message)
            
            # Create a savepoint to allow partial rollback if needed
            await session.begin_nested()
            
            # Process based on message kind
            if message.kind == "identity" and message.identity:
                # Process identity update (user profile)
                await user_repo.upsert_from_identity(message.identity)
                
            elif message.kind == "account" and message.account:
                # Process account status update
                await user_repo.update_from_account(message.account)
                
            elif message.kind == "commit" and message.commit:
                # Process post/content commit
                # Get or create the user with a single operation
                user, created = await user_repo.get_or_create(
                    defaults={
                        "handle": f"unknown-{message.did[-8:]}",  # Temporary handle
                        "seq": 0,
                        "bsky_timestamp": datetime.now(timezone.utc),
                        "active": True
                    },
                    did=message.did
                )
                
                if created:
                    logger.info(f"Created placeholder user for DID: {message.did}")
                
                # Process the post
                await post_repo.process_post_commit(message.commit, user)
            
            # Mark the raw message as processed
            if raw_message:
                message_id = str(raw_message.id)
                await raw_repo.mark_as_processed(message_id)
            
            # Commit the transaction
            await session.commit()
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            await session.rollback()
            
            # Mark the message as processed but with error flag
            try:
                if raw_message and raw_repo:
                    # Include error information for debugging
                    error_data = {
                        "processed": True,
                        "additional_data": {
                            "processing_error": str(e),
                            "error_time": datetime.now(timezone.utc).isoformat()
                        }
                    }
                    await raw_repo.update(str(raw_message.id), error_data)
                    await session.commit()
                    logger.debug(f"Marked message as processed with error: {str(raw_message.id)}")
            except Exception as e2:
                logger.error(f"Failed to mark message with error: {str(e2)}")
                # Don't re-raise to continue processing
    
    async def run(self):
        """
        Run the worker to consume messages from Kafka and persist to PostgreSQL.
        """
        self.running = True
        logger.info("Starting persistence worker...")
        
        await self.kafka_client.create_consumer(
            self.kafka_client.settings.KAFKA_GROUP_ID_BSKY, 
            f"persistence-worker-{datetime.now(timezone.utc).isoformat()}"
        )
        
        try:
            # Subscribe to the Firehose raw topic
            assert self.kafka_client.consumer is not None
            await self.kafka_client.consumer.start()
            self.kafka_client.consumer.subscribe([TOPIC_FIREHOSE_RAW])
            
            logger.info(f"Subscribed to Kafka topic: {TOPIC_FIREHOSE_RAW}")
            
            # Add circuit breaker pattern
            consecutive_errors = 0
            max_consecutive_errors = 5     # Max errors before circuit opens
            backoff_time = 1               # Start with 1 second
            max_backoff_time = 60          # Max 1 minute backoff
            messages_processed = 0
            last_stats_time = datetime.now(timezone.utc)
            
            while self.running:
                try:
                    # Get DB session for this batch
                    session = await self.get_db_session()
                    
                    # Get records from Kafka with timeout
                    records = await self.kafka_client.consumer.getmany(timeout_ms=1000)
                    
                    if not records:
                        # Reset error counter on successful poll with no errors
                        if consecutive_errors > 0:
                            consecutive_errors = max(0, consecutive_errors - 1)  # Graceful reduction
                            backoff_time = max(1, backoff_time // 2)  # Reduce backoff time
                        continue
            
                    batch_size = sum(len(messages) for _, messages in records.items())
                    
                    # Process each partition's records
                    for tp, messages in records.items():
                        logger.info(f"Processing {len(messages)} messages from {tp.topic}:{tp.partition}")
                        
                        for msg in messages:
                            # Deserialize and process
                            message = await self.deserialize_message(msg.value)
                            if message:
                                await self.process_message(message, session)
                                messages_processed += 1
                    
                    # Commit offsets
                    await self.kafka_client.consumer.commit()
                    
                    # Reset circuit breaker on successful processing
                    consecutive_errors = 0
                    backoff_time = 1
                    
                    # Log stats periodically
                    now = datetime.now(timezone.utc)
                    if (now - last_stats_time).total_seconds() >= 60:  # Every minute
                        logger.info(f"Persistence stats: processed {messages_processed} messages in the last minute")
                        messages_processed = 0
                        last_stats_time = now
                
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error in consumer loop ({consecutive_errors}/{max_consecutive_errors}): {str(e)}", exc_info=True)
                    
                    if consecutive_errors >= max_consecutive_errors:
                        # Circuit breaker pattern - back off exponentially
                        backoff_time = min(backoff_time * 2, max_backoff_time)
                        logger.warning(f"Circuit breaker triggered: backing off for {backoff_time} seconds")
                        
                    await asyncio.sleep(backoff_time)  # Backoff with exponential increase
        
        finally:
            # Clean up
            await self.kafka_client.close_consumer()
            logger.info("Persistence worker stopped")
    
    async def stop(self):
        """Stop the worker."""
        self.running = False
        logger.info("Stopping persistence worker...")
        await self.kafka_client.close_consumer()


async def start_persistence_worker():
    """
    Start the persistence worker as a background task.
    """
    worker = PersistenceWorker()
    asyncio.create_task(worker.run())
    return worker
