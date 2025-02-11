# backend/tests/conftest.py
import pytest
import pytest_asyncio
import asyncio
import logging
import os
from typing import AsyncGenerator
from testcontainers.kafka import KafkaContainer

# Set test environment variables before importing app configs
os.environ.update({
    "POSTGRES_USER": "test_user",
    "POSTGRES_PASSWORD": "test_password",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "test_db",
    "ENVIRONMENT": "test",
    "KAFKA_BATCH_SIZE": "16384",
    "KAFKA_LINGER_MS": "0",
    "KAFKA_MAX_POLL_RECORDS": "100",
    "KAFKA_GROUP_ID_BSKY": "test-group",
    "KAFKA_AWS_REGION": "us-east-1",
    "KAFKA_MSK_CLUSTER_ARN": "",
    "KAFKA_BOOTSTRAP_SERVERS": ""
    
})


from app.core.config import KafkaSettings
from app.services.kafka_client import KafkaClient

logger = logging.getLogger(__name__)

@pytest_asyncio.fixture(scope="session")
async def kafka_container() -> AsyncGenerator[KafkaContainer, None]:
    """Create and configure a Kafka container for testing"""
    container = KafkaContainer()
    container.start()
    
    # Set the bootstrap servers environment variable
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = container.get_bootstrap_server()
    # Create topics using the container's kafka-topics.sh command
    topics = ['bsky-posts', 'bsky-actors']
    for topic in topics:
        container.with_command(f'/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic {topic} --replication-factor 1 --partitions 1')
        logger.info(f"Created topic: {topic}")    
    yield container
    container.stop()

@pytest_asyncio.fixture
async def kafka_client(kafka_container) -> AsyncGenerator[KafkaClient, None]:
    """Create a KafkaClient configured for testing"""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    logger.info(f"Bootstrap servers from container: {bootstrap_servers}")
    
    settings = KafkaSettings(
        KAFKA_BOOTSTRAP_SERVERS=bootstrap_servers,
        KAFKA_BATCH_SIZE=16384,
        KAFKA_LINGER_MS=0,
        KAFKA_MAX_POLL_RECORDS=100,
        KAFKA_GROUP_ID_BSKY="test-group"
    )
    logger.info(f"Created settings with bootstrap servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    
    client = KafkaClient(settings=settings)
    logger.info(f"Created client with settings: {client.settings.model_dump_json()}")
    yield client
    await client.close()