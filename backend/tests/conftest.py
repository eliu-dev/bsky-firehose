# backend/tests/conftest.py
import pytest
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
from app.services.jetstream_client import JetstreamClient
from app.services.kafka_client import KafkaClient

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test session"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def kafka_container():
    """Create and configure a Kafka container for testing"""
    container = KafkaContainer()
    container.start()
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = container.get_bootstrap_server()
    
    yield container
    container.stop()

@pytest.fixture
async def kafka_client(kafka_container) -> AsyncGenerator[KafkaClient, None]:
    """Create a KafkaClient configured for testing"""
    test_settings = KafkaSettings(
        KAFKA_BOOTSTRAP_SERVERS=kafka_container.get_bootstrap_server(),
        KAFKA_BATCH_SIZE=16384,
        KAFKA_LINGER_MS=0,
        KAFKA_MAX_POLL_RECORDS=100,
        KAFKA_GROUP_ID_BSKY="test-bsky",
        KAFKA_AWS_REGION=None,
        KAFKA_MSK_CLUSTER_ARN=None
    )
    
    client = KafkaClient(settings=test_settings)
    yield client
    await client.close()