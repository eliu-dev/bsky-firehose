import pytest
from datetime import datetime, timezone
import logging
from app.services.kafka_client import KafkaClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_kafka_message_processing(kafka_client: KafkaClient):
    """Test the processing of a single message through Kafka"""
    test_did = "did:test:123"
    test_collection = "app.bsky.feed.post"
    test_rkey = "test-rkey"
    
    kafka_message = {
        "id": f"{test_did}:{test_collection}:{test_rkey}",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
        "did": test_did,
        "operation": "create",
        "collection": test_collection,
        "record": {
            "$type": "app.bsky.feed.post",
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "text": "test"
        }
    }
    logger.debug(f"Created test message: {kafka_message}")
    
    try:
        logger.info("Producing message to Kafka...")
        await kafka_client.produce_msg('bsky-posts', kafka_message)
        
        logger.info("Consuming messages from Kafka...")
        messages = []
        async for msg in kafka_client.consume_msg():
            messages.append(msg)
            if len(messages) >= 1:
                break
        
        logger.info(f"Received {len(messages)} messages")
        assert len(messages) == 1
        assert messages[0]['did'] == test_did
        assert messages[0]['collection'] == test_collection
    except Exception as e:
        logger.exception(f"Error during test: {e}")
        raise