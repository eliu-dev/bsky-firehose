import asyncio
import json
import logging
from app.models.jetstream_types import Record
from app.core.logging import setup_local_logging
from app.services.jetstream_client import JetstreamClient
from app.services.kafka_client import KafkaClient

setup_local_logging()
logger = logging.getLogger(__name__)


def test_model():
    print('Test Model')
    test_data = {
        "$type": "app.bsky.feed.post",
        "createdAt": "2025-01-27T16:28:41.519Z",
        "text": "test"
    }
    record = Record.model_validate(test_data)
    print(Record.model_json_schema())

async def test_connection():

    # Create a client that listens for post events
    jetstream_client = JetstreamClient(
        host='us-east-1',
        collections=['app.bsky.feed.post'],
        compress=False 
    )

    kafka_client = KafkaClient()
    
    try:
        # Connect and start receiving messages
        async for message in jetstream_client.stream_messages():
            logger.info(f"\nReceived message from: {message.did}")
            #logger.info(message)
            if message.commit and message.commit.record:
                logger.info(f"Collection: {message.commit.collection}")
                logger.info(f"Operation: {message.commit.operation}")
                print(f'Attempting to write {message.did}')
                kafka_message = {
                    "id": f"{message.did}:{message.commit.collection}:{message.commit.rkey}",
                    "timestamp": message.time_us,
                    "did": message.did,
                    "operation": message.commit.operation,
                    "collection": message.commit.collection,
                    "record": message.commit.record.model_dump() if message.commit.record else None
                }
                await kafka_client.produce_msg('bsky-posts', kafka_message)
    except Exception as e:
        logger.exception(f"Error during streaming: {e}")
    finally:
        # Always clean up
        await jetstream_client.disconnect()

if __name__ == "__main__":
    # test_model()
    asyncio.run(test_connection())