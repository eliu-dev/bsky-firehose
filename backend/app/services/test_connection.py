import asyncio
import logging
from app.models.jetstream_types import Record
from app.core.logging import setup_local_logging
from app.services.jetstream_client import JetstreamClient

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
    client = JetstreamClient(
        host='us-east-1',
        collections=['app.bsky.feed.post'],
        compress=False 
    )
    
    try:
        # Connect and start receiving messages
        async for message in client.stream_messages():
            logger.info(f"\nReceived message from: {message.did}")
            logger.info(message)
            if message.commit and message.commit.record:
                logger.info(f"Collection: {message.commit.collection}")
                logger.info(f"Operation: {message.commit.operation}")
    except Exception as e:
        logger.exception(f"Error during streaming: {e}")
    finally:
        # Always clean up
        await client.disconnect()

if __name__ == "__main__":
    # test_model()
    asyncio.run(test_connection())