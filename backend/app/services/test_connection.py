import asyncio
import logging
from backend.app.services.jetstream_client import JetstreamClient

# Set up logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
            print(f"\nReceived message from: {message.did}")
            if message.commit and message.commit.record:
                print(f"Collection: {message.commit.collection}")
                print(f"Operation: {message.commit.operation}")
    except Exception as e:
        print(f"Error during streaming: {e}")
        print(e.args)
    finally:
        # Always clean up
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_connection())