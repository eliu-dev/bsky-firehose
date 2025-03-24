import logging
import asyncio

from app.services.jetstream_client import JetstreamClient
from app.services.kafka_client import KafkaClient
from app.models.jetstream_types import Message

logger = logging.getLogger(__name__)

# Constants for Kafka topics
TOPIC_FIREHOSE_RAW = "bluesky-firehose-raw"

class IngestClient:
    """
    Client for ingesting data from the Bluesky Firehose and pushing it to Kafka.
    
    This client:
    1. Connects to the Bluesky Firehose using JetstreamClient
    2. Processes incoming messages
    3. Pushes the messages to a Kafka topic for further processing
    """

    def __init__(self) -> None:
        """
        Initialize the ingest client with Jetstream and Kafka clients.
        """
        self.jetstream_client = JetstreamClient()
        self.kafka_client = KafkaClient()
        self.running = False

    async def process_message(self, message: Message) -> None:
        """
        Process a message from the Firehose and push it to Kafka.
        
        Args:
            message: The Message object from the Firehose.
        """
        try:
            # We use message.did as the key for consistent partitioning
            await self.kafka_client.produce_msg(
                TOPIC_FIREHOSE_RAW,
                message.model_dump()
            )
            
            # Log based on message type
            if message.kind == "commit" and message.commit:
                logger.debug(f"Processed commit: {message.commit.collection}/{message.commit.rkey}")
            elif message.kind == "identity":
                logger.info(f"Processed identity update for: {message.did}")
            elif message.kind == "account":
                logger.info(f"Processed account update for: {message.did}")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def stream_data(self) -> None:
        """
        Stream data from the Bluesky Firehose to Kafka.
        """
        self.running = True
        logger.info("Starting Bluesky Firehose ingest...")
        
        # Connect to Jetstream
        await self.jetstream_client.connect()
        
        # Ensure Kafka producer is ready
        await self.kafka_client.ensure_producer()
        
        try:
            # Process messages from Bluesky
            async for message in self.jetstream_client.subscribe():
                if not self.running:
                    break
                    
                await self.process_message(message)
                
        except Exception as e:
            logger.error(f"Error in Firehose stream: {e}")
        finally:
            await self.stop()
            
    async def stop(self) -> None:
        """
        Stop the ingest client.
        """
        self.running = False
        
        # Close connections
        await self.jetstream_client.close()
        await self.kafka_client.close_producer()
        
        logger.info("Bluesky Firehose ingest stopped")


async def start_ingest_client():
    """
    Start the ingest client as a background task.
    """
    client = IngestClient()
    asyncio.create_task(client.stream_data())
    return client