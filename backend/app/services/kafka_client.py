from datetime import date, datetime, timezone, timedelta
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.config import kafka_settings, KafkaSettings
from typing import Any, Dict, Optional

logger: logging.Logger = logging.getLogger(__name__)

class KafkaClient:

    def __init__(self, settings: KafkaSettings = kafka_settings):
        self.settings: KafkaSettings = settings
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self._producer_closing = False
        self._consumer_closing = False


    def json_serializer(self, data) -> bytes:
        """
        AIOKafka-compatible serializer that handles datetime, date, and other 
        Python objects automatically. Returns bytes because that's what Kafka expects.
        """
        def default(obj):
            # Handle the most common Python types that json can't serialize
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            # Let pydantic models serialize themselves
            if hasattr(obj, 'model_dump'):
                return obj.model_dump()
            # For any other types, let the JSON encoder raise the TypeError
            return obj
    
        return json.dumps(data, default=default).encode('utf-8')

    async def create_consumer(self, group_id: str, id: str) -> None:

        if self.consumer is None:
            logger.info('Creating consumer. . . .')
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                client_id=f'{group_id}-{id}',
                group_id=group_id,
                max_poll_records=self.settings.KAFKA_MAX_POLL_RECORDS,
                enable_auto_commit=False
            )
            self._consumer_closing = False
            logger.info('Consumer created.')
    
    async def create_producer(self, group_id: str) -> None:
        if self.producer is None:
            logger.info('Creating producer. . . .')
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                linger_ms=self.settings.KAFKA_LINGER_MS,
                max_batch_size=self.settings.KAFKA_BATCH_SIZE,
                value_serializer=self.json_serializer
            )
            self._producer_closing = False
            logger.info('Producer created.')

    async def ensure_producer(self):
        await self.create_producer(self.settings.KAFKA_GROUP_ID_BSKY)
        assert self.producer is not None
        logger.debug('Starting producer. . . .')
        await self.producer.start()
        logger.debug('Producer started.')

    async def ensure_consumer(self):
        await self.create_consumer(self.settings.KAFKA_GROUP_ID_BSKY, f'{datetime.now(timezone(timedelta(0)))}')
        assert self.consumer is not None
        logger.debug('Starting consumer. . . .')
        await self.consumer.start()
        logger.debug('Consumer started.')

    async def produce_msg(self, topic: str, msg: Dict[str, Any]) -> None:

        await self.ensure_producer()
        assert self.producer is not None
        try:
            logger.debug(f'Sending message.')
            # Omit message key to use default round robin partitioning behavior
            result = await self.producer.send_and_wait(topic, value=msg)
            logger.debug(
                f"Successfully sent message {msg['id']} to "
                f"topic={result.topic}, "
                f"partition={result.partition}, "
                f"offset={result.offset}")
        except Exception as e:
            logger.exception(f'Kafka producer exception: {e}')
            raise Exception(e)
            
    async def consume_msg(self):
        
        await self.ensure_consumer()
        assert self.consumer is not None
        try:
            async for msg in self.consumer:
                if self._consumer_closing:
                    break
                yield msg.value
                await self.consumer.commit()
                logger.info(f'Commited message: {msg}')
        except Exception as e:
            logger.exception(f'Kafka consumer exception: {e}')
        
    async def close_producer(self):
        if self.producer:
            try:
                logger.debug('Closing producer. . . .')
                self._producer_closing = True
                await self.producer.stop()
            except Exception as e:
                logger.exception(f'Exception while closing producer: {e}')

    async def close_consumer(self):
        if self.consumer:
            try:
                logger.debug('Closing consumer. . . .')
                self._consumer_closing = True
                await self.consumer.stop()
            except Exception as e:
                logger.exception(f'Exception while closing consumer: {e}')

    async def close(self):
        await self.close_consumer()
        await self.close_producer()


        
        