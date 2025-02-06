from datetime import datetime, timezone, timedelta
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.config import KafkaSettings, settings
from typing import Any, Dict, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)
class KafkaClient:

    def __init__(self, settings: KafkaSettings = settings.kafka_settings):
        self.settings: KafkaSettings = settings
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self._producer_closing = False
        self._consumer_closing = False

    
    def create_consumer(self, group_id: str, id: str) -> None:

        if self.consumer is None:
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers = self.settings.KAFKA_BOOTSTRAP_SERVERS,
                client_id=f'{group_id}-{id}',
                group_id = group_id,
                max_poll_records = self.settings.KAFKA_MAX_POLL_RECORDS,
                enable_auto_commit = False
            )
            self._consumer_closing = False
    
    def create_producer(self, group_id: str) -> None:
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers = self.settings.KAFKA_BOOTSTRAP_SERVERS,
                linger_ms = self.settings.KAFKA_LINGER_MS,
                max_batch_size = self.settings.KAFKA_BATCH_SIZE,            
            )
            self._producer_closing = False

    def ensure_producer(self):
        self.create_producer(self.settings.KAFKA_GROUP_ID_BSKY)


    def ensure_consumer(self):
        self.create_consumer(self.settings.KAFKA_GROUP_ID_BSKY, f'{datetime.now(timezone(timedelta(0)))}')
            
    async def produce_msg(self, msg: Dict[str, Any]) -> None:

        self.ensure_producer()
        assert self.producer is not None
        try:
            logger.debug('Starting producer. . . .')
            await self.producer.start()
            logger.debug('Producer started.')
            logger.debug(f'Sending message.')
            # Omit message key to use default round robin partitioning behavior
            await self.producer.send_and_wait(topic = self.settings.KAFKA_TOPIC_NAME, value = msg)
            logger.debug(f'Message sent.')
        except Exception as e:
            logger.exception(f'Kafka producer exception: {e}')
            raise Exception(e)
            
    async def consume_msg(self):
        
        self.ensure_consumer()
        assert self.consumer is not None
        await self.consumer.start()
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
                logger.exception('Exception while closing producer: {e}')

    async def close_consumer(self):
        if self.consumer:
            try:
                logger.debug('Closing producer. . . .')
                self._consumer_closing = True
                await self.consumer.stop()
            except Exception as e:
                logger.exception('Exception while closing producer: {e}')

    async def close(self):
        await self.close_consumer()
        await self.close_producer()


        
        