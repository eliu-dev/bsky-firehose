import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.config import KafkaSettings, settings
from typing import Optional
from app.core.config import settings

logger = logging.getLogger(__name__)
class KafkaClient:

    def __init__(self, service_name: str, settings: KafkaSettings = settings.kafka_settings):
        self.service_name: str = service_name
        self.settings: KafkaSettings = settings
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self._closing = False

    
    def create_consumer(self, id, group_id: str) -> None:

        if self.consumer is None:
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers = self.settings.KAFKA_BOOTSTRAP_SERVERS,
                client_id=f'{self.service_name}-{group_id}-{id}',
                group_id = group_id,
                max_poll_records = self.settings.KAFKA_MAX_POLL_RECORDS,
                enable_auto_commit = False
            )
    
    def create_producer(self, group_id: str) -> None:

        if self.producer is None:
            if self.producer is None:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers = self.settings.KAFKA_BOOTSTRAP_SERVERS,
                    linger_ms = self.settings.KAFKA_LINGER_MS,
                    max_batch_size = self.settings.KAFKA_BATCH_SIZE,            
                )

    