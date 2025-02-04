from app.core.config import settings

class KafkaClient:

    def __init__(self, host: str, port: int):
        