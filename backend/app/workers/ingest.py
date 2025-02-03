from app.services.jetstream_client import JetstreamClient
from app.models.jetstream_types import Message

class IngestClient:

    def __init__(self) -> None:
        self.jetstream_client = JetstreamClient()

    async def stream_data(self) -> None:

        await self.jetstream_client.connect()