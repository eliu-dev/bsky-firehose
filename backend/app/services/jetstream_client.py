from email import message
import logging
import asyncio
import zstandard as zstd

from multiprocessing import Value
import urllib.parse
from backend.app.models import jetstream_types
from typing import Optional
from pydantic import ValidationError
from websockets import ConnectionClosedOK
from websockets.asyncio.client import connect, ClientConnection

logger: logging.Logger = logging.getLogger(__name__)


class JetstreamClient:
    JETSTREAM_HOSTS: dict[str, str] = {
        'us-east-1': 'jetstream1.us-east.bsky.network',
        'us-east-2': 'jetstream2.us-east.bsky.network',
        'us-west1': 'jetstream1.us-west.bsky.network',
        'us-west2': 'jetstream2.us-west.bsky.network'

    }

    def __init__(self, host: str = 'us-east-1', collections: list[str] = [], max_message_size_bytes: int = 0, wanted_dids: list[str] = [],compress: bool = False):
        if host not in self.JETSTREAM_HOSTS:
            raise ValueError(f'Invalid Jetstream host specified: {host}. Must be one of {', '.join(self.JETSTREAM_HOSTS.keys())}')
        self.host: str = f'wss://{self.JETSTREAM_HOSTS[host]}/subscribe'
        self.collections: list[str] = collections
        self.max_message_size_bytes: int = max_message_size_bytes
        self.wanted_dids: list[str] = wanted_dids
        self.compress: bool = compress
        self.websocket: Optional[ClientConnection]   = None
        self.last_cursor: Optional[int] = None
        self._decompressor: None | zstd.ZstdDecompressor = None if not compress else zstd.ZstdDecompressor()

    def _get_subscription_url(self) -> str:
        query_params: list[str] = []

        # Assemble query parameters manually because url libraries do not support duplicate query parameter keys
        if self.max_message_size_bytes > 0:
            query_params.append(f'max_message_size_bytes={self.max_message_size_bytes}')
        if self.compress:
            query_params.append(f'compress={self.compress}')
        for collection in self.collections:
            query_params.append(f'wantedCollections={urllib.parse.quote_plus(collection)}')
        for wanted_did in self.wanted_dids:
            query_params.append(f'wantedDids={urllib.parse.quote_plus(wanted_did)}')

        if query_params:
            return f'{self.host}?{'&'.join(query_params)}'
        return self.host

    async def connect(self) -> ClientConnection:
        logger.info(f'Connecting to {self.host} . . . .')
        if self.websocket is None:
            try:
                url: str = self._get_subscription_url()
                logger.info(f'Full URL: {url}')
                self.websocket = await connect(url)
                logger.info(f'Connected.')
            except Exception as e:
                logger.exception(e)            
                raise e
        return self.websocket

    async def disconnect(self) -> None:
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
    
    async def stream_messages(self):
        if self.websocket is None:
            self.websocket = await self.connect()
        
        async for msg in self.websocket:
            try:
                parsed_msg: jetstream_types.Message = jetstream_types.Message.model_validate_json(msg)
                print(parsed_msg)
                yield parsed_msg
            except ConnectionClosedOK:
                break
            except ValidationError as e:
                print(f'Jetstream message validation error: {e}')

