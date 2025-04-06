"""
Client for connecting to and streaming from the Bluesky Jetstream.

This module provides a client for establishing a WebSocket connection to the
Bluesky Jetsteram, processing the stream of events, and yielding
parsed Message objects.
"""

import logging
import zstandard as zstd
import json
import asyncio
from datetime import datetime, timezone

import urllib.parse
from app.models import jetstream_types
from app.models.jetstream_types import Message
from typing import Optional, AsyncGenerator, Dict, Any, List
from pydantic import ValidationError
from websockets import ConnectionClosedOK, ConnectionClosedError
from websockets.asyncio.client import connect, ClientConnection

logger: logging.Logger = logging.getLogger(__name__)


class JetstreamClient:
    """Client for connecting to and streaming Jetstream messages."""

    JETSTREAM_HOSTS: Dict[str, str] = {
        'us-east-1': 'jetstream1.us-east.bsky.network',
        'us-east-2': 'jetstream2.us-east.bsky.network',
        'us-west-1': 'jetstream1.us-west.bsky.network',
        'us-west-2': 'jetstream2.us-west.bsky.network'
    }

    def __init__(
        self, 
        host: str = 'us-east-1', 
        collections: List[str] = [], 
        max_message_size_bytes: int = 0, 
        wanted_dids: List[str] = [],
        compress: bool = False
    ) -> None:
        """
        Initialize the Jetstream Client.
        
        Args:
            host: Bluesky host server to use. Defaults to us-east-1.
            collections: List of Bluesky collections to include.
            max_message_size_bytes: Max message size. Defaults to no limit (0).
            wanted_dids: List of DIDs for retrieving specific records.
            compress: Whether to deliver compressed data. Defaults to false.
        """
        if host not in self.JETSTREAM_HOSTS:
            raise ValueError(f'Invalid Jetstream host specified: {host}. Must be one of {", ".join(self.JETSTREAM_HOSTS.keys())}')
        
        self.host: str = f'wss://{self.JETSTREAM_HOSTS[host]}/subscribe'
        self.collections: List[str] = collections
        self.max_message_size_bytes: int = max_message_size_bytes
        self.wanted_dids: List[str] = wanted_dids
        self.compress: bool = compress
        self.websocket: Optional[ClientConnection] = None
        self.last_cursor: Optional[int] = None
        self._decompressor: Optional[zstd.ZstdDecompressor] = None if not compress else zstd.ZstdDecompressor()

    def _get_subscription_url(self) -> str:
        """Get the full subscription URL with query parameters."""
        query_params: List[str] = []

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
            return f'{self.host}?{"&".join(query_params)}'
        return self.host

    async def connect(self) -> ClientConnection:
        """
        Connect to the Jetstream server.
        
        Returns:
            The websocket connection.
        """
        logger.info(f'Connecting to {self.host}...')
        if self.websocket is None:
            try:
                url: str = self._get_subscription_url()
                logger.info(f'Full URL: {url}')
                self.websocket = await connect(url)
                logger.info(f'Connected successfully')
            except Exception as e:
                logger.exception(f'Connection error: {e}')            
                raise e
        return self.websocket

    async def disconnect(self) -> None:
        """Disconnect from the Jetstream server."""
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
            
    async def close(self) -> None:
        """Close the connection (alias for disconnect)."""
        await self.disconnect()
    
    async def stream_messages(self) -> AsyncGenerator[Message, None]:
        """
        Stream raw messages from the Jetstream Jetstream.
        
        Yields:
            Raw message strings from the Jetstream.
        """
        if self.websocket is None:
            self.websocket = await self.connect()
        
        try:
            async for msg in self.websocket:
                # Handle compressed data if needed
                if self.compress and self._decompressor and isinstance(msg, bytes):
                    # Decompress message if compression is enabled and msg is bytes
                    msg_bytes = self._decompressor.decompress(msg)
                    msg_str = msg_bytes.decode('utf-8')
                elif isinstance(msg, str):
                    # If it's already a string, use it directly
                    msg_str = msg
                else:
                    # Convert any other type to string as best as possible
                    msg_str = str(msg)
                
                # Parse the message into a Message object
                try:
                    message_data = json.loads(msg_str)
                    message = Message.model_validate(message_data)
                    yield message
                except json.JSONDecodeError as e:
                    logger.error(f'Failed to parse JSON: {e}')
                    continue
                except ValidationError as e:
                    logger.warning(f'Validation error for message: {e}')
                    # Create a simplified message with basic info to continue processing
                    try:
                        # Extract essential fields from the raw message
                        if isinstance(message_data, dict):
                            basic_message = {
                                "did": message_data.get("did", ""),
                                "time_us": message_data.get("time_us", 0),
                                "kind": message_data.get("kind", "commit"),
                                # Include other top-level fields but without validation
                                "raw_data": message_data  # Store the original data for debugging
                            }
                            
                            # Try to create a simplified message object
                            # This will skip detailed validation
                            logger.debug(f'Attempting to create simplified message for {basic_message["did"]}')
                            continue  # Skip this message for now
                    except Exception as ex:
                        logger.error(f'Failed to create simplified message: {ex}')
                        continue
        except ConnectionClosedOK:
            logger.info('Connection closed normally.')
        except ConnectionClosedError as e:
            logger.error(f'Connection closed with error: {e}')
            # Try to reconnect
            await asyncio.sleep(5)  # Wait before reconnecting
            self.websocket = None
            await self.connect()
        except Exception as e:
            logger.exception(f'Error in stream_messages: {e}')
            raise

    async def subscribe(self) -> AsyncGenerator[Message, None]:
        """
        Subscribe to Jetstream and yield parsed Message objects.
        
        Yields:
            Parsed Message objects from the Jetstream.
        """
        connection_attempts = 0
        max_attempts = 5
        retry_delay = 5  # seconds
        
        while connection_attempts < max_attempts:
            try:
                async for message in self.stream_messages():
                    # Log a condensed version of the message for debugging
                    if message.kind == 'commit' and message.commit:
                        logger.debug(f"Received commit: {message.did} - {message.commit.collection}/{message.commit.rkey}")
                    else:
                        logger.debug(f"Received {message.kind} message from {message.did}")
                    
                    # Update last cursor
                    self.last_cursor = message.time_us
                    
                    yield message
                
                # If we reach here, the connection was closed normally
                # Reset the connection and try again
                logger.info("Connection closed, reconnecting...")
                self.websocket = None
                await asyncio.sleep(retry_delay)
                await self.connect()
                
            except Exception as e:
                # Handle any other exceptions
                connection_attempts += 1
                logger.error(f"Connection error (attempt {connection_attempts}/{max_attempts}): {e}")
                
                if connection_attempts >= max_attempts:
                    logger.error(f"Max connection attempts reached. Giving up.")
                    raise
                
                # Exponential backoff for retry
                wait_time = retry_delay * (2 ** (connection_attempts - 1))
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                
                # Reset the connection
                self.websocket = None
                await self.connect()
        
    async def resume_from_cursor(self, cursor: Optional[int] = None) -> None:
        """
        Resume Jetstream from a specific cursor.
        
        Args:
            cursor: The cursor to resume from. If None, uses the last seen cursor.
        """
        if cursor is None and self.last_cursor is None:
            logger.warning("No cursor available to resume from")
            return
            
        # Use provided cursor or fall back to last seen cursor
        resume_cursor = cursor if cursor is not None else self.last_cursor
        
        # Reconnect with the cursor parameter
        self.websocket = None
        
        query_params = [f'cursor={resume_cursor}']
        # Add other parameters
        if self.max_message_size_bytes > 0:
            query_params.append(f'max_message_size_bytes={self.max_message_size_bytes}')
        if self.compress:
            query_params.append(f'compress={self.compress}')
        for collection in self.collections:
            query_params.append(f'wantedCollections={urllib.parse.quote_plus(collection)}')
        for wanted_did in self.wanted_dids:
            query_params.append(f'wantedDids={urllib.parse.quote_plus(wanted_did)}')
            
        url = f'{self.host}?{"&".join(query_params)}'
        logger.info(f'Resuming from cursor {resume_cursor}')
        logger.info(f'Resume URL: {url}')
        
        try:
            self.websocket = await connect(url)
            logger.info(f'Reconnected from cursor {resume_cursor}')
        except Exception as e:
            logger.exception(f'Failed to resume from cursor: {e}')
            raise
