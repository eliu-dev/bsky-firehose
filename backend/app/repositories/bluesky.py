"""
Repositories for Bluesky-related database operations.

This module provides specialized repositories for Bluesky data persistence
including users, posts and raw messages.
"""

import logging
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Sequence, cast, Union, TypeVar
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, and_

from app.models.db.bluesky import BlueskyUser, BlueskyPost, RawMessage
from app.models.jetstream_types import Message, Commit, Identity, Account
from app.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

T = TypeVar('T')

def ensure_not_none(value: Optional[T], default: T) -> T:
    """Helper function to ensure a value is not None"""
    return value if value is not None else default


class BlueskyUserRepository(BaseRepository[BlueskyUser]):
    """Repository for BlueskyUser model operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(BlueskyUser, session)
    
    async def get_by_did(self, did: str) -> Optional[BlueskyUser]:
        """Get a user by their DID."""
        return await self.get_by(did=did)
    
    async def get_by_handle(self, handle: str) -> Optional[BlueskyUser]:
        """Get a user by their handle."""
        return await self.get_by(handle=handle)
    
    async def upsert_from_identity(self, identity: Identity) -> BlueskyUser:
        """
        Create or update a user from an Identity message.
        
        Args:
            identity: Bluesky Identity object from Firehose.
            
        Returns:
            The created or updated BlueskyUser instance.
        """
        user_data: Dict[str, Any] = {
            "did": identity.did,
            "handle": identity.handle,
            "seq": identity.seq,
            "bsky_timestamp": identity.time,
            "active": True
        }
        
        existing_user = await self.get_by_did(identity.did)
        
        if existing_user is not None:
            # Only update if the new sequence number is higher
            if existing_user.seq is not None:
                try:
                    current_seq_int = int(str(existing_user.seq))
                    new_seq_int = int(str(identity.seq))
                    
                    if current_seq_int >= new_seq_int:
                        logger.debug(f"Skipping update for user {identity.handle} - seq {current_seq_int} >= {new_seq_int}")
                        return existing_user
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not compare sequence numbers: {e}, proceeding with update")
            
            # Proceed with update
            updated_user = await self.update(
                existing_user.id, 
                user_data
            )
            logger.info(f"Updated user: {identity.handle} (DID: {identity.did}) with seq {identity.seq}")
            # Return the existing user if update failed
            return ensure_not_none(updated_user, existing_user)
        else:
            # Create new user
            new_user = await self.create(user_data)
            logger.info(f"Created new user: {identity.handle} (DID: {identity.did})")
            return new_user
    
    async def update_from_account(self, account: Account) -> BlueskyUser:
        """
        Update a user's active status from an Account message.
        
        Args:
            account: Bluesky Account object from Firehose.
            
        Returns:
            The updated BlueskyUser instance or None if not found.
        """
        existing_user = await self.get_by_did(account.did)
        
        # Only update if the user exists and the new sequence number is higher
        if existing_user is not None:
            # Try to compare sequence numbers
            if existing_user.seq is not None:
                try:
                    current_seq_int = int(str(existing_user.seq))
                    new_seq_int = int(str(account.seq))
                    
                    if current_seq_int >= new_seq_int:
                        logger.debug(f"Skipping account update for {existing_user.handle} - seq {current_seq_int} >= {new_seq_int}")
                        return existing_user
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not compare sequence numbers: {e}, proceeding with update")
            
            # Proceed with update since sequence comparison shows newer data or couldn't be performed
            update_data: Dict[str, Any] = {
                "active": account.active,
                "seq": account.seq,
                "bsky_timestamp": account.time
            }
            
            updated_user = await self.update(
                existing_user.id,
                update_data
            )
            # Return the existing user if update failed
            return ensure_not_none(updated_user, existing_user)
        
        # If we don't find a user, create a minimal placeholder
        # This should be replaced with proper user data when we get an Identity message
        user_data: Dict[str, Any] = {
            "did": account.did,
            "handle": f"user_{account.did[-8:]}",  # Create a temporary handle based on DID
            "seq": account.seq,
            "bsky_timestamp": account.time,
            "active": account.active
        }
        
        new_user = await self.create(user_data)
        logger.info(f"Created placeholder user for DID: {account.did}")
        return new_user


class BlueskyPostRepository(BaseRepository[BlueskyPost]):
    """Repository for BlueskyPost model operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(BlueskyPost, session)
    
    async def get_by_uri(self, uri: str) -> Optional[BlueskyPost]:
        """Get a post by its URI."""
        return await self.get_by(uri=uri)
    
    async def get_by_user_did(self, user_did: str, limit: int = 100, offset: int = 0) -> Sequence[BlueskyPost]:
        """
        Get posts by a user's DID.
        
        Args:
            user_did: The DID of the user to get posts for.
            limit: Maximum number of posts to return.
            offset: Number of posts to skip.
            
        Returns:
            List of BlueskyPost instances.
        """
        query = (
            select(BlueskyPost)
            .join(BlueskyUser)
            .where(BlueskyUser.did == user_did)
            .order_by(BlueskyPost.bsky_created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def get_replies_to_post(self, post_uri: str, limit: int = 100, offset: int = 0) -> Sequence[BlueskyPost]:
        """
        Get replies to a specific post.
        
        Args:
            post_uri: The URI of the post to get replies for.
            limit: Maximum number of replies to return.
            offset: Number of replies to skip.
            
        Returns:
            List of BlueskyPost instances representing replies.
        """
        query = (
            select(BlueskyPost)
            .where(BlueskyPost.parent_uri == post_uri)
            .order_by(BlueskyPost.bsky_created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def process_post_commit(self, commit: Commit, user: BlueskyUser) -> Optional[BlueskyPost]:
        """
        Process a post commit message from the Bluesky Firehose.
        
        Args:
            commit: The Commit object from the Firehose.
            user: The BlueskyUser instance for the post author.
            
        Returns:
            The created or updated BlueskyPost instance, or None if not applicable.
        """
        # Skip if not a post-related collection or missing record
        if not commit.collection.startswith("app.bsky.feed.") or not commit.record:
            return None
        
        # For delete operations, we need to find and mark the post
        if commit.operation == "delete":
            post_uri = f"at://{user.did}/{commit.collection}/{commit.rkey}"
            existing_post = await self.get_by_uri(post_uri)
            if existing_post is not None:
                # For delete operations, we update the post with a deletion marker
                # rather than actually removing it, which preserves the historical data
                
                # Create a proper dictionary for additional_data
                update_data: Dict[str, Any] = {
                    "operation": "delete"
                }
                
                # Handle additional_data separately to avoid type issues
                timestamp = datetime.now().isoformat()
                
                # Create a new dictionary with the deletion timestamp
                new_data: Dict[str, Any] = {"deleted_at": timestamp}
                
                # Try to merge with existing data if it exists
                if existing_post.additional_data is not None:
                    try:
                        # SQLAlchemy normally returns JSONB as Python dicts
                        if isinstance(existing_post.additional_data, dict):
                            new_data = {**existing_post.additional_data, **new_data}
                    except Exception as e:
                        logger.warning(f"Could not merge additional_data: {e}")
                
                # Update the post with the new data
                update_data: Dict[str, Any] = {
                    "operation": "delete",
                    "additional_data": new_data
                }
                
                # Update the post
                await self.update(
                    existing_post.id,
                    update_data
                )
                logger.info(f"Marked post as deleted: {existing_post.uri}")
            return existing_post
        
        # For create/update operations
        post_uri = f"at://{user.did}/{commit.collection}/{commit.rkey}"
        existing_post = await self.get_by_uri(post_uri)
        
        # Skip if record is None
        if not commit.record:
            return None
        
        # Extract post data
        post_data: Dict[str, Any] = {
            "uri": post_uri,
            "rev": commit.rev,
            "rkey": commit.rkey,
            "collection": commit.collection,
            "operation": commit.operation,
            "user_id": user.id,
        }
        
        # Add properties from record if it exists
        if commit.record:
            # Get CID safely using a safer approach
            cid_value = ""
            
            # Check if record has a subject attribute
            record_subject = getattr(commit.record, "subject", None)
            
            # If it has a subject, safely extract CID
            if record_subject is not None:
                # Extract CID as a string, if it exists
                if hasattr(record_subject, "cid"):
                    # Convert to string to ensure it's a valid string type
                    cid_str = str(record_subject.cid) if record_subject.cid is not None else ""
                    cid_value = cid_str
            
            # Now use the safely extracted CID value
            record_data: Dict[str, Any] = {
                "cid": cid_value,
                "text": getattr(commit.record, "text", None),
                "langs": getattr(commit.record, "langs", None),
                "record_type": getattr(commit.record, "record_type", ""),
                "bsky_created_at": getattr(commit.record, "createdAt", datetime.now()),
            }
            post_data.update(record_data)
        
        # Handle reply data if present, with proper error handling
        if commit.record and hasattr(commit.record, "reply") and commit.record.reply:
            try:
                reply_data: Dict[str, Any] = {}
                
                # Get parent data if available
                parent = getattr(commit.record.reply, "parent", None)
                if parent is not None:
                    if hasattr(parent, "cid"):
                        reply_data["parent_cid"] = str(parent.cid)
                    if hasattr(parent, "uri"):
                        reply_data["parent_uri"] = str(parent.uri)
                
                # Get root data if available
                root = getattr(commit.record.reply, "root", None)
                if root is not None:
                    if hasattr(root, "cid"):
                        reply_data["root_cid"] = str(root.cid)
                    if hasattr(root, "uri"):
                        reply_data["root_uri"] = str(root.uri)
                
                # Update post_data with reply information
                if reply_data:
                    post_data.update(reply_data)
            except (AttributeError, TypeError) as e:
                # Log error but continue processing
                logger.warning(f"Error processing reply data: {e}")
        
        # Store additional data as JSON
        if commit.record:
            try:
                # Extract record data as a dictionary
                record_dict = {}
                try:
                    # Pydantic v2 approach
                    if hasattr(commit.record, "model_dump"):
                        record_dict = commit.record.model_dump(by_alias=False)
                    # Pydantic v1 approach
                    elif hasattr(commit.record, "dict"):
                        record_dict = commit.record.dict()
                except Exception as e:
                    logger.warning(f"Could not convert record to dictionary: {e}")
                
                # Extract fields not already stored in other columns
                excluded_fields = ["text", "langs", "createdAt", "record_type", "subject", "reply"]
                
                # Safely ensure record_dict is a dict for .items() call
                if isinstance(record_dict, dict):
                    additional_fields = {k: v for k, v in record_dict.items() 
                                    if k not in excluded_fields and not k.startswith('_')}
                    
                    # Store the additional fields
                    post_data["additional_data"] = additional_fields
                    
                    logger.debug(f"Stored additional data with {len(additional_fields)} fields")
                else:
                    logger.warning(f"record_dict is not a dictionary: {type(record_dict)}")
            except Exception as e:
                logger.warning(f"Could not extract additional data: {e}", exc_info=True)
        
        if existing_post is not None:
            # Update the existing post
            updated_post = await self.update(
                existing_post.id, 
                post_data
            )
            logger.info(f"Updated post: {post_uri}")
            return ensure_not_none(updated_post, existing_post)
        else:
            # Create a new post
            new_post = await self.create(post_data)
            logger.info(f"Created new post: {post_uri}")
            return new_post


class RawMessageRepository(BaseRepository[RawMessage]):
    """Repository for RawMessage model operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(RawMessage, session)
    
    def _serialize_for_json(self, obj: Any) -> Any:
        """Convert Python objects to JSON serializable values"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        return obj

    async def store_raw_message(self, message: Message) -> RawMessage:
        """
        Store a raw Bluesky message.
        
        Args:
            message: The Message object from Bluesky.
            
        Returns:
            The created RawMessage instance.
        """
        # Create message data dictionary
        message_data: Dict[str, Any] = {
            "did": message.did,
            "time_us": message.time_us,
            "kind": message.kind,
            "processed": False,
        }
        
        # Basic raw data that's always available
        raw_data: Dict[str, Any] = {
            "did": message.did,
            "time_us": message.time_us,
            "kind": message.kind,
        }
        
        # Try to add more fields from the message
        try:
            # Serialize to get all fields
            obj_dict = None
            if hasattr(message, "model_dump"):
                obj_dict = message.model_dump()
            elif hasattr(message, "dict"):
                obj_dict = message.dict()
            
            # If we got a dictionary, convert datetime objects and merge it
            if isinstance(obj_dict, dict):
                # Convert datetimes to ISO strings so they're JSON serializable
                serialized_dict = self._serialize_for_json(obj_dict)
                for k, v in serialized_dict.items():
                    raw_data[k] = v
        except Exception as e:
            # Log but continue with base data
            logger.warning(f"Error serializing message: {e}")
            raw_data["error"] = str(e)
        
        # Set the processed raw_data
        message_data["raw_data"] = raw_data
        
        raw_message = await self.create(message_data)
        logger.debug(f"Stored raw message: {message.kind} from {message.did}")
        return raw_message
    
    async def mark_as_processed(self, message_id: str) -> Optional[RawMessage]:
        """
        Mark a raw message as processed.
        
        Args:
            message_id: The ID of the RawMessage to mark.
            
        Returns:
            The updated RawMessage instance or None if not found.
        """
        return await self.update(message_id, {"processed": True})
    
    async def get_unprocessed(self, limit: int = 100) -> Sequence[RawMessage]:
        """
        Get unprocessed raw messages.
        
        Args:
            limit: Maximum number of messages to return.
            
        Returns:
            List of unprocessed RawMessage instances.
        """
        query = (
            select(RawMessage)
            .where(RawMessage.processed == False)  # Using standard comparison for clarity
            .order_by(RawMessage.time_us.asc())
            .limit(limit)
        )
        result = await self.session.execute(query)
        return result.scalars().all()
