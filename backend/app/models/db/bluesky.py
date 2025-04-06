import datetime
from typing import Any, Dict, List, Optional, Sequence
import uuid

from sqlalchemy import String, Integer, Boolean, DateTime, Text, ForeignKey, Index, CheckConstraint, BigInteger, Uuid
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import relationship, mapped_column, Mapped
from sqlalchemy.types import TypeDecorator, CHAR

from app.models.db.base import DBBase, TimestampMixin


class BlueskyUser(DBBase, TimestampMixin):
    """
    Represents a Bluesky user account.
    
    This model stores user information from the 'identity' and 'account' message types in the Bluesky Jetstream.
    """
    
    __tablename__ = "bluesky_user"
    
    # Primary key and unique identifiers
    id: Mapped[str] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    did: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    handle: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    
    # Account status
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Sequence number and timestamp from Bluesky
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    bsky_timestamp: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    
    # Relationships
    posts: Mapped[List['BlueskyPost']] = relationship("BlueskyPost", back_populates="user", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        # Main lookup index for finding users by ID or handle
        Index("ix_bluesky_user_did_handle", "did", "handle"),
    )


class BlueskyPost(DBBase, TimestampMixin):
    """
    Represents a Bluesky post (including replies).
    
    This model stores post content from 'commit' message types in Bluesky where the record type is a post.
    """
    
    __tablename__ = "bluesky_post"
    
    # Primary key and unique identifiers
    id: Mapped[str] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    cid: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    uri: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    
    # Post content
    text: Mapped[str] = mapped_column(Text)
    langs: Mapped[List[str]] = mapped_column(ARRAY(String(10)))
    
    # Metadata
    record_type: Mapped[str] = mapped_column(String(255))
    bsky_created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rev: Mapped[str] = mapped_column(String(255))
    rkey: Mapped[str] = mapped_column(String(255), nullable=False)
    collection: Mapped[str] = mapped_column(String(255), nullable=False)
    operation: Mapped[str] = mapped_column(String(50), nullable=False)  # create, update, delete
    
    # Foreign keys
    user_id: Mapped[str] = mapped_column(UUID, ForeignKey("bluesky_user.id"), nullable=False)
    user: Mapped['BlueskyUser'] = relationship("BlueskyUser", back_populates="posts")
    
    # Reply relationships
    parent_cid: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    parent_uri: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    root_cid: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    root_uri: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    
    # Additional data as JSON (extensible)
    additional_data: Mapped[Optional[Dict[str,Any]]] = mapped_column(JSONB, nullable=True)
    
    __table_args__ = (
        Index("ix_bluesky_post_uri_cid", "uri", "cid"),
        Index("ix_bluesky_post_bsky_created_at", "bsky_created_at"),
        Index("ix_bluesky_post_user_timeline", "user_id", "bsky_created_at"),
        Index("ix_bluesky_post_reply_tree", "parent_uri", "root_uri"),
        # Search indexes
        Index("ix_bluesky_post_text_search", "text", postgresql_using='gin', postgresql_ops={'text': 'gin_trgm_ops'}),
        
        # Operation filtering
        Index("ix_bluesky_post_operation_type", "operation", "record_type"),
        
        # Data validation
        CheckConstraint("operation IN ('create', 'update', 'delete')", name="ck_bluesky_post_valid_operation")
    )


class RawMessage(DBBase, TimestampMixin):
    """
    Stores raw messages from Bluesky Jetstream for archival and debugging.
    """
    
    __tablename__ = "raw_message"
    
    # Primary key
    id: Mapped[str] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    
    # Message metadata
    did: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_us: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False, index=True)  # commit, identity, account
    
    # Raw message content
    raw_data: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    
    # Processing status
    processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    
    # Indexes
    __table_args__ = (
        # Filter by message kind and processing status
        Index("ix_raw_message_kind_processed", "kind", "processed"),
        
        # Ensure valid message kinds
        CheckConstraint("kind IN ('commit', 'identity', 'account')", name="ck_raw_message_valid_kind")
    )
