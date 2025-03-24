import datetime
from sqlalchemy import Column, DateTime, func
from sqlalchemy.orm import mapped_column, Mapped
from app.core.database import Base

class TimestampMixin:
    """Mixin that adds created_at and updated_at timestamps to models."""
    
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), 
                        onupdate=func.now(), nullable=False)


class DBBase(Base):
    """Base class for all database models."""
  
    __abstract__ = True