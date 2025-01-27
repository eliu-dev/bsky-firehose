from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, Literal

class JetstreamBase(BaseModel):
    """
    Base class for all Jetstream-related models.
    Defines common configuration and behavior that all Jetstream
    models should share.
    """
    # Pydantic configuration for model validation. Allow extra properties to futureproof against API changes.
    class Config:
        extra: str = 'allow'

class Subject(JetstreamBase):
    cid: str
    uri: str

class Reply(JetstreamBase):
    parent: Subject
    root: Subject
class Record(JetstreamBase):
    record_type: str = Field(alias="$type")
    createdAt: datetime
    subject: Optional[Subject] = None
    text: Optional[str] = None
    langs: Optional[list[str]] = None
    reply: Optional[Reply] = None

class Commit(JetstreamBase):
    rev: str
    operation: Literal['create', 'update', 'delete']
    collection: str
    rkey: str
    record: Optional[Record] = None

class Identity(JetstreamBase):
    did: str
    handle: str
    seq: int
    time: datetime

class Account(JetstreamBase):
    active: bool
    did: str
    seq: int
    time: datetime
    
class Message(JetstreamBase):
    did:str
    time_us: int
    kind: Literal["commit", "identity", "account"]
    commit: Optional[Commit] = None
    identity: Optional[Identity] = None
    account: Optional[Account] = None