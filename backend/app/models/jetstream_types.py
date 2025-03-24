from datetime import datetime
from typing import Optional, Literal, List, Union, Dict, Any
from pydantic import BaseModel, Field, field_validator

class JetstreamBase(BaseModel):
    """
    Base class for all Jetstream-related models.
    Defines common configuration and behavior that all Jetstream
    models should share.
    """
    # Pydantic v2 configuration for model validation
    model_config = {
        "extra": "allow",         # Allow extra fields for forward compatibility
        "populate_by_name": True  # Allow both alias and original field names
    }

class Subject(JetstreamBase):
    cid: str = ""
    uri: str = ""

class Reply(JetstreamBase):
    parent: Subject
    root: Subject
    
class Record(JetstreamBase):
    record_type: str = Field(alias="$type")
    createdAt: datetime
    subject: Optional[Union[Subject, str]] = Field(default=None)
    text: Optional[str] = Field(default=None)
    langs: Optional[List[str]] = Field(default=None)
    reply: Optional[Reply] = Field(default=None)
    
    @field_validator('subject')
    @classmethod
    def validate_subject(cls, v):
        """Handle string subjects by converting them to Subject objects"""
        if isinstance(v, str):
            # If subject is a string (like a DID), create a Subject with the string as URI
            return Subject(uri=v)
        return v

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
    
    @classmethod
    def model_validate(cls, obj, *args, **kwargs):
        """Handle edge cases during validation"""
        try:
            # First try standard validation
            return super().model_validate(obj, *args, **kwargs)
        except Exception as e:
            # If that fails, attempt to preprocess the object
            if isinstance(obj, dict):
                try:
                    # Make a copy to avoid modifying the original
                    processed_obj = obj.copy()
                    
                    # Fix common validation issues
                    if "commit" in processed_obj and isinstance(processed_obj["commit"], dict):
                        commit = processed_obj["commit"]
                        
                        # Handle string subjects in records
                        if "record" in commit and isinstance(commit["record"], dict):
                            record = commit["record"]
                            
                            if "subject" in record and isinstance(record["subject"], str):
                                # Convert string subject to a Subject object
                                record["subject"] = {"uri": record["subject"], "cid": ""}
                    
                    # Try validation again with the processed object
                    return super().model_validate(processed_obj, *args, **kwargs)
                except Exception:
                    # If preprocessing fails, just pass through the original error
                    pass
            
            # Re-raise the original error if preprocessing didn't help
            raise e