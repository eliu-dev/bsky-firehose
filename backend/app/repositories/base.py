from typing import Generic, TypeVar, Type, List, Optional, Any, Dict, Union, Sequence, cast, overload
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.sql import Select

# Define a type variable for any class that can serve as a model
# We don't use DBBase here since it causes type conflicts with actual columns
ModelType = TypeVar("ModelType")


class BaseRepository(Generic[ModelType]):
    """
    Base repository class for CRUD operations.    
    """
    
    def __init__(self, model: Type[ModelType], session: AsyncSession):
        """
        Initialize the repository with a model class and database session.
        
        Args:
            model: The SQLAlchemy model class this repository will handle.
            session: The SQLAlchemy async session for database operations.
        """
        self.model = model
        self.session = session
    
    async def create(self, obj_in: Dict[str, Any]) -> ModelType:
        """
        Create a new record in the database.
        
        Args:
            obj_in: Dictionary of attributes to create the object with.
            
        Returns:
            The created model instance.
        """
        db_obj = self.model(**obj_in)
        self.session.add(db_obj)
        await self.session.flush()
        await self.session.refresh(db_obj)
        return db_obj
        
    async def get_or_create(self, defaults: Dict[str, Any] | None = None, **kwargs) -> tuple[ModelType, bool]:
        """
        Get an object or create it if it doesn't exist.
        
        Args:
            defaults: Default values for creation if object doesn't exist
            **kwargs: Lookup parameters
            
        Returns:
            Tuple of (object, created) where created is a boolean
        """
        defaults = defaults or {}
        instance = await self.get_by(**kwargs)
        if instance:
            return instance, False
            
        # Create new instance with provided defaults and lookup values
        create_data = {**defaults, **kwargs}
        return await self.create(create_data), True
        
    async def bulk_upsert(self, objects: List[Dict[str, Any]], key_fields: List[str]) -> List[ModelType]:
        """
        Upsert multiple objects efficiently.
        
        Args:
            objects: List of dictionaries with object data
            key_fields: List of field names to use as unique constraint
            
        Returns:
            List of updated or created objects
        """
        results: List[ModelType] = []
        
        for obj_data in objects:
            # Extract key fields for lookup
            lookup = {field: obj_data[field] for field in key_fields if field in obj_data}
            
            # Skip if we don't have complete keys
            if len(lookup) != len(key_fields):
                continue
                
            # Try to get existing record
            instance = await self.get_by(**lookup)
            
            if instance:
                # Update existing - we access id attribute with getattr to avoid type issues
                instance_id = getattr(instance, "id")
                updated = await self.update(
                    instance_id, 
                    obj_data
                )
                if updated:
                    results.append(updated)
            else:
                # Create new
                created = await self.create(obj_data)
                results.append(created)
                
        return results
    
    async def create_many(self, objs_in: List[Dict[str, Any]]) -> List[ModelType]:
        """
        Create multiple records in the database.
        
        Args:
            objs_in: List of dictionaries with attributes to create objects with.
            
        Returns:
            List of created model instances.
        """
        db_objs = [self.model(**obj_in) for obj_in in objs_in]
        self.session.add_all(db_objs)
        await self.session.flush()
        return db_objs
    
    async def get(self, id: Any) -> Optional[ModelType]:
        """
        Get a record by primary key id.
        
        Args:
            id: The primary key value of the record to retrieve.
            
        Returns:
            The model instance if found, None otherwise.
        """
        # Use dynamic attribute access for id to avoid type issues
        query = select(self.model).where(getattr(self.model, "id") == id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by(self, **kwargs) -> Optional[ModelType]:
        """
        Get a record by arbitrary field values.
        
        Args:
            **kwargs: Field name and value pairs to filter by.
            
        Returns:
            The model instance if found, None otherwise.
        """
        query = select(self.model)
        for key, value in kwargs.items():
            query = query.where(getattr(self.model, key) == value)
            
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> Sequence[ModelType]:
        """
        Get all records with pagination.
        
        Args:
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            
        Returns:
            List of model instances.
        """
        query = select(self.model).limit(limit).offset(offset)
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def update(self, id: Any, obj_in: Dict[str, Any]) -> Optional[ModelType]:
        """
        Update a record by id.
        
        Args:
            id: The primary key of the record to update.
            obj_in: Dictionary of attributes to update.
            
        Returns:
            The updated model instance if found, None otherwise.
        """
        stmt = (
            update(self.model)
            .where(getattr(self.model, "id") == id)
            .values(**obj_in)
            .returning(self.model)
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()
    
    async def delete(self, id: Any) -> bool:
        """
        Delete a record by id.
        
        Args:
            id: The primary key of the record to delete.
            
        Returns:
            True if the record was deleted, False otherwise.
        """
        stmt = delete(self.model).where(getattr(self.model, "id") == id)
        result = await self.session.execute(stmt)
        return result.rowcount > 0
