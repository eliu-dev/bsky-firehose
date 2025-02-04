from enum import Enum
import os
from typing import Dict, Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Environment(str, Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"

class KafkaSettings(BaseSettings):
    """
    Kafka-specific settings that can be loaded from environment variables.
    Each setting includes a description and default value.
    """

    # Core Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC_NAME: str
    KAFKA_TOPIC_PARTITIONS: int = Field(gt=0)
    KAFKA_TOPIC_REPLICATION_FACTOR: int = Field(gt=0)

    # Producer settings
    KAFKA_BATCH_SIZE: int = Field(ge=16384)
    KAFKA_LINGER_MS: int = Field(ge=0)

    # Consumer settings
    KAFKA_MAX_POLL_RECORDS: int = Field(gt=100)
    
    # AWS MSK settings (placeholder for production)
    AWS_REGION: Optional[str] = None
    MSK_CLUSTER_ARN: Optional[str] = None

    @validator('TOPIC_PARTITIONS', 'TOPIC_REPLICATION_FACTOR', 
              'BATCH_SIZE', 'MAX_POLL_RECORDS')
    def validate_positive_integers(cls, v):
        """Validate that certain settings have positive values."""
        if v <= 0:
            raise ValueError("Value must be positive")
        return v
        
class Settings(BaseSettings):
    # Environment name
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "dev")

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: str
    POSTGRES_DB: str

    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS: str

    # API configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Bluesky Analytics"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def CORS_ORIGINS(self):
        if self.ENVIRONMENT == "dev":
            return ["http://localhost:3000"]
        elif self.ENVIRONMENT == "production":
            return ["https://bsky.app"]

    class Config:
        case_sensitive = True


# Create a global settings object
settings = Settings() # type: ignore
