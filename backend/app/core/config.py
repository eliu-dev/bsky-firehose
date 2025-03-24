from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
class KafkaSettings(BaseSettings):
    """
    Kafka-specific settings loaded from environment variables.
    """
    ENVIRONMENT:str = 'development'
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=f'.env.{ENVIRONMENT}',
        extra='allow'
    )

    # Core Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = Field(frozen=True)

    # Producer settings
    KAFKA_BATCH_SIZE: int = Field(ge=16384, frozen=True)
    KAFKA_LINGER_MS: int = Field(ge=0, frozen=True)
    KAFKA_GROUP_ID_BSKY: str = Field(frozen=True)

    # Consumer settings
    KAFKA_MAX_POLL_RECORDS: int = Field(ge=100, le=1000)
    
    # AWS MSK settings (placeholder for production)
    KAFKA_AWS_REGION: Optional[str] = None
    KAFKA_MSK_CLUSTER_ARN: Optional[str] = None
            
class Settings(BaseSettings):
    """
    Main application settings loaded from environment variables.
    """
    ENVIRONMENT:str = 'development'

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=f'.env.{ENVIRONMENT}',
        extra='allow'
    )

    # PostgreSQL connection settings
    RESET_DB: str = Field(frozen=True)
    POSTGRES_USER: str = Field(frozen=True)
    POSTGRES_PASSWORD: str = Field(frozen=True)
    POSTGRES_HOST: str = Field(frozen=True)
    POSTGRES_PORT: str = Field(frozen=True)
    POSTGRES_DB: str = Field(frozen=True)

    # API configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Bluesky Analytics"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def CORS_ORIGINS(self) -> list[str]:
        if self.ENVIRONMENT == "development":
            return ["http://localhost:3000"]
        elif self.ENVIRONMENT == "production":
            return ["https://bsky.app"]
        # Default to localhost for any other environment
        return ["http://localhost:3000"]


        
        
# Create global settings objects
# type: ignore comments are used here because Pydantic's BaseSettings
# initialization can't be fully type-checked due to runtime environment variable loading
settings = Settings()  # type: ignore
kafka_settings = KafkaSettings()  # type: ignore
