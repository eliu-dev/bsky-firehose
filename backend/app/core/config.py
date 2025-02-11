from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
class KafkaSettings(BaseSettings):
    """
    Kafka-specific settings loaded from environment variables.
    """

    # Core Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = Field(frozen = True)

    # Producer settings
    KAFKA_BATCH_SIZE: int = Field(ge=16384, frozen = True)
    KAFKA_LINGER_MS: int = Field(ge=0, frozen = True)
    KAFKA_GROUP_ID_BSKY: str = Field(frozen = True)

    # Consumer settings
    KAFKA_MAX_POLL_RECORDS: int = Field(ge=100)
    
    # AWS MSK settings (placeholder for production)
    KAFKA_AWS_REGION: Optional[str] = None
    KAFKA_MSK_CLUSTER_ARN: Optional[str] = None

    model_config = SettingsConfigDict(
        case_sensitive = True,
        env_file = '.env.development',
        extra = 'allow'
    )
            
class Settings(BaseSettings):

    # Environment name
    ENVIRONMENT: str = Field(default = 'development', frozen = True)

    POSTGRES_USER: str = Field(frozen = True)
    POSTGRES_PASSWORD: str = Field(frozen = True)
    POSTGRES_HOST: str = Field(frozen = True)
    POSTGRES_PORT: str = Field(frozen = True)
    POSTGRES_DB: str = Field(frozen = True)
    
    # API configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Bluesky Analytics"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def CORS_ORIGINS(self):
        if self.ENVIRONMENT == "development":
            return ["http://localhost:3000"]
        elif self.ENVIRONMENT == "production":
            return ["https://bsky.app"]

    model_config = SettingsConfigDict(
        case_sensitive = True,
        env_file = '.env.{ENVIRONMENT}',
        extra = 'allow'
    )
        
        
# Create global settings objects
settings = Settings() # type: ignore
kafka_settings = KafkaSettings() # type: ignore
