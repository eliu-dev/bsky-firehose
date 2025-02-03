import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


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
