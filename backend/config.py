import os
from pydantic import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """
    Application settings that can be configured through environment variables.
    These settings can be expanded as the application grows.
    """

    # Database configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://user:password@localhost:5432/bluesky"
    )

    # API configuration
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Bluesky Analytics"

    class Config:
        case_sensitive = True


# Create a global settings object
settings = Settings()
