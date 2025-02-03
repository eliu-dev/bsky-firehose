import os
import sys

#sys.path.append(os.path.join(os.getcwd(), ".."))

print("PATH", sys.path)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.services import test_connection
from app.core.logging import setup_local_logging, setup_prod_logging
from app.core.config import settings
import os

ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'dev')
if ENVIRONMENT == 'dev':
    setup_local_logging()
elif ENVIRONMENT == 'prod':
    setup_prod_logging()

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f"{settings.API_V1_STR}/openapi.json"
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """
    Basic health check endpoint
    """
    return {"message": "Bluesky Analytics API is running"}


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy", "version": "0.1.0", "api_version": "v1"}

@app.get("/firehose")
async def get_firehose():
    """
    Shortcut endpoint for initializing firehose
    """
    await test_connection.test_connection()
