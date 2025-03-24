import os
import logging

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import test_connection
from app.core.logging import setup_local_logging, setup_prod_logging
from app.core.config import settings
from app.core.database import get_db
from app.db.init_db import init_db
from app.services.db_test import test_database_connection
from app.workers.persistence import PersistenceWorker, start_persistence_worker
from app.workers.kafka_ingest import start_ingest_client

ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'development')
if ENVIRONMENT == 'development':
    setup_local_logging()
elif ENVIRONMENT == 'production':
    setup_prod_logging()

# Configure logging
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application"""
    global persistence_worker, ingest_client
    
    logger.info("Testing database connection on startup...")
    db_ok: bool = await test_database_connection()
    
    if not db_ok:
        logger.error("Database connection failed on startup")
    else:
        try:
            await init_db()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
    
    try:
        persistence_worker = await start_persistence_worker()
        logger.info("Persistence worker started successfully")
    except Exception as e:
        logger.error(f"Error starting persistence worker: {e}")
    
    try:
        ingest_client = await start_ingest_client()
        logger.info("Ingest client started successfully")
    except Exception as e:
        logger.error(f"Error starting ingest client: {e}")
    
    # Yield control back to FastAPI
    yield
    
    # === SHUTDOWN LOGIC ===
    # Stop the persistence worker
    if persistence_worker:
        await persistence_worker.stop()
        logger.info("Persistence worker stopped")
    
    # Stop the ingest client
    if ingest_client:
        await ingest_client.stop()
        logger.info("Ingest client stopped")

# Create the FastAPI app with lifespan
app = FastAPI(
    title=settings.PROJECT_NAME, 
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
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
    return {"status": "firehose connection initiated"}

@app.get("/db-test")
async def db_test():
    """
    Test database connection and return status.
    """
    db_ok = await test_database_connection()
    return {"status": "ok" if db_ok else "error", "message": "Database connection successful" if db_ok else "Database connection failed"}


# API endpoints for accessing the database
from sqlalchemy.future import select
from app.models.db.bluesky import BlueskyPost, BlueskyUser

@app.get("/api/v1/posts")
async def get_posts(
    limit: int = 10, 
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """
    Get a list of recent posts.
    """
    query = select(BlueskyPost).order_by(BlueskyPost.bsky_created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(query)
    posts = result.scalars().all()
    
    return [{
        "id": post.id,
        "uri": post.uri,
        "text": post.text,
        "created_at": post.bsky_created_at,
        "user_id": post.user_id,
    } for post in posts]


@app.get("/api/v1/users")
async def get_users(
    limit: int = 10, 
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """
    Get a list of users.
    """
    query = select(BlueskyUser).limit(limit).offset(offset)
    result = await db.execute(query)
    users = result.scalars().all()
    
    return [{
        "id": user.id,
        "did": user.did,
        "handle": user.handle,
        "active": user.active,
    } for user in users]