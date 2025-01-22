from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings

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
    Detailed health check endpoint that we can expand later
    """
    return {"status": "healthy", "version": "0.1.0", "api_version": "v1"}
