"""
Database migration utilities.

This module provides functions to run Alembic migrations as part of the application startup.
"""

import os
import logging
from pathlib import Path
from alembic.config import Config
from alembic import command

logger = logging.getLogger(__name__)

def get_project_root():
    """Get the project root directory"""
    # This assumes the migrations.py file is in app/db/
    current_file = Path(__file__)
    # Go up two levels: from app/db/ to the project root
    return current_file.parent.parent.parent

def run_migrations():
    """Run database migrations to the latest version"""
    try:
        logger.info("Running database migrations...")
        
        # Get project root directory
        project_root = get_project_root()
        alembic_ini_path = project_root / "alembic.ini"
        
        if not alembic_ini_path.exists():
            logger.error(f"Alembic configuration file not found at {alembic_ini_path}")
            return False
        
        # Create Alembic config
        alembic_cfg = Config(str(alembic_ini_path))
        
        # Apply all pending migrations
        command.upgrade(alembic_cfg, "head")
        
        logger.info("Database migrations completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error running database migrations: {e}")
        return False

if __name__ == "__main__":
    # Allow running migrations directly with: python -m app.db.migrations
    logging.basicConfig(level=logging.INFO)
    run_migrations()
