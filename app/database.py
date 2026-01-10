"""
ReelForge Marketing Engine - Database Connection
Async PostgreSQL connection pool management
"""

import asyncpg
from typing import Optional
import structlog

from app.config import get_settings

logger = structlog.get_logger()

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def init_database() -> asyncpg.Pool:
    """Initialize the database connection pool."""
    global _pool
    
    if _pool is not None:
        return _pool
    
    settings = get_settings()
    
    try:
        _pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            command_timeout=60,
        )
        
        logger.info(
            "Database pool initialized",
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size
        )
        
        return _pool
        
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise


async def close_database():
    """Close the database connection pool."""
    global _pool
    
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


def get_database() -> asyncpg.Pool:
    """Get the database connection pool."""
    if _pool is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return _pool


class DatabaseTransaction:
    """Context manager for database transactions."""
    
    def __init__(self):
        self.connection = None
        self.transaction = None
    
    async def __aenter__(self):
        pool = get_database()
        self.connection = await pool.acquire()
        self.transaction = self.connection.transaction()
        await self.transaction.start()
        return self.connection
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.transaction.rollback()
        else:
            await self.transaction.commit()
        
        pool = get_database()
        await pool.release(self.connection)
