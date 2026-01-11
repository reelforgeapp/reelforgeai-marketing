"""
ReelForge Marketing Engine - Database Connection
Async PostgreSQL connection pool management with lazy initialization
"""

import asyncio
import asyncpg
from typing import Optional
import structlog

from app.config import get_settings

logger = structlog.get_logger()

# Global connection pool
_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


async def init_database() -> asyncpg.Pool:
    """Initialize the database connection pool."""
    global _pool
    
    if _pool is not None:
        return _pool
    
    async with _pool_lock:
        if _pool is not None:
            return _pool
        
        settings = get_settings()
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
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
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Database connection failed, retrying in {retry_delay}s",
                        attempt=attempt + 1,
                        error=str(e)
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to initialize database pool: {e}")
                    raise


async def close_database():
    """Close the database connection pool."""
    global _pool
    
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


async def get_database_async() -> asyncpg.Pool:
    """Get the database connection pool with lazy initialization."""
    global _pool
    
    if _pool is None:
        await init_database()
    
    return _pool


def get_database() -> asyncpg.Pool:
    """Get the database connection pool (sync version)."""
    if _pool is None:
        raise RuntimeError(
            "Database not initialized. Use get_database_async() for lazy initialization."
        )
    return _pool


class DatabaseTransaction:
    """Context manager for database transactions."""
    
    def __init__(self, pool: asyncpg.Pool = None):
        self._pool = pool
        self.connection = None
        self.transaction = None
    
    async def __aenter__(self):
        if self._pool is None:
            self._pool = await get_database_async()
        
        self.connection = await self._pool.acquire()
        self.transaction = self.connection.transaction()
        await self.transaction.start()
        return self.connection
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                await self.transaction.rollback()
            else:
                await self.transaction.commit()
        finally:
            if self.connection:
                await self._pool.release(self.connection)
