"""
ReelForge Marketing Engine - Database Module (Fixed for Celery)
"""

import asyncio
import asyncpg
import structlog
from contextlib import asynccontextmanager

from app.config import get_settings

logger = structlog.get_logger()

# Global pool for FastAPI (web requests only)
_pool = None


async def init_database():
    """Initialize database pool for FastAPI web server."""
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            command_timeout=60
        )
        logger.info("Database pool initialized", min_size=settings.db_pool_min_size, max_size=settings.db_pool_max_size)
    return _pool


async def close_database():
    """Close database pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


def get_database():
    """Get the database pool (for FastAPI dependency injection)."""
    global _pool
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_database() first.")
    return _pool


async def get_database_async():
    """
    Get database connection for Celery tasks.
    Creates a fresh connection each time to avoid event loop issues.
    """
    settings = get_settings()
    
    # For Celery tasks, create a fresh connection (not from pool)
    # This avoids event loop conflicts
    conn = await asyncpg.connect(
        settings.database_url,
        command_timeout=60
    )
    return CeleryDatabaseWrapper(conn)


class CeleryDatabaseWrapper:
    """Wrapper to make single connection look like a pool for Celery tasks."""
    
    def __init__(self, conn):
        self._conn = conn
    
    async def fetch(self, query, *args):
        return await self._conn.fetch(query, *args)
    
    async def fetchrow(self, query, *args):
        return await self._conn.fetchrow(query, *args)
    
    async def fetchval(self, query, *args):
        return await self._conn.fetchval(query, *args)
    
    async def execute(self, query, *args):
        return await self._conn.execute(query, *args)
    
    async def close(self):
        await self._conn.close()


@asynccontextmanager
async def get_db_connection():
    """Context manager for database connection in Celery tasks."""
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url, command_timeout=60)
    try:
        yield conn
    finally:
        await conn.close()


class DatabaseTransaction:
    """
    Async context manager for database transactions in Celery tasks.
    Creates its own connection to avoid event loop issues.
    """
    
    def __init__(self):
        self._conn = None
        self._transaction = None
    
    async def __aenter__(self):
        settings = get_settings()
        self._conn = await asyncpg.connect(settings.database_url, command_timeout=60)
        self._transaction = self._conn.transaction()
        await self._transaction.start()
        return self._conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                await self._transaction.commit()
            else:
                await self._transaction.rollback()
        finally:
            await self._conn.close()
        return False
