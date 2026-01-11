"""
ReelForge Marketing Engine - Database Connection
"""

import asyncio
import os
import asyncpg
import structlog

from app.config import get_settings

logger = structlog.get_logger()


async def get_database_async() -> asyncpg.Pool:
    """Create a fresh database pool for each task."""
    settings = get_settings()
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=60,
    )
    logger.info("Database pool created")
    return pool


async def init_database() -> asyncpg.Pool:
    return await get_database_async()


async def close_database():
    pass


class DatabaseTransaction:
    def __init__(self, pool=None):
        self._pool = pool
        self._owns_pool = pool is None
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
            if self._owns_pool and self._pool:
                await self._pool.close()
