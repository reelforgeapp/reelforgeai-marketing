"""
ReelForge Marketing Engine - Database Connection
"""

import asyncio
import asyncpg
from typing import Optional
import structlog

from app.config import get_settings

logger = structlog.get_logger()

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


async def init_database() -> asyncpg.Pool:
    global _pool
    
    if _pool is not None:
        return _pool
    
    async with _pool_lock:
        if _pool is not None:
            return _pool
        
        settings = get_settings()
        
        for attempt in range(3):
            try:
                _pool = await asyncpg.create_pool(
                    settings.database_url,
                    min_size=settings.db_pool_min_size,
                    max_size=settings.db_pool_max_size,
                    command_timeout=60,
                )
                logger.info("Database pool initialized", min_size=settings.db_pool_min_size, max_size=settings.db_pool_max_size)
                return _pool
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise


async def close_database():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_database_async() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        await init_database()
    return _pool


class DatabaseTransaction:
    def __init__(self):
        self._pool = None
        self.connection = None
        self.transaction = None
    
    async def __aenter__(self):
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
