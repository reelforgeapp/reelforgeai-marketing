"""
ReelForge Marketing Engine - Database Connection
"""

import asyncio
import os
import asyncpg
from typing import Optional
import structlog

from app.config import get_settings

logger = structlog.get_logger()

_pools = {}


async def get_database_async() -> asyncpg.Pool:
    """Get or create a database pool for the current process."""
    pid = os.getpid()
    
    if pid not in _pools or _pools[pid] is None:
        settings = get_settings()
        _pools[pid] = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        logger.info("Database pool initialized", pid=pid)
    
    return _pools[pid]


async def init_database() -> asyncpg.Pool:
    return await get_database_async()


async def close_database():
    pid = os.getpid()
    if pid in _pools and _pools[pid] is not None:
        await _pools[pid].close()
        _pools[pid] = None


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
