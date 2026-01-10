"""
ReelForge Marketing Engine - Idempotency Service
"""

import hashlib
from datetime import datetime
from typing import Optional
import redis.asyncio as redis
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class IdempotencyService:
    """Dual-layer idempotency protection using Redis + PostgreSQL."""
    
    REDIS_KEY_PREFIX = "idem:"
    REDIS_TTL_SECONDS = 86400
    
    def __init__(self):
        self.settings = get_settings()
        self.redis_client: Optional[redis.Redis] = None
        self._db = None
    
    async def get_db_async(self):
        if self._db is None:
            from app.database import get_database_async
            self._db = await get_database_async()
        return self._db
    
    async def connect(self):
        if self.redis_client is None:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
    
    async def close(self):
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
    
    def generate_key(self, sequence_id: str, step_number: int, prospect_email: str) -> str:
        email_hash = hashlib.sha256(prospect_email.encode()).hexdigest()[:12]
        date_str = datetime.utcnow().strftime("%Y%m%d")
        return f"{sequence_id}:{step_number}:{email_hash}:{date_str}"
    
    async def check_and_acquire(self, key: str) -> bool:
        await self.connect()
        db = await self.get_db_async()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        
        existing = await self.redis_client.get(redis_key)
        if existing:
            return False
        
        db_record = await db.fetchrow(
            "SELECT id, status FROM idempotency_keys WHERE key = $1 AND expires_at > NOW()",
            key
        )
        
        if db_record:
            await self.redis_client.setex(redis_key, self.REDIS_TTL_SECONDS, db_record['status'])
            return False
        
        acquired = await self.redis_client.set(redis_key, "processing", nx=True, ex=self.REDIS_TTL_SECONDS)
        
        if not acquired:
            return False
        
        return True
    
    async def mark_completed(self, key: str) -> None:
        await self.connect()
        db = await self.get_db_async()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        await self.redis_client.setex(redis_key, self.REDIS_TTL_SECONDS, "completed")
        
        await db.execute(
            "INSERT INTO idempotency_keys (key, status, created_at, expires_at) VALUES ($1, 'completed', NOW(), NOW() + INTERVAL '7 days') ON CONFLICT (key) DO UPDATE SET status = 'completed'",
            key
        )
    
    async def mark_failed(self, key: str, error: str = None) -> None:
        await self.connect()
        db = await self.get_db_async()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        await self.redis_client.setex(redis_key, 3600, "failed")
        
        await db.execute(
            "INSERT INTO idempotency_keys (key, status, created_at, expires_at) VALUES ($1, 'failed', NOW(), NOW() + INTERVAL '1 hour') ON CONFLICT (key) DO UPDATE SET status = 'failed', expires_at = NOW() + INTERVAL '1 hour'",
            key
        )
    
    async def cleanup_expired(self) -> int:
        db = await self.get_db_async()
        
        result = await db.execute("DELETE FROM idempotency_keys WHERE expires_at < NOW()")
        
        count = 0
        if result:
            try:
                parts = result.split()
                if len(parts) >= 2:
                    count = int(parts[-1])
            except:
                pass
        
        return count


_idempotency_service: Optional[IdempotencyService] = None


def get_idempotency_service() -> IdempotencyService:
    global _idempotency_service
    if _idempotency_service is None:
        _idempotency_service = IdempotencyService()
    return _idempotency_service
