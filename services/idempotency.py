"""
ReelForge Marketing Engine - Idempotency Service
Prevents duplicate email sends using Redis + PostgreSQL dual-layer protection
"""

import hashlib
from datetime import datetime, timedelta
from typing import Optional
import redis.asyncio as redis
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class IdempotencyService:
    """
    Dual-layer idempotency protection:
    1. Redis (fast check, ephemeral)
    2. PostgreSQL (durable, audit trail)
    
    Flow:
    1. Generate key from sequence_id + step + date
    2. Check Redis (fast path)
    3. If not in Redis, check PostgreSQL (slow path)
    4. If not exists anywhere, set Redis key with "processing" status
    5. After successful send, persist to PostgreSQL
    6. Update Redis to "completed"
    
    On failure:
    - Redis key expires after 24 hours
    - PostgreSQL record marks status as "failed"
    - Retry can proceed after Redis expiry
    """
    
    REDIS_KEY_PREFIX = "idem:"
    REDIS_TTL_SECONDS = 86400  # 24 hours
    DB_RETENTION_DAYS = 7
    
    def __init__(self):
        self.settings = get_settings()
        self.redis_client: Optional[redis.Redis] = None
        self.db = get_database()
    
    async def connect(self):
        """Initialize Redis connection."""
        if self.redis_client is None:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
    
    def generate_key(
        self,
        sequence_id: str,
        step_number: int,
        prospect_email: str
    ) -> str:
        """
        Generate a unique idempotency key.
        
        Format: {sequence_id}:{step}:{email_hash}:{date}
        
        The date component ensures the same sequence/step can be
        retried on a different day if needed.
        """
        email_hash = hashlib.sha256(prospect_email.encode()).hexdigest()[:12]
        date_str = datetime.utcnow().strftime("%Y%m%d")
        
        return f"{sequence_id}:{step_number}:{email_hash}:{date_str}"
    
    async def check_and_acquire(self, key: str) -> bool:
        """
        Check if key exists; if not, acquire it.
        
        Returns:
            True if key was acquired (safe to proceed)
            False if key already exists (duplicate, skip)
        """
        await self.connect()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        
        # Fast path: Check Redis
        existing = await self.redis_client.get(redis_key)
        if existing:
            logger.debug(
                "Idempotency key exists in Redis",
                key=key,
                status=existing
            )
            return False
        
        # Slow path: Check PostgreSQL
        db_record = await self.db.fetchrow(
            """
            SELECT id, status FROM idempotency_keys
            WHERE key = $1 AND expires_at > NOW()
            """,
            key
        )
        
        if db_record:
            # Sync to Redis for faster future checks
            await self.redis_client.setex(
                redis_key,
                self.REDIS_TTL_SECONDS,
                db_record['status']
            )
            logger.debug(
                "Idempotency key exists in DB",
                key=key,
                status=db_record['status']
            )
            return False
        
        # Key doesn't exist - acquire it
        # Use SET NX (set if not exists) for race condition safety
        acquired = await self.redis_client.set(
            redis_key,
            "processing",
            nx=True,
            ex=self.REDIS_TTL_SECONDS
        )
        
        if not acquired:
            # Another worker acquired it between our check and set
            logger.debug("Idempotency key acquired by another worker", key=key)
            return False
        
        logger.info("Idempotency key acquired", key=key)
        return True
    
    async def mark_completed(self, key: str) -> None:
        """
        Mark key as completed after successful operation.
        Persists to PostgreSQL for durability.
        """
        await self.connect()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        
        # Update Redis
        await self.redis_client.setex(
            redis_key,
            self.REDIS_TTL_SECONDS,
            "completed"
        )
        
        # Persist to PostgreSQL
        await self.db.execute(
            """
            INSERT INTO idempotency_keys (key, status, created_at, expires_at)
            VALUES ($1, 'completed', NOW(), NOW() + INTERVAL '7 days')
            ON CONFLICT (key) DO UPDATE SET status = 'completed'
            """,
            key
        )
        
        logger.info("Idempotency key marked completed", key=key)
    
    async def mark_failed(self, key: str, error: str = None) -> None:
        """
        Mark key as failed. Allows retry after Redis TTL expires.
        """
        await self.connect()
        
        redis_key = f"{self.REDIS_KEY_PREFIX}{key}"
        
        # Update Redis with shorter TTL for failed operations
        # Allow retry in 1 hour
        await self.redis_client.setex(
            redis_key,
            3600,  # 1 hour
            "failed"
        )
        
        # Log to PostgreSQL for audit
        await self.db.execute(
            """
            INSERT INTO idempotency_keys (key, status, created_at, expires_at)
            VALUES ($1, 'failed', NOW(), NOW() + INTERVAL '1 hour')
            ON CONFLICT (key) DO UPDATE SET 
                status = 'failed',
                expires_at = NOW() + INTERVAL '1 hour'
            """,
            key
        )
        
        logger.warning("Idempotency key marked failed", key=key, error=error)
    
    async def cleanup_expired(self) -> int:
        """
        Remove expired idempotency keys from PostgreSQL.
        Called by maintenance task.
        
        Returns number of keys deleted.
        """
        result = await self.db.execute(
            """
            DELETE FROM idempotency_keys
            WHERE expires_at < NOW()
            """
        )
        
        # Parse "DELETE N" response
        count = int(result.split()[-1]) if result else 0
        
        logger.info("Cleaned up expired idempotency keys", count=count)
        return count


# Singleton instance
_idempotency_service: Optional[IdempotencyService] = None


def get_idempotency_service() -> IdempotencyService:
    """Get the idempotency service singleton."""
    global _idempotency_service
    if _idempotency_service is None:
        _idempotency_service = IdempotencyService()
    return _idempotency_service
