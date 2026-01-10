"""
ReelForge Marketing Engine - Celery Configuration
Replaces APScheduler with Celery + Redis for better job persistence
"""

from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue
import os

from app.config import get_settings

settings = get_settings()

# Initialize Celery
celery_app = Celery(
    'reelforge_marketing',
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        'tasks.discovery_tasks',
        'tasks.enrichment_tasks',
        'tasks.outreach_tasks',
        'tasks.maintenance_tasks',
    ]
)

# Celery Configuration
celery_app.conf.update(
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/New_York',
    enable_utc=True,
    
    # Task execution
    task_acks_late=True,  # Acknowledge after task completes (for reliability)
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # One task at a time per worker
    
    # Result backend
    result_expires=86400,  # 24 hours
    
    # Task routing (optional - for future scaling)
    task_default_queue='default',
    task_queues=(
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('discovery', Exchange('discovery'), routing_key='discovery'),
        Queue('outreach', Exchange('outreach'), routing_key='outreach'),
    ),
    
    # Retry settings
    task_default_retry_delay=300,  # 5 minutes
    task_max_retries=3,
    
    # Beat scheduler (for periodic tasks)
    beat_schedule={
        # Discovery tasks
        'youtube-discovery-daily': {
            'task': 'tasks.discovery_tasks.run_youtube_discovery',
            'schedule': crontab(hour=7, minute=0),  # 2 AM EST (7 AM UTC)
            'options': {'queue': 'discovery'}
        },
        'apify-discovery-daily': {
            'task': 'tasks.discovery_tasks.run_apify_discovery',
            'schedule': crontab(hour=8, minute=0),  # 3 AM EST
            'options': {'queue': 'discovery'}
        },
        
        # Enrichment tasks
        'email-extraction-periodic': {
            'task': 'tasks.enrichment_tasks.run_email_extraction',
            'schedule': crontab(hour='*/6', minute=0),  # Every 6 hours
            'options': {'queue': 'default'}
        },
        'email-verification-periodic': {
            'task': 'tasks.enrichment_tasks.run_email_verification',
            'schedule': crontab(hour='*/6', minute=30),  # 30 min after extraction
            'options': {'queue': 'default'}
        },
        
        # Outreach tasks
        'sequence-processing-frequent': {
            'task': 'tasks.outreach_tasks.process_pending_sequences',
            'schedule': crontab(minute='*/15'),  # Every 15 minutes
            'options': {'queue': 'outreach'}
        },
        'auto-enrollment-periodic': {
            'task': 'tasks.outreach_tasks.auto_enroll_prospects',
            'schedule': crontab(hour='*/3', minute=0),  # Every 3 hours
            'options': {'queue': 'outreach'}
        },
        
        # Maintenance tasks
        'data-purge-daily': {
            'task': 'tasks.maintenance_tasks.purge_expired_data',
            'schedule': crontab(hour=9, minute=0),  # 4 AM EST
            'options': {'queue': 'default'}
        },
        'idempotency-cleanup-daily': {
            'task': 'tasks.maintenance_tasks.cleanup_idempotency_keys',
            'schedule': crontab(hour=10, minute=0),  # 5 AM EST
            'options': {'queue': 'default'}
        },
    },
)


# Task base class with common error handling
class BaseTaskWithRetry(celery_app.Task):
    """Base task with automatic retry on failure."""
    
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_backoff_max = 3600  # Max 1 hour
    retry_jitter = True
    max_retries = 3
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Log failures to Sentry."""
        import sentry_sdk
        sentry_sdk.capture_exception(exc)
        super().on_failure(exc, task_id, args, kwargs, einfo)


# Export
__all__ = ['celery_app', 'BaseTaskWithRetry']
