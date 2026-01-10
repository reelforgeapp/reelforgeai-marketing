"""
ReelForge Marketing Engine - Celery Configuration
"""

from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue

from app.config import get_settings

settings = get_settings()

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

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/New_York',
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    result_expires=86400,
    task_default_queue='default',
    task_queues=(
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('discovery', Exchange('discovery'), routing_key='discovery'),
        Queue('outreach', Exchange('outreach'), routing_key='outreach'),
    ),
    task_default_retry_delay=300,
    task_max_retries=3,
    beat_schedule={
        'youtube-discovery-daily': {
            'task': 'tasks.discovery_tasks.run_youtube_discovery',
            'schedule': crontab(hour=2, minute=0),
            'options': {'queue': 'discovery'}
        },
        'apify-discovery-daily': {
            'task': 'tasks.discovery_tasks.run_apify_discovery',
            'schedule': crontab(hour=3, minute=0),
            'options': {'queue': 'discovery'}
        },
        'email-extraction-periodic': {
            'task': 'tasks.enrichment_tasks.run_email_extraction',
            'schedule': crontab(hour='*/6', minute=0),
            'options': {'queue': 'default'}
        },
        'email-verification-periodic': {
            'task': 'tasks.enrichment_tasks.run_email_verification',
            'schedule': crontab(hour='*/6', minute=30),
            'options': {'queue': 'default'}
        },
        'sequence-processing-frequent': {
            'task': 'tasks.outreach_tasks.process_pending_sequences',
            'schedule': crontab(minute='*/15'),
            'options': {'queue': 'outreach'}
        },
        'auto-enrollment-periodic': {
            'task': 'tasks.outreach_tasks.auto_enroll_prospects',
            'schedule': crontab(hour='*/3', minute=0),
            'options': {'queue': 'outreach'}
        },
        'data-purge-daily': {
            'task': 'tasks.maintenance_tasks.purge_expired_data',
            'schedule': crontab(hour=4, minute=0),
            'options': {'queue': 'default'}
        },
        'idempotency-cleanup-daily': {
            'task': 'tasks.maintenance_tasks.cleanup_idempotency_keys',
            'schedule': crontab(hour=5, minute=0),
            'options': {'queue': 'default'}
        },
    },
)


class BaseTaskWithRetry(celery_app.Task):
    """Base task with automatic retry on failure."""
    
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_backoff_max = 3600
    retry_jitter = True
    max_retries = 3
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        try:
            import sentry_sdk
            sentry_sdk.capture_exception(exc)
        except:
            pass
        super().on_failure(exc, task_id, args, kwargs, einfo)


__all__ = ['celery_app', 'BaseTaskWithRetry']
