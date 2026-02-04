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
    worker_prefetch_multiplier=1,
    result_expires=86400,
    task_default_queue='default',
    task_queues=(
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('discovery', Exchange('discovery'), routing_key='discovery'),
        Queue('outreach', Exchange('outreach'), routing_key='outreach'),
    ),
    beat_schedule={
        'youtube-discovery-daily': {
            'task': 'tasks.discovery_tasks.run_youtube_discovery',
            'schedule': crontab(hour=2, minute=0),
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
        'brevo-sync-daily': {
            'task': 'tasks.maintenance_tasks.sync_contacts_to_brevo',
            'schedule': crontab(hour=6, minute=0),  # Daily at 6 AM UTC
            'options': {'queue': 'default'}
        },
        'deliverability-check-daily': {
            'task': 'tasks.maintenance_tasks.check_deliverability_metrics',
            'schedule': crontab(hour=9, minute=0),  # Daily at 9 AM UTC (after morning sends)
            'options': {'queue': 'default'}
        },
        'keyword-trends-monthly': {
            'task': 'tasks.maintenance_tasks.analyze_keyword_trends',
            'schedule': crontab(day_of_month=1, hour=3, minute=0),  # 1st of each month at 3 AM UTC
            'options': {'queue': 'default'}
        },
    },
)


class BaseTaskWithRetry(celery_app.Task):
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_backoff_max = 3600
    retry_jitter = True
    max_retries = 3
