"""
ReelForge Marketing Engine - Configuration
"""

from functools import lru_cache
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    database_url: str = Field(default="postgresql://localhost:5432/reelforge")
    db_pool_min_size: int = Field(default=5)
    db_pool_max_size: int = Field(default=20)
    
    # Redis
    redis_url: str = Field(default="redis://localhost:6379/0")
    
    # YouTube
    youtube_api_key: str = Field(default="")
    
    # Apify
    apify_api_token: str = Field(default="")
    apify_instagram_actor: str = Field(default="apify/instagram-profile-scraper")
    apify_tiktok_actor: str = Field(default="clockworks/tiktok-scraper")
    
    # Brevo
    brevo_api_key: str = Field(default="")
    brevo_sender_email: str = Field(default="jr@reelforgeai.io")
    brevo_sender_name: str = Field(default="Jr from ReelForge")
    brevo_webhook_secret: str = Field(default="")
    
    # Email Verification
    bouncer_api_key: str = Field(default="")
    clearout_api_key: str = Field(default="")
    hunter_api_key: str = Field(default="")
    
    # Sentry
    sentry_dsn: str = Field(default="")
    
    # Reditus
    reditus_api_key: str = Field(default="")
    reditus_program_id: str = Field(default="")
    
    # Outreach
    daily_email_limit: int = Field(default=50)
    min_relevance_score: float = Field(default=0.5)
    
    # Discovery Filters
    min_youtube_subscribers: int = Field(default=5000)
    max_youtube_subscribers: int = Field(default=500000)
    min_instagram_followers: int = Field(default=5000)
    min_tiktok_followers: int = Field(default=10000)
    
    # Affiliate
    affiliate_signup_base_url: str = Field(default="https://reelforgeai.io/affiliate")
    default_commission_rate: float = Field(default=0.40)
    
    # Compliance
    data_retention_days: int = Field(default=180)
    require_email_verification: bool = Field(default=True)
    
    # Application
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    timezone: str = Field(default="America/New_York")
    
    # Worker
    enable_discovery_jobs: bool = Field(default=True)
    enable_outreach_jobs: bool = Field(default=True)
    enable_sync_jobs: bool = Field(default=True)
    
    # Server
    webhook_host: str = Field(default="0.0.0.0")
    webhook_port: int = Field(default=8080)
    admin_api_key: str = Field(default="change-me-in-production")
    
    # Constants
    SECONDS_PER_DAY: int = 86400
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
