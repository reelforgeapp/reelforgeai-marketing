"""
ReelForge Marketing Engine - Configuration
Complete settings for production deployment
"""

from functools import lru_cache
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # ===========================================
    # Database
    # ===========================================
    database_url: str = Field(
        default="postgresql://localhost:5432/reelforge",
        description="PostgreSQL connection string"
    )
    db_pool_min_size: int = Field(default=2)
    db_pool_max_size: int = Field(default=10)
    
    # ===========================================
    # Redis (for Celery)
    # ===========================================
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection string for Celery"
    )
    
    # ===========================================
    # YouTube Data API
    # ===========================================
    youtube_api_key: str = Field(default="", description="Google API key for YouTube Data API")
    
    # ===========================================
    # Apify
    # ===========================================
    apify_api_token: str = Field(default="", description="Apify API token")
    apify_instagram_actor: str = Field(default="apify/instagram-profile-scraper")
    apify_tiktok_actor: str = Field(default="clockworks/tiktok-scraper")
    
    # ===========================================
    # Brevo (Email)
    # ===========================================
    brevo_api_key: str = Field(default="", description="Brevo API key")
    brevo_sender_email: str = Field(default="jr@reelforgeai.io")
    brevo_sender_name: str = Field(default="Jr from ReelForge")
    brevo_webhook_secret: str = Field(
        default="",
        description="HMAC secret for webhook validation"
    )
    
    # ===========================================
    # Email Verification
    # ===========================================
    bouncer_api_key: str = Field(
        default="",
        description="Bouncer API key for email verification (primary)"
    )
    clearout_api_key: str = Field(
        default="",
        description="Clearout API key for email verification (alternative)"
    )
    hunter_api_key: str = Field(
        default="",
        description="Hunter.io API key (alternative)"
    )
    
    # ===========================================
    # Sentry (Error Monitoring)
    # ===========================================
    sentry_dsn: str = Field(
        default="",
        description="Sentry DSN for error monitoring"
    )
    
    # ===========================================
    # Reditus (Affiliate Tracking)
    # ===========================================
    reditus_api_key: str = Field(default="", description="Reditus API key")
    reditus_program_id: str = Field(default="", description="Reditus program ID")
    
    # ===========================================
    # Outreach Settings
    # ===========================================
    daily_email_limit: int = Field(
        default=50,
        description="Maximum emails to send per day"
    )
    min_relevance_score: float = Field(
        default=0.5,
        description="Minimum relevance score to contact"
    )
    
    # ===========================================
    # Discovery Filters
    # ===========================================
    min_youtube_subscribers: int = Field(default=5000)
    max_youtube_subscribers: int = Field(default=500000)
    min_instagram_followers: int = Field(default=5000)
    min_tiktok_followers: int = Field(default=10000)
    
    # ===========================================
    # Affiliate Settings
    # ===========================================
    affiliate_signup_base_url: str = Field(
        default="https://reelforgeai.io/affiliate"
    )
    default_commission_rate: float = Field(default=0.40)
    
    # ===========================================
    # Compliance Settings
    # ===========================================
    data_retention_days: int = Field(
        default=180,
        description="Days to retain prospect data before GDPR purge"
    )
    require_email_verification: bool = Field(
        default=True,
        description="Require email verification before outreach"
    )
    
    # ===========================================
    # Application Settings
    # ===========================================
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    timezone: str = Field(default="America/New_York")
    
    # ===========================================
    # Worker Settings
    # ===========================================
    enable_discovery_jobs: bool = Field(default=True)
    enable_outreach_jobs: bool = Field(default=True)
    enable_sync_jobs: bool = Field(default=True)
    
    # ===========================================
    # Server Settings
    # ===========================================
    webhook_host: str = Field(default="0.0.0.0")
    webhook_port: int = Field(default=8080)
    
    # Admin API key for GDPR endpoints
    admin_api_key: str = Field(default="change-me-in-production")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
