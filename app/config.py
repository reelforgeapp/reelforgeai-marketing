"""
ReelForge Marketing Engine - Configuration
"""

from functools import lru_cache
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
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
    brevo_sender_email: str = Field(default="marketing@reelforgeai.io")
    brevo_sender_name: str = Field(default="Larry Barksdale")
    brevo_webhook_secret: str = Field(default="")
    
    # Email Verification
    bouncer_api_key: str = Field(default="")
    clearout_api_key: str = Field(default="")
    hunter_api_key: str = Field(default="")
    
    # Sentry
    sentry_dsn: str = Field(default="")
    
    # Anthropic (AI Personalization)
    anthropic_api_key: str = Field(default="")
    anthropic_model: str = Field(default="claude-3-5-haiku-20241022")
    
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
    default_commission_rate: float = Field(default=0.30)
    
    # Compliance
    data_retention_days: int = Field(default=180)
    
    # Application
    environment: str = Field(default="development")
    admin_api_key: str = Field(default="change-me-in-production")
    
    # Constants
    SECONDS_PER_DAY: int = 86400
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
