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

    # Brevo
    brevo_api_key: str = Field(default="")
    brevo_sender_email: str = Field(default="marketing@reelforgeai.io")
    brevo_sender_name: str = Field(default="Larry Barksdale")
    brevo_webhook_secret: str = Field(default="")
    brevo_list_id: int = Field(default=3)  # Brevo contact list ID for prospects
    
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
    
    # Affiliate
    affiliate_signup_base_url: str = Field(default="https://reelforgeai.io/affiliate")
    default_commission_rate: float = Field(default=0.30)
    
    # Compliance
    data_retention_days: int = Field(default=180)

    # Alerts & Monitoring
    alert_email: str = Field(default="barksdale2004@gmail.com")
    bounce_rate_threshold: float = Field(default=0.02)  # 2% - alert if exceeded
    spam_rate_threshold: float = Field(default=0.001)  # 0.1% - alert if exceeded

    # Pipeline Limits (per task run)
    email_extraction_limit: int = Field(default=75)  # 4 runs/day = 300/day capacity
    email_verification_limit: int = Field(default=100)  # 4 runs/day = 400/day capacity
    auto_enrollment_limit: int = Field(default=50)  # 8 runs/day = 400/day capacity
    discovery_keywords_limit: int = Field(default=10)
    discovery_videos_per_keyword: int = Field(default=50)

    # SerpApi (Google Trends)
    serpapi_api_key: str = Field(default="")
    trends_min_interest_score: int = Field(default=25)  # Minimum Google Trends score to keep keyword active
    trends_rising_threshold: int = Field(default=50)  # Score above this = high priority

    # Rate Limits (seconds between API calls)
    youtube_api_rate_limit: float = Field(default=0.5)  # 500ms between YouTube API calls
    email_verification_rate_limit: float = Field(default=0.1)  # 100ms between verification calls
    trends_api_rate_limit: float = Field(default=1.0)  # 1s between SerpApi calls

    # Sync Limits (batch sizes for external syncs)
    brevo_sync_batch_limit: int = Field(default=100)  # Contacts per Brevo sync batch
    brevo_max_sync_per_run: int = Field(default=500)  # Max contacts to sync per task run

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
