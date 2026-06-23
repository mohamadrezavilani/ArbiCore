# app/core/config.py

from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "ArbiCore"
    DEBUG: bool = False
    ENVIRONMENT: str = "production"
    API_V1_PREFIX: str = "/api/v1"

    # Database
    DATABASE_URL: str = Field(..., description="PostgreSQL async URL")
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # Security
    SECRET_KEY: str = Field(..., min_length=32)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Arbitrage
    ARBITRAGE_CHECK_INTERVAL_SECONDS: int = 5

    # Exchange API keys
    WALLEX_API_KEY: str = ""
    WALLEX_API_SECRET: str = ""

    # Nobitex – new separate fields
    NOBITEX_API_TOKEN: str = ""          # for Authorization header
    NOBITEX_API_PUBLIC_KEY: str = ""     # for Nobitex-Key header
    NOBITEX_API_PRIVATE_KEY: str = ""    # for signing (hex)

    # Old Nobitex fields (kept for backward compatibility, but not used)
    NOBITEX_API_KEY: str = ""            # deprecated – use TOKEN instead
    NOBITEX_API_SECRET: str = ""         # deprecated – use PRIVATE_KEY instead

    BITPIN_API_KEY: str = ""
    BITPIN_API_SECRET: str = ""

    # Timezone (for display purposes)
    TIMEZONE: str = "Asia/Tehran"

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

settings = Settings()
