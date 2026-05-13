from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "ArbiCore"
    DEBUG: bool = False
    ENVIRONMENT: str = "production"
    API_V1_PREFIX: str = "/api/v1"

    # Database - use str to allow any valid SQLAlchemy URL
    DATABASE_URL: str = Field(..., description="PostgreSQL async URL (e.g., postgresql+asyncpg://...)")
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # Security
    SECRET_KEY: str = Field(..., min_length=32)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Arbitrage
    ARBITRAGE_CHECK_INTERVAL_SECONDS: int = 10
    ARBITRAGE_MIN_PROFIT_PERCENT: float = 0.1

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()