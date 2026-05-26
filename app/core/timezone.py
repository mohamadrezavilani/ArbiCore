from datetime import datetime
from zoneinfo import ZoneInfo
from app.core.config import settings

def to_local_time(dt: datetime) -> datetime:
    """Convert a UTC datetime to local timezone (from settings)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo(settings.TIMEZONE))

def format_local_time(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format a UTC datetime in local timezone."""
    local_dt = to_local_time(dt)
    return local_dt.strftime(fmt)