#!/usr/bin/env python3
"""
Force reset the entire database: drop all tables and recreate empty ones.
Works with postgresql+asyncpg URLs by switching to a sync driver.
No superuser required.
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

# Adjust imports to your project structure
try:
    from app.models.base import Base
    from app.core.config import settings
except ImportError:
    print("ERROR: Could not import from app. Make sure you're in the project root and venv is active.")
    sys.exit(1)

def get_sync_database_url():
    """Convert asyncpg URL to psycopg2 (sync) if needed."""
    url = settings.DATABASE_URL
    if "+asyncpg" in url:
        url = url.replace("+asyncpg", "+psycopg2")
        print(f"Using sync driver: {url}")
    return url

def reset_database():
    db_url = get_sync_database_url()
    if not db_url:
        print("ERROR: DATABASE_URL not set in settings or .env")
        sys.exit(1)

    confirm = input("⚠️  This will DELETE ALL TABLES and DATA in the database. Type 'YES' to continue: ")
    if confirm != "YES":
        print("Aborted.")
        return

    engine = create_engine(db_url, echo=True)

    with engine.connect() as conn:
        # Begin a transaction (autocommit is off by default)
        with conn.begin():
            # 1. Drop all tables with CASCADE
            print("Dropping all tables...")
            # Option A: SQLAlchemy's drop_all (handles foreign keys)
            Base.metadata.drop_all(bind=engine)
            print("All tables dropped.")

            # Option B (extra safety): also drop any leftover tables using raw SQL
            # (uncomment if drop_all misses something)
            # result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
            # for row in result:
            #     conn.execute(text(f'DROP TABLE IF EXISTS "{row[0]}" CASCADE;'))

            # 2. Recreate empty tables
            print("Creating empty tables...")
            Base.metadata.create_all(bind=engine)
            print("Tables recreated successfully.")

    print("✅ Database reset complete (all tables are empty).")
    engine.dispose()

if __name__ == "__main__":
    reset_database()