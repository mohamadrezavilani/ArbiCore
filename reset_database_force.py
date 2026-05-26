#!/usr/bin/env python3
import sys
from sqlalchemy import create_engine, text
from app.models.base import Base
from app.core.config import settings

def get_sync_url():
    url = settings.DATABASE_URL
    if "+asyncpg" in url:
        url = url.replace("+asyncpg", "+psycopg2")
    return url

def reset_database():
    db_url = get_sync_url()
    if not db_url:
        print("DATABASE_URL missing")
        sys.exit(1)

    confirm = input("Type 'YES' to delete all data: ")
    if confirm != "YES":
        print("Aborted")
        return

    engine = create_engine(db_url, echo=True)

    with engine.begin() as conn:
        # 1. Drop all tables known to SQLAlchemy
        Base.metadata.drop_all(conn)

        # 2. (Optional) Drop any remaining tables via raw SQL
        result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
        for row in result:
            conn.execute(text(f'DROP TABLE IF EXISTS "{row[0]}" CASCADE;'))

        # 3. Recreate tables from models
        Base.metadata.create_all(conn)

    print("Database reset complete")
    engine.dispose()

if __name__ == "__main__":
    reset_database()