#!/usr/bin/env python3
import asyncio
import sys
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.core.config import settings


async def force_drop_all():
    database_url = settings.DATABASE_URL
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    confirm = input("⚠️ DROP ALL TABLES & DATA? Type 'YES': ")
    if confirm != "YES":
        print("Aborted")
        return

    engine = create_async_engine(database_url, echo=True)
    async with engine.begin() as conn:
        # Disable foreign key checks (PostgreSQL)
        await conn.execute(text("SET session_replication_role = replica;"))

        # Get all table names
        result = await conn.execute(text("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
        """))
        tables = [row[0] for row in result]

        for table in tables:
            await conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))
            print(f"Dropped {table}")

        # Re-enable checks
        await conn.execute(text("SET session_replication_role = DEFAULT;"))

        # Optional: Drop and recreate schema
        # await conn.execute(text("DROP SCHEMA public CASCADE;"))
        # await conn.execute(text("CREATE SCHEMA public;"))

    print("✅ All tables dropped.")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(force_drop_all())