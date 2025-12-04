from sqlalchemy import create_engine
from typing import AsyncGenerator
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine

Base = declarative_base()

db_url = "postgresql+asyncpg://postgres:Edgematics2025@axoma-dev-postgres.cd4keaaye6mk.eu-west-1.rds.amazonaws.com:5432/axoma-etl-migration-tool"
# db_url = "postgresql+asyncpg://postgres:admin@localhost:5432/etl"

async_engine = create_async_engine(db_url, echo=False)

AsyncSessionFactory = async_sessionmaker(autoflush=False, autocommit=False, bind=async_engine)

async def get_db():
    async with AsyncSessionFactory() as session:
        try:
            yield session
        finally:
            await session.aclose()