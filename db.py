from sqlalchemy import create_engine
from typing import AsyncGenerator
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine

Base = declarative_base()

db_url = "postgresql+asyncpg://postgres:Edgematics2025@axoma-dev-postgres.cd4keaaye6mk.eu-west-1.rds.amazonaws.com:5432/axoma-etl-migration-tool"
# db_url = "postgresql+asyncpg://postgres:admin@localhost:5432/etl"

# Number of persistent connections in the pool.
POSTGRES_POOL_SIZE=10
# Maximum number of temporary connections beyond pool_size.
POSTGRES_MAX_OVERFLOW=15
# Timeout for borrowing a connection from the pool (in seconds).
POSTGRES_TIMEOUT=30
# Maximum lifetime of a connection in the pool (in seconds).
POSTGRES_POOL_RECYCLE=1800
# Interval for pinging the database to check connection validity (in seconds).
POSTGRES_POOL_PRE_PING=10
# Whether to print all sql queries
DB_DEBUG_MODE=False

async_engine = create_async_engine(db_url, echo=DB_DEBUG_MODE, pool_size=POSTGRES_POOL_SIZE, max_overflow=POSTGRES_MAX_OVERFLOW, pool_timeout=POSTGRES_TIMEOUT, pool_recycle=POSTGRES_POOL_RECYCLE, pool_pre_ping=POSTGRES_POOL_PRE_PING)

AsyncSessionFactory = async_sessionmaker(autoflush=False, autocommit=False, bind=async_engine)

async def get_db():
    async with AsyncSessionFactory() as session:
        try:
            yield session
        finally:
            await session.aclose()