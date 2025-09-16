# database.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from settings import settings

DATABASE_URL = settings.DATABASE_URL

# Folosim motorul asincron
engine = create_async_engine(DATABASE_URL)

# Creăm o sesiune asincronă
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()

# Funcția get_db devine din nou asincronă
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session