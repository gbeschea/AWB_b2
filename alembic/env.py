from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine


from alembic import context
import sys
import os
import asyncio


# Adaugă calea către directorul rădăcină al proiectului
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))

# Importă modelul Base din aplicația ta
from models import Base


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    # Extragem configuratia
    config_section = config.get_section(config.config_ini_section, {})
    url = config_section['sqlalchemy.url']

    # Cream un motor asincron în mod explicit
    connectable = create_async_engine(url, poolclass=pool.NullPool)

    async def run_async_migrations():
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)

    def do_run_migrations(connection):
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

    try:
        asyncio.run(run_async_migrations())
    except TypeError:
        # Fallback pentru medii unde asyncio.run() nu este disponibil
        # (mai putin probabil in versiuni noi de Python)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run_async_migrations())




if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
