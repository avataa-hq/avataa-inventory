import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import async_engine_from_config, AsyncEngine

from config.database_config import ASYNC_DATABASE_URL, DB_SCHEMA
from models import *

config = context.config
fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", ASYNC_DATABASE_URL)
target_metadata = Base.metadata


async def run_migrations_online():
    connectable: AsyncEngine = async_engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    async with connectable.connect() as connection:
        await connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
        await connection.execute(text(f"SET search_path TO {DB_SCHEMA};"))
        await connection.commit()

        connection.dialect.default_schema_name = DB_SCHEMA
        await connection.run_sync(
            lambda sync_conn: context.configure(
                connection=sync_conn,
                target_metadata=target_metadata,
                include_schemas=True,
            )
        )

        async with connection.begin():
            await connection.run_sync(lambda _: context.run_migrations())


def run_migrations_offline():
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema=DB_SCHEMA,
    )

    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
