import os

DB_TYPE = os.environ.get("DB_TYPE", "postgresql")
ASYNC_DB_TYPE = os.environ.get("ASYNC_DB_TYPE", "postgresql+asyncpg")
DB_USER = os.environ.get("DB_USER", "inventory_admin")
DB_PASS = os.environ.get("DB_PASS")
DB_HOST = os.environ.get("DB_HOST", "pgbouncer")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "inventory")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")

DATABASE_URL = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ASYNC_DATABASE_URL = (
    f"{ASYNC_DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
