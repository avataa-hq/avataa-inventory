import os

TESTS_RUN_CONTAINER_POSTGRES_LOCAL = os.environ.get(
    "TESTS_RUN_CONTAINER_POSTGRES_LOCAL",
    "True",
).upper() in ("TRUE", "Y", "YES", "1")

TESTS_DB_TYPE = os.environ.get("TESTS_DB_TYPE", "postgresql")
TESTS_DB_USER = os.environ.get("TESTS_DB_USER", "local_postgres")
TESTS_DB_PASS = os.environ.get("TESTS_DB_PASS", "local_password")
TESTS_DB_PORT = os.environ.get("TESTS_DB_PORT", "5433")
TESTS_DB_NAME = os.environ.get("TESTS_DB_NAME", "inventory")
TESTS_DB_HOST = os.environ.get("TESTS_DB_HOST", "localhost")

TEST_DATABASE_URL = f"{TESTS_DB_TYPE}://{TESTS_DB_USER}:{TESTS_DB_PASS}@{TESTS_DB_HOST}:{TESTS_DB_PORT}/{TESTS_DB_NAME}"
