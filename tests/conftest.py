import os
import sys

from fastapi.testclient import TestClient
from pytest import fixture
from sqlmodel import Session, create_engine
from sqlmodel.pool import StaticPool
from testcontainers.postgres import (
    PostgresContainer,
)

sys.path.append(os.path.join(sys.path[0], "..", "app"))

from models import Base
from config.test_config import (
    TESTS_RUN_CONTAINER_POSTGRES_LOCAL,
    TEST_DATABASE_URL,
    TESTS_DB_PASS,
    TESTS_DB_NAME,
    TESTS_DB_USER,
    TESTS_DB_PORT,
)

if TESTS_RUN_CONTAINER_POSTGRES_LOCAL:

    class DBContainer(PostgresContainer):
        def get_connection_url(self, host=None, driver=object()):
            return super().get_connection_url()

    @fixture(scope="session", autouse=True)
    def postgres_instance():
        config = {
            "image": "postgres:14",  # Use a specific version for stability
            "username": TESTS_DB_USER,
            "password": TESTS_DB_PASS,
            "dbname": TESTS_DB_NAME,
        }
        postgres_container = DBContainer(**config)

        with postgres_container as container:
            yield container

    @fixture(scope="session", autouse=True)
    def engine(postgres_instance):
        db_url = postgres_instance.get_connection_url().replace(
            "localhost", "127.0.0.1"
        )

        os.environ["DATABASE_URL"] = db_url
        os.environ["DB_USER"] = TESTS_DB_USER
        os.environ["DB_PASS"] = TESTS_DB_PASS
        os.environ["DB_NAME"] = TESTS_DB_NAME
        os.environ["DB_PORT"] = str(TESTS_DB_PORT)

        db_engine = create_engine(
            db_url,
            poolclass=StaticPool,
            echo=True,
        )
        yield db_engine
        db_engine.dispose()

    @fixture(scope="function", autouse=True)
    def session(engine):
        Base.metadata.drop_all(engine)

        Base.metadata.create_all(engine)
        with Session(engine) as session:
            yield session

        Base.metadata.drop_all(engine)

else:

    @fixture(scope="function", autouse=True)
    def engine():
        engine = create_engine(
            TEST_DATABASE_URL,
            pool_size=20,
            max_overflow=100,
        )
        yield engine
        engine.dispose()

    @fixture(scope="function", autouse=True)
    def session(engine):
        """
        Create a new session for each test, ensure tables are dropped and recreated.
        """
        Base.metadata.drop_all(engine)

        Base.metadata.create_all(engine)

        with Session(bind=engine) as session:
            yield session


@fixture(scope="function")
def client(session, engine, mocker):
    def get_session_override():
        return session

    mocker.patch("database.engine", new=engine)
    mocker.patch(
        "services.event_service.processor.get_not_auth_session",
        new=lambda: iter([Session(engine)]),
    )
    mocker.patch("config.database_config.DATABASE_URL", new=engine.url)

    from database import get_session, get_not_auth_session
    from main import app_v1, app

    app_v1.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_session] = get_session_override
    app_v1.dependency_overrides[get_not_auth_session] = get_session_override
    app.dependency_overrides[get_not_auth_session] = get_session_override

    client = TestClient(app=app)
    yield client
    app.dependency_overrides.clear()
    app_v1.dependency_overrides.clear()
