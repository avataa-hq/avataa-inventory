import services.security_service.data.permissions.inventory  # noqa
from pytest import fixture
from services.security_service.data.permissions.permission_template import Base
from sqlmodel import Session, create_engine
from sqlmodel.pool import StaticPool

import init_dataset
from services.security_service.security_data_models import UserData, ClientRoles


@fixture(scope="session")
def engine(postgres_instance):
    db_url = postgres_instance.get_connection_url()
    db_engine = create_engine(db_url, poolclass=StaticPool, echo=True)
    yield db_engine

    db_engine.dispose()


@fixture(scope="function", autouse=True)
def session(engine):
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        init_dataset.from_file(session)
        import security.data.listener  # noqa

        yield session

    Base.metadata.drop_all(engine)


@fixture
def jwt_admin() -> UserData:
    return UserData(
        id="test_id",
        audience="test_aud",
        name="Test User",
        preferred_name="test_user",
        realm_access=ClientRoles(
            "realm_access", roles=["__admin", "other", "__reader"]
        ),
        resource_access=None,
        groups=None,
    )


@fixture
def jwt_reader() -> UserData:
    return UserData(
        id="test_id",
        audience="test_aud",
        name="Test User",
        preferred_name="test_user",
        realm_access=ClientRoles("realm_access", roles=["other", "__reader"]),
        resource_access=None,
        groups=None,
    )


@fixture
def jwt_other() -> UserData:
    return UserData(
        id="test_id",
        audience="test_aud",
        name="Test User",
        preferred_name="test_user",
        realm_access=ClientRoles("realm_access", roles=["other"]),
        resource_access=None,
        groups=None,
    )
