import pickle

import pytest
from sqlalchemy import select
from sqlalchemy.event import listen
from sqlmodel import Session

from models import TMO, TPRM, MO, PRM
from services.listener_service.processor import ListenerService
from val_types.constants import two_way_mo_link_val_type_name


@pytest.fixture(scope="session")
def client_url():
    return "/api/inventory/v1/"


@pytest.fixture(scope="session")
def object_type_url(client_url):
    return f"{client_url}object_type/"


@pytest.fixture(scope="session")
def parameter_type_url(client_url):
    return f"{client_url}param_type/"


@pytest.fixture(scope="session")
def object_url(client_url):
    return f"{client_url}object/"


@pytest.fixture(scope="session")
def batch_url(client_url):
    return f"{client_url}batch/"


@pytest.fixture(scope="function", autouse=True)
def fill_data(session):
    fill_tmo_data(session=session)
    fill_tprm_data(session=session)
    fill_mo(session=session)
    fill_prm(session=session)
    session.commit()
    return session


@pytest.fixture(scope="function")
def kafka_mock(mocker):
    mock = mocker.patch(
        "services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka",
        return_value=True,
    )
    return mock


@pytest.fixture(scope="function")
def kafka_partial_mock(mocker):
    mock = mocker.patch(
        "services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions",
        return_value=True,
    )
    return mock


@pytest.fixture(scope="function")
def listened_session(fill_data: Session):
    session = fill_data
    listen(session, "after_flush", ListenerService.receive_after_flush)
    listen(session, "after_commit", ListenerService.receive_after_commit)
    return session


def fill_tmo_data(session: Session):
    tmos = (
        TMO(
            name="TMO 1",
            id=100500,
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TMO(
            name="TMO 2",
            id=100501,
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TMO(
            name="TMO 3",
            id=100502,
            created_by="TestUser",
            modified_by="TestUser",
        ),
    )
    session.add_all(tmos)


def fill_tprm_data(session: Session):
    tprms = (
        TPRM(
            name="TPRM 1",
            val_type="str",
            multiple=False,
            required=True,
            tmo_id=100500,
            field_value="0",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            name="TPRM 2",
            val_type="str",
            multiple=True,
            required=True,
            tmo_id=100500,
            field_value="80049517000000000000005d948c10d09fd0b0d181d185d0b0d0bbd0bad0b094612e",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            name="TPRM 3",
            val_type="str",
            multiple=False,
            required=False,
            tmo_id=100500,
            field_value=None,
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            name="TPRM 4",
            val_type="int",
            multiple=False,
            required=True,
            tmo_id=100500,
            field_value="0",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            name="TPRM 5",
            val_type="mo_link",
            multiple=False,
            required=True,
            tmo_id=100500,
            field_value="1",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            id=34000,
            backward_link=34001,
            name="TPRM 6",
            val_type=two_way_mo_link_val_type_name,
            multiple=False,
            required=False,
            tmo_id=100500,
            constraint="100501",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            id=34001,
            backward_link=34000,
            name="TPRM 6",
            val_type=two_way_mo_link_val_type_name,
            multiple=False,
            required=False,
            tmo_id=100501,
            constraint="100500",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            id=34002,
            backward_link=34003,
            name="TPRM 7",
            val_type=two_way_mo_link_val_type_name,
            multiple=False,
            required=False,
            tmo_id=100500,
            constraint="100501",
            created_by="TestUser",
            modified_by="TestUser",
        ),
        TPRM(
            id=34003,
            backward_link=34002,
            name="TPRM 7",
            val_type=two_way_mo_link_val_type_name,
            multiple=False,
            required=False,
            tmo_id=100501,
            constraint="100500",
            created_by="TestUser",
            modified_by="TestUser",
        ),
    )
    session.add_all(tprms)


def fill_mo(session: Session):
    mos = (
        MO(name="MO 1", tmo_id=100500, id=100500),
        MO(name="MO 2", tmo_id=100500, id=100501),
        MO(name="MO 3", tmo_id=100501, id=100502),
        MO(name="MO 4", tmo_id=100501, id=100503),
        MO(name="MO 5", tmo_id=100502, id=100504),
    )
    session.add_all(mos)


def fill_prm(session: Session):
    prms = []
    for mo_id in range(100500, 100503):
        mo_prms = (
            PRM(tprm_id=1, mo_id=mo_id, value=f"PRM1 {mo_id}"),
            PRM(
                tprm_id=2,
                mo_id=mo_id,
                value=pickle.dumps(["PRM2", "{str(mo_id)}"]).hex(),
            ),
            PRM(tprm_id=3, mo_id=mo_id, value=f"PRM3 {mo_id}"),
            PRM(tprm_id=4, mo_id=mo_id, value=int(f"4{mo_id}")),
            PRM(tprm_id=5, mo_id=mo_id, value=mo_id + 1),
        )
        prms.extend(mo_prms)
    prms.extend(
        (
            PRM(
                tprm_id=34000,
                mo_id=100500,
                value="100502",
                id=34000,
                backward_link=34001,
            ),
            PRM(
                tprm_id=34001,
                mo_id=100502,
                value="100500",
                id=34001,
                backward_link=34000,
            ),
        )
    )
    session.add_all(prms)


@pytest.fixture(scope="function")
def mo_data():
    return {
        "name": "Test MO instance",
        "pov": {1: "test", "text": 1},
        "geometry": {2: "test2", "text2": 2},
        "label": "Test label",
        "active": True,
        "latitude": 0,
        "longitude": 0,
        "version": 1,
        "tmo_id": None,
        "p_id": None,
        "model": "testurl",
    }


@pytest.fixture(scope="function", autouse=False)
def tprms_dict(fill_data):
    stmt = select(TPRM)
    tprms = {i.id: i for i in fill_data.execute(stmt).scalars()}
    return tprms
