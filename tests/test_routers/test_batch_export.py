"""Tests for batch router"""

import re

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from models import TMO, TPRM, MO, PRM

URL = "/api/inventory/v1/batch/export_obj_with_params/"


TMO_DEFAULT_DATA = {
    "name": "Test TMO",
    "version": 1,
    "created_by": "Test admin",
    "modified_by": "Test admin",
}

TPRM_STR_DEFAULT_DATA = {
    "name": "Test str TPRM",
    "val_type": "str",
    "required": False,
    "created_by": "Test admin",
    "modified_by": "Test admin",
}


@pytest.fixture(scope="function", autouse=True)
def session_fixture(mocker, session, engine):
    mocker.patch(
        "services.event_service.processor.get_not_auth_session",
        new=lambda: iter([Session(engine)]),
    )
    mocker.patch(
        "services.kafka_service.producer.protobuf_producer.kafka_config.KAFKA_TURN_ON",
        new=False,
    )

    tmo = TMO(**TMO_DEFAULT_DATA)
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    session.add(tprm)
    session.flush()
    session.commit()

    mo_1 = MO(name="MO 1", tmo_id=tmo.id, active=True)
    mo_2 = MO(name="MO 2", tmo_id=tmo.id, active=True)
    mo_3 = MO(name="MO 3", tmo_id=tmo.id, active=True)
    session.add(mo_1)
    session.add(mo_2)
    session.add(mo_3)
    session.flush()
    prm_1 = PRM(mo_id=mo_1.id, tprm_id=tprm.id, value="mo 1 value")
    prm_3 = PRM(mo_id=mo_3.id, tprm_id=tprm.id, value="mo 3 value")

    session.add(prm_1)
    session.add(prm_3)
    session.flush()
    session.commit()
    yield session


def test_get_request_is_successful(session: Session, client: TestClient):
    """TEST Successful GET request to the endpoint - 'object_and_param_values' return 200 status code."""

    stmt = select(TMO)
    tmo = session.exec(stmt).first()
    tmo_url = URL + str(tmo.id)

    res = client.get(url=tmo_url, params={"file_type": "csv"})
    assert res.status_code == 200


def test_get_request_with_csv_is_successful(
    session: Session, client: TestClient
):
    """TEST Successful GET request to the endpoint - 'object_and_param_values' return 200 status code."""

    stmt = select(TMO)
    tmo = session.exec(stmt).first()
    tmo_url = URL + str(tmo.id)
    res = client.get(url=tmo_url, params={"file_type": "csv"})

    filename = re.compile(r'(?<=filename=")[^"]*')
    filename = filename.findall(res.headers["content-disposition"])[0]
    file_ext = filename.split(".")[-1]
    assert file_ext == "csv"


def test_get_request_with_xlsx_is_successful(
    session: Session, client: TestClient
):
    """TEST Successful GET request to the endpoint - 'object_and_param_values' return 200 status code."""

    stmt = select(TMO)
    tmo = session.exec(stmt).first()
    tmo_url = URL + str(tmo.id)

    res = client.get(url=tmo_url, params={"file_type": "xlsx"})

    filename = re.compile(r'(?<=filename=")[^"]*')
    filename = filename.findall(res.headers["content-disposition"])[0]
    file_ext = filename.split(".")[-1]
    assert file_ext == "xlsx"
