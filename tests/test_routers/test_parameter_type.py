"""Tests for object type router"""

import datetime
import pickle
from pprint import pprint

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, func, and_
from sqlmodel import Session

from models import TMO, TPRM, MO, Event, PRM

URL = "/api/inventory/v1/param_type/"

TMO_DEFAULT_DATA = {
    "name": "tmo_1",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

TMO_DEFAULT_DATA_2 = {
    "name": "tmo_2",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

TPRM_DEFAULT_DATA = {
    "name": "tprm_1",
    "tmo_id": 1,
    "val_type": "str",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

TPRM_DEFAULT_DATA_2 = {
    "name": "tprm_2",
    "tmo_id": 1,
    "val_type": "str",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}


def get_batch_create_update_url(tmo_id: int):
    return f"/api/inventory/v1/param_types/{tmo_id}/batch_create_or_update_param_types"


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
    tprm_1 = TPRM(**TPRM_DEFAULT_DATA)
    session.add(tprm_1)
    tprm_2 = TPRM(**TPRM_DEFAULT_DATA_2)
    session.add(tprm_2)
    session.commit()
    yield session


def test_read_all_created_param_types(session: Session, client: TestClient):
    tprm_read_url = "/api/inventory/v1/param_types/"

    res = client.get(tprm_read_url)

    all_tprm = session.execute(select(TPRM)).scalars().all()

    assert res.status_code == 200
    assert len(res.json()) == len(all_tprm)


def test_create_param_type(session: Session, client: TestClient):
    tprm = {"name": "new_tprm", "tmo_id": 1, "val_type": "str"}

    res = client.post(URL, json=tprm)

    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    assert res.status_code == 200
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_multiple_attribute(
    session: Session, client: TestClient
):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "multiple": True,
    }

    res = client.post(URL, json=tprm)

    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm

    assert res.status_code == 200
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_val_type_attribute(
    session: Session, client: TestClient
):
    tprm = {"name": "new_tprm", "tmo_id": 1, "val_type": "some_wrong_val_type"}

    res = client.post(URL, json=tprm)
    assert res.status_code == 422


def test_create_param_type_error_tmo_id_attribute(
    session: Session, client: TestClient
):
    tprm = {"name": "new_tprm", "tmo_id": 1234, "val_type": "str"}

    res = client.post(URL, json=tprm)
    assert res.status_code == 404


def test_create_param_type_error_formula_and_multiple(
    session: Session, client: TestClient
):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "formula",
        "multiple": True,
        "constraint": "test",
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 409
    assert res.json() == {
        "detail": "Not allowed to create multiple parameter for formula val_type."
    }


def test_create_param_type_error_formula_prm_link_and_required(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "prm_link",
        "required": True,
        "constraint": 1,
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 422
    assert res.json() == {
        "detail": [
            {
                "input": 1,
                "loc": ["body", "constraint"],
                "msg": "Input should be a valid string",
                "type": "string_type",
            }
        ]
    }


def test_create_param_type_error_formula_mo_link_and_required(
    session: Session, client: TestClient
):
    """Update AD-1458 allow to create required mo_link"""
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "mo_link",
        "required": True,
        "field_value": "1",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Parameter 1 does not valid. Requested object with id 1 does not "
        "exists"
    }


def test_create_param_type_error_formula_and_required(
    session: Session, client: TestClient
):
    tprm_formula = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "formula",
        "required": True,
    }

    res = client.post(URL, json=tprm_formula)
    assert res.status_code == 422


def test_param_type_validation_required_attribute(
    session: Session, client: TestClient
):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "required": True,
        "val_type": "str",
        "field_value": "some_value",
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_str_constraint(session: Session, client: TestClient):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "aaaa",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm_mo_link["name"])
    ).scalar()
    assert tprm

    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_str_constraint(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "((0)",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_float_constraint(
    session: Session, client: TestClient
):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": "1.2:3.4",
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm

    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_enum(session: Session, client: TestClient):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": '["some_value", "some_value1"]',
        "required": True,
        "field_value": "some_value",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == requested_tprm["name"])
    ).scalar()
    assert tprm
    assert tprm.constraint == requested_tprm["constraint"]
    assert tprm.val_type == requested_tprm["val_type"]


def test_create_param_type_enum_duplicated_name(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "tprm_1",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": '["some_value", "some_value1"]',
        "required": True,
        "field_value": "some_value",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "TPRM names already exist in TMO tmo_1: {'tprm_1'}"
    }


def test_create_param_type_enum_not_valid_field_value(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": '["some_value", "some_value1"]',
        "required": True,
        "field_value": "not_valid_field_value",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Field value for TPRM new_enum_tprm doesn't match its constraint."
    }
    tprm = session.execute(
        select(TPRM).where(TPRM.name == requested_tprm["name"])
    ).scalar()
    assert not tprm


def test_create_param_type_multiple_enum(session: Session, client: TestClient):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "required": True,
        "multiple": True,
        "constraint": "[1, 2]",
        "field_value": "[1]",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == requested_tprm["name"])
    ).scalar()

    assert tprm.constraint == requested_tprm["constraint"]
    assert tprm.val_type == requested_tprm["val_type"]


def test_create_param_type_not_valid_constraint_enum(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": "[some_value, some_value1]",
        "required": False,
    }

    res = client.post(URL, json=requested_tprm)
    assert res.status_code == 422

    assert res.json() == {
        "detail": "Enum constraint must be a list of values. Constraint for TPRM "
        "new_enum_tprm is invalid."
    }


def test_create_param_type_not_valid_constraint_enum_1(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": "sdfsdf",
    }

    res = client.post(URL, json=requested_tprm)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Enum constraint must be a list of values. Constraint for TPRM "
        "new_enum_tprm is invalid."
    }


def test_create_param_type_without_constraint_enum(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
    }

    res = client.post(URL, json=requested_tprm)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Please, pass the constraint parameter. It is required for enum "
        "val_type."
    }


def test_create_param_type_without_field_value_enum(
    session: Session, client: TestClient
):
    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": "[1, 2, 3]",
        "required": True,
    }

    res = client.post(URL, json=requested_tprm)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "TPRM with name new_enum_tprm is required but lacks a field value."
    }


def test_create_param_type_enum_with_creating_default_values(
    session: Session, client: TestClient
):
    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": []}

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    mo_2 = MO(**MO_DEFAULT_DATA_1)
    session.add_all([mo_1, mo_2])
    session.commit()

    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": "['1', '2', '3']",
        "required": True,
        "field_value": "1",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == requested_tprm["name"])
    ).scalar()

    assert tprm
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.mo_id == 1, PRM.value == "1"
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.mo_id == 2, PRM.value == "1"
        )
    ).scalar()


def test_create_param_type_enum_with_creating_default_values_for_multiple(
    session: Session, client: TestClient
):
    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": []}

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    mo_2 = MO(**MO_DEFAULT_DATA_1)
    session.add_all([mo_1, mo_2])
    session.commit()

    requested_tprm = {
        "name": "new_enum_tprm",
        "tmo_id": 1,
        "val_type": "enum",
        "constraint": "['1', '2', '3']",
        "required": True,
        "multiple": True,
        "field_value": "['1', '2']",
    }

    res = client.post(URL, json=requested_tprm)
    print(res.json())
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == requested_tprm["name"])
    ).scalar()
    assert tprm

    prm_1: PRM = session.execute(
        select(PRM).where(PRM.tprm_id == tprm.id, PRM.mo_id == 1)
    ).scalar()
    prm_2: PRM = session.execute(
        select(PRM).where(PRM.tprm_id == tprm.id, PRM.mo_id == 2)
    ).scalar()
    assert pickle.loads(bytes.fromhex(prm_1.value)) == ["1", "2"]
    assert pickle.loads(bytes.fromhex(prm_2.value)) == ["1", "2"]


def test_update_param_type_enum(session: Session, client: TestClient):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[2,3,4]",
        "force": True,
        "required": True,
        "field_value": 2,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    assert res.status_code == 200

    updated_tprm = session.execute(
        select(TPRM).where(TPRM.name == "updated_exists_enum_tprm")
    ).scalar()
    assert updated_tprm

    old_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    assert not old_tprm


def test_update_param_type_enum_2(session: Session, client: TestClient):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[2,3,4]",
        "force": True,
        "required": True,
        "field_value": 2,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 200

    result = res.json()
    del result["modification_date"]
    del result["creation_date"]
    assert result == {
        "name": "updated_exists_enum_tprm",
        "description": None,
        "val_type": "enum",
        "multiple": False,
        "required": True,
        "returnable": False,
        "constraint": "[2,3,4]",
        "prm_link_filter": None,
        "group": None,
        "tmo_id": 1,
        "field_value": "2",
        "id": 3,
        "version": 2,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    updated_tprm = session.execute(
        select(TPRM).where(TPRM.name == "updated_exists_enum_tprm")
    ).scalar()
    assert updated_tprm

    old_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    assert not old_tprm


def test_update_param_type_not_valid_constraint(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[qwe, rew]",
        "force": True,
        "required": True,
        "field_value": "qwe",
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Enum constraint must be a list of values. Invalid constraint for TPRM updated_exists_enum_tprm"
    }


def test_update_param_type_without_field_value(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[1, 2]",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "TPRM updated_exists_enum_tprm is required but missing a field value"
    }


def test_update_param_type_without_force(session: Session, client: TestClient):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[1, 2]",
        "required": True,
        "field_value": 1,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "To update constraint you have to activate force attribute. Data "
        "will be changed, if parameters will not match updated constraint"
    }


def test_update_param_type_without_force_2(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[1, 2]",
        "required": True,
        "field_value": 1,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "To update constraint you have to activate force attribute. Data "
        "will be changed, if parameters will not match updated constraint"
    }


def test_update_param_type_with_duplicated_name(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "tprm_1",
        "constraint": "[1, 2]",
        "required": True,
        "force": True,
        "field_value": 1,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There is TPRM name in request, which already exist tprm_1"
    }


def test_update_param_type_for_not_exists_tprm(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[1, 2]",
        "required": True,
        "field_value": 1,
    }

    res = client.patch(URL + str(123), json=requested_tprm)
    print(res.json())
    assert res.status_code == 404
    assert res.json() == {"detail": "Parameter type with id 123 not found."}


def test_update_param_type_enum_set_required_values(
    session: Session, client: TestClient
):
    TPRM_DEFAULT_DATA_1 = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": []}

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    mo_2 = MO(**MO_DEFAULT_DATA_1)
    new_tprm = TPRM(**TPRM_DEFAULT_DATA_1)

    session.add_all([new_tprm, mo_1, mo_2])
    session.commit()

    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[2,3,4]",
        "force": True,
        "required": True,
        "field_value": 2,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 200
    result = res.json()
    del result["modification_date"]
    del result["creation_date"]
    assert result == {
        "name": "updated_exists_enum_tprm",
        "description": None,
        "val_type": "enum",
        "multiple": False,
        "required": True,
        "returnable": False,
        "constraint": "[2,3,4]",
        "prm_link_filter": None,
        "group": None,
        "tmo_id": 1,
        "field_value": "2",
        "id": 3,
        "version": 2,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    updated_tprm = session.execute(
        select(TPRM).where(TPRM.name == "updated_exists_enum_tprm")
    ).scalar()
    assert updated_tprm

    prm_1: PRM = session.execute(
        select(PRM).where(PRM.tprm_id == updated_tprm.id, PRM.mo_id == 1)
    ).scalar()
    prm_2: PRM = session.execute(
        select(PRM).where(PRM.tprm_id == updated_tprm.id, PRM.mo_id == 2)
    ).scalar()
    assert prm_1.value == "2"
    assert prm_2.value == "2"


def test_update_param_type_enum_updated_values_for_updated_constraint(
    session: Session, client: TestClient
):
    TPRM_DEFAULT_DATA_1 = {
        "name": "exists_enum_tprm",  # id = 3
        "tmo_id": 1,
        "constraint": "[1,2,3,5]",
        "val_type": "enum",
        "required": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": []}

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    mo_2 = MO(**MO_DEFAULT_DATA_1)
    mo_3 = MO(**MO_DEFAULT_DATA_1)
    mo_4 = MO(**MO_DEFAULT_DATA_1)
    new_tprm = TPRM(**TPRM_DEFAULT_DATA_1)

    prm_1 = PRM(value="1", mo_id=1, tprm_id=3, version=1)
    prm_2 = PRM(value="2", mo_id=2, tprm_id=3, version=1)
    prm_3 = PRM(value="5", mo_id=3, tprm_id=3, version=1)

    session.add_all([new_tprm, mo_1, mo_2, mo_3, mo_4, prm_1, prm_2, prm_3])

    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    requested_tprm = {
        "version": 1,
        "name": "updated_exists_enum_tprm",
        "constraint": "[5,6]",
        "force": True,
        "field_value": 5,
    }

    res = client.patch(URL + str(exists_tprm.id), json=requested_tprm)
    print(res.json())
    assert res.status_code == 200
    result = res.json()
    del result["modification_date"]
    del result["creation_date"]
    assert result == {
        "name": "updated_exists_enum_tprm",
        "description": None,
        "val_type": "enum",
        "multiple": False,
        "required": True,
        "returnable": False,
        "constraint": "[5,6]",
        "prm_link_filter": None,
        "group": None,
        "tmo_id": 1,
        "field_value": "5",
        "id": 3,
        "version": 2,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    updated_tprm = session.execute(
        select(TPRM).where(TPRM.name == "updated_exists_enum_tprm")
    ).scalar()
    assert updated_tprm

    prm_with_old_value: PRM = session.execute(
        select(PRM).where(
            PRM.tprm_id == updated_tprm.id, PRM.mo_id == 1, PRM.value == "1"
        )
    ).scalar()
    prm_with_old_value_1: PRM = session.execute(
        select(PRM).where(
            PRM.tprm_id == updated_tprm.id, PRM.mo_id == 2, PRM.value == "2"
        )
    ).scalar()
    prm_which_has_to_be_stay: PRM = session.execute(
        select(PRM).where(
            PRM.tprm_id == updated_tprm.id, PRM.mo_id == 3, PRM.value == "5"
        )
    ).scalar()

    prm_which_dont_have_to_be_created: PRM = session.execute(
        select(PRM).where(
            PRM.tprm_id == updated_tprm.id, PRM.mo_id == 4, PRM.value == "5"
        )
    ).scalar()

    assert not prm_with_old_value
    assert not prm_with_old_value_1
    assert prm_which_has_to_be_stay
    assert not prm_which_dont_have_to_be_created


def test_delete_param_type(session: Session, client: TestClient):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    exists_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    res = client.delete(URL + str(exists_tprm.id))

    assert res.status_code == 200

    deleted_tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    assert not deleted_tprm


def test_delete_param_type_not_exists(session: Session, client: TestClient):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    res = client.delete(URL + str(123))

    assert res.status_code == 404
    print(res.json())

    assert res.json() == {"detail": "Parameter type with id 123 not found."}


def test_update_val_type_from_enum_without_parameters(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    session.commit()

    data = {"version": 1, "val_type": "float", "force": True}

    res = client.patch(
        "/api/inventory/v1/param_type/3/change_val_type/", json=data
    )
    print(res.json())

    assert res.status_code == 200
    result = res.json()
    del result["modification_date"]
    del result["creation_date"]
    assert result == {
        "name": "exists_enum_tprm",
        "description": None,
        "val_type": "float",
        "multiple": False,
        "required": False,
        "returnable": False,
        "constraint": None,
        "prm_link_filter": None,
        "group": None,
        "tmo_id": 1,
        "field_value": None,
        "id": 3,
        "version": 2,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }


def test_update_val_type_from_enum_with_parameters(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": []}

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    new_tprm = TPRM(**new_tprm)
    prm_1 = PRM(value="1", mo_id=1, tprm_id=3, version=1)
    session.add_all([new_tprm, mo_1, prm_1])
    session.commit()

    data = {"version": 1, "val_type": "float", "force": True}

    res = client.patch(
        "/api/inventory/v1/param_type/3/change_val_type/", json=data
    )
    print(res.json())

    assert res.status_code == 422
    assert res.json() == {
        "detail": "Not allowed to change val_type from enum_val_type"
    }


def test_update_val_type_to_enum(session: Session, client: TestClient):
    data = {"version": 1, "val_type": "enum", "force": True}

    res = client.patch(
        "/api/inventory/v1/param_type/1/change_val_type/", json=data
    )
    print(res.json())

    assert res.status_code == 422
    assert res.json() == {"detail": "Not allowed to change val_type to enum."}


def test_batch_create_or_update_param_types_enum(
    session: Session, client: TestClient
):
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    new_tprm = TPRM(**new_tprm)
    session.add(new_tprm)
    session.commit()

    data_to_request = [
        {
            "name": "exists_enum_tprm",
            "constraint": "[4,5]",
            "required": True,
            "val_type": "enum",
            "field_value": 4,
        },
        {
            "name": "not_exists_enum_tprm",
            "constraint": "[4,5]",
            "val_type": "enum",
        },
    ]
    url = get_batch_create_update_url(tmo_id=1) + "?check=False"

    res = client.post(url, json=data_to_request)
    assert res.status_code == 200
    result = res.json()
    del result[0]["modification_date"]
    del result[0]["creation_date"]
    del result[1]["modification_date"]
    del result[1]["creation_date"]
    assert result == [
        {
            "name": "exists_enum_tprm",
            "description": None,
            "val_type": "enum",
            "multiple": False,
            "required": True,
            "returnable": False,
            "constraint": "[4,5]",
            "prm_link_filter": None,
            "group": None,
            "tmo_id": 1,
            "field_value": "4",
            "id": 3,
            "version": 2,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        },
        {
            "name": "not_exists_enum_tprm",
            "description": None,
            "val_type": "enum",
            "multiple": False,
            "required": False,
            "returnable": False,
            "constraint": "[4,5]",
            "prm_link_filter": None,
            "group": None,
            "tmo_id": 1,
            "field_value": None,
            "id": 4,
            "version": 1,
            "created_by": "",
            "modified_by": "",
        },
    ]


def test_create_param_type_error_float_constraint(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": "33.2:3.4",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_float_constraint_1(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": ":3.4",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_float_constraint_2(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": "asdasd",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_int_constraint(session: Session, client: TestClient):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "1:12",
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm

    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_int_constraint_1(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "1.14:12",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_int_constraint_2(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "13:12",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_int_constraint_3(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "13:13",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_prm_link_constraint(
    session: Session, client: TestClient
):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": str(tprm1.id),
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_prm_link_constraint_not_exists(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": "123",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_prm_link_constraint_wrong_type(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": "not_int",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_prm_link_constraint_wrong_type_1(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": "not_int",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_param_type_constraint_prm_link_with_prm_link_filter(
    session: Session, client: TestClient
):
    """
    TPRM id's in prm_link_filter can be referred to the same TPRM
    """
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    tmo_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_2)
    session.commit()

    tmo_2 = session.execute(select(TMO).where(TMO.name == "tmo_2")).scalar()

    main_tprm_data = {
        "name": "main_tprm",
        "val_type": "str",
        "constraint": "some_str_value",
        "tmo_id": tmo_1.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    main_tprm = TPRM(**main_tprm_data)
    session.add(main_tprm)
    session.commit()

    main_tprm = session.execute(
        select(TPRM).where(TPRM.name == "main_tprm")
    ).scalar()

    internal_tprm_data = {
        "name": "internal_tprm",
        "val_type": "prm_link",
        "constraint": str(main_tprm.id),
        "tmo_id": tmo_2.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    internal_tprm = TPRM(**internal_tprm_data)
    session.add(internal_tprm)

    external_tprm_data = {
        "name": "external_tprm",
        "val_type": "prm_link",
        "constraint": str(main_tprm.id),
        "tmo_id": tmo_1.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    external_tprm = TPRM(**external_tprm_data)
    session.add(external_tprm)
    session.commit()

    external_tprm = session.execute(
        select(TPRM).where(TPRM.name == "external_tprm")
    ).scalar()
    internal_tprm = session.execute(
        select(TPRM).where(TPRM.name == "internal_tprm")
    ).scalar()

    some_new_tprm_data = {
        "name": "some_new_tprm",
        "val_type": "prm_link",
        "constraint": str(main_tprm.id),
        "tmo_id": tmo_2.id,
        "prm_link_filter": f"{internal_tprm.id}:{external_tprm.id}",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    res = client.post(URL, json=some_new_tprm_data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == some_new_tprm_data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    assert (
        res.json()["prm_link_filter"]
        == f"{internal_tprm.id}:{external_tprm.id}"
    )
    assert res.json()["constraint"] == str(main_tprm.id)


def test_create_param_type_mo_link_constraint(
    session: Session, client: TestClient
):
    mo = MO(**{"name": "new_mo", "tmo_id": 1})
    session.add(mo)
    session.commit()
    mo = session.execute(select(MO).where(MO.name == "new_mo")).scalar()

    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "mo_link",
        "constraint": str(mo.id),
    }

    res = client.post(URL, json=tprm)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_mo_link_constraint_not_exists(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "mo_link",
        "constraint": "56",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_mo_link_constraint_wrong_type(
    session: Session, client: TestClient
):
    tprm_mo_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "mo_link",
        "constraint": "dsdf",
    }

    res = client.post(URL, json=tprm_mo_link)
    assert res.status_code == 422


def test_create_param_type_error_prm_link_and_not_constraint(
    session: Session, client: TestClient
):
    tprm_prm_link = {"name": "new_tprm", "tmo_id": 1, "val_type": "prm_link"}

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 422


def test_create_param_type_error_tprm_already_exists(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "a",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm_prm_link))
    session.commit()

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 409


def test_create_param_type_tprm_event_created(
    session: Session, client: TestClient
):
    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "a",
    }

    res = client.post(URL, json=tprm)

    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    assert res.status_code == 200


def test_create_param_type_required(session: Session, client: TestClient):
    mo = MO(**{"name": "mo", "tmo_id": 1})
    session.add(mo)
    session.commit()

    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "field_value": "some_str_value",
    }

    res = client.post(URL, json=tprm)
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.value == "some_str_value")
    ).scalar()

    assert res.status_code == 200
    assert prm
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_required_field_value_not_exists(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 400


def test_create_param_type_error_required_field_value_none(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "field_value": None,
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 400


def test_create_param_type_error_required_multiple_not_list(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "multiple": True,
        "field_value": "se",
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 422


def test_create_param_type_error_required_multiple_list_empty(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "multiple": True,
        "field_value": [],
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 422


def test_create_param_type_required_str_multiple(
    session: Session, client: TestClient
):
    mo = MO(**{"name": "new_mo", "tmo_id": 1})
    session.add(mo)
    session.commit()

    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "multiple": True,
        "field_value": ["sdf"],
    }

    res = client.post(URL, json=tprm)

    mo = session.execute(select(MO).where(MO.name == "new_mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    assert prm


def test_create_param_type_required_multiple_str_constraint(
    session: Session, client: TestClient
):
    mo = MO(**{"name": "new_mo", "tmo_id": 1})
    session.add(mo)
    session.commit()

    tprm = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "multiple": True,
        "field_value": ["sdf"],
        "constraint": "sdf",
    }

    res = client.post(URL, json=tprm)

    mo = session.execute(select(MO).where(MO.name == "new_mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_param_type_error_required_multiple_str_constraint_not_all_relevant(
    session: Session, client: TestClient
):
    tprm_prm_link = {
        "name": "new_tprm",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "multiple": True,
        "field_value": ["sdf", "a"],
        "constraint": "sdf",
    }

    res = client.post(URL, json=tprm_prm_link)
    assert res.status_code == 422


def test_create_param_type_required_and_multiple_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    tprm = {
        "name": "new_tprm",
        "val_type": "date",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2023-03-12"],
    }
    res = client.post(URL, json=tprm)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == tprm["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event

    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "date",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2023asd"],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_str_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "str",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_value",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event

    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.value == "some_value")
    ).scalar()
    assert prm


def test_create_param_type_required_and_str_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": 1})
    session.add(mo)
    session.commit()
    data = {
        "name": "new_tprm",
        "val_type": "str",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_value",
        "constraint": "some_value",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.value == "some_value")
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_str_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "str",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_value",
        "constraint": "another_regex",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "date",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "2023-03-12",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.value == "2023-03-12")
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "date",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_wrong_data",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "datetime",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "2023-06-15T14:30:45.123456Z",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.value == data["field_value"])
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "datetime",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_wrong_data",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_datetime_field_value_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "datetime",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "some_wrong_data",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 1.3,
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(
            PRM.mo_id == mo.id, PRM.value == str(data["field_value"])
        )
    ).scalar()
    assert prm


def test_create_param_error_type_required_and_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "asd",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_float_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 1.3,
        "constraint": "1:15",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(
            PRM.mo_id == mo.id, PRM.value == str(data["field_value"])
        )
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_float_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 1.3,
        "constraint": "5:15",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_float_field_value_constraint_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 17,
        "constraint": "5:15",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_float_field_value_constraint_2(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 17,
        "constraint": "15:5",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 1,
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200


def test_create_param_error_type_required_and_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": "asd",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_int_field_value_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 2,
        "constraint": "1:3",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(
            PRM.mo_id == mo.id, PRM.value == str(data["field_value"])
        )
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 2,
        "constraint": "4:6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_int_field_value_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 29,
        "constraint": "4:6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_int_field_value_2(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 4.5,
        "constraint": "1:15",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200


def test_create_param_type_error_required_and_int_field_value_3(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 29,
        "constraint": "7:6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_multiple_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "datetime",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2023-06-15T14:30:45.123456Z"],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "datetime",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2023asd"],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_multiple_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [1.2, 2],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2", "1.2"],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200


def test_create_param_type_required_and_multiple_constraint_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2.7, 6.2],
        "constraint": "2.2:12.6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(
        select(PRM).where(
            PRM.mo_id == mo.id,
        )
    ).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_constraint_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [1.0, 6.2],
        "constraint": "2.2:12.6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2.7, 17.2],
        "constraint": "2.2:12.6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422

    data = {
        "name": "tprm_data",
        "val_type": "float",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2.7, 17.2],
        "constraint": "15:12.6",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_multiple_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2", "1.2"],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_multiple_constraint_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2, 6],
        "constraint": "1:12",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_constraint_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [1, 6],
        "constraint": "2:12",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2, 17],
        "constraint": "1:12",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422

    data = {
        "name": "tprm_data",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [2, 13],
        "constraint": "15:12",
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_required_and_multiple_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "new_tprm",
        "val_type": "int",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [1, 3],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2", 1.2],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        url=f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200


def test_create_object_type_param_type_required_and_multiple_bool_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = {
        "name": "tprm_data",
        "val_type": "bool",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [True, False],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    prm = session.execute(select(PRM).where(PRM.mo_id == mo.id)).scalar()
    assert prm


def test_create_param_type_error_required_and_multiple_bool_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "bool",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": ["2", 1.2],
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_multiple_mo_link(
    session: Session, client: TestClient
):
    """Update AD-1458 allow to create required mo_link"""
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "mo_link",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": [1],
    }
    res = client.post(URL, json=data)
    assert res.json() == {
        "detail": "Parameter does not valid.\n"
        "Requested object to be linked are not exists"
    }

    assert res.status_code == 422


def test_create_param_type_error_required_and_multiple_prm_link(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "prm_link",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": 1,
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_and_multiple_formula(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "formula",
        "tmo_id": tmo_1.id,
        "required": True,
        "multiple": True,
        "field_value": 1,
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_required_formula(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "formula",
        "tmo_id": tmo_1.id,
        "required": True,
        "field_value": 1,
    }
    res = client.post(URL, json=data)
    assert res.status_code == 422


def test_create_param_type_error_tprm_already_exists_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = {
        "name": "tprm_data",
        "val_type": "str",
        "tmo_id": tmo_1.id,
        "created_by": "test",
        "modified_by": "test",
    }
    tprm = TPRM(**data)
    session.add(tprm)
    session.commit()

    res = client.post(URL, json=data)
    assert res.status_code == 409


def test_read_param_type_error_by_not_exists_id(
    session: Session, client: TestClient
):
    res = client.get(URL + f"{123}/")

    assert res.status_code == 404


def test_update_param_type(session: Session, client: TestClient):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    update_data = {"version": 1, "name": "updated_name"}
    res = client.patch(URL + f"{tprm1.id}/", json=update_data)

    updated_tprm = session.execute(
        select(TPRM).where(TPRM.name == "updated_name")
    ).scalar()

    assert updated_tprm
    assert res.status_code == 200


def test_update_param_type_1(session: Session, client: TestClient):
    """
    Test if TPRM was changed
    """
    data = {
        "name": "string1111",
        "description": "string",
        "val_type": "str",
        "multiple": False,
        "required": False,
        "": False,
        "returnable": False,
        "constraint": "str",
        "prm_link_filter": "string",
        "group": "string",
        "tmo_id": 1,
        "field_value": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm = TPRM(**data)
    session.add(tprm)
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data["name"])
    ).scalar()
    update_data = {
        "version": 1,
        "name": "string1111",
        "description": "string",
        "required": False,
        "field_value": "str",
        "returnable": False,
        "constraint": "str",
        "group": "string",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)

    assert res.status_code == 200
    assert res.json()["version"] == tprm.version
    assert "forse" not in res.json()


def test_update_param_type_error_tprm_already_exists(
    session: Session, client: TestClient
):
    """TPRM with 'tprm_2' name was already created in the begging of module"""
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    update_data = {"version": 1, "name": "tprm_2"}
    res = client.patch(URL + f"{tprm1.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_tprm_required_and_in_tmo_primary(
    session: Session, client: TestClient
):
    """
    Primary parameter type should be required.
    """
    tmo = TMO(
        **{
            "name": "tmo_1_1",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "primary": [3],
        }
    )
    session.add(tmo)
    session.commit()

    tprm = TPRM(
        **{
            "name": "tprm_1111",
            "tmo_id": 2,
            "required": True,
            "multiple": False,
            "val_type": "str",
            "created_by": "Test creator",
            "field_value": "string",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm)
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()
    update_data = {
        "version": 1,
        "name": "tprm_1111",
        "description": "string",
        "required": False,
        "field_value": "str",
        "constraint": "str",
        "group": "string",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)

    assert res.status_code == 422


def test_update_param_type_error_val_type_mo_link_has_constraint(
    session: Session, client: TestClient
):
    """
    Not allowed to change constraint for required parameter
    """
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "mo_link",
        "required": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "1",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 409


def test_update_param_type_error_val_type_prm_link_has_constraint(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "prm_link",
        "created_by": "Test creator",
        "required": True,
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "1",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 409

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "required": True,
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 409


def test_update_param_type_error_val_type_formula_has_constraint(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "1",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "required": True,
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 409


def test_update_param_type_error_required_is_none(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "required": None,
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_required_and_constraint(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "srt",
        "required": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "required": True,
        "constraint": "aaa",
        "field_value": "aaa",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)

    assert res.status_code == 409


def test_update_param_type_constraint_str_validation(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "aaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "bbb",
        "field_value": "bbb",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


# TODO: add here test for force = True
def test_update_param_type_error_different_constraint_add_force_as_true(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "aaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "bbb",
        "field_value": "bbb",
        "force": False,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 409


def test_update_param_error_type_constraint_str_validation(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "constraint": "aaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "(()",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_constraint_float_validation(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": "1.14:15.42",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15.4:21.6",
        "field_value": "bbb",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_constraint_float_validation(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "constraint": "1.14:15.42",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15.2:11.6",
        "field_value": "bbb",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15.2:15.2",
        "field_value": "bbb",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": ":11.6",
        "field_value": "bbb",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_constraint_int_validation(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "1:15",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15:21",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_constraint_int_validation_1(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "constraint": "1:15",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15:11",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "15:15",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": ":11",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "asd:11.1",
        "force": True,
    }
    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_str_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "required": False,
        "constraint": "aaaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "aaaa",
        "field_value": ["aaaa", "aaaa"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_str_multiple_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "required": False,
        "constraint": "aaaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "aaaa",
        "field_value": ["aaaa", "aaaa"],
        # 'constraint': 'aaaa',
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_str_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "required": False,
        "constraint": "aaaa",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": ["aaaa", "aaaa"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_old_not_required_new_required_str_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "required": False,
        "constraint": "aaaa123",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": ["aaaa", "aaaa123"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_old_not_required_new_required_str_multiple_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "str",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "constraint": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",  # noqa: W605
        "field_value": ["example@mail.com", "example@mail"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_date_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "date",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": ["2023-03-12", "2021-04-12"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_old_not_required_new_required_date_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "date",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": ["2023.03.12", 111],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_datetime_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "datetime",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [
            "2023-06-15T14:30:45.123456Z",
            "2023-06-15T14:31:45.123456Z",
        ],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_datetime_multiple_field_value_1(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "datetime",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": ["2023.06-15T14:30:45.123456Z", 1231],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_float_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [1.1, 12],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_float_multiple_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [3.1, 4],
        "constraint": "1.1:12.4",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_float_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "constraint": "1.1:12.4",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [3.1, 4],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_old_not_required_new_required_float_multiple_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [3.1, 4],
        "constraint": "4:12.4",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [5, 16],
        "constraint": "4:12.4",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [5, 16],
        "constraint": "4:4",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_old_not_required_new_required_float_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "constraint": "4:12.4",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [3.1, 4],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    tprm = {
        "name": "tprm_11111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "constraint": "4:12.4",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_11111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [5, 16],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    tprm = {
        "name": "tprm_111111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "constraint": "4:4",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_111111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [5, 16],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_old_not_required_new_required_float_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "float",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [1.1, "asd"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_int_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [1, 12],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_int_multiple_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [2, 7],
        "constraint": "1:12",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_old_not_required_new_required_int_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "constraint": "1:12",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [2, 7],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_old_not_required_new_required_int_multiple_db_constraint_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "constraint": "4:12",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [2, 7],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    tprm = {
        "name": "tprm_11111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "constraint": "4:12",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_11111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [6, 15],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [6, 15],
        "constraint": "12:12",
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_error_old_not_required_new_required_int_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "int",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [1.1, "asd"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 422


def test_update_param_type_old_not_required_new_required_bool_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "bool",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_1111")
    ).scalar()

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [True, False],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_update_param_type_error_old_not_required_new_required_bool_multiple_field_value(
    session: Session, client: TestClient
):
    tprm = {
        "name": "tprm_1111",
        "tmo_id": 1,
        "val_type": "bool",
        "required": False,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "multiple": True,
    }
    session.add(TPRM(**tprm))
    session.commit()
    tprm = (
        session.execute(select(TPRM).where(TPRM.name == "tprm_1111"))
        .scalars()
        .first()
    )

    update_data = {
        "version": 1,
        "name": "tprm_11111",
        "field_value": [True, "True"],
        "force": True,
        "required": True,
    }

    res = client.patch(URL + f"{tprm.id}/", json=update_data)
    assert res.status_code == 200


def test_delete_param_type_2(session: Session, client: TestClient):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    res = client.delete(URL + f"{tprm1.id}/")

    assert not session.execute(
        select(TPRM).where(TPRM.name == "tprm_1")
    ).scalar()
    assert res.status_code == 200


def test_delete_param_type_error_with_id_not_exist(
    session: Session, client: TestClient
):
    res = client.delete(URL + f"{123}/")
    assert res.status_code == 404


def test_read_tmo_tprms(session: Session, client: TestClient):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    res = client.get(f"/api/inventory/v1/object_type/{tprm1.id}/param_types/")

    tmo_tprms_quantity = session.execute(
        select(func.count()).select_from(TPRM).where(TPRM.tmo_id == 1)
    ).scalar()

    assert len(res.json()) == tmo_tprms_quantity
    assert res.status_code == 200


def test_read_tmo_tprms_with_group_param(session: Session, client: TestClient):
    tprm = TPRM(
        **{
            "name": "new",
            "tmo_id": 1,
            "val_type": "str",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "group": "new_group",
        }
    )
    session.add(tprm)
    tprm1 = TPRM(
        **{
            "name": "new_1",
            "tmo_id": 1,
            "val_type": "str",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "group": "new_group",
        }
    )
    session.add(tprm1)
    session.commit()

    tmo_tprms_quantity = session.execute(
        select(func.count())
        .select_from(TPRM)
        .where(TPRM.tmo_id == 1, TPRM.group == "new_group")
    ).scalar()

    res = client.get(
        f"/api/inventory/v1/object_type/{1}/param_types/",
        params={"group": "new_group"},
    )
    pprint(res.json())
    assert len(res.json()) == tmo_tprms_quantity
    assert res.status_code == 200


def test_read_tmo_tprms_with_error_group_param(
    session: Session, client: TestClient
):
    res = client.get(
        f"/api/inventory/v1/object_type/{1}/param_types/",
        params={"group": "some_uncreated_name"},
    )
    assert not len(res.json())
    assert res.status_code == 200


def test_read_tmo_tprms_error_tmo_not_exists(
    session: Session, client: TestClient
):
    res = client.get(f"/api/inventory/v1/object_type/{123}/param_types/")

    assert res.status_code == 404


def test_update_val_type_error_version_not_relevant(
    session: Session, client: TestClient
):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    data = {"version": 123, "val_type": "float", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )

    assert res.status_code == 409


def test_update_val_type_error_val_type_required(
    session: Session, client: TestClient
):
    tprm = TPRM(
        **{
            "name": "new_1",
            "tmo_id": 1,
            "val_type": "str",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "required": True,
        }
    )
    session.add(tprm)
    session.commit()

    tprm1 = session.execute(select(TPRM).where(TPRM.name == "new_1")).scalar()

    data = {"version": tprm1.version, "val_type": "float", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )

    assert res.status_code == 409


def test_update_val_type_error_unallowed_val_type(
    session: Session, client: TestClient
):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    data = {"version": tprm1.version, "val_type": "prm_link", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    assert res.status_code == 422

    data = {"version": tprm1.version, "val_type": "mo_link", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    assert res.status_code == 422

    data = {"version": tprm1.version, "val_type": "user_link", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    assert res.status_code == 422

    data = {"version": tprm1.version, "val_type": "formula", "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    assert res.status_code == 200


def test_update_val_type_error_val_type(session: Session, client: TestClient):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    data = {
        "version": tprm1.version,
        "val_type": "not_exist_val_type",
        "force": True,
    }

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    assert res.status_code == 422


def test_update_val_type_if_not_changed(session: Session, client: TestClient):
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    data = {"version": tprm1.version, "val_type": tprm1.val_type, "force": True}

    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm1.id}/change_val_type/", json=data
    )
    tprm1 = session.execute(select(TPRM).where(TPRM.name == "tprm_1")).scalar()

    assert res.status_code == 200
    assert res.json()["version"] == tprm1.version
    assert res.json()["val_type"] == tprm1.val_type


def test_create_object_type_param_type_error_id_not_exists(
    session: Session, client: TestClient
):
    data = [{"name": "new_tprm111", "val_type": "str"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{123}/param_types/", json=data
    )
    assert res.status_code == 404


def test_create_object_type_param_type_error_val_type_not_exists(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {"name": "new_tprm111", "val_type": "string", "field_value": "string"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409


def test_create_object_type_param_type_error_formula_and_multiple(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "formula", "multiple": True}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409


def test_create_object_type_param_type_error_prm_link_required(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "prm_link", "required": True}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409


def test_create_object_type_param_type_error_mo_link_required(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "mo_link", "required": True}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409


def test_create_object_type_param_type_error_formula_required(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "formula", "required": True}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409


def test_create_object_type_param_type_error_prm_link_constraint_not_added(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "new_tprm111",
            "val_type": "prm_link",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_error_formula_constraint_not_added(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "new_tprm111",
            "val_type": "formula",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_error_field_value_and_required(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "str", "required": True}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_str(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {"name": "new_tprm111", "val_type": "str", "constraint": "some_string"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_object_type_param_error_type_constraint_str(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "new_tprm111",
            "val_type": "str",
            "constraint": "(some_string))",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {"name": "new_tprm111", "val_type": "str", "constraint": "[some_string"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {"name": "new_tprm111", "val_type": "str", "constraint": "(some_string"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_float(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {"name": "new_tprm111", "val_type": "float", "constraint": "1.2:3.3"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_object_type_param_type_error_constraint_float(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "float", "constraint": "1.2"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409

    data = [
        {"name": "new_tprm111", "val_type": "float", "constraint": "1.2:0.2"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {"name": "new_tprm111", "val_type": "float", "constraint": "0.2:0.2"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_int(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "int", "constraint": "1:2"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_object_type_param_type_error_constraint_int(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "int", "constraint": "3:2"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [{"name": "new_tprm111", "val_type": "int", "constraint": "2:2"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )

    assert res.status_code == 409

    data = [{"name": "new_tprm111", "val_type": "int", "constraint": "2.2:9.7"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_mo_link(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"tmo_id": tmo_1.id, "name": "mo_1"})
    session.add(mo)
    session.commit()
    mo = session.execute(select(MO).where(MO.name == "mo_1")).scalar()
    data = [
        {"name": "new_tprm111", "val_type": "mo_link", "constraint": str(mo.id)}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_object_type_param_type_error_constraint_mo_link(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [{"name": "new_tprm111", "val_type": "mo_link", "constraint": "123"}]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "new_tprm111",
            "val_type": "mo_link",
            "constraint": "some_str_value",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_prm_link(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    another_tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_2")
    ).scalar()
    data = [
        {
            "name": "new_tprm111",
            "val_type": "prm_link",
            "constraint": str(another_tprm.id),
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event


def test_create_object_type_param_type_error_constraint_prm_link(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    data = [
        {"name": "new_tprm111", "val_type": "prm_link", "constraint": "123"}
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "new_tprm111",
            "val_type": "prm_link",
            "constraint": "some_str_value",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_constraint_prm_link_with_prm_link_filter(
    session: Session, client: TestClient
):
    """
    TPRM id's in prm_link_filter can be referred to the same TPRM
    """
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    tmo_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_2)
    session.commit()

    tmo_2 = session.execute(select(TMO).where(TMO.name == "tmo_2")).scalar()

    main_tprm_data = {
        "name": "main_tprm",
        "val_type": "str",
        "constraint": "some_str_value",
        "tmo_id": tmo_1.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    main_tprm = TPRM(**main_tprm_data)
    session.add(main_tprm)
    session.commit()

    main_tprm = session.execute(
        select(TPRM).where(TPRM.name == "main_tprm")
    ).scalar()

    internal_tprm_data = {
        "name": "internal_tprm",
        "val_type": "prm_link",
        "constraint": str(main_tprm.id),
        "tmo_id": tmo_2.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    internal_tprm = TPRM(**internal_tprm_data)
    session.add(internal_tprm)

    external_tprm_data = {
        "name": "external_tprm",
        "val_type": "prm_link",
        "constraint": str(main_tprm.id),
        "tmo_id": tmo_1.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    external_tprm = TPRM(**external_tprm_data)
    session.add(external_tprm)
    session.commit()

    external_tprm = session.execute(
        select(TPRM).where(TPRM.name == "external_tprm")
    ).scalar()
    internal_tprm = session.execute(
        select(TPRM).where(TPRM.name == "internal_tprm")
    ).scalar()

    some_new_tprm_data = [
        {
            "name": "some_new_tprm",
            "val_type": "prm_link",
            "constraint": str(main_tprm.id),
            "prm_link_filter": f"{internal_tprm.id}:{external_tprm.id}",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_2.id}/param_types/",
        json=some_new_tprm_data,
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == some_new_tprm_data[0]["name"])
    ).scalar()
    assert tprm
    event = session.execute(
        select(Event).where(
            Event.model_id == tprm.id, Event.event_type == "TPRMCreate"
        )
    ).scalar()
    assert event
    assert (
        res.json()["data"][0]["prm_link_filter"]
        == f"{internal_tprm.id}:{external_tprm.id}"
    )
    assert res.json()["data"][0]["constraint"] == str(main_tprm.id)


def test_create_object_type_param_type_error_field_value_is_none_and_required(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "constraint": "str_data",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": None,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_error_required_and_multiple(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "constraint": "str_data",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": "str_not_list_data",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    print(res.json())
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_not_multiple_field_value_str_val_type_with_mo(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "some_str_value",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(
            PRM.value == "some_str_value",
            PRM.mo_id == mo.id,
            PRM.tprm_id == tprm.id,
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_not_multiple_field_value_bool_val_type(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "bool",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "true",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_data")
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo

    prm = session.execute(
        select(PRM).where(
            PRM.value == "true", PRM.mo_id == mo.id, PRM.tprm_id == tprm.id
        )
    ).scalar()
    assert prm

    data = [
        {
            "name": "tprm_data_1",
            "val_type": "bool",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "false",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_data")
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event


def test_create_object_type_param_type_error_required_and_not_multiple_field_value_bool_val_type(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "bool",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "some_another_value",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_not_multiple_field_value_int_val_type_with_mo(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": 123,
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_data")
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo

    prm = session.execute(
        select(PRM).where(
            PRM.value == str(123), PRM.mo_id == mo.id, PRM.tprm_id == tprm.id
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_error_and_not_multiple_field_value_int_val_type(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "some_str_value",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_not_multiple_field_value_float_val_type_with_mo(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": 123.1,
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    tprm = session.execute(
        select(TPRM).where(TPRM.name == "tprm_data")
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo

    prm = session.execute(
        select(PRM).where(
            PRM.value == str(123.1), PRM.mo_id == mo.id, PRM.tprm_id == tprm.id
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_error_and_not_multiple_field_value_float_val_type(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "some_str_value",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_error_required_not_multiple_field_value_not_exists_val_type(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            # can be only str, bool, int, float
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "field_value": "some_str_value",
            "multiple": False,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_not_multiple_field_value_str_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "constraint": "a",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "a",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(
            PRM.value == data[0]["field_value"],
            PRM.mo_id == mo.id,
            PRM.tprm_id == tprm.id,
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_not_multiple_field_value_float_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "1:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.0",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(
            PRM.value == data[0]["field_value"],
            PRM.mo_id == mo.id,
            PRM.tprm_id == tprm.id,
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_not_multiple_field_value_float_constraint_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "1:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.0",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(
            PRM.value == data[0]["field_value"],
            PRM.mo_id == mo.id,
            PRM.tprm_id == tprm.id,
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_not_multiple_field_value_float_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "1.14:",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.23",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": ":15.12",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.23",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "15.23:15.23",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.23",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "115.131:15.42",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.23",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "12.6:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "134.1",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "constraint": "12:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "11",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_not_multiple_field_value_int_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "1:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(
            PRM.value == data[0]["field_value"],
            PRM.mo_id == mo.id,
            PRM.tprm_id == tprm.id,
        )
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_not_multiple_field_value_int_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "1.14:",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13.23",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "1:",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": 13,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": ":15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "15:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "115:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "13",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409
    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "12:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "134",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409
    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "constraint": "12:15",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": False,
            "field_value": "11",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [1, 2, 3],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": None,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_str_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["asd", "fds"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_str_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [1, 1],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200


def test_create_object_type_param_type_required_and_multiple_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2023-03-12"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_date_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2023asd"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [123],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "datetime",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2023-06-15T14:30:45.123456Z"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_datetime_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "datetime",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2023asd"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "datetime",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [123],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [1.2, 2],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_float_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2", "1.2"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200


def test_create_object_type_param_type_required_and_multiple_int_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [1, 3],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_int_field_value_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2", 1.2],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200


def test_create_object_type_param_type_required_and_multiple_bool_field_value_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "bool",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": [True, False],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_bool_field_value(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "bool",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "field_value": ["2", 1.2],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_str_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "a",
            "field_value": ["a", "a"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_multiple_str_field_value_constraint_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "str",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",  # noqa: W605
            "field_value": ["example@mail.com", "example@mail"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_date_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "non constraint",
            "field_value": ["2022-3-2", "2002-3-12"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_multiple_date_field_value_constraint_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "date",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "non constraint",
            "field_value": ["asdasd", "asdas"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_datetime_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "datetime",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "non constraint",
            "field_value": [
                "2023-06-15T14:30:45.123456Z",
                "2023-07-15T14:30:45.123456Z",
            ],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_required_and_multiple_datetime_field_value_constraint_1(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "datetime",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "non constraint",
            "field_value": ["asdasd", "asdas"],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_float_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "1.0:15.0",
            "field_value": [3.4, 13.0, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_float_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "1.0:15.0",
            "field_value": [3.4, 17.0, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "2.0:15.0",
            "field_value": [1.4, 13.0, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "float",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "2.0:15.0",
            "field_value": ["asda", 2.5],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


def test_create_object_type_param_type_required_and_multiple_int_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()
    mo = MO(**{"name": "mo", "tmo_id": tmo_1.id})
    session.add(mo)
    session.commit()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "1:15",
            "field_value": [3, 13, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200

    tprm = session.execute(
        select(TPRM).where(TPRM.name == data[0]["name"])
    ).scalar()
    assert tprm
    create_event = (
        session.execute(
            select(Event).where(
                Event.event_type == "TPRMCreate", Event.model_id == tprm.id
            )
        )
        .scalars()
        .all()
    )
    assert create_event
    mo = session.execute(select(MO).where(MO.name == "mo")).scalar()
    assert mo
    prm = session.execute(
        select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm.id)
    ).scalar()
    assert prm


def test_create_object_type_param_type_error_required_and_multiple_int_field_value_constraint(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "1:15",
            "field_value": [3, 17, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "2:15",
            "field_value": [1, 13, 7],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "2:15",
            "field_value": [3, 4.5],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 200


def test_create_object_type_param_type_db_error(
    session: Session, client: TestClient
):
    tmo_1 = session.execute(select(TMO).where(TMO.name == "tmo_1")).scalar()

    data = [
        {
            "name": "tprm_data",
            "val_type": "int",
            "tmo_id": tmo_1.id,
            "required": True,
            "multiple": True,
            "constraint": "1:15",
            "field_value": [3, 4],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    ]

    tprm = TPRM(**data[0])
    session.add(tprm)
    session.commit()
    res = client.post(
        f"/api/inventory/v1/object_type/{tmo_1.id}/param_types/", json=data
    )
    assert res.status_code == 409


# start tests for batch_create_or_update_param_types endpoint
def test_batch_create_or_update_param_types_endpoint_can_create_tprm(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = False - can create tprm"""

    unique_name = "New name unique test"
    TPRM_DEFAULT_DATA["name"] = unique_name
    TPRM_DEFAULT_DATA["required"] = False

    stmt = select(TPRM).where(TPRM.name == unique_name)
    res = session.execute(stmt).scalar()
    assert res is None

    url = get_batch_create_update_url(1) + "?check=False"

    res = client.post(url, json=[TPRM_DEFAULT_DATA])

    assert res.status_code == 200
    res = session.execute(stmt).scalar()
    assert res is not None


def test_batch_create_or_update_param_types_endpoint_can_update_tprm(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = False - can update tprm"""

    updated_desription = "New description test"
    stmt = select(TPRM)
    tprm_from_bd = session.execute(stmt).scalar()
    payload = {k: getattr(tprm_from_bd, k) for k in TPRM_DEFAULT_DATA.keys()}
    payload["description"] = updated_desription
    assert tprm_from_bd.description != updated_desription

    url = get_batch_create_update_url(tprm_from_bd.tmo_id) + "?check=False"

    res = client.post(url, json=[payload])

    assert res.status_code == 200
    stmt = select(TPRM).where(TPRM.id == tprm_from_bd.id)
    res = session.execute(stmt).scalar()
    assert res.description == updated_desription


def test_batch_create_or_update_param_types_endpoint_does_not_create_tprm_if_check(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = True - does not create tprm"""

    unique_name = "New name unique test1"
    TPRM_DEFAULT_DATA["name"] = unique_name
    TPRM_DEFAULT_DATA["required"] = False

    stmt = select(TPRM).where(TPRM.name == unique_name)
    res = session.execute(stmt).scalar()
    assert res is None

    url = get_batch_create_update_url(1) + "?check=True"

    res = client.post(url, json=[TPRM_DEFAULT_DATA])

    assert res.status_code == 200
    res = session.execute(stmt).scalar()
    assert res is None


def test_batch_create_or_update_param_types_endpoint_does_not_update_tprm_if_check(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = True - does not update tprm"""

    updated_desription = "New description test1"
    stmt = select(TPRM)
    tprm_from_bd = session.execute(stmt).scalar()
    payload = {k: getattr(tprm_from_bd, k) for k in TPRM_DEFAULT_DATA.keys()}
    payload["description"] = updated_desription
    assert tprm_from_bd.description != updated_desription

    url = get_batch_create_update_url(tprm_from_bd.tmo_id) + "?check=True"

    res = client.post(url, json=[payload])

    assert res.status_code == 200
    stmt = select(TPRM).where(TPRM.id == tprm_from_bd.id)
    res = session.execute(stmt).scalar()
    assert res.description != updated_desription


def test_batch_create_or_update_param_types_endpoint_does_not_create_tprm_if_check_return_count(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = True - returns amount of tprms which can be created"""

    unique_name = "New name unique test"
    TPRM_DEFAULT_DATA["name"] = unique_name
    TPRM_DEFAULT_DATA["required"] = False

    stmt = select(TPRM).where(TPRM.name == unique_name)
    res = session.execute(stmt).scalar()

    assert res is None

    url = get_batch_create_update_url(1) + "?check=True"

    res = client.post(url, json=[TPRM_DEFAULT_DATA])

    assert res.status_code == 200
    assert res.json()["will_be_created"] == 1


def test_batch_create_or_update_param_types_endpoint_does_not_update_tprm_if_check_return_count(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = True - returns amount of tprms which can be updated"""

    updated_desription = "New description test1"
    stmt = select(TPRM)
    tprm_from_bd = session.execute(stmt).scalar()
    payload = {k: getattr(tprm_from_bd, k) for k in TPRM_DEFAULT_DATA.keys()}
    payload["description"] = updated_desription
    assert tprm_from_bd.description != updated_desription

    url = get_batch_create_update_url(tprm_from_bd.tmo_id) + "?check=True"

    res = client.post(url, json=[payload])

    assert res.status_code == 200
    assert res.json()["will_be_updated"] == 1


def test_batch_create_or_update_param_types_endpoint_does_not_update_tprm_if_check_return_count_zero(
    session: Session, client: TestClient
):
    """TEST Successful POST request into test_batch_create_or_update_param_types endpoint
    with check = True - if request TPRM data equals to existing TPRM  - returns amount
    of tprms which can be updated and updated equal to 0"""

    stmt = select(TPRM)
    tprm_from_bd = session.execute(stmt).scalar()
    payload = tprm_from_bd.dict()
    del payload["creation_date"]
    del payload["modification_date"]
    url = get_batch_create_update_url(tprm_from_bd.tmo_id) + "?check=True"

    res = client.post(url, json=[payload])

    assert res.status_code == 200
    assert res.json()["will_be_updated"] == 0
    assert res.json()["will_be_created"] == 0


def test_prm_creation_based_on_formula_tprm(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create mo with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Automated create new PRM based on formula TPRM"""
    prm_start_value = 10
    formula_increment = 5
    tprm_name_a = "TPRM_A"
    TPRM_A = {
        "name": tprm_name_a,
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_a = TPRM(**TPRM_A)
    session.add(tprm_a)
    session.commit()
    session.refresh(tprm_a)
    data = {
        "tmo_id": 1,
        "params": [{"value": prm_start_value, "tprm_id": tprm_a.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]

    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": f"parameter['{tprm_name_a}'] + {formula_increment}",
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200

    formula_tprm = session.execute(
        select(TPRM.id).where(TPRM.name == "FORMULA_TPRM")
    ).scalar()

    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200

    stmt = select(PRM).where(PRM.mo_id == mo_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[1].tprm_id == res.json()["id"]
    assert prm[1].mo_id == mo_id
    assert prm[1].value == str(prm_start_value + formula_increment)


def test_prm_creation_based_on_formula_tprm_2(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create mo with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Automated create new PRM based on formula TPRM"""
    prm_value = 10
    tprm_name_a = "TPRM_A"
    TPRM_A = {
        "name": tprm_name_a,
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_a = TPRM(**TPRM_A)
    session.add(tprm_a)
    session.commit()
    session.refresh(tprm_a)
    data = {"tmo_id": 1, "params": [{"value": prm_value, "tprm_id": tprm_a.id}]}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]
    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": f"if parameter['{tprm_name_a}'] > 5 then int('ffff', 16); else 10/5",
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200

    formula_tprm = session.execute(
        select(TPRM.id).where(TPRM.name == "FORMULA_TPRM")
    ).scalar()
    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200
    stmt = select(PRM).where(PRM.mo_id == mo_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[1].tprm_id == res.json()["id"]
    assert prm[1].mo_id == mo_id
    assert prm[1].value == "65535"


def test_prm_creation_based_on_formula_tprm_with_equal_string(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create mo with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Automated create new PRM based on formula TPRM"""
    prm_value = "qwerty"
    tprm_name_a = "TPRM_A"
    TPRM_A = {
        "name": tprm_name_a,
        "tmo_id": 1,
        "val_type": "str",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_a = TPRM(**TPRM_A)
    session.add(tprm_a)
    session.commit()
    session.refresh(tprm_a)
    data = {"tmo_id": 1, "params": [{"value": prm_value, "tprm_id": tprm_a.id}]}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]
    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": f"if parameter['{tprm_name_a}'] == '{prm_value}' then 'qwerty'; else 'asdfgh'",
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm = session.execute(
        select(TPRM.id).where(TPRM.name == "FORMULA_TPRM")
    ).scalar()
    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200
    stmt = select(PRM).where(PRM.mo_id == mo_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[1].tprm_id == res.json()["id"]
    assert prm[1].mo_id == mo_id
    assert prm[1].value == prm_value


def test_prm_creation_based_on_formula_tprm_with_equal_string_and_right_parameter(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create mo with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Automated create new PRM based on formula TPRM"""
    prm_value = "qwerty"
    tprm_name_a = "TPRM_A"
    formula_constraint = (
        f"if parameter['{tprm_name_a}'] == '{prm_value}' then parameter['{tprm_name_a}'][:3]; "
        f"else parameter['{tprm_name_a}'][3:]"
    )
    TPRM_A = {
        "name": tprm_name_a,
        "tmo_id": 1,
        "val_type": "str",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_a = TPRM(**TPRM_A)
    session.add(tprm_a)
    session.commit()
    session.refresh(tprm_a)

    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula_id = res.json().get("id")

    data = {"tmo_id": 1, "params": [{"value": prm_value, "tprm_id": tprm_a.id}]}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]

    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_id, PRM.tprm_id == tprm_formula_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.value == prm_value[:3]


def test_prm_update_based_on_formula_tprm(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create 2 mo-s with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Update tprm_a for every mo and update formula PRM"""
    prm_value = 10
    prm_increment = 1
    tprm_name_a = "TPRM_A"
    formula = f"if parameter['{tprm_name_a}'] > {prm_value - 1} then int('ffff', 16); else 10/5"
    TPRM_A = {
        "name": tprm_name_a,
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_a = TPRM(**TPRM_A)
    session.add(tprm_a)
    session.commit()
    session.refresh(tprm_a)
    data = {"tmo_id": 1, "params": [{"value": prm_value, "tprm_id": tprm_a.id}]}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id_1 = res.json()["id"]
    data = {
        "tmo_id": 1,
        "params": [{"value": prm_value + prm_increment, "tprm_id": tprm_a.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id_2 = res.json()["id"]
    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": formula,
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula_id = res.json()["id"]
    formula_tprm = session.execute(
        select(TPRM.id).where(TPRM.name == "FORMULA_TPRM")
    ).scalar()
    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id_1}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200

    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id_2}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200
    stmt = select(PRM).where(PRM.tprm_id == tprm_formula_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[0].mo_id == mo_id_1
    assert prm[0].value == "65535"
    assert prm[1].mo_id == mo_id_2
    assert prm[1].value == "65535"

    data = [
        {
            "value": prm_value - prm_increment - 1,
            "version": 1,
            "tprm_id": tprm_a.id,
        }
    ]
    res = client.patch(
        f"/api/inventory/v1/object/{mo_id_1}/parameters/", json=data
    )
    assert res.status_code == 200
    stmt = select(PRM).where(PRM.tprm_id == tprm_formula_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[0].mo_id == mo_id_2
    assert prm[0].value == "65535"
    assert prm[1].mo_id == mo_id_1
    assert prm[1].value == "2.0"

    data = {
        "value": "4",
        "version": 1,
    }
    res = client.patch(
        f"/api/inventory/v1/object/{mo_id_2}/param_types/{tprm_a.id}/parameter/",
        json=data,
    )
    assert res.status_code == 200
    stmt = select(PRM).where(PRM.tprm_id == tprm_formula_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[0].mo_id == mo_id_1
    assert prm[0].value == "2.0"
    assert prm[1].mo_id == mo_id_2
    assert prm[1].value == "2.0"


def test_tprm_update_with_formula(
    session: Session,
    client: TestClient,
):
    """Create tprm for formula equation
    Create mo with tprm_a
    Add new TPRM with formula contain link to early created TPRM
    Update formula TPRM"""
    prm_start_value = 10
    formula_increment = 5
    tprm_name_b = "TPRM_B"
    TPRM_B = {
        "name": tprm_name_b,
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_b = TPRM(**TPRM_B)
    session.add(tprm_b)
    session.commit()
    session.refresh(tprm_b)
    data = {
        "tmo_id": 1,
        "params": [{"value": prm_start_value, "tprm_id": tprm_b.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]
    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": f"parameter['{tprm_name_b}'] + {formula_increment}",
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula_id = res.json()["id"]
    formula_tprm = session.execute(
        select(TPRM.id).where(TPRM.name == "FORMULA_TPRM")
    ).scalar()
    assert tprm_formula_id == formula_tprm
    formula_res = client.post(
        url=f"/api/inventory/v1/object/{mo_id}/param_types/{formula_tprm}/parameter/",
        json={"value": 1.0},
    )
    assert formula_res.status_code == 200
    stmt = select(PRM).where(PRM.mo_id == mo_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[1].tprm_id == tprm_formula_id
    assert prm[1].mo_id == mo_id
    assert prm[1].value == str(prm_start_value + formula_increment)

    data = {
        "version": 1,
        "constraint": f"parameter['{tprm_name_b}'] + {formula_increment} - 1",
        "force": True,
    }
    res = client.patch(
        f"/api/inventory/v1/param_type/{tprm_formula_id}", json=data
    )
    assert res.status_code == 200
    stmt = select(PRM).where(PRM.mo_id == mo_id)
    prm = session.execute(stmt).scalars().all()
    assert len(prm) == 2
    assert prm[1].tprm_id == tprm_formula_id
    assert prm[1].mo_id == mo_id
    # According AD-2573 Value doesn't change after update constraint.
    assert prm[1].value == str(prm_start_value + formula_increment)


def test_formula_with_date(session: Session, client: TestClient):
    """Test date formula in all cases"""
    pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
    diff_time = 7
    prm_value_1 = 1
    prm_case_2_create_time = datetime.datetime.now() - datetime.timedelta(
        days=diff_time + 1
    )
    prm_case_2_update_time = datetime.datetime.now()
    prm_value_2 = (
        prm_case_2_update_time - prm_case_2_create_time
    ).total_seconds()

    prm_case_3_create_time = datetime.datetime.now() - datetime.timedelta(
        days=diff_time + 1
    )
    prm_case_3_update_time = datetime.datetime.now() - datetime.timedelta(
        days=diff_time + 1
    )
    prm_value_3 = 0
    tprm_creation_date_name = "creation_date"
    tprm_update_time_name = "update_time"
    formula = (
        f"if parameter['creation_date'] > datetime.now() - datetime.timedelta(days={diff_time})"
        f" then {prm_value_1};"
        f" elif parameter['update_time'] > datetime.now() - datetime.timedelta(days={diff_time})"
        f" then parameter['update_time'] - parameter['creation_date'];"
        f" else {prm_value_3}"
    )
    TPRM_CREATION_DATE = {
        "name": tprm_creation_date_name,
        "tmo_id": 1,
        "val_type": "datetime",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_UPDATE_TIME = {
        "name": tprm_update_time_name,
        "tmo_id": 1,
        "val_type": "datetime",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_creation_date = TPRM(**TPRM_CREATION_DATE)
    session.add(tprm_creation_date)
    tprm_update_time = TPRM(**TPRM_UPDATE_TIME)
    session.add(tprm_update_time)
    session.commit()
    session.refresh(tprm_creation_date)
    session.refresh(tprm_update_time)

    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": formula,
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm_id = res.json()["id"]

    # First case creation date is less than 7 days = 1
    data = {
        "tmo_id": 1,
        "params": [
            {
                "value": datetime.datetime.strftime(
                    datetime.datetime.now(), pattern
                ),
                "tprm_id": tprm_creation_date.id,
            },
            {
                "value": datetime.datetime.strftime(
                    datetime.datetime.now(), pattern
                ),
                "tprm_id": tprm_update_time.id,
            },
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]

    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm_id, PRM.mo_id == mo_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_1)

    # Second case creation date more than diff time at 7 days == diff in seconds
    data = {
        "tmo_id": 1,
        "params": [
            {
                "value": datetime.datetime.strftime(
                    prm_case_2_create_time, pattern
                ),
                "tprm_id": tprm_creation_date.id,
            },
            {
                "value": datetime.datetime.strftime(
                    prm_case_2_update_time, pattern
                ),
                "tprm_id": tprm_update_time.id,
            },
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm_id, PRM.mo_id == mo_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_2)

    # Third case creation date is more than diff time at 7 days and update date is more than 7 days == 0
    data = {
        "tmo_id": 1,
        "params": [
            {
                "value": datetime.datetime.strftime(
                    prm_case_3_create_time, pattern
                ),
                "tprm_id": tprm_creation_date.id,
            },
            {
                "value": datetime.datetime.strftime(
                    prm_case_3_update_time, pattern
                ),
                "tprm_id": tprm_update_time.id,
            },
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm_id, PRM.mo_id == mo_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_3)


def test_formula_with_string(session: Session, client: TestClient):
    """Test string concatenation in formula"""
    prm_type_value = "LTE"
    prm_frequnecy_value = 1800
    formula = "parameter['Type'] + '_' + parameter['Frequency']"
    prm_value = prm_type_value + "_" + str(prm_frequnecy_value)
    TPRM_TYPE = {
        "name": "Type",
        "tmo_id": 1,
        "val_type": "str",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_FREQUENCY = {
        "name": "Frequency",
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_type = TPRM(**TPRM_TYPE)
    session.add(tprm_type)
    tprm_frequency = TPRM(**TPRM_FREQUENCY)
    session.add(tprm_frequency)
    session.commit()
    session.refresh(tprm_type)
    session.refresh(tprm_frequency)

    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": formula,
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm_id = res.json()["id"]

    data = {
        "tmo_id": 1,
        "params": [
            {"value": prm_type_value, "tprm_id": tprm_type.id},
            {"value": prm_frequnecy_value, "tprm_id": tprm_frequency.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]

    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm_id, PRM.mo_id == mo_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == prm_value


def test_formula_with_math(session: Session, client: TestClient):
    """Test math formula in all cases"""
    prm_type_value = "LTE"
    prm_power_value = 2
    prm_power_gain_value = 3.14
    prm_rb_used_value = 5.17
    prm_rb_total_value = 7.19
    prm_radius_value = 11
    prm_value_1 = (prm_power_value**2 * prm_rb_used_value) / prm_rb_total_value
    prm_value_2 = prm_power_value**0.5 + prm_power_gain_value * prm_radius_value

    formula = (
        f"if parameter['Type'] == '{prm_type_value}'"
        f" then (math.pow(parameter['Power'], 2) * parameter['RB Used']) / parameter['RB Total'];"
        f" else math.sqrt(parameter['Power']) + parameter['Power Gain'] * parameter['Radius']"
    )
    TPRM_TYPE = {
        "name": "Type",
        "tmo_id": 1,
        "val_type": "str",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_POWER = {
        "name": "Power",
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_RB_USED = {
        "name": "RB Used",
        "tmo_id": 1,
        "val_type": "float",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_RB_TOTAL = {
        "name": "RB Total",
        "tmo_id": 1,
        "val_type": "float",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_POWER_GAIN = {
        "name": "Power Gain",
        "tmo_id": 1,
        "val_type": "float",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_RADIUS = {
        "name": "Radius",
        "tmo_id": 1,
        "val_type": "int",
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_type = TPRM(**TPRM_TYPE)
    session.add(tprm_type)
    tprm_power = TPRM(**TPRM_POWER)
    session.add(tprm_power)
    tprm_power_gain = TPRM(**TPRM_POWER_GAIN)
    session.add(tprm_power_gain)
    tprm_rb_used = TPRM(**TPRM_RB_USED)
    session.add(tprm_rb_used)
    tprm_rb_total = TPRM(**TPRM_RB_TOTAL)
    session.add(tprm_rb_total)
    tprm_radius = TPRM(**TPRM_RADIUS)
    session.add(tprm_radius)
    session.commit()
    session.refresh(tprm_type)
    session.refresh(tprm_power)
    session.refresh(tprm_power_gain)
    session.refresh(tprm_rb_used)
    session.refresh(tprm_rb_total)
    session.refresh(tprm_radius)

    data = {
        "name": "FORMULA_TPRM",
        "val_type": "formula",
        "constraint": formula,
        "returnable": True,
        "tmo_id": 1,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm_id = res.json()["id"]

    # First case creation date is less than 7 days = 1
    data = {
        "tmo_id": 1,
        "params": [
            {"value": prm_type_value, "tprm_id": tprm_type.id},
            {"value": prm_power_value, "tprm_id": tprm_power.id},
            {"value": prm_power_gain_value, "tprm_id": tprm_power_gain.id},
            {"value": prm_rb_used_value, "tprm_id": tprm_rb_used.id},
            {"value": prm_rb_total_value, "tprm_id": tprm_rb_total.id},
            {"value": prm_radius_value, "tprm_id": tprm_radius.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_id = res.json()["id"]

    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm_id, PRM.mo_id == mo_id)
    )
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_1)

    # Second case else
    data = {"value": prm_type_value + "_", "version": 1}
    res = client.patch(
        f"/api/inventory/v1/object/{mo_id}/param_types/{tprm_type.id}/parameter/",
        json=data,
    )
    assert res.status_code == 200
    prm = session.execute(stmt).scalars().first()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_2)

    data = [{"value": prm_type_value, "version": 2, "tprm_id": tprm_type.id}]
    res = client.patch(
        f"/api/inventory/v1/object/{mo_id}/parameters/", json=data
    )
    assert res.status_code == 200
    prm = session.execute(stmt).scalar()
    assert prm.tprm_id == formula_tprm_id
    assert prm.mo_id == mo_id
    assert prm.value == str(prm_value_1)


def test_formula_with_inner_field(session: Session, client: TestClient):
    """Add new type of formula for self TPRM without changes existed PRM values
    Add TPRM with int val_type
    Create couple MO with this TPRM
    Change TPRM val_type to formula
    Add formula constraint
    Create new MO and PRM with formula type will be created automatically with correct value"""
    tprm_name = "int_to_formula"
    tprm_type_old = "int"
    tprm_type_new = "formula"
    mo_1_prm_value = 10
    mo_2_prm_value = 0
    formula_increment = 2
    tmo_id = 1
    formula_constraint = f"INNER_MAX['{tprm_name}'] + {formula_increment}"
    # Create TPRM
    data = {
        "name": tprm_name,
        "val_type": tprm_type_old,
        "returnable": True,
        "tmo_id": tmo_id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm: TPRM = TPRM(**res.json())
    # Create couple MO
    data = {
        "tmo_id": tmo_id,
        "params": [{"value": mo_1_prm_value, "tprm_id": formula_tprm.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1_id = res.json()["id"]
    data = {
        "tmo_id": tmo_id,
        "params": [{"value": mo_2_prm_value, "tprm_id": formula_tprm.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2_id = res.json()["id"]
    # Update TPRM val_type
    data = {
        "version": formula_tprm.version,
        "val_type": tprm_type_new,
        "force": True,
    }
    res = client.patch(
        f"/api/inventory/v1/param_type/{formula_tprm.id}/change_val_type/",
        json=data,
    )
    assert res.status_code == 200
    assert res.json().get("val_type") == "formula"
    current_tprm_version = res.json().get("version")
    # Add constraint to formula
    data = {
        "version": current_tprm_version,
        "constraint": formula_constraint,
        "force": True,
    }
    res = client.patch(
        f"/api/inventory/v1/param_type/{formula_tprm.id}", json=data
    )
    assert res.status_code == 200
    assert res.json().get("constraint") == formula_constraint
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_1_id)
    )
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == str(mo_1_prm_value)
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_2_id)
    )
    prm_for_mo_2: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_2.value == str(mo_2_prm_value)
    # Create test new mo without formula PRM and it will be created with help formula
    data = {"tmo_id": tmo_id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_3: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_3.id)
    )
    prm_for_mo_3: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_3.value == str(
        max(mo_1_prm_value, mo_2_prm_value) + formula_increment
    )


def test_formula_with_split(session: Session, client: TestClient):
    """Add new function for formula string.split()
    With help formula split input data to result
    examples
    INPUT DATA -> RESULT
    NY001-MAN-372-11 TR01 NY001-MAN-261-11 010 -> 372-261 RING XH 1
    NY001-MAN-372-11 TR01 NY001-MAN-261-11 010 -> 3G 372 1
    NY001-CHI-20K0P0-30 TR01 NY001-MAN-YYYY-11 133 -> FIBER FTTx CHI-20K0P0 YYYY
    """
    tmo_formula_name = "TMO_FORMULA"
    tmo_for_molink_name = "TMO_MOLINK"
    tprm_all_services_name = "All services"
    tprm_all_services_type = "str"
    tprm_service_ID_name = "Service ID"
    tprm_service_id_type = "str"
    tprm_service_type_name = "Service Type"
    tprm_service_type_type = "mo_link"
    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"
    prm_service_name_1 = "SERVICE_NAME_1"
    prm_service_name_2 = "SERVICE_NAME_2"
    prm_service_name_3 = "SERVICE_NAME_3"
    prm_service_id_1 = "NY001-MAN-372-11 TR01 NY001-MAN-261-11 010"
    prm_service_id_2 = "NY001-CHI-20K0P0-30 TR01 NY001-MAN-YYYY-11 133"
    prm_result_1 = "372-261 RING XH 01"
    prm_result_2 = "3G 372 01"
    prm_result_3 = "FIBER FTTx CHI-20K0P0 YYYY"

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TMO_MO_LINK = {
        "name": tmo_for_molink_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    tmo_mo_link = TMO(**TMO_MO_LINK)
    session.add(tmo_formula)
    session.add(tmo_mo_link)
    session.commit()
    session.refresh(tmo_formula)
    session.refresh(tmo_mo_link)
    TPRM_ALL_SERVICES = {
        "name": tprm_all_services_name,
        "val_type": tprm_all_services_type,
        "returnable": True,
        "tmo_id": tmo_mo_link.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_all_services = TPRM(**TPRM_ALL_SERVICES)
    session.add(tprm_all_services)
    session.commit()
    session.refresh(tmo_formula)
    session.refresh(tmo_mo_link)
    session.refresh(tprm_all_services)

    TPRM_SERVICE_ID = {
        "name": tprm_service_ID_name,
        "val_type": tprm_service_id_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_SERVICE_TYPE = {
        "name": tprm_service_type_name,
        "val_type": tprm_service_type_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "constraint": str(tmo_mo_link.id),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_service_id = TPRM(**TPRM_SERVICE_ID)
    tprm_service_type = TPRM(**TPRM_SERVICE_TYPE)
    session.add(tprm_service_id)
    session.add(tprm_service_type)
    session.commit()
    session.refresh(tmo_formula)
    session.refresh(tmo_mo_link)
    session.refresh(tprm_all_services)
    session.refresh(tprm_service_id)
    session.refresh(tprm_service_type)

    # Create MO for MO_LINK
    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {"value": prm_service_name_1, "tprm_id": tprm_all_services.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_1: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {"value": prm_service_name_2, "tprm_id": tprm_all_services.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_2: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {"value": prm_service_name_3, "tprm_id": tprm_all_services.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_3: MO = MO(**res.json())
    # Create TPRM for formula
    formula_constraint = (
        f"if parameter['{tprm_service_type_name}'] == {mo_service_1.name} "
        f"then parameter['{tprm_service_ID_name}'].split()[0].split('-')[2] + "
        f"'-' + parameter['{tprm_service_ID_name}'].split()[2].split('-')[2] + ' RING XH ' + "
        f"parameter['{tprm_service_ID_name}'].split()[1][-2:]; "
        f"elif parameter['{tprm_service_type_name}'] == {mo_service_2.name} then '3G ' + "
        f"parameter['{tprm_service_ID_name}'].split()[0].split('-')[2] + ' ' + "
        f"parameter['{tprm_service_ID_name}'].split()[1][-2:]; else 'FIBER FTTx ' + "
        f"parameter['{tprm_service_ID_name}'].split()[0].split('-')[1] + '-' + "
        f"parameter['{tprm_service_ID_name}'].split()[0].split('-')[2] + ' ' + "
        f"parameter['{tprm_service_ID_name}'].split()[2].split('-')[2]"
    )

    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())
    # Create FORMULA MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_service_id_1, "tprm_id": tprm_service_id.id},
            {"value": str(mo_service_1.id), "tprm_id": tprm_service_type.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == prm_result_1

    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_service_id_1, "tprm_id": tprm_service_id.id},
            {"value": str(mo_service_2.id), "tprm_id": tprm_service_type.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == prm_result_2

    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_service_id_2, "tprm_id": tprm_service_id.id},
            {"value": str(mo_service_3.id), "tprm_id": tprm_service_type.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_3: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_3.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == prm_result_3


def test_formula_with_parameter_string_value(
    session: Session, client: TestClient
):
    """Test for correct work with string cast to int."""
    tprm_for_formula_name = "TPRM_STRING"
    tprm_formula_name = "TPRM_FORMULA"
    tprm_formula_val_type = "formula"
    tprm_string_val_type = "str"
    prm_string_value = "010"
    tmo_id = 1
    true_value = 10

    formula_constraint = f"if parameter['{tprm_for_formula_name}'] == '{prm_string_value}' then {true_value}; else 2"
    TPRM_FOR_FORMULA = {
        "name": tprm_for_formula_name,
        "tmo_id": tmo_id,
        "val_type": tprm_string_val_type,
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_formula = TPRM(**TPRM_FOR_FORMULA)
    session.add(tprm_for_formula)
    session.commit()
    session.refresh(tprm_for_formula)

    data = {
        "name": tprm_formula_name,
        "val_type": tprm_formula_val_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())

    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [{"value": prm_string_value, "tprm_id": tprm_for_formula.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(true_value)


def test_formula_with_mo_link_with_primary(
    session: Session, client: TestClient
):
    """Test for correct work with string cast to int."""
    tmo_formula_name = "TMO_FORMULA"
    tmo_for_molink_name = "TMO_MOLINK"
    tprm_for_molink_service_name = "All services"
    tprm_for_molink_service_type = "str"

    tprm_for_molink_primary_name = "Primary name TPRM"
    tprm_for_molink_primary_type = "str"

    tprm_service_type_name = "Service Type"
    tprm_service_type_type = "mo_link"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"

    prm_for_molink_service_value_1 = "SERVICE_NAME_1"
    prm_for_molink_primary_value_1 = "010"

    prm_for_molink_service_value_2 = "SERVICE_NAME_2"
    prm_for_molink_primary_value_2 = "133"

    prm_for_molink_service_value_3 = "SERVICE_NAME_3"
    prm_for_molink_primary_value_3 = "020"
    first_value_for_formula = 10
    second_value_for_formula = 20
    else_value_for_formula = 30

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TMO_MO_LINK = {
        "name": tmo_for_molink_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    tmo_mo_link = TMO(**TMO_MO_LINK)
    session.add(tmo_formula)
    session.add(tmo_mo_link)
    session.commit()
    session.refresh(tmo_formula)
    session.refresh(tmo_mo_link)
    TPRM_SERVICE = {
        "name": tprm_service_type_name,
        "val_type": tprm_service_type_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "constraint": str(tmo_mo_link.id),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_service = TPRM(**TPRM_SERVICE)
    session.add(tprm_service)

    TPRM_FOR_MOLINK_SERVICE = {
        "name": tprm_for_molink_service_name,
        "val_type": tprm_for_molink_service_type,
        "returnable": True,
        "tmo_id": tmo_mo_link.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_molink_service = TPRM(**TPRM_FOR_MOLINK_SERVICE)
    session.add(tprm_for_molink_service)
    TPRM_FOR_MOLINK_PRIMARY = {
        "name": tprm_for_molink_primary_name,
        "val_type": tprm_for_molink_primary_type,
        "required": True,
        "returnable": True,
        "tmo_id": tmo_mo_link.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_molink_primary = TPRM(**TPRM_FOR_MOLINK_PRIMARY)
    session.add(tprm_for_molink_primary)
    session.commit()
    session.refresh(tmo_formula)
    session.refresh(tmo_mo_link)
    session.refresh(tprm_service)
    session.refresh(tprm_for_molink_service)
    session.refresh(tprm_for_molink_primary)

    # Set primary for MO_LINK TMO
    data = {
        "version": tmo_mo_link.version,
        "primary": [tprm_for_molink_primary.id],
    }
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_mo_link.id}", json=data
    )
    assert res.status_code == 200

    # Create MO for MO_LINK
    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {
                "value": prm_for_molink_service_value_1,
                "tprm_id": tprm_for_molink_service.id,
            },
            {
                "value": prm_for_molink_primary_value_1,
                "tprm_id": tprm_for_molink_primary.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_1: MO = MO(**res.json())
    assert mo_service_1.name == prm_for_molink_primary_value_1

    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {
                "value": prm_for_molink_service_value_2,
                "tprm_id": tprm_for_molink_service.id,
            },
            {
                "value": prm_for_molink_primary_value_2,
                "tprm_id": tprm_for_molink_primary.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_2: MO = MO(**res.json())
    assert mo_service_2.name == prm_for_molink_primary_value_2

    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {
                "value": prm_for_molink_service_value_3,
                "tprm_id": tprm_for_molink_service.id,
            },
            {
                "value": prm_for_molink_primary_value_3,
                "tprm_id": tprm_for_molink_primary.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_service_3: MO = MO(**res.json())
    assert mo_service_3.name == prm_for_molink_primary_value_3

    # Create TPRM for formula
    formula_constraint = (
        f"if parameter['{tprm_service_type_name}'] == '{mo_service_1.name}' "
        f"then {first_value_for_formula}; elif parameter['{tprm_service_type_name}'] == "
        f"'{mo_service_2.name}' then {second_value_for_formula}; else {else_value_for_formula}"
    )

    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())
    # Create MO for create formula PRM value
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": str(mo_service_1.id), "tprm_id": tprm_service.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(first_value_for_formula)

    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": str(mo_service_2.id), "tprm_id": tprm_service.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(second_value_for_formula)

    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": str(mo_service_3.id), "tprm_id": tprm_service.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_3: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_3.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(else_value_for_formula)


def test_user_refresh_formula(session: Session, client: TestClient):
    """Create a couple MO without formula, then create formula TPRM and then refresh all PRM"""
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"
    tprm_for_int_name = "TPRM INT"
    tprm_for_int_type = "int"

    prm_value_int_1 = 10
    prm_value_int_2 = 20
    prm_value_int_3 = 30
    formula_increment = 7
    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)

    TPRM_FOR_INT = {
        "name": tprm_for_int_name,
        "val_type": tprm_for_int_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_for_int = TPRM(**TPRM_FOR_INT)
    session.add(tprm_for_int)
    session.commit()
    session.refresh(tprm_for_int)
    session.refresh(tmo_formula)

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": prm_value_int_1, "tprm_id": tprm_for_int.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())

    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": prm_value_int_2, "tprm_id": tprm_for_int.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())

    # Add formula TPRM
    formula_constraint = (
        f"parameter['{tprm_for_int_name}'] + {formula_increment}"
    )
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "field_value": "",
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())

    # Create one more MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": prm_value_int_3, "tprm_id": tprm_for_int.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_3: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_3.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_3: PRM = session.execute(stmt).scalars().first()
    assert prm_3.value == str(prm_value_int_3 + formula_increment)

    stmt_prm_1 = select(PRM).where(
        and_(PRM.mo_id == mo_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_1: PRM = session.execute(stmt_prm_1).scalars().first()
    assert prm_1 is None

    stmt_prm_2 = select(PRM).where(
        and_(PRM.mo_id == mo_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_2: PRM = session.execute(stmt_prm_2).scalars().first()
    assert prm_2 is None

    # Invoke refresh formula
    res = client.post(
        f"api/inventory/v1/param_type/{tprm_formula.id}/recalc_formula/"
    )
    assert res.status_code == 200

    prm_1: PRM = session.execute(stmt_prm_1).scalars().first()
    assert prm_1.value == str(prm_value_int_1 + formula_increment)

    prm_2: PRM = session.execute(stmt_prm_2).scalars().first()
    assert prm_2.value == str(prm_value_int_2 + formula_increment)


def test_user_create_tprm_formula_primary(session: Session, client: TestClient):
    # create tmo
    # create formula tprm with required
    # update tmo with add primary
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_int_first_name = "TPRM INT 1"
    tprm_for_int_first_type = "int"
    tprm_for_int_second_name = "TPRM INT 2"
    tprm_for_int_second_type = "int"

    tprm_formula_name = "TPRM_FORMULA"
    tprm_formula_val_type = "formula"

    formula_increment = 7
    prm_for_int_first_1 = 2
    prm_for_int_second_1 = 6
    prm_for_int_first_2 = 3
    prm_for_int_second_2 = 4

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)
    TPRM_FOR_INT_FIRST = {
        "name": tprm_for_int_first_name,
        "val_type": tprm_for_int_first_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "required": True,
        "default_value": "1",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_first = TPRM(**TPRM_FOR_INT_FIRST)
    session.add(tprm_for_int_first)
    TPRM_FOR_INT_SECOND = {
        "name": tprm_for_int_second_name,
        "val_type": tprm_for_int_second_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "required": True,
        "default_value": "1",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_second = TPRM(**TPRM_FOR_INT_SECOND)
    session.add(tprm_for_int_second)
    session.commit()
    session.refresh(tprm_for_int_first)
    session.refresh(tprm_for_int_second)
    session.refresh(tmo_formula)
    formula_constraint = (
        f"parameter['{tprm_for_int_first_name}'] * parameter['{tprm_for_int_second_name}'] "
        f"+ {formula_increment}"
    )
    data = {
        "name": tprm_formula_name,
        "val_type": tprm_formula_val_type,
        "constraint": formula_constraint,
        "required": True,
        "returnable": True,
        # 'field_value': '',
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())

    data = {
        "version": tmo_formula.version,
        "primary": [tprm_for_int_first.id, tprm_formula.id],
    }
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_formula.id}", json=data
    )
    assert res.status_code == 200
    tmo: TMO = TMO(**res.json())
    assert tmo.primary == [tprm_for_int_first.id, tprm_formula.id]

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {
                "value": str(prm_for_int_first_1),
                "tprm_id": tprm_for_int_first.id,
            },
            {
                "value": str(prm_for_int_second_1),
                "tprm_id": tprm_for_int_second.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(
        prm_for_int_first_1 * prm_for_int_second_1 + formula_increment
    )
    new_name = (
        str(prm_for_int_first_1)
        + "-"
        + str(prm_for_int_first_1 * prm_for_int_second_1 + formula_increment)
    )
    assert mo_formula_1.name == new_name
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {
                "value": str(prm_for_int_first_1),
                "tprm_id": tprm_for_int_first.id,
            },
            {
                "value": str(prm_for_int_second_1),
                "tprm_id": tprm_for_int_second.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 409
    assert res.json() == {
        "detail": f"Object with name '{new_name}' already exists."
    }

    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {
                "value": str(prm_for_int_first_2),
                "tprm_id": tprm_for_int_first.id,
            },
            {
                "value": str(prm_for_int_second_2),
                "tprm_id": tprm_for_int_second.id,
            },
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(
        prm_for_int_first_2 * prm_for_int_second_2 + formula_increment
    )
    assert mo_formula_2.name == str(prm_for_int_first_2) + "-" + str(
        prm_for_int_first_2 * prm_for_int_second_2 + formula_increment
    )


def test_update_formula_to_incorrect_value(
    session: Session, client: TestClient
):
    """Create formula TPRM, create TMO, then change formula constraint"""
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"

    tprm_for_int_first_name = "TPRM INT 1"
    tprm_for_int_first_type = "int"
    tprm_for_int_second_name = "TPRM INT 2"
    tprm_for_int_second_type = "int"

    prm_value_int_1 = 10
    if_value = 1

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)

    TPRM_FOR_INT_FIRST = {
        "name": tprm_for_int_first_name,
        "val_type": tprm_for_int_first_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_first = TPRM(**TPRM_FOR_INT_FIRST)
    session.add(tprm_for_int_first)
    TPRM_FOR_INT_SECOND = {
        "name": tprm_for_int_second_name,
        "val_type": tprm_for_int_second_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_second = TPRM(**TPRM_FOR_INT_SECOND)
    session.add(tprm_for_int_second)
    session.commit()
    session.refresh(tprm_for_int_first)
    session.refresh(tprm_for_int_second)
    session.refresh(tmo_formula)

    # Add formula TPRM
    formula_constraint = (
        f"if parameter['{tprm_for_int_first_name}'] == {prm_value_int_1} then {if_value}; "
        f"else parameter['{tprm_for_int_second_name}'] + 1"
    )
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_value_int_1, "tprm_id": tprm_for_int_first.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    stmt_prm_1 = select(PRM).where(
        and_(PRM.mo_id == mo_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_1: PRM = session.execute(stmt_prm_1).scalars().first()
    assert prm_1.value == str(if_value)
    new_formula_constraint = (
        f"if parameter['{tprm_for_int_first_name}'] == {prm_value_int_1 + 1} then {if_value}; "
        f"else parameter['{tprm_for_int_second_name}'] + 1"
    )
    data = {
        "version": tprm_formula.version,
        "constraint": new_formula_constraint,
        "force": True,
    }
    res = client.patch(
        f"api/inventory/v1/param_type/{tprm_formula.id}", json=data
    )
    assert res.status_code == 200
    prm_1: PRM = session.execute(stmt_prm_1).scalars().first()
    # According AD-2573 Value doesn't change after change constraint
    assert prm_1.value == str(if_value)


def test_formula_inner_field_without_start_value(
    session: Session, client: TestClient
):
    """Add new type of formula for self TPRM without changes existed PRM values
    Create TPRM with INNER MAX formula
    Then create first MO and set default value as 0"""
    tprm_name = "inner formula"
    formula_increment = 2
    tmo_id = 1
    formula_constraint = f"INNER_MAX['{tprm_name}'] + {formula_increment}"
    # Create TPRM
    data = {
        "name": tprm_name,
        "val_type": "formula",
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm: TPRM = TPRM(**res.json())

    # Create couple MO
    data = {"tmo_id": tmo_id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_1.id)
    )
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == "0"
    data = {"tmo_id": tmo_id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_2.id)
    )
    prm_for_mo_2: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_2.value == str(formula_increment)


def test_formula_required_creation(session: Session, client: TestClient):
    """Invoke error if we can create MO with incorrect formula"""
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"
    formula_else_value = 11

    tprm_for_int_first_name = "TPRM INT 1"
    tprm_for_int_first_type = "int"
    tprm_for_int_second_name = "TPRM INT 2"
    tprm_for_int_second_type = "int"

    prm_value_int_first = 10
    prm_value_int_second = 20

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)

    TPRM_FOR_INT_FIRST = {
        "name": tprm_for_int_first_name,
        "val_type": tprm_for_int_first_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_first = TPRM(**TPRM_FOR_INT_FIRST)
    session.add(tprm_for_int_first)
    TPRM_FOR_INT_SECOND = {
        "name": tprm_for_int_second_name,
        "val_type": tprm_for_int_second_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_second = TPRM(**TPRM_FOR_INT_SECOND)
    session.add(tprm_for_int_second)
    session.commit()
    session.refresh(tprm_for_int_first)
    session.refresh(tprm_for_int_second)
    session.refresh(tmo_formula)
    formula_constraint = (
        f"if parameter['{tprm_for_int_first_name}'] > 1 "
        f"then parameter['{tprm_for_int_second_name}']; "
        f"else {formula_else_value}"
    )
    # Create TPRM
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "required": True,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    tprm_formula: TPRM = TPRM(**res.json())

    # Create MO
    data = {"tmo_id": tmo_formula.id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == str(formula_else_value)

    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_value_int_first, "tprm_id": tprm_for_int_first.id},
            {"value": prm_value_int_second, "tprm_id": tprm_for_int_second.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())
    stmt_prm_1 = select(PRM).where(
        and_(PRM.mo_id == mo_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_for_mo_2: PRM = session.execute(stmt_prm_1).scalar()
    assert prm_for_mo_2.value == str(prm_value_int_second)


def test_formula_complex_inner_max(session: Session, client: TestClient):
    """Create complex formula with inner max"""
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"

    tprm_for_int_first_name = "TPRM INT 1"
    tprm_for_int_first_type = "int"
    tprm_for_int_second_name = "TPRM INT 2"
    tprm_for_int_second_type = "int"

    prm_value_int_first = 20
    prm_value_int_second = 10

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)

    TPRM_FOR_INT_FIRST = {
        "name": tprm_for_int_first_name,
        "val_type": tprm_for_int_first_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_first = TPRM(**TPRM_FOR_INT_FIRST)
    session.add(tprm_for_int_first)
    TPRM_FOR_INT_SECOND = {
        "name": tprm_for_int_second_name,
        "val_type": tprm_for_int_second_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_int_second = TPRM(**TPRM_FOR_INT_SECOND)
    session.add(tprm_for_int_second)
    session.commit()
    session.refresh(tprm_for_int_first)
    session.refresh(tprm_for_int_second)
    session.refresh(tmo_formula)
    formula_constraint = (
        f"if INNER_MAX['{tprm_for_int_first_name}'] > INNER_MAX['{tprm_for_int_second_name}'] "
        f"then INNER_MAX['{tprm_for_int_first_name}'] + 1; "
        f"else INNER_MAX['{tprm_for_int_second_name}'] + 1"
    )
    # Create TPRM
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm: TPRM = TPRM(**res.json())

    # Create couple MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": prm_value_int_first, "tprm_id": tprm_for_int_first.id},
            {"value": prm_value_int_second, "tprm_id": tprm_for_int_second.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_1.id)
    )
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == str(
        max(prm_value_int_first, prm_value_int_second) + 1
    )


def test_formula_with_sequence(session: Session, client: TestClient):
    """Create tprm with sequence and tprm with string.
    Then create formula parameter['string'] + '-' + parameter['seq']"""

    # Arrange
    tmo_formula_id = 1

    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"
    tprm_with_data_name = "tprm_data"
    tprm_with_data_val_type = "str"
    tprm_with_data_value = "Asdfgh"
    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"

    formula_constraint = f"parameter['{tprm_with_data_name}'] + '-' + parameter['{tprm_seq_name}']"

    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "returnable": True,
        "tmo_id": tmo_formula_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_data = {
        "name": tprm_with_data_name,
        "val_type": tprm_with_data_val_type,
        "returnable": True,
        "required": True,
        "tmo_id": tmo_formula_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_sequence = TPRM(**tprm_sequence)
    session.add(tprm_for_sequence)
    tprm_for_data = TPRM(**tprm_data)
    session.add(tprm_for_data)
    session.commit()
    session.refresh(tprm_for_sequence)
    session.refresh(tprm_for_data)

    # Act
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": False,
        "required": True,
        "tmo_id": tmo_formula_id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    assert res.status_code == 200
    formula_tprm: TPRM = TPRM(**res.json())

    # Create MO
    data = {
        "tmo_id": tmo_formula_id,
        "params": [
            {"value": tprm_with_data_value, "tprm_id": tprm_for_data.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    # Validate
    mo_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_1.id)
    )
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == f"{tprm_with_data_value}-1"
    stmt = select(PRM).where(
        and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_2.id)
    )
    prm_for_mo_2: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_2.value == f"{tprm_with_data_value}-2"


def test_simple_sequence(session: Session, client: TestClient):
    """Create simple sequence TPRM and create 2 MO's. Check if prm value is ascending."""
    # Arrange
    tmo_id = 1
    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"

    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "returnable": True,
        "tmo_id": tmo_id,
    }
    # Act
    res = client.post(URL, json=tprm_sequence)
    assert res.status_code == 200
    tprm: TPRM = TPRM(**res.json())
    assert tprm.name == tprm_seq_name
    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())
    # Validate
    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_1.id))
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == "1"
    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_2.id))
    prm_for_mo_2: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_2.value == "2"


def test_sequence_with_constraint(session: Session, client: TestClient):
    """Simple example with constraint.
    We create two type of constraints and every type of constraints counts independently"""
    # Arrange
    tmo_id = 1
    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"

    tprm_with_data_name = "tprm_data"
    tprm_with_data_val_type = "int"
    tprm_with_data_value_100 = 100
    tprm_with_data_value_1000 = 1000
    tprm_data = {
        "name": tprm_with_data_name,
        "val_type": tprm_with_data_val_type,
        "returnable": True,
        "required": True,
        "tmo_id": tmo_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_data = TPRM(**tprm_data)
    session.add(tprm_for_data)
    session.commit()
    session.refresh(tprm_for_data)

    # Act
    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "constraint": str(tprm_for_data.id),
        "returnable": True,
        "tmo_id": tmo_id,
    }
    res = client.post(URL, json=tprm_sequence)
    assert res.status_code == 200
    tprm: TPRM = TPRM(**res.json())
    assert tprm.name == tprm_seq_name
    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo = MO(**res.json())
    # Validate
    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo.id))
    prm_for_mo: PRM = session.execute(stmt).scalar()
    assert prm_for_mo.value == "1"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo = MO(**res.json())

    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo.id))
    prm_for_mo: PRM = session.execute(stmt).scalar()
    assert prm_for_mo.value == "2"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo = MO(**res.json())

    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo.id))
    prm_for_mo: PRM = session.execute(stmt).scalar()
    assert prm_for_mo.value == "1"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo = MO(**res.json())

    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo.id))
    prm_for_mo: PRM = session.execute(stmt).scalar()
    assert prm_for_mo.value == "2"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo = MO(**res.json())

    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo.id))
    prm_for_mo: PRM = session.execute(stmt).scalar()
    assert prm_for_mo.value == "3"


def test_sequence_with_insert(session: Session, client: TestClient):
    """Add sequence TPRM. And create 3 MO's with sequence PRM value 1,2,3.
    Then we create new MO with sequence value = 2 and we're expecting what old MO value 2 and 3
    will be recalculated to 3 and 4 value"""
    # Arrange
    tmo_id = 1
    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"

    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "returnable": True,
        "tmo_id": tmo_id,
    }
    # Act
    res = client.post(URL, json=tprm_sequence)
    assert res.status_code == 200
    tprm: TPRM = TPRM(**res.json())
    assert tprm.name == tprm_seq_name
    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_3: MO = MO(**res.json())

    stmt = select(PRM).where(and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_1.id))
    prm_for_mo_1: PRM = session.execute(stmt).scalar()
    assert prm_for_mo_1.value == "1"
    stmt_prm_for_mo_2 = select(PRM).where(
        and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_2.id)
    )
    prm_for_mo_2: PRM = session.execute(stmt_prm_for_mo_2).scalar()
    assert prm_for_mo_2.value == "2"
    stmt_prm_for_mo_3 = select(PRM).where(
        and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_3.id)
    )
    prm_for_mo_3: PRM = session.execute(stmt_prm_for_mo_3).scalar()
    assert prm_for_mo_3.value == "3"

    data = {
        "tmo_id": tmo_id,
        "params": [{"value": 2, "tprm_id": tprm.id}],
    }
    # Validate
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2_new: MO = MO(**res.json())
    stmt_for_mo_2_new = select(PRM).where(
        and_(PRM.tprm_id == tprm.id, PRM.mo_id == mo_2_new.id)
    )
    prm_for_mo_2_new: PRM = session.execute(stmt_for_mo_2_new).scalar()
    assert prm_for_mo_2_new.value == "2"
    prm_for_mo_2: PRM = session.execute(stmt_prm_for_mo_2).scalar()
    assert prm_for_mo_2.value == "3"
    prm_for_mo_3: PRM = session.execute(stmt_prm_for_mo_3).scalar()
    assert prm_for_mo_3.value == "4"


def test_sequence_insert_with_constraint(session: Session, client: TestClient):
    """Create two sequence TPRM and insert mo inside sequence"""
    # Arrange
    tmo_id = 1
    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"

    tprm_with_data_name = "tprm_data"
    tprm_with_data_val_type = "int"
    tprm_with_data_value_100 = 100
    tprm_with_data_value_1000 = 1000
    tprm_data = {
        "name": tprm_with_data_name,
        "val_type": tprm_with_data_val_type,
        "returnable": True,
        "required": True,
        "tmo_id": tmo_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_for_data = TPRM(**tprm_data)
    session.add(tprm_for_data)
    session.commit()
    session.refresh(tprm_for_data)
    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "constraint": str(tprm_for_data.id),
        "returnable": True,
        "tmo_id": tmo_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_sequence = TPRM(**tprm_sequence)
    session.add(tprm_for_sequence)
    session.commit()
    session.refresh(tprm_for_sequence)

    # Act
    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_1 = MO(**res.json())

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_2 = MO(**res.json())

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1000_1 = MO(**res.json())

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1000_2 = MO(**res.json())

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_3 = MO(**res.json())

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1000_3 = MO(**res.json())

    # Insert new element into existed sequence
    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id},
            {"value": 2, "tprm_id": tprm_for_sequence.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_2_new = MO(**res.json())
    stmt_for_mo_100_1 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_1.id
    )
    prm_for_mo_100_1: PRM = session.execute(stmt_for_mo_100_1).scalar()
    assert prm_for_mo_100_1.value == "1"
    stmt_for_mo_100_2_new = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_2_new.id
    )
    prm_for_mo_100_2_new: PRM = session.execute(stmt_for_mo_100_2_new).scalar()
    assert prm_for_mo_100_2_new.value == "2"
    stmt_for_mo_100_2 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_2.id
    )
    prm_for_mo_100_2: PRM = session.execute(stmt_for_mo_100_2).scalar()
    assert prm_for_mo_100_2.value == "3"
    stmt_for_mo_100_3 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_3.id
    )
    prm_for_mo_100_3: PRM = session.execute(stmt_for_mo_100_3).scalar()
    assert prm_for_mo_100_3.value == "4"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_1000, "tprm_id": tprm_for_data.id},
            {"value": 2, "tprm_id": tprm_for_sequence.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1000_2_new = MO(**res.json())
    stmt_for_mo_1000_1 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_1000_1.id
    )
    prm_for_mo_1000_1: PRM = session.execute(stmt_for_mo_1000_1).scalar()
    assert prm_for_mo_1000_1.value == "1"
    stmt_for_mo_1000_2_new = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_1000_2_new.id
    )
    prm_for_mo_1000_2_new: PRM = session.execute(
        stmt_for_mo_1000_2_new
    ).scalar()
    assert prm_for_mo_1000_2_new.value == "2"
    stmt_for_mo_1000_2 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_1000_2.id
    )
    prm_for_mo_1000_2: PRM = session.execute(stmt_for_mo_1000_2).scalar()
    assert prm_for_mo_1000_2.value == "3"
    stmt_for_mo_1000_3 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_1000_3.id
    )
    prm_for_mo_1000_3: PRM = session.execute(stmt_for_mo_1000_3).scalar()
    assert prm_for_mo_1000_3.value == "4"


def test_sequence_insert_with_constraint_couple(
    session: Session, client: TestClient
):
    """Create couple MO's with sequence don't invoke error
    [AD-3370]
    """
    # Arrange
    tmo_id = 1
    tprm_seq_name = "tprm_seq"
    tprm_seq_val_type = "sequence"

    tprm_with_data_name = "tprm_data"
    tprm_with_data_val_type = "int"
    tprm_with_data_value_100 = 100
    tprm_data = {
        "name": tprm_with_data_name,
        "val_type": tprm_with_data_val_type,
        "returnable": True,
        "required": True,
        "tmo_id": tmo_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_for_data = TPRM(**tprm_data)
    session.add(tprm_for_data)
    session.commit()
    session.refresh(tprm_for_data)
    tprm_sequence = {
        "name": tprm_seq_name,
        "val_type": tprm_seq_val_type,
        "constraint": str(tprm_for_data.id),
        "returnable": True,
        "tmo_id": tmo_id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_sequence = TPRM(**tprm_sequence)
    session.add(tprm_for_sequence)
    session.commit()
    session.refresh(tprm_for_sequence)

    # Act
    # Create MO
    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id}
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_1 = MO(**res.json())
    stmt_for_mo_100_1 = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_1.id
    )
    prm_for_mo_100_1: PRM = session.execute(stmt_for_mo_100_1).scalar()
    assert prm_for_mo_100_1.value == "1"

    data = {
        "tmo_id": tmo_id,
        "params": [
            {"value": tprm_with_data_value_100, "tprm_id": tprm_for_data.id},
            {"value": 1, "tprm_id": tprm_for_sequence.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_100_1_new = MO(**res.json())

    # Assert
    prm_for_mo_100_1: PRM = session.execute(stmt_for_mo_100_1).scalar()
    assert prm_for_mo_100_1.value == "2"

    stmt_for_mo_100_1_new = select(PRM).where(
        PRM.tprm_id == tprm_for_sequence.id, PRM.mo_id == mo_100_1_new.id
    )
    prm_for_mo_100_1_new: PRM = session.execute(stmt_for_mo_100_1_new).scalar()
    assert prm_for_mo_100_1_new.value == "1"
