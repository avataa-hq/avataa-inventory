"""Tests for object (MO) router"""

import datetime
import pickle
from pprint import pprint

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, and_
from sqlmodel import Session

from models import TMO, TPRM, MO, PRM

URL = "/api/inventory/v1/object/"

TMO_DEFAULT_DATA_1 = {
    "name": "tmo_1",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

TPRM_DEFAULT_DATA_1 = {
    "name": "tprm_1",
    "tmo_id": 1,
    "val_type": "int",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

TPRM_DEFAULT_DATA_2 = {
    "name": "tprm_2",
    "tmo_id": 1,
    "val_type": "bool",
    "created_by": "Test creator",
    "modified_by": "Test modifier",
}

MO_DEFAULT_DATA = {
    "tmo_id": 1,
    "name": "1",
    "params": [
        {
            "value": 42,
            "tprm_id": 1,
        },
        {"value": True, "tprm_id": 2},
    ],
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

    tmo = TMO(**TMO_DEFAULT_DATA_1)
    session.add(tmo)
    tprm_1 = TPRM(**TPRM_DEFAULT_DATA_1)
    session.add(tprm_1)
    tprm_2 = TPRM(**TPRM_DEFAULT_DATA_2)
    session.add(tprm_2)
    mo = MO(**MO_DEFAULT_DATA)
    session.add(mo)
    session.commit()
    yield session


def test_create_object_with_parameter_with_val_type_formula(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:
            if parameter['tprm'] == 1 or parameter['tprm'] == 1 then parameter['tprm'] + 1; else parameter['tprm'] + 1

    So in this formula: check for or/and/then/else conditions
    """
    number_for_fromula = 24423
    tprm_value = 3
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_fromula} or "
        f"parameter['tprm_1'] == {number_for_fromula} then "
        f"parameter['tprm_1'] + {number_for_fromula} + 1; "
        f"else parameter['tprm_1'] + {number_for_fromula}",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": tprm_value, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {
            "value": 1,  # some random value, we will get value by formula by prms in this formula
            "tprm_id": 3,
        }  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert tprm_value + number_for_fromula == float(
        res.json()["data"][0]["value"]
    )


def test_create_object_with_parameter_with_val_type_formula_1(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view: parameter['tprm_name'] + 24423;(some random value)

    So in this formula just adding values
    """
    number_for_fromula = 24423
    tprm_value = 3
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"parameter['tprm_1'] + {number_for_fromula}",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": tprm_value, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {
            "value": 1,  # some random value, we will get value by formula by prms in this formula
            "tprm_id": 3,
        }  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert tprm_value + number_for_fromula == float(
        res.json()["data"][0]["value"]
    )


def test_create_parameter_with_enum(session: Session, client: TestClient):
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

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    data = [{"value": 1, "tprm_id": tprm.id}]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert res.json() == {
        "data": [
            {
                "value": 1,
                "id": 1,
                "version": 1,
                "tprm_id": 3,
                "mo_id": 1,
                "backward_link": None,
            }
        ],
        "errors": [],
    }


def test_create_parameter_with_enum_not_valid_valid_by_constraint(
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

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()

    data = [{"value": 4, "tprm_id": tprm.id}]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 400
    assert res.json() == {
        "detail": [
            "Parameter 4 is not valid for TPRM exists_enum_tprm constraint"
        ]
    }


def test_create_parameter_with_enum_not_valid_valid_by_constraint_1(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    session.add(PRM(value=1, mo_id=1, tprm_id=tprm.id))
    session.commit()

    data = [
        {"value": 1, "tprm_id": tprm.id},
        {"value": 4, "tprm_id": tprm_1.id},
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [
            {
                "value": 4,
                "id": 2,
                "version": 1,
                "tprm_id": 4,
                "mo_id": 1,
                "backward_link": None,
            }
        ],
        "errors": [
            "Parameter for exists_enum_tprm and object with id 1 already exists"
        ],
    }


def test_create_parameter_with_enum_not_valid_valid_by_constraint_2(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()

    data = [
        {"value": 1, "tprm_id": tprm.id},
        {"value": 2, "tprm_id": tprm_1.id},
        {"value": 6, "tprm_id": tprm_2.id},
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [
            {
                "value": 1,
                "id": 1,
                "version": 1,
                "tprm_id": 3,
                "mo_id": 1,
                "backward_link": None,
            },
            {
                "value": 6,
                "id": 2,
                "version": 1,
                "tprm_id": 5,
                "mo_id": 1,
                "backward_link": None,
            },
        ],
        "errors": [
            "Parameter 2 is not valid for TPRM exists_enum_tprm_1 constraint"
        ],
    }
    assert not session.execute(
        select(PRM).where(PRM.value == "2", PRM.tprm_id == tprm_1.id)
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.value == "1", PRM.tprm_id == tprm.id)
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.value == "6", PRM.tprm_id == tprm_2.id)
    ).scalar()


def test_update_parameter_with_enum(session: Session, client: TestClient):
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

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    new_prm = PRM(version=1, value="1", mo_id=1, tprm_id=tprm.id)
    session.add(new_prm)
    session.commit()

    data = [{"version": 1, "value": 2, "tprm_id": tprm.id}]

    res = client.patch("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [
            {
                "backward_link": None,
                "id": 1,
                "mo_id": 1,
                "tprm_id": 3,
                "value": "2",
                "version": 2,
            }
        ],
        "errors": [],
    }


def test_update_parameter_with_enum_not_valid_valid_by_constraint(
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

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    new_prm = PRM(version=1, value="1", mo_id=1, tprm_id=tprm.id)
    session.add(new_prm)
    session.commit()

    data = [{"version": 1, "value": 4, "tprm_id": tprm.id}]

    res = client.patch("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [],
        "errors": [
            "Parameter 4 is not valid for TPRM exists_enum_tprm constraint"
        ],
    }


def test_update_parameter_with_enum_not_valid_valid_by_constraint_1(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    session.add(PRM(value=1, mo_id=1, tprm_id=tprm.id, version=1))
    session.commit()

    data = [
        {"version": 1, "value": 6, "tprm_id": tprm.id},
        {"version": 1, "value": 4, "tprm_id": tprm_1.id},
    ]

    res = client.patch("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [],
        "errors": [
            "Parameter 6 is not valid for TPRM exists_enum_tprm constraint",
            "Parameter for exists_enum_tprm_1 and object id 1 is not exists",
        ],
    }


def test_update_parameter_with_enum_not_exists_parameters(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()

    data = [
        {"version": 1, "value": 1, "tprm_id": tprm.id},
        {"version": 1, "value": 2, "tprm_id": tprm_1.id},
        {"version": 1, "value": 6, "tprm_id": tprm_2.id},
    ]

    res = client.patch("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [],
        "errors": [
            "Parameter 2 is not valid for TPRM exists_enum_tprm_1 constraint",
            "Parameter for exists_enum_tprm and object id 1 is not exists",
            "Parameter for exists_enum_tprm_2 and object id 1 is not exists",
        ],
    }


def test_update_parameter_with_enum_not_valid_constraint_and_updated_params(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()

    prm_1 = PRM(value="2", version=1, tprm_id=tprm_1.id, mo_id=1)
    prm_2 = PRM(value="7", version=1, tprm_id=tprm_2.id, mo_id=1)
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    data = [
        {"version": 1, "value": 1, "tprm_id": tprm.id},
        {"version": 1, "value": 5, "tprm_id": tprm_1.id},
        {"version": 1, "value": 6, "tprm_id": tprm_2.id},
    ]

    res = client.patch("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "data": [
            {
                "backward_link": None,
                "id": 1,
                "mo_id": 1,
                "tprm_id": 4,
                "value": "5",
                "version": 2,
            },
            {
                "backward_link": None,
                "id": 2,
                "mo_id": 1,
                "tprm_id": 5,
                "value": "6",
                "version": 2,
            },
        ],
        "errors": [
            "Parameter for exists_enum_tprm and object id 1 is not exists"
        ],
    }
    assert session.execute(
        select(PRM).where(PRM.value == "5", PRM.tprm_id == tprm_1.id)
    ).scalar()
    assert not session.execute(
        select(PRM).where(PRM.value == "1", PRM.tprm_id == tprm.id)
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.value == "6", PRM.tprm_id == tprm_2.id)
    ).scalar()


def test_multiple_parameter_update_with_enum(
    session: Session, client: TestClient
):
    """
    This test check endpoint of multiple parameter updating
    And have in result 200 status
    """
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()
    session.add(PRM(mo_id=1, tprm_id=tprm.id, value="1"))
    session.add(PRM(mo_id=1, tprm_id=tprm_1.id, value="4"))
    session.add(PRM(mo_id=1, tprm_id=tprm_2.id, value="6"))
    session.commit()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": tprm.id, "new_value": 3},
                {"tprm_id": tprm_1.id, "new_value": 5},
                {"tprm_id": tprm_2.id, "new_value": 7},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "updated_params": [
            {"id": 1, "mo_id": 1, "tprm_id": 3, "value": "3", "version": 2},
            {"id": 2, "mo_id": 1, "tprm_id": 4, "value": "5", "version": 2},
            {"id": 3, "mo_id": 1, "tprm_id": 5, "value": "7", "version": 2},
        ]
    }
    assert session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "3", PRM.version == 2)
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.id == 2, PRM.value == "5", PRM.version == 2)
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.id == 3, PRM.value == "7", PRM.version == 2)
    ).scalar()


def test_multiple_parameter_update_with_enum_not_valid_constraint(
    session: Session, client: TestClient
):
    """
    This test check endpoint of multiple parameter updating
    And have in result 200 status
    """
    new_tprm = {
        "name": "exists_enum_tprm",
        "tmo_id": 1,
        "constraint": "[1,2,3]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm))
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()
    session.add(PRM(mo_id=1, tprm_id=tprm.id, value="1"))
    session.add(PRM(mo_id=1, tprm_id=tprm_1.id, value="4"))
    session.add(PRM(mo_id=1, tprm_id=tprm_2.id, value="6"))
    session.commit()
    pprint(session.execute(select(PRM)).scalars().all())

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": tprm.id, "new_value": 3},
                {"tprm_id": tprm_1.id, "new_value": 5},
                {"tprm_id": tprm_2.id, "new_value": 9},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Parameter 9 for parameter type with id 5 not valid"
    }


def test_multiple_parameter_create_with_enum(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()
    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": tprm.id, "new_value": 3},
                {"tprm_id": tprm_1.id, "new_value": 5},
                {"tprm_id": tprm_2.id, "new_value": 6},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "created_params": [
            {"id": 1, "version": 1, "tprm_id": 3, "mo_id": 1, "value": 3},
            {"id": 2, "version": 1, "tprm_id": 4, "mo_id": 1, "value": 5},
            {"id": 3, "version": 1, "tprm_id": 5, "mo_id": 1, "value": 6},
        ]
    }
    pprint(session.execute(select(PRM)).scalars().all())

    assert session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "3")
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.id == 2, PRM.value == "5")
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.id == 3, PRM.value == "6")
    ).scalar()


def test_multiple_parameter_create_with_enum_not_valid_constraint(
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
    new_tprm_1 = {
        "name": "exists_enum_tprm_1",
        "tmo_id": 1,
        "constraint": "[4,5]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_1))
    new_tprm_2 = {
        "name": "exists_enum_tprm_2",
        "tmo_id": 1,
        "constraint": "[6,7]",
        "val_type": "enum",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**new_tprm_2))
    session.commit()
    assert not session.execute(select(PRM)).scalars().all()

    tprm = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm")
    ).scalar()
    tprm_1 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_1")
    ).scalar()
    tprm_2 = session.execute(
        select(TPRM).where(TPRM.name == "exists_enum_tprm_2")
    ).scalar()
    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": tprm.id, "new_value": 3},
                {"tprm_id": tprm_1.id, "new_value": 5},
                {"tprm_id": tprm_2.id, "new_value": 9},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Parameter 9 is not valid for TPRM exists_enum_tprm_2 constraint"
    }


def test_create_object_with_parameter_with_val_type_formula_1_client_create(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view: parameter['tprm_name'] + 24423;(some random value)

    So in this formula just adding values
    """
    number_for_fromula = 24423
    tprm_value = 3
    data = {
        "name": "tprm_with_val_type_formula",
        "val_type": "formula",
        "returnable": "true",
        "constraint": f"parameter['tprm_1'] + {number_for_fromula}",
        "tmo_id": 1,
    }
    res = client.post("api/inventory/v1/param_type/", json=data)
    assert res.status_code == 200
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": tprm_value, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(prm)
    session.commit()

    data = [
        {
            "value": 1,  # some random value, we will get value by formula by prms in this formula
            "tprm_id": 3,
        }  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert tprm_value + number_for_fromula == float(
        res.json()["data"][0]["value"]
    )


def test_create_object_with_parameter_with_val_type_formula_2(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view: True, just bool value

    So this formula just return bool value, that we added to constraint
    """

    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": True,
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": 1, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {
            "value": 1,  # some random value, we will get value by formula by prms in this formula
            "tprm_id": 3,
        }  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200


def test_create_object_with_parameter_with_val_type_formula_3(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view: "X",

    So in this formula just get value from endpoint as "X"
    """
    X_value = 1
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"x + {number_for_formula}",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": 1, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": X_value, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert X_value + number_for_formula == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_4(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

                            if parameter['tprm_name'] == 15 then parameter['tprm_name'] + 1;

    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula} "
        f"then parameter['tprm_1'] + 1;",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": 1, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": 1, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert number_for_formula + 1 == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_5(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

                            if parameter['tprm_name'] == 15 then parameter['tprm_name'] + 1;

    So in this formula just get value from endpoint as "X"
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula + 1} "
        f"then parameter['tprm_1'] + 1;",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": 1, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": 1, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 400
    assert ["Alternatively, you can add an else expression."] == res.json()[
        "detail"
    ][0]["error"]


def test_create_object_with_parameter_with_val_type_formula_mixed_with_x(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

                            if parameter['tprm_name'] == 15 then parameter['tprm_name'] + X;

    So in this formula just get value from endpoint as "X"
    """
    number_for_formula = 1
    x_value = 100
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula} "
        f"then parameter['tprm_1'] + x;",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {"mo_id": 1, "tprm_id": 1, "value": 1, "version": 1}
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": x_value, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    assert number_for_formula + x_value == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_mixed_with_x_1(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

                            if parameter['tprm_name'] == X then parameter['tprm_name'] + 1;

    So in this formula just get value from endpoint as "X"
    """
    number_for_formula = 100
    x_value = 100
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula} "
        f"then parameter['tprm_1'] + x;",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": x_value, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert number_for_formula + x_value == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_6(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

        if parameter['tprm_1'] > 1 then parameter['tprm_1'] + 1; elif parameter['tprm_1'] == 2
        then parameter['tprm_1'] + 1; else parameter['tprm_1'] + 22

    So in this formula check how work with 'elif' in formula and with 'then'
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula} then parameter['tprm_1'] + 1;"
        f" elif parameter['tprm_1'] == {number_for_formula + 1} then parameter['tprm_1'] + 9;"
        f" else parameter['tprm_1'] + 22",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": 1, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert number_for_formula + 1 == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_6_1(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

        if parameter['tprm_1'] > 1 then parameter['tprm_1'] + 1; elif parameter['tprm_1'] == 2
        then parameter['tprm_1'] + 1; else parameter['tprm_1'] + 22

    So in this formula check how work with 'elif' in formula and with 'then'
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula - 1} then parameter['tprm_1'] + 1;"
        f" elif parameter['tprm_1'] == {number_for_formula} then parameter['tprm_1'] + 9;"
        f" else parameter['tprm_1'] + 22",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": 1, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert number_for_formula + 9 == float(res.json()["data"][0]["value"])


def test_create_object_with_parameter_with_val_type_formula_6_2(
    session: Session, client: TestClient
):
    """
    Test how formula works with tprms inside this formula in view:

        if parameter['tprm_1'] > 1 then parameter['tprm_1'] + 1; elif parameter['tprm_1'] == 2
        then parameter['tprm_1'] + 1; else parameter['tprm_1'] + 22

    So in this formula check how work with 'elif' in formula and with 'then'
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula - 1} then parameter['tprm_1'] + 1;"
        f" elif parameter['tprm_1'] == {number_for_formula - 1} then parameter['tprm_1'] + 9;"
        f" else parameter['tprm_1'] + 22",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [
        {"value": 1, "tprm_id": 3}  # TPRM with val_type == formula
    ]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    assert float(res.json()["data"][0]["value"]) == number_for_formula + 22


def test_create_object_with_parameter_with_val_type_not_valid_formula(
    session: Session, client: TestClient
):
    """
    Test how formula works with not valid formula. In this case it need to have ";" as delimeter
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['tprm_1'] == {number_for_formula - 1} then parameter['tprm_1'] + 1 "
        f" elif parameter['tprm_1'] == {number_for_formula - 1} then parameter['tprm_1'] + 9 "
        f" else parameter['tprm_1'] + 22",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [{"value": 1, "tprm_id": 3}]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    print(res.json())
    assert res.status_code == 400
    assert res.json()["detail"][0]["error"] == ["Could not parse formula"]


def test_create_object_with_parameter_with_val_type_not_valid_formula_1(
    session: Session, client: TestClient
):
    """
    Test how formula works with not valid formula. In this case there is tprm which doesn't exist
    """
    number_for_formula = 1
    TPRM_FORMULA = {
        "name": "tprm_with_val_type_formula",
        "tmo_id": 1,
        "val_type": "formula",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "constraint": f"if parameter['TPRM'] == {number_for_formula - 1} then parameter['TPRM'] + 1;"
        f" elif parameter['TPRM'] == {number_for_formula - 1} then parameter['TPRM'] + 9;"
        f" else parameter['TPRM'] + 22",
    }

    tprm = TPRM(**TPRM_FORMULA)
    PRM_DATA = {
        "mo_id": 1,
        "tprm_id": 1,
        "value": number_for_formula,
        "version": 1,
    }
    prm = PRM(**PRM_DATA)
    session.add(tprm)
    session.add(prm)
    session.commit()

    data = [{"value": 1, "tprm_id": 3}]

    res = client.post("/api/inventory/v1/object/1/parameters", json=data)
    assert res.status_code == 200
    # MO must be created if formula TPRM is not corrected.
    # assert res.json()['detail'][0]['error'] == ["Incorrect prm value with names=['TPRM'] values={} in mo."]


def test_set_lat_lon_prm_and_update_mo_attrs(
    session: Session, client: TestClient
):
    # Set lat/lon prm in creation
    tmo_id = mo_id = 1
    lat_val = 14
    lon_val = 15
    TPRM_LAT = {
        "name": "Latitude",
        "tmo_id": tmo_id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }
    TPRM_LON = {
        "name": "Longitude",
        "tmo_id": tmo_id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    # Set info for TMO
    data = {
        "id": tmo_id,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
        "version": 1,
    }
    res = client.patch(f"/api/inventory/v1/object_type/{tmo_id}", json=data)
    assert res.status_code == 200
    # Set PRM for MO
    data = [
        {"value": lat_val, "tprm_id": tprm_lat.id},
        {"value": lon_val, "tprm_id": tprm_lon.id},
    ]
    res = client.post(f"/api/inventory/v1/object/{mo_id}/parameters", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_id)
    mo: MO = session.execute(stmt).scalars().first()
    assert mo.latitude == lat_val
    assert mo.longitude == lon_val


def test_update_lat_lon_prm_and_update_not_geometry_tprm(
    session: Session, client: TestClient
):
    # Create point TMO
    # Create TPRM for point TMO
    # Update PRM for non lat/long not change geometry
    lat_val = 14
    lon_val = 15
    TMO_POINT = {
        "name": "TMO_POINT",
        "geometry_type": "point",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tmo_point = TMO(**TMO_POINT)
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)
    TMO_LINE = {
        "name": "TMO_LINE",
        "geometry_type": "line",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_LAT = {
        "name": "Latitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_LON = {
        "name": "Longitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_STR = {
        "name": "TEST",
        "tmo_id": tmo_point.id,
        "val_type": "str",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    tmo_line = TMO(**TMO_LINE)
    session.add(tmo_line)
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    tprm_str = TPRM(**TPRM_STR)
    session.add(tprm_str)

    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line)

    # Set info for TMO
    data = {
        "id": tmo_point.id,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
        "version": 1,
    }
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    assert res.status_code == 200
    # Create MO for point
    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_point: MO = MO(**res.json())
    # Set PRM for MO
    data = [
        {"value": lat_val, "tprm_id": tprm_lat.id},
        {"value": lon_val, "tprm_id": tprm_lon.id},
        {"value": "test", "tprm_id": tprm_str.id},
    ]
    res = client.post(
        f"/api/inventory/v1/object/{mo_point.id}/parameters", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_point.id)
    mo: MO = session.execute(stmt).scalars().first()
    assert mo.latitude == lat_val
    assert mo.longitude == lon_val
    data = {"tmo_id": tmo_line.id, "point_a_id": mo_point.id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200

    # Update PRM for point_a
    data = [{"value": "test_string", "version": 1, "tprm_id": tprm_str.id}]
    res = client.patch(
        f"/api/inventory/v1/object/{mo_point.id}/parameters/", json=data
    )
    assert res.status_code == 200
    mo: MO = session.execute(stmt).scalars().first()
    assert mo.geometry is None


def test_update_lat_lon_prm_and_incorrect_update_geometry(
    session: Session, client: TestClient
):
    # Create point TMO
    # Create TPRM for point TMO
    # Updae PRM for lat/long not change geometry for line if missing point
    lat_val = 14
    lon_val = 15
    TMO_POINT = {
        "name": "TMO_POINT",
        "geometry_type": "point",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tmo_point = TMO(**TMO_POINT)
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)
    TMO_LINE = {
        "name": "TMO_LINE",
        "geometry_type": "line",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_LAT = {
        "name": "Latitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_LON = {
        "name": "Longitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_STR = {
        "name": "TEST",
        "tmo_id": tmo_point.id,
        "val_type": "str",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    tmo_line = TMO(**TMO_LINE)
    session.add(tmo_line)
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    tprm_str = TPRM(**TPRM_STR)
    session.add(tprm_str)

    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line)

    # Set info for TMO
    data = {
        "id": tmo_point.id,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
        "version": 1,
    }
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    assert res.status_code == 200
    # Create MO for point
    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_point: MO = MO(**res.json())
    # Set PRM for MO
    data = [
        {"value": lat_val, "tprm_id": tprm_lat.id},
        {"value": lon_val, "tprm_id": tprm_lon.id},
        {"value": "test", "tprm_id": tprm_str.id},
    ]
    res = client.post(
        f"/api/inventory/v1/object/{mo_point.id}/parameters", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_point.id)
    mo: MO = session.execute(stmt).scalars().first()
    assert mo.latitude == lat_val
    assert mo.longitude == lon_val
    data = {"tmo_id": tmo_line.id, "point_a_id": mo_point.id, "params": []}
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200

    # Update PRM for point_a
    data = [{"value": str(lat_val + 1), "tprm_id": tprm_lat.id, "version": 1}]
    res = client.patch(
        f"/api/inventory/v1/object/{mo_point.id}/parameters/", json=data
    )
    assert res.status_code == 200
    mo: MO = session.execute(stmt).scalars().first()
    assert mo.geometry is None


def test_update_lat_lon_prm_and_correct_update_geometry(
    session: Session, client: TestClient
):
    # Create point TMO
    # Create TPRM for point TMO
    # Correct update geometry if PRM changes
    lat_val = 14
    lon_val = 15
    geometry = {
        "path": {
            "type": "LineString",
            "coordinates": [
                [float(lon_val), float(lat_val + 1)],
                [float(lon_val + 10), float(lat_val + 10)],
            ],
        },
        "path_length": 1446.0736489141018,
    }
    TMO_POINT = {
        "name": "TMO_POINT",
        "geometry_type": "point",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tmo_point = TMO(**TMO_POINT)
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)
    TMO_LINE = {
        "name": "TMO_LINE",
        "geometry_type": "line",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_LAT = {
        "name": "Latitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_LON = {
        "name": "Longitude",
        "tmo_id": tmo_point.id,
        "val_type": "float",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    TPRM_STR = {
        "name": "TEST",
        "tmo_id": tmo_point.id,
        "val_type": "str",
        "created_by": "Test creator",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }

    tmo_line = TMO(**TMO_LINE)
    session.add(tmo_line)
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    tprm_str = TPRM(**TPRM_STR)
    session.add(tprm_str)

    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line)

    # Set info for TMO
    data = {
        "id": tmo_point.id,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
        "version": 1,
    }
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    assert res.status_code == 200
    # Create MO for point
    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": lat_val, "tprm_id": tprm_lat.id},
            {"value": lon_val, "tprm_id": tprm_lon.id},
            {"value": "test", "tprm_id": tprm_str.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_point_a: MO = MO(**res.json())
    assert mo_point_a.latitude == lat_val
    assert mo_point_a.longitude == lon_val

    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": lat_val + 10, "tprm_id": tprm_lat.id},
            {"value": lon_val + 10, "tprm_id": tprm_lon.id},
            {"value": "test", "tprm_id": tprm_str.id},
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_point_b: MO = MO(**res.json())
    assert mo_point_b.latitude == lat_val + 10
    assert mo_point_b.longitude == lon_val + 10

    data = {
        "tmo_id": tmo_line.id,
        "point_a_id": mo_point_a.id,
        "point_b_id": mo_point_b.id,
        "params": [],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_line: MO = MO(**res.json())

    # Update PRM for point_a
    data = [{"value": str(lat_val + 1), "tprm_id": tprm_lat.id, "version": 1}]
    res = client.patch(
        f"/api/inventory/v1/object/{mo_point_a.id}/parameters/", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_line.id)
    mo: MO = session.execute(stmt).scalars().first()

    assert mo.geometry == geometry


def test_param_value_with_formula_mo_link(session: Session, client: TestClient):
    # Create TMO for mo_link objects. Add primary TPRM for TMO_MO_LINK.
    # Create MO for mo_link.
    # Create TMO_FORMULA. Add TPRM for MO_LINK and add formula TPRM with parameter['TPRM_MO_LINK']
    # Create MO with prm based on TPRM for MO_LINK
    # Formula must correct work.
    mo_link_name_prm = "irrumator"
    then_value = "Correct"
    else_value = "Incorrect"
    tprm_name_for_mo_link = "TPRM_MO_LINK"

    TMO_MO_LINK = {
        "name": "TMO_MO_LINK",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_mo_link: TMO = TMO(**TMO_MO_LINK)
    session.add(tmo_mo_link)
    session.commit()
    session.refresh(tmo_mo_link)
    TPRM_PRIMARY = {
        "name": "TPRM_PRIMARY",
        "tmo_id": tmo_mo_link.id,
        "val_type": "str",
        "created_by": "Test creator",
        "required": True,
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "modified_by": "Test modifier",
    }
    TMO_FORMULA = {
        "name": "TMO_FORMULA",
        "creation_date": datetime.datetime.now(),
        "modification_date": datetime.datetime.now(),
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_primary: TPRM = TPRM(**TPRM_PRIMARY)
    session.add(tprm_primary)
    tmo_formula: TMO = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_mo_link)
    session.refresh(tmo_formula)
    session.refresh(tprm_primary)
    # Update primary
    data = {"version": 1, "primary": [tprm_primary.id]}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_mo_link.id}", json=data
    )
    assert res.status_code == 200

    # Create mo for mo_link
    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [{"value": mo_link_name_prm, "tprm_id": tprm_primary.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    test_mo_link_correct: MO = MO(**res.json())

    data = {
        "tmo_id": tmo_mo_link.id,
        "params": [
            {"value": mo_link_name_prm + "_", "tprm_id": tprm_primary.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    test_mo_link_incorrect: MO = MO(**res.json())

    # Create tprm for mo_link
    data = {
        "name": tprm_name_for_mo_link,
        "val_type": "mo_link",
        "returnable": True,
        "constraint": str(tmo_mo_link.id),
        "tmo_id": tmo_formula.id,
        "field_value": "test",
    }
    res = client.post("api/inventory/v1/param_type/", json=data)
    print(res.json())
    assert res.status_code == 200
    tprm_for_mo_link: TPRM = TPRM(**res.json())

    # Create tprm for formula
    data = {
        "name": "TPRM_FORMULA",
        "val_type": "formula",
        "returnable": True,
        "constraint": f"if parameter['{tprm_name_for_mo_link}'] == "
        f"'{mo_link_name_prm}' then '{then_value}'; else '{else_value}'",
        "tmo_id": tmo_formula.id,
    }
    res = client.post("api/inventory/v1/param_type/", json=data)
    assert res.status_code == 200
    tprm_for_formula: TPRM = TPRM(**res.json())

    # Create mo for formula
    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": test_mo_link_correct.id, "tprm_id": tprm_for_mo_link.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    correct_formula_mo: MO = MO(**res.json())
    stmt = select(PRM.value).where(
        PRM.mo_id == correct_formula_mo.id, PRM.tprm_id == tprm_for_formula.id
    )
    formula_value = session.execute(stmt).scalar_one_or_none()
    assert formula_value == then_value

    data = {
        "tmo_id": tmo_formula.id,
        "params": [
            {"value": test_mo_link_incorrect.id, "tprm_id": tprm_for_mo_link.id}
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    incorrect_formula_mo: MO = MO(**res.json())
    stmt = select(PRM.value).where(
        PRM.mo_id == incorrect_formula_mo.id, PRM.tprm_id == tprm_for_formula.id
    )
    formula_value = session.execute(stmt).scalar_one_or_none()
    assert formula_value == else_value


def test_change_prm_value_with_mandatory_recalc_formula(
    session: Session, client: TestClient
):
    """Mandatory update prm if formula isn't complete."""
    tmo_formula_name = "TMO_FORMULA"

    tprm_for_formula_name = "TPRM FORMULA"
    tprm_for_formula_type = "formula"

    tprm_for_str_name = "TPRM STR"
    tprm_for_str_type = "str"
    tprm_for_int_name = "TPRM INT"
    tprm_for_int_type = "int"

    prm_value_str_1 = "00 Test"

    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)

    TPRM_FOR_STR = {
        "name": tprm_for_str_name,
        "val_type": tprm_for_str_type,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_for_str = TPRM(**TPRM_FOR_STR)
    session.add(tprm_for_str)

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
    session.refresh(tprm_for_str)
    session.refresh(tprm_for_int)
    session.refresh(tmo_formula)

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": prm_value_str_1, "tprm_id": tprm_for_str.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())

    # Add formula TPRM
    formula_constraint = (
        f"if parameter['{tprm_for_int_name}'] == 1 then parameter['{tprm_for_str_name}'].split()[0]; "
        f"else parameter['{tprm_for_str_name}']"
    )
    data = {
        "name": tprm_for_formula_name,
        "val_type": tprm_for_formula_type,
        "constraint": formula_constraint,
        "returnable": True,
        "tmo_id": tmo_formula.id,
    }
    res = client.post("/api/inventory/v1/param_type", json=data)
    tprm_formula: TPRM = TPRM(**res.json())

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": prm_value_str_1, "tprm_id": tprm_for_str.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula: PRM = session.execute(stmt).scalar_one_or_none()
    assert prm_formula.value == prm_value_str_1


def test_multiple_parameter_update(session: Session, client: TestClient):
    """
    This test check endpoint of multiple parameter updating
    And have in result 200 status
    """
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {"tprm_id": 2, "new_value": False},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "updated_params": [
            {"tprm_id": 1, "id": 1, "mo_id": 1, "value": "100", "version": 2},
            {"tprm_id": 2, "id": 2, "mo_id": 1, "value": "False", "version": 2},
        ]
    }
    assert session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "100")
    ).scalar()
    assert session.execute(
        select(PRM).where(PRM.id == 2, PRM.value == "False")
    ).scalar()


def test_multiple_parameter_update_with_val_type_error(
    session: Session, client: TestClient
):
    """
    This test check endpoint of multiple parameter updating

    It needs to response with error, because we try to update value by wrong val type
    """
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 1, "new_value": "string value for int"},
                {"tprm_id": 2, "new_value": False},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    assert res.status_code == 422


def test_multiple_parameter_update_primary_check(
    session: Session, client: TestClient
):
    """
    This test check endpoint of multiple parameter updating

    If we update parameters, which take part in primary (object name generation) we need to check
    if object name will be updated
    """
    TMO_DEFAULT_DATA = {
        "name": "tmo_2",
        "primary": [3, 4],
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 2,
        "val_type": "int",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 2,
        "val_type": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA = {
        "tmo_id": 2,
        "name": "1234-some_value-parent_name",
        "params": [
            {
                "value": 42,
                "tprm_id": 3,
            },
            {"value": True, "tprm_id": 4},
        ],
    }

    session.add(TMO(**TMO_DEFAULT_DATA))
    session.add(TPRM(**TPRM_DEFAULT_DATA_3))
    session.add(TPRM(**TPRM_DEFAULT_DATA_4))
    session.add(MO(**MO_DEFAULT_DATA))
    session.add(PRM(mo_id=2, tprm_id=3, value="1234"))
    session.add(PRM(mo_id=2, tprm_id=4, value="some_value"))
    session.commit()

    data = [
        {
            "object_id": 2,
            "new_values": [
                {"tprm_id": 3, "new_value": 111},
                {"tprm_id": 4, "new_value": "new_value"},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    assert res.status_code == 200
    assert session.execute(
        select(MO).where(MO.id == 2, MO.name == "111-new_value")
    ).scalar()


def test_multiple_parameter_update_multiple_values(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA = {
        "name": "tmo_2",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 2,
        "val_type": "int",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 2,
        "val_type": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA = {
        "tmo_id": 2,
        "params": [
            {
                "value": 42,
                "tprm_id": 3,
            },
            {"value": True, "tprm_id": 4},
        ],
    }

    session.add(TMO(**TMO_DEFAULT_DATA))
    session.add(TPRM(**TPRM_DEFAULT_DATA_3))
    session.add(TPRM(**TPRM_DEFAULT_DATA_4))
    session.add(MO(**MO_DEFAULT_DATA))
    session.add(PRM(mo_id=2, tprm_id=3, value=pickle.dumps([1234]).hex()))
    session.add(PRM(mo_id=2, tprm_id=4, value="some_value"))
    session.commit()

    data = [
        {
            "object_id": 2,
            "new_values": [
                {"tprm_id": 3, "new_value": [111, 222, 333]},
                {"tprm_id": 4, "new_value": "new_value"},
            ],
        }
    ]

    res = client.patch("/api/inventory/v1/multiple_parameter_update", json=data)
    print(res.json())
    assert res.status_code == 200
    assert res.json() == {
        "updated_params": [
            {
                "tprm_id": 3,
                "id": 1,
                "mo_id": 2,
                "value": "8004950c000000000000005d94284b6f4bde4d4d01652e",
                "version": 2,
            },
            {
                "tprm_id": 4,
                "id": 2,
                "mo_id": 2,
                "value": "new_value",
                "version": 2,
            },
        ]
    }


def test_multiple_parameter_create(session: Session, client: TestClient):
    assert not session.execute(select(PRM).where(PRM.mo_id == 1)).scalar()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {"tprm_id": 2, "new_value": False},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 200
    assert res.json() == {
        "created_params": [
            {"tprm_id": 1, "id": 1, "mo_id": 1, "value": 100, "version": 1},
            {"tprm_id": 2, "id": 2, "mo_id": 1, "value": False, "version": 1},
        ]
    }
    first_parameter = session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "100")
    ).scalar()
    second_parameter = session.execute(
        select(PRM).where(PRM.id == 2, PRM.value == "false")
    ).scalar()

    assert first_parameter == PRM(
        version=1, id=1, mo_id=1, value="100", tprm_id=1, backward_link=None
    )
    assert second_parameter == PRM(
        id=2, version=1, mo_id=1, value="false", tprm_id=2, backward_link=None
    )


def test_multiple_parameter_create_already_created_parameter(
    session: Session, client: TestClient
):
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {"tprm_id": 2, "new_value": False},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422

    assert res.json() == {
        "detail": "There are parameters, which already exist: with mo_id-tprm_id pairs: ['1-1', '1-2']."
    }


def test_multiple_parameter_delete(session: Session, client: TestClient):
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert prm_1
    assert prm_2

    data = [
        {
            "object_id": 1,
            "tprm_id": 1,
        },
        {
            "object_id": 1,
            "tprm_id": 2,
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_delete", json=data)
    print(res.json())
    assert res.status_code == 200

    assert res.json() == {
        "deleted_params": [
            {"id": 1, "version": 1, "tprm_id": 1, "mo_id": 1, "value": "42"},
            {"id": 2, "version": 1, "tprm_id": 2, "mo_id": 1, "value": "True"},
        ]
    }

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert not prm_1
    assert not prm_2


def test_multiple_parameter_delete_0(session: Session, client: TestClient):
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert prm_1
    assert prm_2

    data = [
        {
            "object_id": 1,
            "tprm_id": 1,
        },
        {
            "object_id": 1,
            "tprm_id": 1,
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_delete", json=data)
    assert res.status_code == 200

    assert res.json() == {
        "deleted_params": [
            {"id": 1, "version": 1, "tprm_id": 1, "mo_id": 1, "value": "42"}
        ]
    }

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert not prm_1
    assert prm_2


def test_multiple_parameter_delete_1(session: Session, client: TestClient):
    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))
    session.commit()

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert prm_1
    assert prm_2

    data = []

    res = client.post("/api/inventory/v1/multiple_parameter_delete", json=data)
    assert res.status_code == 200

    assert res.json() == {"deleted_params": []}

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert prm_1
    assert prm_2


def test_multiple_parameter_delete_2(session: Session, client: TestClient):
    MO_2_DEFAULT_DATA = MO(
        **{
            "tmo_id": 1,
            "params": [
                {
                    "value": 421,
                    "tprm_id": 1,
                },
                {"value": True, "tprm_id": 2},
            ],
        }
    )

    MO_3_DEFAULT_DATA = MO(
        **{
            "tmo_id": 1,
            "params": [
                {
                    "value": 422,
                    "tprm_id": 1,
                },
                {"value": True, "tprm_id": 2},
            ],
        }
    )

    session.add(PRM(mo_id=1, tprm_id=1, value="42"))
    session.add(PRM(mo_id=1, tprm_id=2, value="True"))

    session.add(PRM(mo_id=2, tprm_id=1, value="421"))
    session.add(PRM(mo_id=2, tprm_id=2, value="True"))

    session.add(PRM(mo_id=3, tprm_id=1, value="422"))
    session.add(PRM(mo_id=3, tprm_id=2, value="True"))
    session.add(MO_2_DEFAULT_DATA)
    session.add(MO_3_DEFAULT_DATA)
    session.commit()

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    assert prm_1
    assert prm_2

    data = [
        {
            "object_id": 1,
            "tprm_id": 1,
        },
        {
            "object_id": 2,
            "tprm_id": 2,
        },
        {
            "object_id": 1,
            "tprm_id": 2,
        },
        {
            "object_id": 3,
            "tprm_id": 2,
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_delete", json=data)
    assert res.status_code == 200

    assert res.json() == {
        "deleted_params": [
            {"id": 1, "version": 1, "tprm_id": 1, "mo_id": 1, "value": "42"},
            {"id": 2, "version": 1, "tprm_id": 2, "mo_id": 1, "value": "True"},
            {"id": 4, "version": 1, "tprm_id": 2, "mo_id": 2, "value": "True"},
            {"id": 6, "version": 1, "tprm_id": 2, "mo_id": 3, "value": "True"},
        ]
    }

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 2, PRM.tprm_id == 2)
    ).scalar()
    prm_3 = session.execute(
        select(PRM).where(PRM.mo_id == 1, PRM.tprm_id == 2)
    ).scalar()
    prm_4 = session.execute(
        select(PRM).where(PRM.mo_id == 3, PRM.tprm_id == 2)
    ).scalar()
    assert not prm_1
    assert not prm_2
    assert not prm_3
    assert not prm_4

    prm_1 = session.execute(
        select(PRM).where(PRM.mo_id == 3, PRM.tprm_id == 1)
    ).scalar()
    prm_2 = session.execute(
        select(PRM).where(PRM.mo_id == 2, PRM.tprm_id == 1)
    ).scalar()
    assert prm_1
    assert prm_2


def test_multiple_parameter_delete_for_not_exists_params(
    session: Session, client: TestClient
):
    data = [
        {
            "object_id": 1,
            "tprm_id": 1,
        },
        {
            "object_id": 1,
            "tprm_id": 2,
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_delete", json=data)
    print(res.json())
    assert res.status_code == 422

    assert res.json() == {
        "detail": "There are parameters, which are not exists: "
        "[{'object_id': 1, 'tprm_id': 1}, {'object_id': 1, 'tprm_id': 2}]"
    }


def test_multiple_parameter_create_not_exists_tprm(
    session: Session, client: TestClient
):
    assert not session.execute(select(PRM).where(PRM.mo_id == 1)).scalar()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 100, "new_value": 100},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {"detail": "TPRM(s) with id(s) [100] do not exist"}


def test_multiple_parameter_create_not_exists_several_tprms(
    session: Session, client: TestClient
):
    assert not session.execute(select(PRM).where(PRM.mo_id == 1)).scalar()

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 100, "new_value": 100},
                {"tprm_id": 300, "new_value": 100},
            ],
        },
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 200, "new_value": 100},
            ],
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "TPRM(s) with id(s) [200, 100, 300] do not exist"
    }


def test_multiple_parameter_create_not_exists_mo(
    session: Session, client: TestClient
):
    assert not session.execute(select(PRM).where(PRM.mo_id == 1)).scalar()

    data = [
        {
            "object_id": 100,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {"tprm_id": 2, "new_value": False},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {"detail": "MO(s) with id(s) [100] do not exist"}


def test_multiple_parameter_create_not_exist_several_mos(
    session: Session, client: TestClient
):
    assert not session.execute(select(PRM).where(PRM.mo_id == 1)).scalar()

    data = [
        {
            "object_id": 100,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {"tprm_id": 2, "new_value": False},
            ],
        },
        {
            "object_id": -1,
            "new_values": [
                {"tprm_id": 3, "new_value": 100},
                {"tprm_id": 4, "new_value": False},
            ],
        },
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {"detail": "MO(s) with id(s) [100, -1] do not exist"}


def test_multiple_parameter_create_primary_check(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA = {
        "name": "tmo_2",
        "primary": [3, 4],
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 2,
        "val_type": "int",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 2,
        "val_type": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA = {"tmo_id": 2, "params": []}

    session.add(TMO(**TMO_DEFAULT_DATA))
    session.add(TPRM(**TPRM_DEFAULT_DATA_3))
    session.add(TPRM(**TPRM_DEFAULT_DATA_4))
    session.add(MO(**MO_DEFAULT_DATA))
    session.commit()

    data = [
        {
            "object_id": 2,
            "new_values": [
                {"tprm_id": 3, "new_value": 111},
                {"tprm_id": 4, "new_value": "new_value"},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Can't create parameter for tprm with id: 3, because it`s primary tprm."
    }


def test_multiple_parameter_create_multiple_params(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA = {
        "name": "tmo_2",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 2,
        "val_type": "int",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 2,
        "val_type": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    MO_DEFAULT_DATA = {"tmo_id": 2, "params": []}

    session.add(TMO(**TMO_DEFAULT_DATA))
    session.add(TPRM(**TPRM_DEFAULT_DATA_3))
    session.add(TPRM(**TPRM_DEFAULT_DATA_4))
    session.add(MO(**MO_DEFAULT_DATA))
    session.commit()

    data = [
        {
            "object_id": 2,
            "new_values": [
                {"tprm_id": 3, "new_value": [111, 222, 333]},
                {"tprm_id": 4, "new_value": "new_value"},
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 200
    assert res.json() == {
        "created_params": [
            {
                "value": "8004950c000000000000005d94284b6f4bde4d4d01652e",
                "id": 1,
                "version": 1,
                "tprm_id": 3,
                "mo_id": 2,
            },
            {
                "value": "new_value",
                "id": 2,
                "version": 1,
                "tprm_id": 4,
                "mo_id": 2,
            },
        ]
    }


def test_multiple_parameter_create_incorrect_value_type(
    session: Session,
    client: TestClient,
):
    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 1,
        "val_type": "int",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    session.add(TPRM(**TPRM_DEFAULT_DATA_3))

    data = [
        {
            "object_id": 1,
            "new_values": [
                {"tprm_id": 1, "new_value": 100},
                {
                    "tprm_id": 3,
                    "new_value": True,  # boolean, but must be integer
                },
            ],
        }
    ]

    res = client.post("/api/inventory/v1/multiple_parameter_create", json=data)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Parameter True for parameter type with id 3 does not valid"
    }
