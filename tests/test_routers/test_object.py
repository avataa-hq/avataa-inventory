"""Tests for object (MO) router"""

import datetime
import pickle
from pprint import pprint

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlmodel import Session, select as select_sqlmodel

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


def test_update_mo_without_change_version(session: Session, client: TestClient):
    mo = session.exec(select(MO).where(MO.id == 1)).first()
    data = {
        "version": mo[0].version,
        "name": "qqq",
        "active": True,
    }
    res = client.patch(f"{URL}{mo[0].id}", json=data)
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Object for update has no difference, compare to original"
    }


def test_get_all_parent(session: Session, client: TestClient):
    mo1 = MO(**{"tmo_id": 1, "params": [], "p_id": 1})
    mo2 = MO(**{"tmo_id": 1, "params": [], "p_id": 2})
    session.add(mo1)
    session.add(mo2)
    session.commit()
    res = client.get("/api/inventory/v1/get_all_parent/3")
    assert res.status_code == 200
    assert len(res.json()) == 2
    assert res.json()[0]["id"] == 2
    assert res.json()[1]["id"] == 1


def test_create_object_with_point_a_constraint(
    session: Session, client: TestClient
):
    tmo = TMO(
        **{
            "name": "tmo_1111",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo)
    session.commit()

    tmo = session.execute(select(TMO).where(TMO.name == "tmo_1111")).first()[0]
    data = {"tmo_id": tmo.id, "active": True, "params": []}
    res1 = client.post("/api/inventory/v1/object_with_parameters/", json=data)

    data = {"tmo_id": tmo.id, "active": True, "params": []}
    res2 = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    print(f"{URL}object_with_parameters/")
    print(res2.json())
    assert res1.status_code == 200

    tmo = TMO(
        **{
            "name": "tmo_2222",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "points_constraint_by_tmo": [tmo.id],
        }
    )
    session.add(tmo)
    session.commit()
    tmo = session.exec(select(TMO).where(TMO.name == "tmo_2222")).first()[0]

    data = {
        "tmo_id": tmo.id,
        "active": True,
        "point_a_id": res1.json()["id"],
        "point_b_id": res2.json()["id"],
        "params": [],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)

    assert res.status_code == 200


def test_error_create_object_with_point_a_constraint(
    session: Session, client: TestClient
):
    tmo = TMO(
        **{
            "name": "tmo_1111",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo)
    session.commit()  # id 2

    tmo = session.exec(select(TMO).where(TMO.name == "tmo_1111")).first()[0]

    tmo = TMO(
        **{
            "name": "tmo_2222",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "points_constraint_by_tmo": [tmo.id],
            "geometry_type": "line",
        }
    )
    session.add(tmo)
    session.commit()  # id 3

    main_tmo = session.exec(select(TMO).where(TMO.name == "tmo_2222")).first()[
        0
    ]

    tmo = TMO(
        **{
            "name": "tmo_3333",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo)
    session.commit()  # id 4

    tmo = session.exec(select(TMO).where(TMO.name == "tmo_3333")).first()[0]
    data = {"tmo_id": tmo.id, "active": True, "params": []}
    res1 = client.post("/api/inventory/v1/object_with_parameters/", json=data)

    data = {"tmo_id": tmo.id, "active": True, "params": []}
    res2 = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res1.status_code == 200

    data = {
        "tmo_id": main_tmo.id,
        "active": True,
        "point_a_id": res1.json()["id"],
        "point_b_id": res2.json()["id"],
        "params": [],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "You try to add point_a to MO with id 2, which is not match with object constraint"
    }


def test_update_mo_with_change_version(session: Session, client: TestClient):
    mo = session.exec(select(MO).where(MO.id == 1)).first()
    data = {"version": mo[0].version, "pov": {"qqq": 1234}, "active": True}
    res = client.patch(f"{URL}{mo[0].id}", json=data)
    print(res.json())
    assert res.status_code == 200
    mo = session.exec(select(MO).where(MO.id == 1)).first()

    assert res.json()["version"] == mo[0].version
    assert mo[0].version == 2


def test_update_mo_incorrect_version(session: Session, client: TestClient):
    mo = session.exec(select(MO).where(MO.id == 1)).first()
    data = {
        "version": mo[0].version + 1,
        "pov": {"qqq": 1234},
    }
    res = client.patch(f"{URL}{mo[0].id}", json=data)

    assert res.status_code == 409
    assert res.json()["detail"] == "Actual version of MO: 1."
    assert mo[0].version == 1


def test_search_by_prm_values(session: Session, client: TestClient):
    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 1,
        "val_type": "str",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }  # id = 3

    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 1,
        "val_type": "str",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }  # id = 4

    TPRM_DEFAULT_DATA_5 = {
        "name": "tprm_5",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": 3,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }  # id = 5

    TPRM_DEFAULT_DATA_6 = {
        "name": "tprm_6",
        "tmo_id": 1,
        "val_type": "prm_link",
        "constraint": 4,
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }  # id = 6

    MO_DEFAULT_DATA_1 = {"tmo_id": 1, "params": [], "name": "2"}  # id = 2

    MO_DEFAULT_DATA_2 = {"tmo_id": 1, "params": [], "name": "3"}  # id = 3
    MO_DEFAULT_DATA_3 = {"tmo_id": 1, "params": [], "name": "4"}  # id = 4

    mul_value = pickle.dumps(["first_value", "second_value"]).hex()
    mul_id_1 = pickle.dumps([1]).hex()
    mul_ids_2 = pickle.dumps([1, 2]).hex()

    PRM_VALUE = {"mo_id": 2, "tprm_id": 3, "value": "some_one_value"}  # id = 1
    PRM_VALUE_1 = {"mo_id": 2, "tprm_id": 4, "value": mul_value}  # id = 2

    PRM_VALUE_2 = {"mo_id": 3, "tprm_id": 5, "value": 1}  # id = 3
    PRM_VALUE_3 = {"mo_id": 3, "tprm_id": 6, "value": mul_id_1}  # id = 4
    PRM_VALUE_4 = {"mo_id": 4, "tprm_id": 5, "value": 2}  # id = 5
    PRM_VALUE_5 = {"mo_id": 4, "tprm_id": 6, "value": mul_ids_2}  # id = 6

    tprm_3 = TPRM(**TPRM_DEFAULT_DATA_3)
    session.add(tprm_3)
    session.flush()

    tprm_4 = TPRM(**TPRM_DEFAULT_DATA_4)
    session.add(tprm_4)
    session.flush()

    tprm_5 = TPRM(**TPRM_DEFAULT_DATA_5)
    session.add(tprm_5)
    session.flush()

    tprm_6 = TPRM(**TPRM_DEFAULT_DATA_6)
    session.add(tprm_6)
    session.flush()

    mo_1 = MO(**MO_DEFAULT_DATA_1)
    session.add(mo_1)
    session.flush()

    mo_2 = MO(**MO_DEFAULT_DATA_2)
    session.add(mo_2)
    session.flush()

    mo_3 = MO(**MO_DEFAULT_DATA_3)
    session.add(mo_3)
    session.flush()

    prm_1 = PRM(**PRM_VALUE)
    session.add(prm_1)
    session.flush()

    prm_2 = PRM(**PRM_VALUE_1)
    session.add(prm_2)
    session.flush()

    prm_3 = PRM(**PRM_VALUE_2)
    session.add(prm_3)
    session.flush()

    prm_4 = PRM(**PRM_VALUE_3)
    session.add(prm_4)
    session.flush()

    prm_5 = PRM(**PRM_VALUE_4)
    session.add(prm_5)
    session.flush()

    prm_6 = PRM(**PRM_VALUE_5)
    session.add(prm_6)
    session.flush()

    session.commit()

    """ TEST FOR TPRM MULTIPLE = FALSE """
    res = client.get(f"{URL}search_by_values/{5}")
    assert len(res.json()["data"]) == res.json()["total"] == 2
    assert res.json()["data"][0]["prm_value"] == "1"
    assert res.json()["data"][1]["prm_value"] == "2"

    res = client.get(f"{URL}search_by_values/{5}", params={"value": "1"})
    assert len(res.json()["data"]) == res.json()["total"] == 1
    assert res.json()["data"][0]["prm_value"] == "1"

    res = client.get(f"{URL}search_by_values/{5}", params={"value": "value"})
    assert len(res.json()["data"]) == res.json()["total"] == 0


def test_create_object_with_delimiter(session: Session, client: TestClient):
    # Create TMO, TPRM. Update TMO with adding primary.
    # Creat new MO with PRM with help test client.
    check_name = "TEMP_NAME-TEMP_NAME2"
    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_3",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_DEFAULT_DATA_4 = {
        "name": "tprm_4",
        "tmo_id": 1,
        "val_type": "str",
        "required": True,
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_3 = TPRM(**TPRM_DEFAULT_DATA_3)
    session.add(tprm_3)
    tprm_4 = TPRM(**TPRM_DEFAULT_DATA_4)
    session.add(tprm_4)
    stmt = select(TMO).where(TMO.id == 1)
    tmo = session.execute(stmt).scalars().first()
    tmo.primary = [3, 4]
    session.commit()

    data = {
        "tmo_id": 1,
        "params": [
            {
                "value": 42,
                "tprm_id": 1,
            },
            {"value": True, "tprm_id": 2},
            {
                "value": check_name.split("-")[0],
                "tprm_id": 3,
            },
            {
                "value": check_name.split("-")[1],
                "tprm_id": 4,
            },
        ],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == 2)
    mo = session.execute(stmt).scalars().first()
    assert mo.name == check_name


def test_create_object_with_name_consider_mo_link(
    session: Session, client: TestClient
):
    """Create TMO for test and TMO for link.
    Create TRPRM's for primary fields mo_link TMO and test TMO.
    Crete MO's for mo_link and PRM from value's for MO mo_link
    Create PRM for correct naming mo_link.
    Set primary fields for mo_link TMO
    Create TPRM for test object with val type mo_link.
    Create test MO and PRM's for test object.
    Set primary fields for tmo with mo_link
    """
    tprm_default_data_3_name = "tprm_3"
    tprm_default_data_3_type = "str"
    tprm_default_data_4_name = "tprm_4"
    tprm_default_data_4_type = "str"
    tprm_default_data_5_name = "tprm_5"
    tprm_default_data_5_type = "mo_link"
    mo_name = "default_string-name_for_mo"
    tmo_id = 1
    mo_id = 1
    TPRM_DEFAULT_DATA_3 = {
        "name": tprm_default_data_3_name,
        "tmo_id": tmo_id,
        "val_type": tprm_default_data_3_type,
        "required": True,
        "returnable": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_3 = TPRM(**TPRM_DEFAULT_DATA_3)
    session.add(tprm_3)
    session.commit()
    session.refresh(tprm_3)

    data = [{"value": mo_name.split("-")[0], "tprm_id": tprm_3.id}]
    res = client.post(f"/api/inventory/v1/object/{mo_id}/parameters", json=data)
    print(res.json())
    assert res.status_code == 200
    data = {"version": 1, "primary": [tprm_3.id]}
    res = client.patch(f"/api/inventory/v1/object_type/{tmo_id}", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_id)
    mo_from_db = session.execute(stmt).scalars().first()
    assert mo_from_db.name == mo_name.split("-")[0]

    TMO_DEFAULT_DATA_2 = {
        "name": "tmo_for_mo_link",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_default_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_default_2)
    session.commit()
    session.refresh(tmo_default_2)
    session.refresh(tprm_3)
    TPRM_DEFAULT_DATA_4 = {
        "name": tprm_default_data_4_name,
        "tmo_id": tmo_default_2.id,
        "val_type": tprm_default_data_4_type,
        "required": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_4 = TPRM(**TPRM_DEFAULT_DATA_4)
    session.add(tprm_4)
    session.commit()
    session.add(tmo_default_2)
    session.refresh(tmo_default_2)
    session.refresh(tprm_3)
    session.refresh(tprm_4)
    data = {
        "tmo_id": tmo_default_2.id,
        "params": [{"value": mo_name.split("-")[1], "tprm_id": tprm_4.id}],
    }
    res = client.post("/api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_2: MO = MO(**res.json())
    data = {"version": 1, "primary": [tprm_4.id]}
    res = client.patch(f"/api/inventory/v1/object_type/{mo_2.id}", json=data)
    assert res.status_code == 200
    tmo_default: TMO = TMO(**res.json())
    stmt = select(MO).where(MO.id == mo_2.id)
    mo = session.execute(stmt).scalars().first()
    assert mo.name == mo_name.split("-")[1]

    TPRM_DEFAULT_DATA_5 = {
        "name": tprm_default_data_5_name,
        "tmo_id": tmo_id,
        "val_type": tprm_default_data_5_type,
        "required": True,
        "returnable": True,
        "constraint": str(2),  # tmo_id for mo_link
        "field_value": str(2),  # mo_id for mo_link
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_5 = TPRM(**TPRM_DEFAULT_DATA_5)
    session.add(tprm_5)
    session.commit()
    data = [{"value": mo_2.id, "tprm_id": tprm_5.id}]
    res = client.post(
        f"/api/inventory/v1/object/{mo_id}/parameters/", json=data
    )
    assert res.status_code == 200
    data = {"version": tmo_default.version, "primary": [tprm_3.id, tprm_5.id]}
    res = client.patch(f"/api/inventory/v1/object_type/{tmo_id}", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_id)
    mo = session.execute(stmt).scalars().first()
    assert mo.name == mo_name


def test_inherit_location_point(session: Session, client: TestClient):
    """
    Create parent TMO with geometry_type POINT
    Create TPRM for TMO long and lat. Add their to TMO as long and lat TPRM id
    Create child TMO with geometry_type POINT and false inherit_location
    Create parent MO with PRM for long and lat
    Create child MO with p_id to parent MO
    Set inherit location for child TMO
    Try update parent MO tprm with new data
    Try update parent MO long and lat
    inherit location - False
    All data in childs MO will be Null for long and lat
    """
    lat_val = 21
    lon_val = 42
    TMO_PARENT_DATA_POINT = {
        "name": "tmo_parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_parent = TMO(**TMO_PARENT_DATA_POINT)
    session.add(tmo_parent)
    session.commit()
    session.refresh(tmo_parent)

    TMO_CHILD_DATA_POINT = {
        "name": "tmo_child",
        "p_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    TPRM_LAT = {
        "name": "latitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_LON = {
        "name": "longitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    tmo_child = TMO(**TMO_CHILD_DATA_POINT)
    session.add(tmo_child)
    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_parent.id}", json=data
    )
    assert res.status_code == 200
    data = {
        "tmo_id": tmo_parent.id,
        "params": [
            {"value": str(lat_val), "tprm_id": tprm_lat.id},
            {"value": str(lon_val), "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db = session.execute(stmt).scalars().first()
    data = {
        "tmo_id": tmo_child.id,
        "p_id": tmo_parent.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()
    assert mo_parent_from_db.latitude != mo_child_from_db.latitude
    assert mo_parent_from_db.longitude != mo_child_from_db.longitude
    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db: MO = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_parent_from_db.latitude == mo_child_from_db.latitude == lat_val
    assert mo_parent_from_db.longitude == mo_child_from_db.longitude == lon_val
    data = [
        {"value": str(lat_val + 10), "version": 1, "tprm_id": tprm_lat.id},
        {"value": str(lon_val + 10), "version": 1, "tprm_id": tprm_lon.id},
    ]
    res = client.patch(
        f"api/inventory/v1/object/{tmo_parent.id}/parameters/", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()
    assert (
        mo_parent_from_db.latitude == mo_child_from_db.latitude == lat_val + 10
    )
    assert (
        mo_parent_from_db.longitude
        == mo_child_from_db.longitude
        == lon_val + 10
    )

    data = {"version": 2, "inherit_location": False}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()
    assert mo_parent_from_db.latitude == lat_val + 10
    assert mo_parent_from_db.longitude == lon_val + 10
    assert mo_child_from_db.latitude is None
    assert mo_child_from_db.longitude is None


def test_inherit_location_line(session: Session, client: TestClient):
    """
    Create Parent TMO with geometry_type LINE
    Create Second TMO for points MO
    Create Child TMO with geometry_type LINE and false inherit_location.
    Create PointsA MO with lat/long with help Second TMO
    Create PointsB MO with lat/long with help Second TMO
    Create Parent MO and add point_a_id as PointsA and add point_b_id as PointsB, add geometry with correct
    coords and intermediate coords and path
    Create child MO for test Parent MO without anything
    Trying to change parent MO without effect on child MO
    Update Child TMO with inherit location to True
    Check data for child MO
    Update geometry, lat/long for point_a, point_b
    Set inherit_location to False
    point_a_id, point_b_id, geometry at child MO will be set to Null
    """
    point_a_lat_val = 25
    point_a_lon_val = 52
    point_b_lat_val = point_a_lat_val + 10
    point_b_lon_val = point_a_lon_val + 10
    TMO_DEFAULT_DATA_LINE = {
        "name": "tmo_line",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "line",
    }
    TMO_DEFAULT_POINTS = {
        "name": "tmo_for_points",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tmo_line = TMO(**TMO_DEFAULT_DATA_LINE)
    session.add(tmo_line)
    tmo_points = TMO(**TMO_DEFAULT_POINTS)
    session.add(tmo_points)
    session.commit()
    session.refresh(tmo_line)
    session.refresh(tmo_points)
    TMO_CHILD = {
        "name": "tmo_child",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "line",
        "p_id": tmo_line.id,
    }

    TPRM_LAT = {
        "name": "latitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_points.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_LON = {
        "name": "longitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_points.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tmo_child = TMO(**TMO_CHILD)
    session.add(tmo_child)
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    session.commit()
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line)
    session.refresh(tmo_points)

    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_points.id}", json=data
    )
    assert res.status_code == 200

    data = {
        "tmo_id": tmo_points.id,
        "params": [
            {"value": point_a_lat_val, "tprm_id": tprm_lat.id},
            {"value": point_a_lon_val, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_a: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_points.id,
        "params": [
            {"value": point_b_lat_val, "tprm_id": tprm_lat.id},
            {"value": point_b_lon_val, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_b: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_line.id,
        "point_a_id": point_a.id,
        "point_b_id": point_b.id,
        "geometry": {
            "path": [
                [point_a_lon_val, point_a_lat_val],
                [88.8, 88.8],
                [point_b_lon_val, point_b_lat_val],
            ],
            "path_length": 111.111,
        },
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    parent_mo: MO = MO(**res.json())

    data = {"tmo_id": tmo_child.id, "p_id": parent_mo.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    child_mo: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == parent_mo.id)
    mo_parent_from_bd = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == child_mo.id)
    mo_child_from_bd = session.execute(stmt).scalars().first()
    assert mo_parent_from_bd.point_a_id != mo_child_from_bd.point_a_id
    assert mo_parent_from_bd.point_b_id != mo_child_from_bd.point_b_id

    data = {"version": 1, "inherit_location": True}
    res = client.patch("api/inventory/v1/object_type/4", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == parent_mo.id)
    mo_parent_from_bd = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == child_mo.id)
    mo_child_from_bd = session.execute(stmt).scalars().first()
    assert mo_parent_from_bd.point_a_id == mo_child_from_bd.point_a_id
    assert mo_parent_from_bd.point_b_id == mo_child_from_bd.point_b_id
    assert mo_parent_from_bd.geometry == mo_child_from_bd.geometry
    data = [
        {"value": point_a_lat_val + 15, "version": 1, "tprm_id": tprm_lat.id},
        {"value": point_a_lon_val + 15, "version": 1, "tprm_id": tprm_lon.id},
    ]
    res = client.patch(
        f"api/inventory/v1/object/{point_a.id}/parameters/", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == parent_mo.id)
    mo_parent_from_bd = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == child_mo.id)
    mo_child_from_bd = session.execute(stmt).scalars().first()
    assert mo_parent_from_bd.geometry == {
        "path": {
            "type": "LineString",
            "coordinates": [
                [float(point_a_lon_val + 15), float(point_a_lat_val + 15)],
                [88.8, 88.8],
                [float(point_b_lon_val), float(point_b_lat_val)],
            ],
        },
        "path_length": 11456.098816976772,
    }
    assert mo_parent_from_bd.point_a_id == mo_child_from_bd.point_a_id
    assert mo_parent_from_bd.point_b_id == mo_child_from_bd.point_b_id
    assert mo_parent_from_bd.geometry == mo_child_from_bd.geometry

    data = {"version": 2, "inherit_location": False}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == parent_mo.id)
    mo_parent = session.execute(stmt).scalars().first()
    stmt = select(MO).where(MO.id == child_mo.id)
    mo_child = session.execute(stmt).scalars().first()
    assert mo_parent.geometry == {
        "path": {
            "type": "LineString",
            "coordinates": [
                [float(point_a_lon_val + 15), float(point_a_lat_val + 15)],
                [88.8, 88.8],
                [float(point_b_lon_val), float(point_b_lat_val)],
            ],
        },
        "path_length": 11456.098816976772,
    }
    assert mo_child.point_a_id is None
    assert mo_child.point_b_id is None
    assert mo_child.geometry is None


def test_deep_inherit_location_point(session: Session, client: TestClient):
    """
    Create parent TMO with geometry_type POINT
    Create TPRM for TMO long and lat. Add their to TMO as long and lat TPRM id
    Create child TMO with geometry_type POINT and false inherit_location
    Create grandchild TMO with geometry_type POINT and false inherit_location
    Create parent MO with PRM for long and lat
    Create child MO with p_id to parent MO
    Create grandchild MO with p_id to child MO
    Set inherit location for grandchild TMO
    Check correct parent MO and tprm
    """
    latitude_val = 25
    longitude_val = 52
    TMO_PARENT_DATA_POINT = {
        "name": "tmo_parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }

    tmo_parent = TMO(**TMO_PARENT_DATA_POINT)
    session.add(tmo_parent)
    session.commit()
    session.refresh(tmo_parent)
    TPRM_LAT = {
        "name": "latitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_LONG = {
        "name": "longitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LONG)
    session.add(tprm_lon)
    TMO_CHILD_DATA_POINT = {
        "name": "tmo_child",
        "p_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_child = TMO(**TMO_CHILD_DATA_POINT)
    session.add(tmo_child)
    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    TMO_GRANDCHILD_DATA_POINT = {
        "name": "tmo_grandchild",
        "p_id": tmo_child.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_grandchild = TMO(**TMO_GRANDCHILD_DATA_POINT)
    session.add(tmo_grandchild)

    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_grandchild)

    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_parent.id}", json=data
    )
    assert res.status_code == 200
    data = {
        "tmo_id": tmo_parent.id,
        "params": [
            {"value": str(latitude_val), "tprm_id": tprm_lat.id},
            {"value": str(longitude_val), "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_parent.latitude == mo_parent_from_db.latitude
    assert mo_parent.latitude == mo_parent_from_db.latitude

    # Create child
    data = {"tmo_id": tmo_child.id, "p_id": mo_parent.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_child_from_db.latitude
    assert mo_parent_from_db.longitude != mo_child_from_db.longitude

    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude == mo_child_from_db.latitude
    assert mo_parent_from_db.longitude == mo_child_from_db.longitude

    # Create grandchild
    data = {"tmo_id": tmo_grandchild.id, "p_id": mo_child.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_grandchild: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_grandchild_from_db.latitude
    assert mo_parent_from_db.longitude != mo_grandchild_from_db.longitude
    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_grandchild.id}", json=data
    )
    assert res.status_code == 200

    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()
    assert mo_parent.latitude == mo_grandchild_from_db.latitude
    assert mo_parent.longitude == mo_grandchild_from_db.longitude

    # Update parent mo coordinate
    data = [
        {"value": latitude_val + 10, "version": 1, "tprm_id": tprm_lat.id},
        {"value": longitude_val + 10, "version": 1, "tprm_id": tprm_lon.id},
    ]
    res = client.patch(
        f"api/inventory/v1/object/{mo_parent.id}/parameters/", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db: MO = session.execute(stmt).scalars().first()

    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_parent_from_db.latitude == mo_grandchild_from_db.latitude
    assert mo_parent_from_db.longitude == mo_grandchild_from_db.longitude


def test_find_deep_inherit(session: Session, client: TestClient):
    """
    Create parent TMO with geometry_type POINT
    Create TPRM for TMO long and lat. Add their to TMO as long and lat TPRM id
    Create child TMO with geometry_type POINT and false inherit_location
    Create grandchild TMO with geometry_type POINT and false inherit_location
    Create parent MO with PRM for long and lat
    Create child MO with p_id to parent MO
    Create grandchild MO with p_id to parent MO
    Set inherit location for grandchild TMO
    Try update parent MO tprm with new data
    Try update parent MO long and lat
    inherit location - False
    All data in childs MO will be Null for long and lat
    """
    latitude_val = 25
    longitude_val = 52
    TMO_PARENT_DATA_POINT = {
        "name": "tmo_parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }

    tmo_parent = TMO(**TMO_PARENT_DATA_POINT)
    session.add(tmo_parent)
    session.commit()
    session.refresh(tmo_parent)
    TPRM_LAT = {
        "name": "latitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_LON = {
        "name": "longitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    TMO_CHILD_DATA_POINT = {
        "name": "tmo_child",
        "p_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_child = TMO(**TMO_CHILD_DATA_POINT)
    session.add(tmo_child)
    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    TMO_GRANDCHILD_DATA_POINT = {
        "name": "tmo_grandchild",
        "p_id": tmo_child.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_grandchild = TMO(**TMO_GRANDCHILD_DATA_POINT)
    session.add(tmo_grandchild)

    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_grandchild)

    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_parent.id}", json=data
    )
    assert res.status_code == 200
    data = {
        "tmo_id": tmo_parent.id,
        "params": [
            {"value": str(latitude_val), "tprm_id": tprm_lat.id},
            {"value": str(longitude_val), "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_parent.latitude == mo_parent_from_db.latitude
    assert mo_parent.latitude == mo_parent_from_db.latitude

    # Create child
    data = {"tmo_id": tmo_child.id, "p_id": mo_parent.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_child_from_db.latitude
    assert mo_parent_from_db.longitude != mo_child_from_db.longitude

    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude == mo_child_from_db.latitude
    assert mo_parent_from_db.longitude == mo_child_from_db.longitude

    # Create grandchild
    data = {"tmo_id": tmo_grandchild.id, "p_id": mo_child.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_grandchild: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_grandchild_from_db.latitude
    assert mo_parent_from_db.longitude != mo_grandchild_from_db.longitude
    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_grandchild.id}", json=data
    )
    assert res.status_code == 200

    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()
    assert mo_parent.latitude == mo_grandchild_from_db.latitude
    assert mo_parent.longitude == mo_grandchild_from_db.longitude

    res = client.get(
        f"api/inventory/v1/object/{mo_grandchild_from_db.id}/inherit_location"
    )
    assert res.status_code == 200
    assert res.json().get("parent_mo").get("id") == mo_parent.id
    assert res.json().get("tprm_latitude") == tprm_lat.id
    assert res.json().get("tprm_longitude") == tprm_lon.id


def test_find_deep_inherit_without_parent(session: Session, client: TestClient):
    """
    Create parent TMO with geometry_type POINT
    Create TPRM for TMO long and lat. Add their to TMO as long and lat TPRM id
    Create child TMO with geometry_type POINT and false inherit_location
    Create grandchild TMO with geometry_type POINT and false inherit_location
    Create parent MO with PRM for long and lat
    Create child MO without p_id
    Create grandchild MO with p_id to child MO
    Set inherit location for grandchild TMO
    Try update parent MO tprm with new data
    Try update parent MO long and lat
    inherit location - False
    All data in childs MO will be Null for long and lat
    """
    latitude_val = 25
    longitude_val = 52
    TMO_PARENT_DATA_POINT = {
        "name": "tmo_parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }

    tmo_parent = TMO(**TMO_PARENT_DATA_POINT)
    session.add(tmo_parent)
    session.commit()
    session.refresh(tmo_parent)
    TPRM_LAT = {
        "name": "latitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    TPRM_LON = {
        "name": "longitude",
        "val_type": "float",
        "returnable": True,
        "tmo_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    session.add(tprm_lat)
    tprm_lon = TPRM(**TPRM_LON)
    session.add(tprm_lon)
    TMO_CHILD_DATA_POINT = {
        "name": "tmo_child",
        "p_id": tmo_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_child = TMO(**TMO_CHILD_DATA_POINT)
    session.add(tmo_child)
    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    TMO_GRANDCHILD_DATA_POINT = {
        "name": "tmo_grandchild",
        "p_id": tmo_child.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "geometry_type": "point",
    }
    tmo_grandchild = TMO(**TMO_GRANDCHILD_DATA_POINT)
    session.add(tmo_grandchild)

    session.commit()
    session.refresh(tmo_parent)
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_grandchild)

    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_parent.id}", json=data
    )
    assert res.status_code == 200
    data = {
        "tmo_id": tmo_parent.id,
        "params": [
            {"value": str(latitude_val), "tprm_id": tprm_lat.id},
            {"value": str(longitude_val), "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent.id)
    mo_parent_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_parent.latitude == mo_parent_from_db.latitude
    assert mo_parent.latitude == mo_parent_from_db.latitude

    # Create child
    data = {"tmo_id": tmo_child.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_child_from_db.latitude
    assert mo_parent_from_db.longitude != mo_child_from_db.longitude

    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_child.id}", json=data
    )
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_child.id)
    mo_child_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_child_from_db.latitude
    assert mo_parent_from_db.longitude != mo_child_from_db.longitude

    # Create grandchild
    data = {"tmo_id": tmo_grandchild.id, "p_id": mo_child.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_grandchild: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()

    assert mo_parent_from_db.latitude != mo_grandchild_from_db.latitude
    assert mo_parent_from_db.longitude != mo_grandchild_from_db.longitude
    data = {"version": 1, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_grandchild.id}", json=data
    )
    assert res.status_code == 200

    stmt = select(MO).where(MO.id == mo_grandchild.id)
    mo_grandchild_from_db = session.execute(stmt).scalars().first()
    assert mo_child.latitude == mo_grandchild_from_db.latitude
    assert mo_child.longitude == mo_grandchild_from_db.longitude

    res = client.get(
        f"api/inventory/v1/object/{mo_grandchild_from_db.id}/inherit_location"
    )
    assert res.status_code == 200
    assert res.json().get("parent_mo").get("id") == mo_child.id
    assert res.json().get("tprm_latitude") is None
    assert res.json().get("tprm_longitude") is None


def test_create_mo_with_point_a_point_b_without_coordinates(
    session: Session, client: TestClient
):
    """Create TMO for point and line.
    Create MO's for point_a, point_b
    Create target MO for check geometry work.
    Update point TMO to set lat, long
    Update point MO with set lat, long"""
    tmo_point_name = "TMO_POINT"
    tmo_point_geometry_type = "point"

    tmo_line_name = "TMO_LINE"
    tmo_line_geometry_type = "line"

    tprm_lat_name = "TPRM_LAT"
    tprm_lon_name = "TPRM_LON"
    tprm_lat_type = tprm_lon_type = "float"
    point_a_lat = 10
    point_a_lon = 11
    point_b_lat = 20
    point_b_lon = 21

    null_geometry = {
        "path": {"type": "LineString", "coordinates": []},
        "path_length": 0,
    }
    fill_geometry = {
        "path": {
            "type": "LineString",
            "coordinates": [
                [float(point_a_lon), float(point_a_lat)],
                [float(point_b_lon), float(point_b_lat)],
            ],
        },
        "path_length": 1541.8564339502927,
    }

    tmo_point = TMO(
        **{
            "name": tmo_point_name,
            "geometry_type": tmo_point_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)

    tprm_lat = TPRM(
        **{
            "name": tprm_lat_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lat_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tprm_lon = TPRM(
        **{
            "name": tprm_lon_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lon_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tmo_line = TMO(
        **{
            "name": tmo_line_name,
            "geometry_type": tmo_line_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm_lat)
    session.add(tprm_lon)
    session.add(tmo_line)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.add(tmo_line)
    session.add(tmo_point)

    # Create MO for point_a, point_b
    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_a: MO = MO(**res.json())

    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_b: MO = MO(**res.json())

    # Create main mo
    data = {
        "tmo_id": tmo_line.id,
        "point_a_id": point_a.id,
        "point_b_id": point_b.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    target_mo: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == target_mo.id)
    target_mo_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_from_db.geometry == null_geometry

    # Update TMO with add lat,lang prm
    data = {
        "version": tmo_point.version,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
    }
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    assert res.status_code == 200
    tmo_point: TMO = TMO(**res.json())

    data = [
        {"value": point_a_lat, "tprm_id": tprm_lat.id},
        {"value": point_a_lon, "tprm_id": tprm_lon.id},
    ]
    res = client.post(
        f"api/inventory/v1/object/{point_a.id}/parameters/", json=data
    )
    assert res.status_code == 200
    target_mo_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_from_db.geometry == null_geometry

    data = [
        {"value": point_b_lat, "tprm_id": tprm_lat.id},
        {"value": point_b_lon, "tprm_id": tprm_lon.id},
    ]
    res = client.post(
        f"api/inventory/v1/object/{point_b.id}/parameters/", json=data
    )
    assert res.status_code == 200
    target_mo_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_from_db.geometry == fill_geometry


def test_update_geometry_with_point_b_without_coordinates(
    session: Session, client: TestClient
):
    """Create TMO for point and line.
    Create MO's for point_a, point_b
    Create target MO to set point_a only.
    Update target MO to set point_b"""
    tmo_point_name = "TMO_POINT"
    tmo_point_geometry_type = "point"

    tmo_line_name = "TMO_LINE"
    tmo_line_geometry_type = "line"

    tprm_lat_name = "TPRM_LAT"
    tprm_lon_name = "TPRM_LON"
    tprm_lat_type = tprm_lon_type = "float"

    null_geometry = {
        "path": {"type": "LineString", "coordinates": []},
        "path_length": 0,
    }

    tmo_point = TMO(
        **{
            "name": tmo_point_name,
            "geometry_type": tmo_point_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)

    tprm_lat = TPRM(
        **{
            "name": tprm_lat_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lat_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tprm_lon = TPRM(
        **{
            "name": tprm_lon_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lon_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tmo_line = TMO(
        **{
            "name": tmo_line_name,
            "geometry_type": tmo_line_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm_lat)
    session.add(tprm_lon)
    session.add(tmo_line)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)

    # Create MO for point_a, point_b
    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_a: MO = MO(**res.json())

    data = {"tmo_id": tmo_point.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_b: MO = MO(**res.json())

    # Create main mo
    data = {"tmo_id": tmo_line.id, "point_a_id": point_a.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    target_mo: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == target_mo.id)
    target_mo_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_from_db.geometry is None

    data = {
        "version": target_mo.version,
        "point_b_id": point_b.id,
        "point_a_id": point_a.id,
        "active": target_mo.active,
        "geometry": {},
    }
    res = client.patch(f"api/inventory/v1/object/{target_mo.id}", json=data)
    assert res.status_code == 200
    target_mo: MO = MO(**res.json())
    assert target_mo.point_a_id == point_a.id
    assert target_mo.point_b_id == point_b.id

    stmt = select(MO).where(MO.id == target_mo.id)
    target_mo_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_from_db.geometry == null_geometry

    data = {"tmo_id": tmo_line.id, "point_b_id": point_b.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    target_mo_2: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == target_mo_2.id)
    target_mo_2_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_2_from_db.geometry is None

    data = {
        "version": target_mo_2.version,
        "point_a_id": point_a.id,
        "point_b_id": point_b.id,
        "active": target_mo.active,
        "geometry": target_mo.geometry,
    }
    res = client.patch(f"api/inventory/v1/object/{target_mo_2.id}", json=data)
    assert res.status_code == 200
    target_mo_2: MO = MO(**res.json())
    assert target_mo_2.point_a_id == point_a.id
    assert target_mo_2.point_b_id == point_b.id

    stmt = select(MO).where(MO.id == target_mo_2.id)
    target_mo_2_from_db: MO = session.execute(stmt).scalar()
    assert target_mo_2_from_db.geometry == null_geometry


def test_massive_objects_delete(session: Session, client: TestClient):
    MO_DEFAULT_DATA_2 = {
        "tmo_id": 1,
        "params": [
            {
                "value": 43,
                "tprm_id": 1,
            },
            {"value": False, "tprm_id": 2},
        ],
    }

    MO_DEFAULT_DATA_3 = {
        "tmo_id": 1,
        "params": [
            {
                "value": 44,
                "tprm_id": 1,
            },
            {"value": True, "tprm_id": 2},
        ],
    }

    mo_2 = MO(**MO_DEFAULT_DATA_2)
    mo_3 = MO(**MO_DEFAULT_DATA_3)
    session.add(mo_2)
    session.add(mo_3)
    session.commit()

    data = {"mo_ids": [1, 2, 3], "erase": False}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)
    print(res.json())
    assert res.status_code == 200

    objects = (
        session.execute(select(MO).where(MO.id.in_([1, 2, 3]))).scalars().all()
    )

    for mo in objects:
        assert mo.active is False
        assert mo.version == 2


def test_massive_objects_delete_2(session: Session, client: TestClient):
    MO_DEFAULT_DATA_2 = {
        "tmo_id": 1,
        "params": [
            {
                "value": 43,
                "tprm_id": 1,
            },
            {"value": False, "tprm_id": 2},
        ],
    }

    MO_DEFAULT_DATA_3 = {
        "tmo_id": 1,
        "params": [
            {
                "value": 44,
                "tprm_id": 1,
            },
            {"value": True, "tprm_id": 2},
        ],
    }

    mo_2 = MO(**MO_DEFAULT_DATA_2)
    mo_3 = MO(**MO_DEFAULT_DATA_3)
    session.add(mo_2)
    session.add(mo_3)
    session.commit()

    data = {"mo_ids": [1, 2, 3], "erase": True}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)

    assert res.status_code == 200

    objects = (
        session.execute(select(MO).where(MO.id.in_([1, 2, 3]))).scalars().all()
    )

    assert not objects


def test_massive_objects_delete_3(session: Session, client: TestClient):
    data = {"mo_ids": [11, 22, 33], "erase": True}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)

    assert res.status_code == 422


def test_massive_objects_delete_4(session: Session, client: TestClient):
    TMO_DEFAULT_DATA_2 = {
        "name": "tmo_2",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "primary": [3],
    }

    tmo_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_2)
    MO_DEFAULT_DATA_2 = {"tmo_id": 2, "params": [{"tprm_id": 3, "value": "1"}]}

    TPRM_DEFAULT_DATA_3 = {
        "name": "tprm_2",
        "tmo_id": 2,
        "val_type": "mo_link",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }

    tprm_3 = TPRM(**TPRM_DEFAULT_DATA_3)
    mo_2 = MO(**MO_DEFAULT_DATA_2)
    prm = PRM(**{"value": "1", "tprm_id": 3, "mo_id": 2})
    session.add(tprm_3)
    session.add(prm)
    session.add(mo_2)
    session.commit()

    data = {"mo_ids": [1], "erase": True}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)

    assert res.status_code == 422
    assert res.json() == {
        "detail": "Objects with id: [1] can't be deleted, because their names by "
        "primary are part of other object names"
    }


def test_massive_objects_delete_5(session: Session, client: TestClient):
    TMO_DEFAULT_DATA_2 = {
        "name": "tmo_2",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "p_id": 1,
    }

    tmo_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_2)
    MO_DEFAULT_DATA_2 = {"tmo_id": 2, "p_id": 1, "params": []}

    session.add(MO(**MO_DEFAULT_DATA_2))
    session.commit()

    data = {"mo_ids": [1], "erase": True, "delete_children": False}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)

    assert res.status_code == 200

    assert session.execute(select(MO.id).where(MO.id == 2)).scalars().all() == [
        2
    ]


def test_massive_objects_delete_6(session: Session, client: TestClient):
    TMO_DEFAULT_DATA_2 = {
        "name": "tmo_2",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
        "p_id": 1,
    }

    tmo_2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo_2)
    MO_DEFAULT_DATA_2 = {"tmo_id": 2, "p_id": 1, "params": []}

    session.add(MO(**MO_DEFAULT_DATA_2))
    session.commit()

    data = {"mo_ids": [1], "erase": True, "delete_children": True}
    res = client.post("/api/inventory/v1/massive_objects_delete/", json=data)

    assert res.status_code == 200

    assert (
        session.execute(select(MO.id).where(MO.id == 2)).scalars().all() == []
    )


def test_massive_objects_update(session: Session, client: TestClient):
    assert session.execute(select(MO.pov).where(MO.id == 1)).scalar() is None

    data = [
        {
            "object_id": 1,
            "data_for_update": {
                "version": 1,
                "pov": {"sdfsdf": 2},
                "active": True,
            },
        }
    ]
    res = client.patch("/api/inventory/v1/massive_update_object/", json=data)
    assert res.status_code == 200

    assert session.execute(select(MO.pov).where(MO.id == 1)).scalar() == {
        "sdfsdf": 2
    }


def test_massive_objects_update_2(session: Session, client: TestClient):
    assert session.execute(select(MO.pov).where(MO.id == 1)).scalar() is None

    data = [
        {
            "object_id": 1,
            "data_for_update": {
                "version": 1,
                "pov": {"sdfsdf": 2},
                "active": True,
            },
        }
    ]
    res = client.patch("/api/inventory/v1/massive_update_object/", json=data)

    assert res.status_code == 200

    assert session.execute(select(MO.pov).where(MO.id == 1)).scalar() == {
        "sdfsdf": 2
    }


def test_get_all_children(session: Session, client: TestClient):
    TMO_DEFAULT_DATA_1 = {
        "name": "parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    parent_tmo = TMO(**TMO_DEFAULT_DATA_1)
    session.add(parent_tmo)
    session.flush()

    TMO_DEFAULT_DATA_2 = {
        "name": "child",
        "p_id": parent_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_tmo = TMO(**TMO_DEFAULT_DATA_2)

    session.add(child_tmo)
    session.flush()

    MO_DEFAULT_DATA = {
        "tmo_id": parent_tmo.id,
        "name": "parent_name",
        "params": [],
    }

    parent_mo = MO(**MO_DEFAULT_DATA)

    session.add(parent_mo)
    session.flush()

    for new_object_id in range(parent_mo.id + 1, parent_mo.id + 10):
        child_mo = MO(
            **{
                "name": new_object_id,
                "tmo_id": child_tmo.id,
                "p_id": parent_mo.id,
                "params": [],
            }
        )

        session.add(child_mo)

    session.flush()
    session.commit()

    res = client.get(f"/api/inventory/v1/get_all_children/{parent_mo.id}")

    assert res.status_code == 200
    pprint(res.json())
    assert res.json() == {
        "children": [
            {
                "children": [],
                "object_id": 3,
                "object_name": "3",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 4,
                "object_name": "4",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 5,
                "object_name": "5",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 6,
                "object_name": "6",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 7,
                "object_name": "7",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 8,
                "object_name": "8",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 9,
                "object_name": "9",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 10,
                "object_name": "10",
                "object_type_id": 3,
                "parent_id": 2,
            },
            {
                "children": [],
                "object_id": 11,
                "object_name": "11",
                "object_type_id": 3,
                "parent_id": 2,
            },
        ],
        "object_id": 2,
        "object_name": "parent_name",
        "object_type_id": 2,
        "parent_id": None,
    }


def test_get_all_children_not_exists_object(
    session: Session, client: TestClient
):
    res = client.get("/api/inventory/v1/get_all_children/100500")

    assert res.status_code == 422
    assert res.json() == {"detail": "Object with id 100500 does not exist"}


def test_get_all_children_three_levels(session: Session, client: TestClient):
    TMO_DEFAULT_DATA_1 = {
        "name": "parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    parent_tmo = TMO(**TMO_DEFAULT_DATA_1)
    session.add(parent_tmo)
    session.flush()

    TMO_DEFAULT_DATA_2 = {
        "name": "child",
        "p_id": parent_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_tmo = TMO(**TMO_DEFAULT_DATA_2)

    session.add(child_tmo)
    session.flush()

    TMO_DEFAULT_DATA_3 = {
        "name": "child of child",
        "p_id": child_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_of_child_tmo = TMO(**TMO_DEFAULT_DATA_3)

    session.add(child_of_child_tmo)
    session.flush()

    MO_DEFAULT_DATA = {
        "tmo_id": parent_tmo.id,
        "name": "parent_name",
        "params": [],
    }

    parent_mo = MO(**MO_DEFAULT_DATA)

    session.add(parent_mo)
    session.flush()

    child_mo = MO(
        **{
            "name": str(parent_mo.id + 1),
            "tmo_id": child_tmo.id,
            "p_id": parent_mo.id,
            "params": [],
        }
    )

    session.add(child_mo)
    session.flush()

    child_of_child_mo = MO(
        **{
            "name": str(child_mo.id + 1),
            "tmo_id": child_of_child_tmo.id,
            "p_id": child_mo.id,
            "params": [],
        }
    )

    session.add(child_of_child_mo)
    session.flush()
    session.commit()

    res = client.get(f"/api/inventory/v1/get_all_children/{parent_mo.id}")
    pprint(res.json())

    assert res.status_code == 200
    assert res.json() == {
        "object_id": parent_mo.id,
        "object_name": "parent_name",
        "object_type_id": parent_tmo.id,
        "parent_id": None,
        "children": [
            {
                "object_id": child_mo.id,
                "object_name": str(child_mo.id),
                "object_type_id": child_tmo.id,
                "parent_id": parent_mo.id,
                "children": [
                    {
                        "object_id": child_of_child_mo.id,
                        "object_name": str(child_of_child_mo.id),
                        "object_type_id": child_of_child_tmo.id,
                        "parent_id": child_mo.id,
                        "children": [],
                    }
                ],
            }
        ],
    }


def test_get_all_children_three_levels_with_exists_parent(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA_1 = {
        "name": "parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    parent_tmo = TMO(**TMO_DEFAULT_DATA_1)
    session.add(parent_tmo)
    session.flush()

    TMO_DEFAULT_DATA_2 = {
        "name": "child",
        "p_id": parent_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_tmo = TMO(**TMO_DEFAULT_DATA_2)

    session.add(child_tmo)
    session.flush()

    TMO_DEFAULT_DATA_3 = {
        "name": "child of child",
        "p_id": child_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_of_child_tmo = TMO(**TMO_DEFAULT_DATA_3)

    session.add(child_of_child_tmo)
    session.flush()

    MO_DEFAULT_DATA = {
        "tmo_id": parent_tmo.id,
        "name": "parent_name",
        "params": [],
    }

    parent_mo = MO(**MO_DEFAULT_DATA)

    session.add(parent_mo)
    session.flush()

    child_mo = MO(
        **{
            "name": str(parent_mo.id + 1),
            "tmo_id": child_tmo.id,
            "p_id": parent_mo.id,
            "params": [],
        }
    )

    session.add(child_mo)
    session.flush()

    child_of_child_mo = MO(
        **{
            "name": str(child_mo.id + 1),
            "tmo_id": child_of_child_tmo.id,
            "p_id": child_mo.id,
            "params": [],
        }
    )

    session.add(child_of_child_mo)
    session.flush()
    session.commit()

    res = client.get(f"/api/inventory/v1/get_all_children/{child_mo.id}")
    pprint(res.json())

    assert res.status_code == 200
    assert res.json() == {
        "children": [
            {
                "children": [],
                "object_id": 4,
                "object_name": "4",
                "object_type_id": 4,
                "parent_id": 3,
            }
        ],
        "object_id": 3,
        "object_name": "3",
        "object_type_id": 3,
        "parent_id": 2,
    }


def test_get_all_children_node_limit_error(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA_1 = {
        "name": "parent",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    parent_tmo = TMO(**TMO_DEFAULT_DATA_1)
    session.add(parent_tmo)
    session.flush()

    TMO_DEFAULT_DATA_2 = {
        "name": "child",
        "p_id": parent_tmo.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    child_tmo = TMO(**TMO_DEFAULT_DATA_2)

    session.add(child_tmo)
    session.flush()

    MO_DEFAULT_DATA = {
        "tmo_id": parent_tmo.id,
        "name": "parent_name",
        "params": [],
    }

    parent_mo = MO(**MO_DEFAULT_DATA)

    session.add(parent_mo)
    session.flush()

    for new_object_id in range(parent_mo.id + 1, parent_mo.id + 101):
        child_mo = MO(
            **{
                "name": new_object_id,
                "tmo_id": child_tmo.id,
                "p_id": parent_mo.id,
                "params": [],
            }
        )

        session.add(child_mo)

    session.flush()
    session.commit()

    res = client.get(f"/api/inventory/v1/get_all_children/{parent_mo.id}")

    assert res.status_code == 422
    assert res.json() == {
        "detail": "Result exceeds the limit of 100 descendants"
    }


def test_change_child_mo_point_a_id(session: Session, client: TestClient):
    """Check correct update point_a for child mo with inherit location
    Main problem was consist of incorrect use exec/execute/select"""
    tmo_point_name = "TMO_POINT"
    tmo_point_geometry_type = "point"

    tmo_line_name = "TMO_LINE"
    tmo_line_child_name = "TMO_LINE_CHILD"
    tmo_line_geometry_type = "line"

    tprm_lat_name = "TPRM_LAT"
    tprm_lon_name = "TPRM_LON"
    tprm_lat_type = tprm_lon_type = "float"

    point_a1_lat = 20.0
    point_a1_lon = 21.0
    point_a2_lat = 22.0
    point_a2_lon = 23.0
    point_b_lat = 30.0
    point_b_lon = 31.0

    a1_b_geometry = {
        "path": {
            "coordinates": [
                [point_a1_lon, point_a1_lat],
                [point_b_lon, point_b_lat],
            ],
            "type": "LineString",
        },
        "path_length": 1497.1489241504332,
    }

    a2_b_geometry = {
        "path": {
            "coordinates": [
                [point_a2_lon, point_a2_lat],
                [point_b_lon, point_b_lat],
            ],
            "type": "LineString",
        },
        "path_length": 1193.763405547427,
    }

    tmo_point = TMO(
        **{
            "name": tmo_point_name,
            "geometry_type": tmo_point_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_point)

    tprm_lat = TPRM(
        **{
            "name": tprm_lat_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lat_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tprm_lon = TPRM(
        **{
            "name": tprm_lon_name,
            "tmo_id": tmo_point.id,
            "val_type": tprm_lon_type,
            "creation_date": datetime.datetime.now(),
            "modification_date": datetime.datetime.now(),
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tmo_line = TMO(
        **{
            "name": tmo_line_name,
            "geometry_type": tmo_line_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )

    session.add(tprm_lat)
    session.add(tprm_lon)
    session.add(tmo_line)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line)

    tmo_line_child = TMO(
        **{
            "name": tmo_line_child_name,
            "geometry_type": tmo_line_geometry_type,
            "p_id": tmo_line.id,
            "inherit_location": True,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_line_child)
    session.commit()
    session.refresh(tmo_line_child)

    # Add lat/long to TMO
    data = {"version": 1, "latitude": tprm_lat.id, "longitude": tprm_lon.id}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    print(res.json())
    assert res.status_code == 200

    # Create MO for point_a, point_b
    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": point_a1_lat, "tprm_id": tprm_lat.id},
            {"value": point_a1_lon, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_a_1: MO = MO(**res.json())

    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": point_a2_lat, "tprm_id": tprm_lat.id},
            {"value": point_a2_lon, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_a_2: MO = MO(**res.json())

    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": point_b_lat, "tprm_id": tprm_lat.id},
            {"value": point_b_lon, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_b: MO = MO(**res.json())

    # Create main mo
    data = {
        "tmo_id": tmo_line.id,
        "point_a_id": point_a_1.id,
        "point_b_id": point_b.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    target_mo: MO = MO(**res.json())
    stmt = select_sqlmodel(MO).where(MO.id == target_mo.id)
    target_mo_from_db: MO = session.exec(stmt).first()
    assert target_mo_from_db.geometry == a1_b_geometry

    # Create child mo
    data = {
        "tmo_id": tmo_line_child.id,
        "point_a_id": point_a_1.id,
        "point_b_id": point_b.id,
        "p_id": target_mo.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    target_child_mo: MO = MO(**res.json())
    stmt_child = select_sqlmodel(MO).where(MO.id == target_child_mo.id)
    target_child_mo_from_db: MO = session.exec(stmt_child).first()
    assert target_child_mo_from_db.geometry == a1_b_geometry

    # Update main mo
    data = {"version": target_mo.version, "point_a_id": point_a_2.id}
    res = client.patch(f"api/inventory/v1/object/{target_mo.id}", json=data)
    assert res.status_code == 200
    target_mo_updated: MO = MO(**res.json())
    stmt = select_sqlmodel(MO).where(MO.id == target_mo_updated.id)
    target_mo_from_db: MO = session.exec(stmt).first()
    assert target_mo_from_db.geometry == a2_b_geometry

    # We got updated child mo
    target_child_mo_from_db: MO = session.exec(stmt_child).first()
    assert target_child_mo_from_db.geometry == a2_b_geometry
