"""Tests for object type router"""

import pickle

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, and_
from sqlmodel import Session

from models import TMO, TPRM, Event, MO, PRM

URL = "/api/inventory/v1/object_type/"

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
    tmo2 = TMO(**TMO_DEFAULT_DATA_2)
    session.add(tmo2)
    session.commit()
    yield session


def test_read_all_created_object_types(session: Session, client: TestClient):
    tmo_read_url = "/api/inventory/v1/object_types/"

    res = client.get(tmo_read_url)

    all_tmo = session.exec(select(TMO)).all()
    assert res.status_code == 200
    assert len(res.json()) == len(all_tmo)


def test_object_type_creation(session: Session, client: TestClient):
    object_type = {"name": "tmo2"}
    res = client.post(URL, json=object_type)
    current_tmo = session.exec(
        select(TMO).where(TMO.name == object_type["name"])
    ).first()

    assert current_tmo
    assert len(current_tmo[0].dict()) == len(res.json())
    assert res.status_code == 200


# def test_object_type_creation_with_lifecycle_process_definition(
#     session: Session, client: TestClient
# ):
#     object_type = {
#         "name": "tmo2",
#         "lifecycle_process_definition": "some_str_data:123",
#     }
#
#     res = client.post(URL, json=object_type)
#
#     assert res.status_code == 200


def test_object_type_error_creation_with_lifecycle_process_definition(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo2", "lifecycle_process_definition": "123:123"}
    res = client.post(URL, json=object_type)
    assert res.status_code == 422


def test_object_type_error_creation_with_lifecycle_process_definition_1(
    session: Session, client: TestClient
):
    object_type = {
        "name": "tmo2",
        "lifecycle_process_definition": "asdasd:asdasd",
    }

    res = client.post(URL, json=object_type)
    assert res.status_code == 422


def test_object_type_error_creation_with_lifecycle_process_definition_3(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo2", "lifecycle_process_definition": ":123"}

    res = client.post(URL, json=object_type)
    assert res.status_code == 422


def test_object_type_error_creation_with_lifecycle_process_definition_4(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo2", "lifecycle_process_definition": "asdasd:"}

    res = client.post(URL, json=object_type)
    assert res.status_code == 422


def test_object_type_error_parent_id_not_exists(
    session: Session, client: TestClient
):
    object_type = {"name": "string", "p_id": 23}
    res = client.post(URL, json=object_type)
    assert res.status_code == 422


def test_object_type_points_constraint_by_tmo_creation(
    session: Session, client: TestClient
):
    object_type = {
        "name": "tmo_1111",
    }
    res = client.post(URL, json=object_type)
    assert res.status_code == 200
    tmo = session.exec(select(TMO).where(TMO.name == "tmo_1111")).first()[0]
    object_type = {"name": "tmo_2222", "points_constraint_by_tmo": [tmo.id]}
    res = client.post(URL, json=object_type)
    assert res.status_code == 200


def test_error_object_type_points_constraint_by_tmo_creation(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo_1111", "points_constraint_by_tmo": [1234]}
    res = client.post(URL, json=object_type)
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are TMO`s, which can't be used by constraint, "
        "because of they are not exists: {1234}"
    }


def test_read_created_object_type(session: Session, client: TestClient):
    tmo_id_first = session.exec(select(TMO.id)).first()
    res = client.get(URL + f"{tmo_id_first[0]}/")
    assert res.status_code == 200


def test_wrong_read_created_object_type(session: Session, client: TestClient):
    res = client.get(URL + "1232/")
    assert res.status_code == 404


def test_delete_object_type(session: Session, client: TestClient):
    res = client.delete(URL + "2/")
    tmo_exist = session.get(TMO, 2)

    assert not tmo_exist
    assert res.status_code == 200


def test_delete_object_type_linked_mos(session: Session, client: TestClient):
    TPRM_DEFAULT_DATA = {
        "name": "tprm",
        "tmo_id": 2,
        "val_type": "mo_link",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA))

    TPRM_DEFAULT_DATA_1 = {
        "name": "tprm_1",
        "tmo_id": 2,
        "val_type": "prm_link",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA_1))

    session.add(MO(tmo_id=1))
    session.add(MO(tmo_id=2))
    session.commit()

    session.add(PRM(mo_id=2, value=1, tprm_id=1))
    session.add(PRM(mo_id=2, value=1, tprm_id=2))
    session.commit()
    linked_param = session.execute(select(PRM).where(PRM.id == 1)).scalar()
    assert linked_param

    linked_param = session.execute(select(PRM).where(PRM.id == 2)).scalar()
    assert linked_param

    res = client.delete(URL + "1/")
    assert res.status_code == 200

    tmo = session.execute(select(TMO).where(TMO.id == 1)).scalar()
    assert not tmo

    linked_param = session.execute(select(PRM).where(PRM.id == 1)).scalar()
    assert not linked_param

    linked_param = session.execute(select(PRM).where(PRM.id == 2)).scalar()
    assert not linked_param


def test_delete_object_type_linked_mos_multiple(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA = {
        "name": "tmo_3",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TMO(**TMO_DEFAULT_DATA))

    TPRM_DEFAULT_DATA = {
        "name": "tprm",
        "tmo_id": 2,
        "val_type": "mo_link",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA))

    TPRM_DEFAULT_DATA_1 = {
        "name": "tprm_1",
        "tmo_id": 2,
        "val_type": "prm_link",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA_1))

    session.add(MO(tmo_id=1))
    session.add(MO(tmo_id=2))
    session.add(MO(tmo_id=3))
    session.commit()

    value_1 = pickle.dumps([1, 3]).hex()
    value_2 = pickle.dumps([1]).hex()
    session.add(PRM(mo_id=2, value=value_1, tprm_id=1))
    session.add(PRM(mo_id=2, value=value_2, tprm_id=2))
    session.add(MO(tmo_id=3))

    res = client.delete(URL + "1/")
    assert res.status_code == 200

    tmo = session.execute(select(TMO).where(TMO.id == 1)).scalar()
    assert not tmo

    linked_param = session.execute(select(PRM).where(PRM.id == 1)).scalar()
    assert pickle.loads(bytes.fromhex(linked_param.value)) == [3]


def test_delete_object_type_linked_mos_multiple_with_prm(
    session: Session, client: TestClient
):
    TMO_DEFAULT_DATA = {
        "name": "tmo_3",
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TMO(**TMO_DEFAULT_DATA))

    TPRM_DEFAULT_DATA = {
        "name": "tprm",
        "tmo_id": 2,
        "val_type": "mo_link",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA))

    TPRM_DEFAULT_DATA_1 = {
        "name": "tprm_1",
        "tmo_id": 2,
        "val_type": "prm_link",
        "multiple": True,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    session.add(TPRM(**TPRM_DEFAULT_DATA_1))

    session.add(MO(tmo_id=1))
    session.add(MO(tmo_id=2))
    session.add(MO(tmo_id=3))
    session.commit()

    value_1 = pickle.dumps([1]).hex()
    value_2 = pickle.dumps([1]).hex()
    session.add(PRM(mo_id=2, value=value_1, tprm_id=1))
    session.add(PRM(mo_id=2, value=value_2, tprm_id=2))
    session.add(MO(tmo_id=3))

    res = client.delete(URL + "1/")
    assert res.status_code == 200

    tmo = session.execute(select(TMO).where(TMO.id == 1)).scalar()
    assert not tmo

    linked_param = session.execute(select(PRM).where(PRM.id == 1)).scalar()
    assert not linked_param

    linked_param = session.execute(select(PRM).where(PRM.id == 2)).scalar()
    assert not linked_param


def test_delete_object_type_child_tmo(session: Session, client: TestClient):
    tmo_parent = TMO(
        **{
            "name": "parent",
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_parent)
    session.commit()

    tmo_parent_id = session.exec(
        select(TMO.id).where(TMO.name == "parent")
    ).first()

    tmo_child = TMO(
        **{
            "name": "child",
            "p_id": tmo_parent_id[0],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_child)
    session.commit()

    assert session.exec(select(TMO.id).where(TMO.name == "child")).first()

    res = client.delete(
        URL + f"{tmo_parent_id[0]}/", params={"delete_childs": True}
    )

    assert not session.exec(select(TMO.id).where(TMO.name == "child")).first()

    assert res.status_code == 200


def test_update_object_type(session: Session, client: TestClient):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()
    tprm_latitude = TPRM(
        **{
            "name": "tprm_latitude",
            "val_type": "float",
            "tmo_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm_latitude)

    tprm_longitude = TPRM(
        **{
            "name": "tprm_longitude",
            "val_type": "float",
            "tmo_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm_longitude)
    session.commit()

    data = {
        "version": 1,
        "latitude": 1,
        "longitude": 2,
    }

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    stmt = session.get(TMO, tmo_id[0])

    assert stmt.latitude == 1
    assert stmt.longitude == 2
    assert stmt.version == data["version"] + 1
    assert res.status_code == 200


def test_create_object_type_with_line_type(
    session: Session, client: TestClient
):
    # CREATE TMO WITH LINE_TYPE
    tmo_line_type = {
        "name": "new_tmo",
        "line_type": "some_type",
    }

    res = client.post(URL, json=tmo_line_type)
    assert res.status_code == 200
    assert "line_type" in res.json()

    tmo_exists = session.execute(
        select(TMO).where(TMO.name == "new_tmo")
    ).first()
    assert tmo_exists
    assert tmo_exists[0].line_type == "some_type"


def test_update_object_type_with_line_type(
    session: Session, client: TestClient
):
    tmo_line_type = {
        "version": 1,
        "line_type": "some_type",
    }

    res = client.patch(URL + "1", json=tmo_line_type)
    assert res.status_code == 200
    assert "line_type" in res.json()

    tmo_exists = session.execute(select(TMO).where(TMO.name == "tmo_1")).first()
    assert tmo_exists
    assert tmo_exists[0].line_type == "some_type"


def test_get_specific_object_type_with_line_type(
    session: Session, client: TestClient
):
    tmo_exists = session.exec(select(TMO).where(TMO.name == "tmo_1")).first()[0]
    tmo_exists.line_type = "string1"
    session.add(tmo_exists)
    session.commit()

    res = client.get(URL + "1")
    assert res.status_code == 200
    assert "line_type" in res.json()
    assert res.json()["line_type"] == "string1"


def test_get_object_type_with_line_type(session: Session, client: TestClient):
    tmo_exists = session.exec(select(TMO).where(TMO.name == "tmo_1")).first()[0]
    tmo_exists.line_type = "string1"
    session.add(tmo_exists)
    session.commit()

    res = client.get(
        "/api/inventory/v1/object_types/?object_types_ids=1&with_tprms=false"
    )
    assert res.status_code == 200
    assert "line_type" in res.json()[0]
    assert res.json()[0]["line_type"] == "string1"


def test_get_child_object_type_with_line_type(
    session: Session, client: TestClient
):
    new_tmo = TMO(
        **{
            "name": "new_tmo",
            "p_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "line_type": "some_type",
        }
    )
    session.add(new_tmo)
    session.commit()

    res = client.get("/api/inventory/v1/child_object_types/1/")
    assert res.status_code == 200
    assert "line_type" in res.json()[0]
    assert res.json()[0]["line_type"] == "some_type"


def test_get_breadcrumbs_object_type_with_line_type(
    session: Session, client: TestClient
):
    new_tmo = TMO(
        **{
            "name": "new_tmo",
            "p_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "line_type": "some_type",
        }
    )
    session.add(new_tmo)
    session.commit()

    res = client.get("/api/inventory/v1/breadcrumbs/1/")
    assert res.status_code == 200
    assert "line_type" in res.json()[0]
    assert res.json()[0]["line_type"] is None


def test_search_object_type_with_line_type(
    session: Session, client: TestClient
):
    new_tmo = TMO(
        **{
            "name": "new_tmo",
            "p_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "line_type": "some_type",
        }
    )
    session.add(new_tmo)
    session.commit()

    res = client.get("/api/inventory/v1/search_obj_types/?name=new_tmo")
    assert res.status_code == 200
    assert "line_type" in res.json()[0]
    assert res.json()[0]["line_type"] == "some_type"


def test_all_children_tmos_with_data_object_type_with_line_type(
    session: Session, client: TestClient
):
    new_tmo = TMO(
        **{
            "name": "new_tmo",
            "p_id": 1,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
            "line_type": "some_type",
        }
    )
    session.add(new_tmo)
    session.commit()

    res = client.get(
        "/api/inventory/v1/object_type/1/all_children_tmos_with_data?with_params=false"
    )
    assert res.status_code == 200
    assert "line_type" in res.json()[1]
    assert res.json()[1]["line_type"] == "some_type"


def test_update_error_object_type_p_id_not_exists(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "p_id": 123}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_with_lifecycle_process_definition(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": None}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)
    print(res.json())
    assert res.status_code == 200


# def test_update_with_lifecycle_process_definition_1(
#     session: Session, client: TestClient
# ):
#     tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()
#
#     data = {"version": 1, "lifecycle_process_definition": "some_str_data:123"}
#
#     res = client.patch(URL + f"{tmo_id[0]}/", json=data)
#
#     assert res.status_code == 200


def test_update_error_with_lifecycle_process_definition_2(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": "12312:123"}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_error_with_lifecycle_process_definition_3(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": "12312:123"}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_error_with_lifecycle_process_definition_4(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": " : "}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_error_with_lifecycle_process_definition_5(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": "asda: "}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_error_with_lifecycle_process_definition_6(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    data = {"version": 1, "lifecycle_process_definition": ":123"}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_update_error_object_type_primary_not_relevant(
    session: Session, client: TestClient
):
    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo_1")).first()

    tprm1 = TPRM(
        **{
            "name": "tprm1",
            "val_type": "str",
            "tmo_id": 1,
            "required": True,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tprm1)
    session.commit()

    data = {"version": 1, "primary": [234]}

    res = client.patch(URL + f"{tmo_id[0]}/", json=data)

    assert res.status_code == 422


def test_child_object_types(session: Session, client: TestClient):
    tmo_parent_id = session.exec(
        select(TMO.id).where(TMO.name == "tmo_1")
    ).first()
    tmo_child = TMO(
        **{
            "name": "child",
            "p_id": tmo_parent_id[0],
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_child)
    session.commit()

    res = client.get(
        f"/api/inventory/v1/child_object_types/{tmo_parent_id[0]}/"
    )
    assert res.json()
    assert res.status_code == 200


def test_child_object_types_without_exists_child_tmo(
    session: Session, client: TestClient
):
    tmo_parent_id = session.exec(
        select(TMO.id).where(TMO.name == "tmo_1")
    ).first()
    res = client.get(
        f"/api/inventory/v1/child_object_types/{tmo_parent_id[0]}/"
    )

    assert not res.json()
    assert res.status_code == 200


def test_child_object_types_error_with_not_relevant_tmo(
    session: Session, client: TestClient
):
    res = client.get(f"/api/inventory/v1/child_object_types/{1111}/")

    assert res.status_code == 404


def test_search_object_types(session: Session, client: TestClient):
    tmo_name = session.exec(select(TMO.name).where(TMO.name == "tmo_1")).first()
    res = client.get(
        "/api/inventory/v1/search_obj_types/", params={"name": tmo_name[0]}
    )

    assert res.status_code == 200


def test_search_object_types_error_name_not_exists(
    session: Session, client: TestClient
):
    res = client.get(
        "/api/inventory/v1/search_obj_types/",
        params={"name": "some_wrong_name"},
    )

    assert not res.json()
    assert res.status_code == 200


def test_search_object_types_name_near_to_realy_exists(
    session: Session, client: TestClient
):
    # In fact - there are no object type with the name 'tmo', but exist 'tmo_1'
    res = client.get(
        "/api/inventory/v1/search_obj_types/", params={"name": "tmo"}
    )
    assert res.json()[0]["name"] == "tmo_1"
    assert res.status_code == 200


def test_object_type_get_history_for_create_event(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo2"}
    client.post(URL, json=object_type)
    current_tmo = session.exec(
        select(TMO).where(TMO.name == object_type["name"])
    ).first()
    event = session.exec(
        select(Event).where(
            Event.model_id == current_tmo[0].id, Event.event_type == "TMOCreate"
        )
    ).first()

    assert event
    res = client.get(
        f"/api/inventory/v1/object_type/{current_tmo[0].id}/history"
    )
    assert res.status_code == 200


def test_object_type_get_history_for_update_event(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo_1"}
    current_tmo = session.exec(
        select(TMO).where(TMO.name == object_type["name"])
    ).first()
    print("SSSSSSSSSSSSSSSSSSSSS")
    print(current_tmo)
    object_type_for_update = {"version": 1, "name": "updated_name"}

    client.patch(URL + f"{current_tmo[0].id}/", json=object_type_for_update)

    event = session.exec(
        select(Event).where(
            Event.model_id == current_tmo[0].id, Event.event_type == "TMOUpdate"
        )
    ).first()
    assert event

    res = client.get(
        f"/api/inventory/v1/object_type/{current_tmo[0].id}/history"
    )
    assert res.status_code == 200


def test_object_type_get_history_response_for_create_and_update(
    session: Session, client: TestClient
):
    object_type = {"name": "tmo2"}
    client.post(URL, json=object_type)

    object_type_for_update = {"version": 1, "name": "updated_name"}

    current_tmo = session.exec(select(TMO).where(TMO.name == "tmo2")).first()

    client.patch(URL + f"{current_tmo[0].id}/", json=object_type_for_update)
    res = client.get(
        f"/api/inventory/v1/object_type/{current_tmo[0].id}/history"
    )

    required_attributes = [
        "id",
        "user",
        "event",
        "event_time",
        "event_type",
        "model_id",
    ]
    response_attributes = res.json()[0].keys()
    assert len(res.json()) == 2
    assert set(response_attributes) == set(required_attributes)


def test_object_type_get_history_error_dates_query_params(
    session: Session, client: TestClient
):
    current_tmo = session.exec(select(TMO).where(TMO.name == "tmo_1")).first()

    res = client.get(
        f"/api/inventory/v1/object_type/{current_tmo[0].id}/history",
        params={"date_from": 123, "date_to": "not_date_format"},
    )

    assert res.status_code == 422


def test_object_type_get_history_dates_query_params(
    session: Session, client: TestClient
):
    current_tmo = session.exec(select(TMO).where(TMO.name == "tmo_1")).first()

    res = client.get(
        f"/api/inventory/v1/object_type/{current_tmo[0].id}/history",
        params={
            "date_to": current_tmo[0].modification_date,
            "date_from": current_tmo[0].creation_date,
        },
    )

    assert res.status_code == 200


def test_incorrect_inherit_location(session: Session, client: TestClient):
    """
    Create parent TMO with geometry_type POINT
    Create TPRM for TMO long and lat. Add their to TMO as long and lat TPRM id
    Create child TMO with geometry_type POINT and false inherit_location without p_id
    Create parent MO with PRM for long and lat
    Create child MO with p_id
    Try to set child TMO inherit location to True will be expose error
    Set p_id for child TMO
    Set inherit_location in child TMO to True
    All data in childs MO will be correct
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

    # Create child MO
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


def test_update_value_for_inherit_location(
    session: Session, client: TestClient
):
    """Correct update inherit location for existed MO Bug AD-2277"""
    tmo_point_parent_name = "TMO_POINT_PARENT"
    tmo_point_child_name = "TMO_POINT_CHILD"
    tmo_target_name = "TMO_TARGET"
    tmo_point_geometry_type = "point"
    tmo_line_geometry_type = "line"
    tprm_lat_name = "TPRM_LATITUDE"
    tprm_lon_name = "TPRM_LONGITUDE"
    tprm_coord_type = "float"
    point_a_lat_value = 10
    point_a_lon_value = 20
    point_b_lat_value = 30
    point_b_lon_value = 40
    TMO_POINT_PARENT = {
        "name": tmo_point_parent_name,
        "geometry_type": tmo_point_geometry_type,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_point_parent = TMO(**TMO_POINT_PARENT)
    session.add(tmo_point_parent)
    session.commit()
    session.refresh(tmo_point_parent)
    TPRM_LAT = {
        "name": tprm_lat_name,
        "val_type": tprm_coord_type,
        "returnable": True,
        "tmo_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    TPRM_LON = {
        "name": tprm_lon_name,
        "val_type": tprm_coord_type,
        "returnable": True,
        "tmo_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lon = TPRM(**TPRM_LON)
    TMO_POINT_CHILD = {
        "name": tmo_point_child_name,
        "geometry_type": tmo_point_geometry_type,
        "p_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_point_child = TMO(**TMO_POINT_CHILD)
    TMO_TARGET = {
        "name": tmo_target_name,
        "geometry_type": tmo_line_geometry_type,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_target = TMO(**TMO_TARGET)
    session.add(tprm_lat)
    session.add(tprm_lon)
    session.add(tmo_point_child)
    session.add(tmo_target)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_point_child)
    session.refresh(tmo_target)
    session.refresh(tmo_point_parent)

    # Update TMO_PARENT with add lat/long tprm id
    data = {
        "version": tmo_point_parent.version,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
    }

    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point_parent.id}", json=data
    )
    assert res.status_code == 200

    # Create MO for parent
    data = {
        "tmo_id": tmo_point_parent.id,
        "params": [
            {"value": point_a_lat_value, "tprm_id": tprm_lat.id},
            {"value": point_a_lon_value, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent_point_a: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent_point_a.id)
    mo_parent_point_a_from_db: MO = session.execute(stmt).scalar()
    assert mo_parent_point_a_from_db.latitude == point_a_lat_value
    assert mo_parent_point_a_from_db.longitude == point_a_lon_value
    data = {
        "tmo_id": tmo_point_parent.id,
        "params": [
            {"value": point_b_lat_value, "tprm_id": tprm_lat.id},
            {"value": point_b_lon_value, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent_point_b: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent_point_b.id)
    mo_parent_point_b_from_db: MO = session.execute(stmt).scalar()
    assert mo_parent_point_b_from_db.latitude == point_b_lat_value
    assert mo_parent_point_b_from_db.longitude == point_b_lon_value

    # Create MO for child
    data = {
        "tmo_id": tmo_point_child.id,
        "p_id": mo_parent_point_a.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child_point_a: MO = MO(**res.json())
    stmt_mo_child_point_a = select(MO).where(MO.id == mo_child_point_a.id)
    mo_child_point_a_from_db: MO = session.execute(
        stmt_mo_child_point_a
    ).scalar()
    assert mo_child_point_a_from_db.latitude is None
    assert mo_child_point_a_from_db.longitude is None
    data = {
        "tmo_id": tmo_point_child.id,
        "p_id": mo_parent_point_b.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child_point_b: MO = MO(**res.json())
    stmt_mo_child_point_b = select(MO).where(MO.id == mo_child_point_b.id)
    mo_child_point_b_from_db: MO = session.execute(
        stmt_mo_child_point_b
    ).scalar()
    assert mo_child_point_b_from_db.latitude is None
    assert mo_child_point_b_from_db.longitude is None

    # Set inherit location to True on child TMO
    data = {"version": tmo_point_child.version, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point_child.id}", json=data
    )
    assert res.status_code == 200
    mo_child_point_a_from_db: MO = session.execute(
        stmt_mo_child_point_a
    ).scalar()
    assert mo_child_point_a_from_db.latitude == point_a_lat_value
    assert mo_child_point_a_from_db.longitude == point_a_lon_value
    mo_child_point_b_from_db: MO = session.execute(
        stmt_mo_child_point_b
    ).scalar()
    assert mo_child_point_b_from_db.latitude == point_b_lat_value
    assert mo_child_point_b_from_db.longitude == point_b_lon_value

    # Create MO for Target TMO
    data = {"tmo_id": tmo_target.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_target: MO = MO(**res.json())
    # Update MO with point_a, point_b added
    data = {
        "version": mo_target.version,
        "point_a_id": mo_child_point_a.id,
        "point_b_id": mo_child_point_b.id,
    }
    res = client.patch(f"api/inventory/v1/object/{mo_target.id}", json=data)
    assert res.status_code == 200
    stmt = select(MO).where(MO.id == mo_target.id)
    mo_target_from_db: MO = session.execute(stmt).scalar()
    assert mo_target_from_db.geometry == {
        "path": {
            "type": "LineString",
            "coordinates": [
                [float(point_a_lon_value), float(point_a_lat_value)],
                [float(point_b_lon_value), float(point_b_lat_value)],
            ],
        },
        "path_length": 3035.7289569056334,
    }


def test_create_new_object_with_inherit_location(
    session: Session, client: TestClient
):
    """Correct filling lat/long for object with parent_id  and TMO inherit location AD-2294"""
    tmo_point_parent_name = "TMO_POINT_PARENT"
    tmo_point_child_name = "TMO_POINT_CHILD"
    tmo_point_geometry_type = "point"
    tprm_lat_name = "TPRM_LATITUDE"
    tprm_lon_name = "TPRM_LONGITUDE"
    tprm_coord_type = "float"
    point_lat_value = 10
    point_lon_value = 20
    TMO_POINT_PARENT = {
        "name": tmo_point_parent_name,
        "geometry_type": tmo_point_geometry_type,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_point_parent = TMO(**TMO_POINT_PARENT)
    session.add(tmo_point_parent)
    session.commit()
    session.refresh(tmo_point_parent)
    TPRM_LAT = {
        "name": tprm_lat_name,
        "val_type": tprm_coord_type,
        "returnable": True,
        "tmo_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lat = TPRM(**TPRM_LAT)
    TPRM_LON = {
        "name": tprm_lon_name,
        "val_type": tprm_coord_type,
        "returnable": True,
        "tmo_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_lon = TPRM(**TPRM_LON)
    TMO_POINT_CHILD = {
        "name": tmo_point_child_name,
        "geometry_type": tmo_point_geometry_type,
        "p_id": tmo_point_parent.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_point_child = TMO(**TMO_POINT_CHILD)
    session.add(tprm_lat)
    session.add(tprm_lon)
    session.add(tmo_point_child)
    session.commit()
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_point_child)
    session.refresh(tmo_point_parent)

    # Update TMO_PARENT with add lat/long tprm id and set inherit_location
    data = {
        "version": tmo_point_parent.version,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
    }
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point_parent.id}", json=data
    )
    assert res.status_code == 200

    # Set inherit location to True on child TMO
    data = {"version": tmo_point_child.version, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_point_child.id}", json=data
    )
    assert res.status_code == 200
    # Create MO for parent
    data = {
        "tmo_id": tmo_point_parent.id,
        "params": [
            {"value": point_lat_value, "tprm_id": tprm_lat.id},
            {"value": point_lon_value, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_parent_point: MO = MO(**res.json())
    stmt = select(MO).where(MO.id == mo_parent_point.id)
    mo_parent_point_from_db: MO = session.execute(stmt).scalar()
    assert mo_parent_point_from_db.latitude == point_lat_value
    assert mo_parent_point_from_db.longitude == point_lon_value

    # Create MO for child
    data = {
        "tmo_id": tmo_point_child.id,
        "p_id": mo_parent_point.id,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_child_point: MO = MO(**res.json())
    stmt_mo_child_point_a = select(MO).where(MO.id == mo_child_point.id)
    mo_child_point_from_db: MO = session.execute(stmt_mo_child_point_a).scalar()
    assert mo_child_point_from_db.latitude == point_lat_value
    assert mo_child_point_from_db.longitude == point_lon_value


def test_formula_primary(session: Session, client: TestClient):
    """Add primary as formula tprm with inner max
    At first create int tprm, then create a couple MO
    Then change val_type TPRM to formula with inner max
    Then set a primary as formula tprm."""
    tmo_formula_name = "TMO_FORMULA"
    tprm_formula_name = "TPRM_FORMULA"
    tprm_formula_type_int = "int"
    tprm_formula_type_formula = "formula"
    mo_value_1 = 10
    mo_value_2 = 20
    formula_increment = 7
    formula_constraint = (
        f"INNER_MAX['{tprm_formula_name}'] + {formula_increment}"
    )
    TMO_FORMULA = {
        "name": tmo_formula_name,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tmo_formula = TMO(**TMO_FORMULA)
    session.add(tmo_formula)
    session.commit()
    session.refresh(tmo_formula)
    TPRM_FORMULA = {
        "name": tprm_formula_name,
        "val_type": tprm_formula_type_int,
        "returnable": True,
        "tmo_id": tmo_formula.id,
        "created_by": "Test creator",
        "modified_by": "Test modifier",
    }
    tprm_formula = TPRM(**TPRM_FORMULA)
    session.add(tprm_formula)
    session.commit()
    session.refresh(tprm_formula)
    session.refresh(tmo_formula)

    # Create MO
    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": str(mo_value_1), "tprm_id": tprm_formula.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_1: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_1.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(mo_value_1)
    assert mo_formula_1.name == str(mo_formula_1.id)

    data = {
        "tmo_id": tmo_formula.id,
        "params": [{"value": str(mo_value_2), "tprm_id": tprm_formula.id}],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_2: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_2.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(mo_value_2)
    assert mo_formula_2.name == str(mo_formula_2.id)

    # Change TPRM to Formula
    data = {
        "version": tprm_formula.version,
        "val_type": tprm_formula_type_formula,
        "force": True,
    }
    res = client.patch(
        f"api/inventory/v1/param_type/{tprm_formula.id}/change_val_type/",
        json=data,
    )
    assert res.status_code == 200
    tprm_formula_result: TPRM = TPRM(**res.json())
    assert tprm_formula_result.val_type == tprm_formula_type_formula

    # Add constraint
    data = {
        "version": tprm_formula_result.version,
        "constraint": formula_constraint,
        "force": True,
    }
    res = client.patch(
        f"api/inventory/v1/param_type/{tprm_formula.id}", json=data
    )
    assert res.status_code == 200
    tprm_formula_result: TPRM = TPRM(**res.json())
    assert tprm_formula_result.val_type == tprm_formula_type_formula
    assert tprm_formula_result.constraint == formula_constraint

    # Create Additional MO for check correct work INNER formula
    data = {"tmo_id": tmo_formula.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    mo_formula_3: MO = MO(**res.json())
    stmt = select(PRM).where(
        and_(PRM.mo_id == mo_formula_3.id, PRM.tprm_id == tprm_formula.id)
    )
    prm_formula_from_db: PRM = session.execute(stmt).scalars().first()
    assert prm_formula_from_db.value == str(
        max(mo_value_1, mo_value_2) + formula_increment
    )
    assert mo_formula_3.name == str(mo_formula_3.id)

    # Add required
    data = {
        "version": tprm_formula_result.version,
        "field_value": "",
        "required": True,
    }
    res = client.patch(
        f"api/inventory/v1/param_type/{tprm_formula.id}", json=data
    )
    assert res.status_code == 200
    tprm_formula_result: TPRM = TPRM(**res.json())

    # Add primary to TMO
    data = {"version": tmo_formula.version, "primary": [tprm_formula.id]}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_formula.id}", json=data
    )
    assert res.status_code == 200

    # Check mo names
    stmt = select(MO).where(MO.id == mo_formula_1.id)
    mo_formula_1_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_formula_1_from_db.name == str(mo_value_1)

    stmt = select(MO).where(MO.id == mo_formula_2.id)
    mo_formula_2_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_formula_2_from_db.name == str(mo_value_2)

    stmt = select(MO).where(MO.id == mo_formula_3.id)
    mo_formula_3_from_db: MO = session.execute(stmt).scalars().first()
    assert mo_formula_3_from_db.name == str(
        max(mo_value_1, mo_value_2) + formula_increment
    )


def test_set_inherit_location_for_incorrect_mo(
    session: Session, client: TestClient
):
    """We create parent TMO
    create child TMO with inherit location and p_id
    create grandChild TMO with p_id
    create MO for grandChild TMO without p_id
    Then set inherit_location for grandchild TMO didn't raise any Error.
    We must skip incorrect inheritance instead raise error."""
    tmo_parent_name = "TMO_PARENT"
    tmo_child_name = "TMO_CHILD"
    tmo_grandchild_name = "TMO_GRANDCHILD"
    tmo_geometry_type = "line"

    tmo_parent = TMO(
        **{
            "name": tmo_parent_name,
            "geometry_type": tmo_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_parent)
    session.commit()
    session.refresh(tmo_parent)

    tmo_child = TMO(
        **{
            "name": tmo_child_name,
            "geometry_type": tmo_geometry_type,
            "p_id": tmo_parent.id,
            "inherit_location": True,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_child)
    session.commit()
    session.refresh(tmo_child)

    tmo_grandchild = TMO(
        **{
            "name": tmo_grandchild_name,
            "geometry_type": tmo_geometry_type,
            "p_id": tmo_child.id,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    session.add(tmo_grandchild)
    session.commit()
    session.refresh(tmo_grandchild)
    session.refresh(tmo_child)
    session.refresh(tmo_parent)

    # Create MO
    data = {"tmo_id": tmo_grandchild.id, "params": []}
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200

    # Update TMO_GRANDCHILD
    data = {"version": tmo_grandchild.version, "inherit_location": True}
    res = client.patch(
        f"api/inventory/v1/object_type/{tmo_grandchild.id}", json=data
    )
    assert res.status_code == 200
