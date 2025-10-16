"""Tests for batch router"""

import csv
import io
import pickle
from datetime import datetime
from pprint import pprint

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from models import TMO, TPRM, MO, PRM

URL = "/api/inventory/v1/batch/object_and_param_values/"

TMO_DEFAULT_DATA = {
    "name": "Test TMO",
    "version": 1,
    "created_by": "Test admin",
    "modified_by": "Test admin",
}

TPRM_STR_DEFAULT_DATA = {
    "name": "Test str TPRM",
    "val_type": "int",
    "required": False,
    "created_by": "Test admin",
    "modified_by": "Test admin",
}

MO_DEFAULT_DATA = {"name": "Test MO"}

DEFAULT_FILE_DATA = [["pov", "geometry"], ['{"test": 1}', '{"test": 1}']]

MO_DEFAULT_DATA_FOR_TMO2 = {
    "name": "tprm_data1",
    "tmo_id": 2,
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

    # session.exec('pragma foreign_keys=ON')
    tmo = TMO(**TMO_DEFAULT_DATA)
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    session.add(tprm)
    session.commit()
    yield session


def generate_csv_in_memory(default_data=None):
    """Create csv in memory and returns it bytes representation"""
    if default_data is None:
        default_data = DEFAULT_FILE_DATA

    csv_string_file = io.StringIO()
    csv.writer(csv_string_file).writerows(default_data)
    csv_string_file.seek(0)

    file = io.BytesIO()
    file.write(csv_string_file.getvalue().encode())
    file.seek(0)
    file.name = "data.csv"
    return file


def test_post_object_and_param_values_success(
    session: Session, client: TestClient
):
    """TEST Successful POST request to the endpoint - 'object_and_param_values' return 201 status code."""
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory()
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    created_mo = session.execute(select(MO).where(MO.id == 1)).scalar()
    assert created_mo.pov == {"test": 1}
    assert created_mo.geometry == {"test": 1}
    assert res.status_code == 201


def test_post_object_and_param_values_error_tmo_not_exists(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' with not existing tmo_id raises
    error."""
    post_url = URL + "25"
    file = generate_csv_in_memory()
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {"detail": "Object type with id 25 not found."}


def test_post_object_and_param_values_error_not_allowed_content_type_1(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' with not allowed file content
    type."""

    file = io.BytesIO()
    file.write(bytes("some text", "utf-8"))
    file.seek(0)
    file.name = "data.png"

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 422
    assert "Content type" in res.json()["detail"]


def test_post_object_and_param_values_error_empty_file(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' with empty file."""

    default_data = [[]]

    file = generate_csv_in_memory(default_data)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {"detail": "No columns to parse from file"}


def test_post_object_and_param_values_error_not_unique_column_names(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are duplicated names
    of columns."""
    data_with_not_unique_column_names = [
        ["name", "name"],
        ["Test name", "Test name"],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "Column names must be unique. Not unique columns: ['name']."
    }


def test_post_object_and_param_values_error_mo_non_existent_attributes(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are names in the columns
    header that do not exist as MO attributes."""

    data_with_not_unique_column_names = [
        ["name1", "name2"],
        ["Test name", "Test name"],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are TPRM in header, which are not exists: ['name1', 'name2']"
    }


def test_post_object_and_param_values_error_p_id_not_exists(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are p_id column,
    and some value of this colum doesn`t exist in the database."""

    data_with_not_unique_column_names = [["p_id"], ["25"]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201


def test_post_object_and_param_values_error_p_id_exists_but_not_valid(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are p_id column,
    and all values of this colum exist in the database but one or more objects with id == p_id can`t be parents for
    current object. (TMO.p_id is used for make constrain  - what type of objects can be parent of objects of another
    type)"""

    stm = select(TMO)
    res = session.exec(stm).first()
    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = res.id
    session.add(mo)
    session.commit()
    session.refresh(mo)

    data_with_not_unique_column_names = [["p_id"], [mo.id]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201


def test_post_object_and_param_values_success_p_id_exists(
    session: Session, client: TestClient
):
    """TEST Doesn't raise error with POST request to the endpoint - 'object_and_param_values' if there is p_id column,
    and all values of this colum exist in the database and all objects with id == p_id  can be parents for
    a current object."""
    stm = select(TMO)
    parent_tmo = session.exec(stm).first()

    child_tmo = TMO(**TMO_DEFAULT_DATA)
    child_tmo.name = "Second TMO name"
    child_tmo.p_id = parent_tmo.id
    session.add(child_tmo)
    session.commit()
    session.refresh(child_tmo)

    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = parent_tmo.id
    session.add(mo)
    session.commit()
    session.refresh(mo)

    data_with_not_unique_column_names = [["p_id"], [mo.id]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{child_tmo.id}"
    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert session.execute(
        select(MO).where(MO.id == 2, MO.p_id == mo.id)
    ).scalar()
    assert res.status_code == 201


def test_post_object_and_param_values_error_point_a_id_not_exists(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are point_a_id column,
    and some value of this colum doesn't exist in the database."""

    data_with_not_unique_column_names = [["point_a_id"], ["25"]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422


def test_post_object_and_param_values_success_point_a_id_exists(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are point_a_id
    column, and all values of this colum exist in the database."""

    stm = select(TMO)
    res = session.exec(stm).first()
    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = res.id
    session.add(mo)
    session.commit()
    session.refresh(mo)

    data_with_not_unique_column_names = [["point_a_name"], [mo.name]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201


def test_post_object_and_param_values_success_point_a_id_exists_with_constraint(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are point_a_id
    column, and all values of this colum exist in the database and exists inside TMO from constraint."""
    stm = select(TMO)
    res = session.exec(stm).first()

    TMO_DEFAULT_DATA = {
        "name": "Test TMO 1111",
        "version": 1,
        "created_by": "Test admin",
        "modified_by": "Test admin",
        "points_constraint_by_tmo": [res.id],
    }

    tmo = TMO(**TMO_DEFAULT_DATA)
    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = res.id
    session.add(mo)
    session.add(tmo)
    session.commit()

    data_with_not_unique_column_names = [["point_a_name"], [mo.name]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)
    tmo = session.exec(select(TMO).where(TMO.name == "Test TMO 1111")).first()
    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201

    assert session.execute(
        select(MO).where(MO.id == 2, MO.point_a_id == mo.id)
    ).scalar()


def test_error_post_object_and_param_values_success_point_a_id_exists_with_constraint(
    session: Session, client: TestClient
):
    """TEST raise error with POST request to the endpoint - 'object_and_param_values' if there are point_a_id
    column, and all values of this colum exist in the database and not exists inside TMO from constraint."""
    stm = select(TMO)
    res = session.exec(stm).first()

    TMO_DEFAULT_DATA1 = {
        "name": "Test TMO 1111",
        "version": 1,
        "created_by": "Test admin",
        "modified_by": "Test admin",
        "points_constraint_by_tmo": [res.id],
    }

    TMO_DEFAULT_DATA2 = {
        "name": "Test TMO 2222",
        "version": 1,
        "created_by": "Test admin",
        "modified_by": "Test admin",
    }

    tmo1 = TMO(**TMO_DEFAULT_DATA1)
    tmo2 = TMO(**TMO_DEFAULT_DATA2)
    mo1 = MO(**{"name": "first_mo"})
    mo1.tmo_id = res.id
    session.add(mo1)
    session.add(tmo1)
    session.add(tmo2)
    session.commit()
    tmo2 = session.exec(select(TMO).where(TMO.name == "Test TMO 2222")).first()
    mo2 = MO(**{"name": "second_mo"})
    mo2.tmo_id = tmo2.id
    session.add(mo2)
    session.commit()
    mo2 = session.exec(select(MO).where(MO.name == "second_mo")).first()

    data_with_not_unique_column_names = [["point_a_name"], [mo2.name]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)
    tmo1 = session.exec(select(TMO).where(TMO.name == "Test TMO 1111")).first()
    post_url = URL + f"{tmo1.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    expected_res = {
        "detail": "There are error data in column point_a_name. Error statuses by "
        "index:{0: 'not_valid_value_by_constraint'}"
    }
    assert res.json() == expected_res


def test_post_object_and_param_values_error_point_b_id_not_exists(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are point_b_id column,
    and some value of this colum doesn`t exist in the database."""
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are point_a_id
     column, and all values of this colum exist in the database and exists inside TMO from constraint."""
    stm = select(TMO)
    res = session.exec(stm).first()

    TMO_DEFAULT_DATA = {
        "name": "Test TMO 1111",
        "version": 1,
        "created_by": "Test admin",
        "modified_by": "Test admin",
        "points_constraint_by_tmo": [res.id],
    }

    tmo = TMO(**TMO_DEFAULT_DATA)
    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = res.id
    session.add(mo)
    session.add(tmo)
    session.commit()

    data_with_not_unique_column_names = [
        ["point_a_name"],
        ["not exists object"],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)
    tmo = session.exec(select(TMO).where(TMO.name == "Test TMO 1111")).first()
    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422

    assert res.json() == {
        "detail": "There are error data in column point_a_name."
        " Error statuses by index:{0: 'not_valid_value_by_constraint'}"
    }


def test_post_object_and_param_values_success_point_b_id_exists(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are point_b_id
    column, and all values of this colum exist in the database."""
    stm = select(TMO)
    res = session.exec(stm).first()

    TMO_DEFAULT_DATA = {
        "name": "Test TMO 1111",
        "version": 1,
        "created_by": "Test admin",
        "modified_by": "Test admin",
        "points_constraint_by_tmo": [res.id],
    }

    tmo = TMO(**TMO_DEFAULT_DATA)
    mo = MO(**MO_DEFAULT_DATA)
    mo.tmo_id = res.id
    session.add(mo)
    session.add(tmo)
    session.commit()

    data_with_not_unique_column_names = [["point_b_name"], [mo.name]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)
    tmo = session.exec(select(TMO).where(TMO.name == "Test TMO 1111")).first()
    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201

    assert session.execute(
        select(MO).where(MO.id == 2, MO.point_b_id == mo.id)
    ).scalar()


def test_post_object_and_param_values_error_if_column_pov_data_not_valid(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are pov column,
    and some value of this colum does not valid to MO.pov type ."""
    data_with_not_unique_column_names = [["pov"], ["sdfsf"]]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column pov. Error statuses by index:{0: 'not_valid_attribute_value_type'}"
    }


def test_post_object_and_param_values_success_if_column_pov_data_is_empty(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are pov column,
    and some value of this colum is empty."""
    data_with_not_unique_column_names = [["pov"], [""]]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201
    assert session.execute(select(MO)).scalars().all() == []


def test_post_object_and_param_values_success_if_column_pov_data_is_valid(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are pov column,
    and some value of this colum is valid to MO.pov type."""
    data_with_not_unique_column_names = [["pov"], ['{"test": 1}']]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )

    exist_object = session.execute(select(MO).where(MO.id == 1)).scalar()

    assert exist_object.pov == {"test": 1}
    assert res.status_code == 201


def test_post_object_and_param_values_error_if_column_geometry_data_not_valid(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are geometry column,
    and some value of this colum does not valid to MO.geometry type ."""
    data_with_not_unique_column_names = [["geometry"], ["dfsf"]]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column geometry. Error statuses"
        " by index:{0: 'not_valid_attribute_value_type'}"
    }


def test_post_object_and_param_values_success_if_column_geometry_data_is_empty(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are geometry
    column, and some value of this colum is empty."""
    data_with_not_unique_column_names = [["geometry"], [""]]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201
    assert session.execute(select(MO)).scalars().all() == []


def test_post_object_and_param_values_success_if_column_geometry_data_is_valid(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are geometry
    column, and some value of this colum is valid to MO.geometry type."""
    data_with_not_unique_column_names = [["geometry"], ['{"test": 1}']]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    exist_object = session.execute(select(MO).where(MO.id == 1)).scalar()
    assert res.status_code == 201
    assert exist_object.geometry == {"test": 1}


def test_post_object_and_param_values_success_if_column_model_data_is_empty(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are model column,
    and some value of this colum is empty."""
    data_with_not_unique_column_names = [["model"], [""]]
    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201


def test_post_object_and_param_values_success_if_required_parameter_column_exists(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are required
    MO parameter column."""

    stm = select(TMO)
    res = session.exec(stm).first()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique name"
    tprm.required = True
    tprm.tmo_id = res.id
    session.add(tprm)
    session.commit()
    session.refresh(tprm)

    data_with_not_unique_column_names = [
        ["geometry", tprm.name],
        ['{"test": 1}', "1"],
    ]

    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    exist_parameter = session.execute(
        select(PRM).where(TPRM.id == tprm.id, PRM.value == "1")
    ).scalar()
    print(res.json())
    assert res.status_code == 201
    assert exist_parameter


def test_post_object_and_param_values_success_if_required_parameter_column_exists_1(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values' if there are required
    MO parameter column."""

    stm = select(TMO)
    res = session.exec(stm).first()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique name"
    tprm.required = True
    tprm.tmo_id = res.id
    session.add(tprm)
    session.commit()
    session.refresh(tprm)

    data_with_not_unique_column_names = [
        ["geometry", tprm.name],
        ['{"test": 1}', "1"],
    ]

    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    exist_parameter = session.execute(
        select(PRM).where(TPRM.id == tprm.id, PRM.value == "1")
    ).scalar()
    print(res.json())
    assert res.status_code == 201
    assert exist_parameter


def test_post_object_and_param_values_error_if_required_parameter_column_empty(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are required
    MO parameter column and at least one value of this colum is empty."""

    stm = select(TMO)
    res = session.exec(stm).first()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique name"
    tprm.required = True
    tprm.tmo_id = res.id
    session.add(tprm)
    session.commit()
    session.refresh(tprm)

    data_with_not_unique_column_names = [["geometry", tprm.name], ["", ""]]

    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column Unique name. Error statuses by index:{0: 'empty_values_in_required'}"
    }


def test_post_object_and_param_values_error_if_required_parameter_column_empty_1(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are required
    MO parameter column and at least one value of this colum is empty."""

    stm = select(TMO)
    res = session.exec(stm).first()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique name"
    tprm.tmo_id = res.id
    session.add(tprm)
    session.commit()
    session.refresh(tprm)

    data_with_not_unique_column_names = [["geometry", tprm.name], ["", ""]]

    post_url = URL + f"{res.id}"
    file = generate_csv_in_memory(
        default_data=data_with_not_unique_column_names
    )
    res = client.post(
        post_url,
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    assert session.execute(select(MO)).scalars().all() == []


def test_post_object_and_param_values_error_mo_non_existent_parameters(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are names in the columns
    header that do not exist as MO parameters."""

    data_with_not_unique_column_names = [["model", 2555], ["", "dsfs"]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are TPRM in header, which are not exists: {2555}"
    }


def test_post_object_and_param_values_error_mo_non_existent_parameters_1(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if there are names in the columns
    header that do not exist as MO parameters."""

    data_with_not_unique_column_names = [["model"], [""]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201


def test_post_object_and_param_values_error_not_valid_param_value_type(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if the parameter value type
    is invalid."""

    data_with_not_unique_column_names = [[""], ["sfs"]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    stm = select(TMO)
    res = session.exec(stm).first()
    post_url = URL + f"{res.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are TPRM in header, which are not exists: ['Unnamed: 0']"
    }


def test_post_object_and_param_values_error_if_tmo_virtual_is_true(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if TMO.virtual is true."""

    data_with_not_unique_column_names = [["1"], ["sfs"]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "Unique name"
    tmo.virtual = True
    session.add(tmo)
    session.commit()
    session.refresh(tmo)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 400
    assert res.json() == {
        "detail": "TMO (Unique name) is virtual. You can`t create objects with TMO.virtual equal to True."
    }


def test_post_object_and_param_values_error_primary_param_is_missing(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values' if the parameters  specified
    in the TMO.primary are missing ."""

    file = generate_csv_in_memory(DEFAULT_FILE_DATA)
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique TPRM name"
    tprm.required = True
    tprm.tmo_id = 1
    session.add(tprm)
    session.flush()

    stm = select(TMO)
    tmo = session.exec(stm).first()
    tmo.primary = [tprm.id]
    session.add(tprm)
    session.commit()

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are required TPRMs, that were not added: ['Unique TPRM name']"
    }


def test_post_object_and_param_values_error_primary_param_is_empty(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file but one or more values are empty ."""
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "Unique TPRM name"
    tprm.tmo_id = 1
    tprm.required = True
    session.add(tprm)
    session.flush()

    stm = select(TMO)
    tmo = session.exec(stm).first()
    tmo.primary = [tprm.id]
    session.add(tprm)
    session.commit()

    data_with_not_unique_column_names = [[str(tprm.name)], [""]]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column Unique TPRM name. "
        "Error statuses by index:{0: 'empty_values_in_required'}"
    }


def test_post_object_and_param_values_error_primary_param_and_global_uniqueness_duplicate_in_file(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file and TMO.global_uniqueness is True but one or more concatenation values of
    primary parameters values are non-unique in the context of the file."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tprm_def_data = dict()
    tprm_def_data.update(TPRM_STR_DEFAULT_DATA)
    tprm_def_data["tmo_id"] = tmo.id
    tprm_def_data["val_type"] = "str"

    tprm1 = TPRM(**tprm_def_data)
    tprm1.name = "Unique TPRM name"
    tprm1.required = True
    tprm2 = TPRM(**tprm_def_data)
    tprm2.name = "Unique2 TPRM name"

    session.add(tprm1)
    session.add(tprm2)
    session.flush()

    stm = select(TMO)
    tmo = session.exec(stm).first()
    tmo.primary = [tprm1.id, tprm2.id]
    session.add(tmo)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm1.name), str(tprm2.name)],
        ["wr", "sd"],
        ["wr", "sd"],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are duplicated object name in file: wr-sd"
    }


def test_post_object_and_param_values_error_primary_and_global_uniqueness_false_duplicate_in_db1(
    session: Session, client: TestClient
):
    """TEST POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file and TMO.global_uniqueness is False but one or more concatenation values of
    primary parameters values are non-unique in the scope of the same parent it the database."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tmo2 = TMO(**TMO_DEFAULT_DATA)
    tmo2.name = "Unique TMO name"
    tmo2.p_id = tmo.id
    tmo2.global_uniqueness = False
    session.add(tmo2)

    mo_parent = MO(**MO_DEFAULT_DATA)
    mo_parent.name = "mo_parent_name"
    mo_parent.tmo_id = tmo.id
    session.add(mo_parent)
    session.flush()

    # ad tprm and make it primary
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "parameter uses to create object name"
    tprm.val_type = "str"
    tprm.tmo_id = tmo2.id
    session.add(tprm)
    session.flush()

    tmo2.primary = [tprm.id]
    session.add(tmo2)

    mo_children = MO(**MO_DEFAULT_DATA)
    mo_children.name = "mo_children_name"
    mo_children.tmo_id = tmo2.id
    mo_children.p_id = mo_parent.id
    session.add(mo_children)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm.name), "p_id"],
        ["mo_children_name", mo_parent.id],
        ["test 2", ""],
        ["test 2", mo_parent.id],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo2.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201


def test_post_object_and_param_values_error_primary_and_global_uniqueness_false_duplicate_in_file1(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file and TMO.global_uniqueness is False but one or more concatenation values of
    primary parameters values are non-unique in the scope of the same parent it the file."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tmo2 = TMO(**TMO_DEFAULT_DATA)
    tmo2.name = "Unique TMO name"
    tmo2.p_id = tmo.id
    tmo2.global_uniqueness = False
    session.add(tmo2)

    mo_parent = MO(**MO_DEFAULT_DATA)
    mo_parent.name = "mo_parent_name"
    mo_parent.tmo_id = tmo.id
    session.add(mo_parent)
    session.flush()

    # ad tprm and make it primary

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "parameter uses to create object name"
    tprm.val_type = "str"
    tprm.tmo_id = tmo2.id
    session.add(tprm)
    session.flush()

    tmo2.primary = [tprm.id]
    session.add(tmo2)

    mo_children = MO(**MO_DEFAULT_DATA)
    mo_children.name = "mo_children_name"
    mo_children.tmo_id = tmo2.id
    mo_children.p_id = mo_parent.id
    session.add(mo_children)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm.name), "p_id"],
        ["mo_children_name1", mo_parent.id],
        ["test n", ""],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo2.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name-mo_children_name1",
            MO.p_id == mo_parent.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(MO.name == "test n", MO.p_id.is_(None))
    ).scalar()


def test_post_object_and_param_values_error_primary_and_global_uniqueness_false_duplicate_in_file2(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file and TMO.global_uniqueness is False but one or more concatenation values of
    primary parameters values are non-unique in the scope of the same parent it the file."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tmo2 = TMO(**TMO_DEFAULT_DATA)
    tmo2.name = "Unique TMO name"
    tmo2.p_id = tmo.id
    tmo2.global_uniqueness = False
    session.add(tmo2)

    mo_parent = MO(**MO_DEFAULT_DATA)
    mo_parent.name = "mo_parent_name"
    mo_parent.tmo_id = tmo.id
    session.add(mo_parent)
    session.flush()

    # ad tprm and make it primary

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "parameter uses to create object name"
    tprm.val_type = "str"
    tprm.tmo_id = tmo2.id
    session.add(tprm)
    session.flush()

    tmo2.primary = [tprm.id]
    session.add(tmo2)

    mo_children = MO(**MO_DEFAULT_DATA)
    mo_children.name = "mo_children_name"
    mo_children.tmo_id = tmo2.id
    mo_children.p_id = mo_parent.id
    session.add(mo_children)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm.name), "parent_name"],
        ["mo_children_name1", mo_parent.name],
        ["test n", mo_parent.name],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo2.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print()
    pprint(session.execute(select(MO)).scalars().all())
    assert res.status_code == 201
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name-mo_children_name1",
            MO.p_id == mo_parent.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name-test n", MO.p_id == mo_parent.id
        )
    ).scalar()


def test_post_object_and_param_values_primary_and_global_uniqueness_success(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values'  if the parameters
    specified in the TMO.primary are present in file and TMO.global_uniqueness is False but one or more
    concatenation values of primary parameters values are non-unique in the scope of the different parents it the
    db."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tmo2 = TMO(**TMO_DEFAULT_DATA)
    tmo2.name = "Unique TMO name"
    tmo2.p_id = tmo.id
    tmo2.global_uniqueness = False
    session.add(tmo2)

    mo_parent = MO(**MO_DEFAULT_DATA)
    mo_parent.name = "mo_parent_name"
    mo_parent.tmo_id = tmo.id
    session.add(mo_parent)

    mo_parent2 = MO(**MO_DEFAULT_DATA)
    mo_parent2.name = "mo_parent_name2"
    mo_parent2.tmo_id = tmo.id
    session.add(mo_parent2)

    session.flush()

    # ad tprm and make it primary

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "parameter uses to create object name"
    tprm.val_type = "str"
    tprm.tmo_id = tmo2.id
    session.add(tprm)
    session.flush()

    tmo2.primary = [tprm.id]
    session.add(tmo2)

    mo_children = MO(**MO_DEFAULT_DATA)
    mo_children.name = "mo_children_name"
    mo_children.tmo_id = tmo2.id
    mo_children.p_id = mo_parent.id
    session.add(mo_children)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm.name), "p_id"],
        ["mo_children_name1", mo_parent.id],
        ["mo_children_name1", mo_parent2.id],
        ["test n", ""],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo2.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name-mo_children_name1",
            MO.p_id == mo_parent.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name2-mo_children_name1",
            MO.p_id == mo_parent2.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(MO.name == "test n", MO.p_id.is_(None))
    ).scalar()


def test_post_object_and_param_values_primary_and_global_uniqueness_success_1(
    session: Session, client: TestClient
):
    """TEST Doesn`t raise error with POST request to the endpoint - 'object_and_param_values'  if the parameters
    specified in the TMO.primary are present in file and TMO.global_uniqueness is False but one or more
    concatenation values of primary parameters values are non-unique in the scope of the different parents it the
    db."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    tmo2 = TMO(**TMO_DEFAULT_DATA)
    tmo2.name = "Unique TMO name"
    tmo2.p_id = tmo.id
    tmo2.global_uniqueness = False
    session.add(tmo2)

    mo_parent = MO(**MO_DEFAULT_DATA)
    mo_parent.name = "mo_parent_name"
    mo_parent.tmo_id = tmo.id
    session.add(mo_parent)

    mo_parent2 = MO(**MO_DEFAULT_DATA)
    mo_parent2.name = "mo_parent_name2"
    mo_parent2.tmo_id = tmo.id
    session.add(mo_parent2)

    session.flush()

    # ad tprm and make it primary

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "parameter uses to create object name"
    tprm.val_type = "str"
    tprm.tmo_id = tmo2.id
    session.add(tprm)
    session.flush()

    tmo2.primary = [tprm.id]
    session.add(tmo2)

    mo_children = MO(**MO_DEFAULT_DATA)
    mo_children.name = "mo_children_name"
    mo_children.tmo_id = tmo2.id
    mo_children.p_id = mo_parent.id
    session.add(mo_children)
    session.commit()

    data_with_not_unique_column_names = [
        [str(tprm.name), "parent_name"],
        ["mo_children_name1", mo_parent.name],
        ["mo_children_name1", mo_parent2.name],
        ["test n", ""],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{tmo2.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name-mo_children_name1",
            MO.p_id == mo_parent.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(
            MO.name == "mo_parent_name2-mo_children_name1",
            MO.p_id == mo_parent2.id,
        )
    ).scalar()
    assert session.execute(
        select(MO).where(MO.name == "test n", MO.p_id.is_(None))
    ).scalar()


def test_mo_update(session: Session, client: TestClient):
    """
    Test for update object from csv file. Test updating mo, if mo name in csv file.
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.status = 3
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "str"

    status_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    status_tprm.name = "status_tprm"
    status_tprm.tmo_id = tmo.id
    status_tprm.val_type = "str"
    session.add(tprm)
    session.add(status_tprm)
    prm = PRM(tprm_id=2, mo_id=1, value="tprm_data1", version=1)
    session.add(prm)
    session.add(MO(**MO_DEFAULT_DATA_FOR_TMO2))

    session.commit()
    session.refresh(tprm)
    session.refresh(status_tprm)

    updated_mo: MO = session.execute(
        select(MO).where(MO.name == "tprm_data1")
    ).scalar()
    assert updated_mo.active is True
    assert updated_mo.status is None
    assert updated_mo.geometry is None
    assert updated_mo.pov is None

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, 1],
        ['{"test": 3}', '{"test": 5}', "new_object", True, 2],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "tprm_data1",
            MO.active.is_(False),
            MO.status == "1",
            MO.version == 2,
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.geometry == {"test": 2}

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "new_object", MO.active.is_(True), MO.status == "2"
        )
    ).scalar()
    assert created_mo.pov == {"test": 3}
    assert created_mo.geometry == {"test": 5}

    print(session.execute(select(PRM)).scalars().all())
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id,
            PRM.value == "tprm_data1",
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id,
            PRM.value == "new_object",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == status_tprm.id,
            PRM.value == "1",
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == status_tprm.id,
            PRM.value == "2",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()


def test_mo_update_with_create_new_mo(session: Session, client: TestClient):
    """
    Test for update object from csv file. Test if in csv file there are object, wasn't created in db.
    So we need to create it. Also, provided object, which was created before and need to be updated
    """

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.status = 3
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "str"
    session.add(tprm)
    status_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    status_tprm.name = "status_tprm"
    status_tprm.tmo_id = tmo.id
    status_tprm.val_type = "str"
    session.add(status_tprm)

    created_mo = MO(**MO_DEFAULT_DATA_FOR_TMO2)
    created_mo.pov = {"1": "test"}
    created_mo.geometry = {"2": "test111"}
    created_mo.status = "old_status"
    session.add(created_mo)
    prm = PRM(tprm_id=2, mo_id=1, value="tprm_data1", version=1)
    session.add(prm)
    prm = PRM(tprm_id=3, mo_id=1, value="old_status", version=1)
    session.add(prm)
    session.commit()

    updated_mo: MO = session.execute(
        select(MO).where(MO.name == "tprm_data1")
    ).scalar()
    assert updated_mo.active is True
    assert updated_mo.status == "old_status"
    assert updated_mo.geometry == {"2": "test111"}
    assert updated_mo.pov == {"1": "test"}

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, None],
        [
            '{"test": 3}',
            '{"test": 4}',
            "need_to_be_created",
            True,
            "new_status",
        ],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "tprm_data1", MO.active.is_(False), MO.version == 2
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.status is None
    assert updated_mo.geometry == {"test": 2}

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "need_to_be_created",
            MO.active.is_(True),
            MO.status == "new_status",
        )
    ).scalar()
    assert created_mo.pov == {"test": 3}
    assert created_mo.geometry == {"test": 4}

    print(session.execute(select(PRM)).scalars().all())
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id,
            PRM.value == "tprm_data1",
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id,
            PRM.value == "need_to_be_created",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()

    assert not session.execute(
        select(PRM).where(
            PRM.tprm_id == status_tprm.id, PRM.mo_id == updated_mo.id
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == status_tprm.id,
            PRM.value == "new_status",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()


def test_mo_update_with_exists_param(session: Session, client: TestClient):
    """
    Test for update object from csv file. Test if there are mo in csv file with exists params.
    We need to left them, if they were created
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.global_uniqueness = False
    tmo.latitude = 3
    tmo.longitude = 4
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "int"
    session.add(tprm)

    latitude_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    latitude_tprm.name = "latitude_tprm"
    latitude_tprm.tmo_id = tmo.id
    latitude_tprm.val_type = "float"
    session.add(latitude_tprm)

    longitude_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    longitude_tprm.name = "longitude_tprm"
    longitude_tprm.tmo_id = tmo.id
    longitude_tprm.val_type = "float"
    session.add(longitude_tprm)
    old_mo = MO(**MO_DEFAULT_DATA_FOR_TMO2)
    old_mo.name = "1"
    session.add(old_mo)
    session.add(PRM(**{"mo_id": 1, "tprm_id": 2, "value": "1"}))
    session.commit()

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        [
            "pov",
            "geometry",
            "Test str TPRM",
            "active",
            "latitude_tprm",
            "longitude_tprm",
        ],
        ['{"test": 1}', None, "1", True, 43.234234, 62.23423],
        ['{"test": 2}', None, "3", True, 41.567568, 67.131231],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "1",
            MO.active.is_(True),
            MO.latitude == 43.234234,
            MO.longitude == 62.23423,
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.geometry is None

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "3",
            MO.active.is_(True),
            MO.latitude == 41.567568,
            MO.longitude == 67.131231,
        )
    ).scalar()
    assert created_mo.pov == {"test": 2}
    assert created_mo.geometry is None

    pprint(session.execute(select(PRM)).scalars().all())
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.value == "1", PRM.mo_id == updated_mo.id
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.value == "3", PRM.mo_id == created_mo.id
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.mo_id == updated_mo.id,
            PRM.value == "43.234234",
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.mo_id == created_mo.id,
            PRM.value == "41.567568",
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.mo_id == updated_mo.id,
            PRM.value == "62.23423",
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.mo_id == created_mo.id,
            PRM.value == "67.131231",
        )
    ).scalar()


def test_mo_update_error_wrong_value_active(
    session: Session, client: TestClient
):
    """
    Test for an update object from csv file. Check active and status value types.
    Active must be True/False, and status string type
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.val_type = "str"
    session.add(tprm)

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "multi_str"
    tprm.tmo_id = tmo.id
    tprm.multiple = True
    tprm.val_type = "str"
    session.add(tprm)

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "multi_datetime"
    tprm.tmo_id = tmo.id
    tprm.multiple = True
    tprm.val_type = "datetime"
    session.add(tprm)

    session.add(PRM(**{"mo_id": 1, "tprm_id": 2, "value": "tprm_data1"}))

    session.add(MO(**MO_DEFAULT_DATA_FOR_TMO2))
    session.commit()

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        ["Test str TPRM", "multi_str", "multi_datetime"],
        [
            "tprm_data1",
            ["some_1", "some_2"],
            ["2022-04-27T14:55:19.000000Z", "2022-04-27 14:55:19"],
        ],
        [
            "new_object",
            ["some_3", "some_4"],
            ["2002-02-04T14:55:19.000000Z", "2003-01-21 13:52:25"],
        ],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 201

    updated_mo: MO = session.execute(
        select(MO).where(MO.name == "tprm_data1", MO.active.is_(True))
    ).scalar()
    assert updated_mo

    created_mo: MO = session.execute(
        select(MO).where(MO.name == "new_object", MO.active.is_(True))
    ).scalar()
    assert created_mo

    pprint(session.execute(select(PRM)).scalars().all())
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 2,
            PRM.value == "tprm_data1",
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 2,
            PRM.value == "new_object",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.value == pickle.dumps(["some_1", "some_2"]).hex(),
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.value == PRM.value == pickle.dumps(["some_3", "some_4"]).hex(),
            PRM.mo_id == created_mo.id,
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.value
            == pickle.dumps(
                ["2022-04-27T14:55:19.000000Z", "2022-04-27 14:55:19"]
            ).hex(),
            PRM.mo_id == updated_mo.id,
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.value
            == PRM.value
            == pickle.dumps(
                ["2002-02-04T14:55:19.000000Z", "2003-01-21 13:52:25"]
            ).hex(),
            PRM.mo_id == created_mo.id,
        )
    ).scalar()


def test_mo_update_error_wrong_value_active_1(
    session: Session, client: TestClient
):
    """
    Test for an update object from csv file. Check active and status value types.
    Active must be True/False, and status string type
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.val_type = "str"
    session.add(tprm)

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "multi_str"
    tprm.tmo_id = tmo.id
    tprm.multiple = True
    tprm.required = True
    tprm.val_type = "str"
    session.add(tprm)

    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.name = "multi_datetime"
    tprm.tmo_id = tmo.id
    tprm.multiple = True
    tprm.val_type = "datetime"
    session.add(tprm)

    session.add(PRM(**{"mo_id": 1, "tprm_id": 2, "value": "tprm_data1"}))

    session.add(MO(**MO_DEFAULT_DATA_FOR_TMO2))
    session.commit()

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        ["Test str TPRM", "multi_str", "multi_datetime"],
        [
            "tprm_data1",
            ["some_1", "some_2"],
            ["2022-04-27T14:55:19.000000Z", "2022-04-27 14:55:19"],
        ],
        [
            "new_object",
            None,
            ["2002-02-04T14:55:19.000000Z", "2003-01-21 13:52:25"],
        ],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column multi_str. Error statuses by index:{1: 'empty_values_in_required'}"
    }


def test_mo_create_without_tmo_primary(session: Session, client: TestClient):
    """
    Test for update object from csv file. If mo in csv file doesn't exist - just create without update.
    So TMO should not have primary
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = True
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.val_type = "str"
    session.add(tprm)
    session.commit()

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active"],
        ['{"test": 1}', None, "tprm_data1", "0"],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.status_code == 201

    print(session.execute(select(MO)).scalars().all())
    created_mo: MO = session.execute(
        select(MO).where(MO.name == "1", MO.id == 1, MO.active.is_(False))
    ).scalar()
    assert created_mo

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 2,
            PRM.value == "tprm_data1",
            PRM.mo_id == created_mo.id,
        )
    ).scalar()


def test_update_object_error_object_name_exist_in_another_tmo(
    session: Session, client: TestClient
):
    """TEST Raises error with POST request to the endpoint - 'object_and_param_values'  if the parameters  specified
    in the TMO.primary are present in file and TMO.global_uniqueness is True but one or more concatenation values of
    primary parameters values are non-unique in the context of MO.names in database."""

    stm = select(TMO)
    tmo = session.exec(stm).first()

    mo = MO(**MO_DEFAULT_DATA)
    mo.name = "parent_object"
    mo.tmo_id = tmo.id
    session.add(mo)
    session.commit()

    child_tmo = {
        "name": "child TMO",
        "version": 1,
        "p_id": tmo.id,
        "global_uniqueness": False,
        "created_by": "Test admin",
        "modified_by": "Test admin",
    }
    child_tmo = TMO(**child_tmo)

    string_tprm = {
        "name": "Test int TPRM",
        "val_type": "int",
        "required": True,
        "tmo_id": 2,
        "created_by": "Test admin",
        "modified_by": "Test admin",
    }
    string_tprm = TPRM(**string_tprm)

    new_mo = MO(name="2", tmo_id=2)
    new_prm = PRM(value="1", tprm_id=2, mo_id=2)
    session.add_all([child_tmo, string_tprm, new_mo, new_prm])
    session.commit()
    data_with_not_unique_column_names = [
        ["Test int TPRM", "active", "parent_name"],
        ["123", "FaLSe", "parent_object"],
    ]

    file = generate_csv_in_memory(data_with_not_unique_column_names)

    post_url = URL + f"{2}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 201

    print(session.execute(select(MO)).scalars().all())
    parent_mo: MO = session.execute(
        select(MO).where(MO.name == "parent_object", MO.id == 1)
    ).scalar()
    assert parent_mo

    already_exists: MO = session.execute(
        select(MO).where(MO.name == "2", MO.id == 2, MO.active.is_(True))
    ).scalar()
    assert already_exists

    created_mo: MO = session.execute(
        select(MO).where(MO.name == "3", MO.id == 3, MO.active.is_(False))
    ).scalar()
    assert created_mo

    assert session.execute(
        select(PRM).where(PRM.tprm_id == 2, PRM.value == "123", PRM.mo_id == 3)
    ).scalar()


def test_mo_update_with_exists_param_1(session: Session, client: TestClient):
    """
    Test for update object from csv file. Test if there are mo in csv file with exists params.
    We need to left them, if they were created
    """
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.global_uniqueness = False
    tmo.latitude = 3
    tmo.longitude = 4
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "int"
    session.add(tprm)

    latitude_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    latitude_tprm.name = "latitude_tprm"
    latitude_tprm.tmo_id = tmo.id
    latitude_tprm.val_type = "float"
    session.add(latitude_tprm)

    longitude_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    longitude_tprm.name = "longitude_tprm"
    longitude_tprm.tmo_id = tmo.id
    longitude_tprm.val_type = "float"
    session.add(longitude_tprm)
    old_mo = MO(**MO_DEFAULT_DATA_FOR_TMO2)
    old_mo.latitude = 31.1
    old_mo.longitude = 51.1
    old_mo.name = "1"
    session.add(old_mo)
    session.add(PRM(**{"mo_id": 1, "tprm_id": 2, "value": "1"}))
    session.add(PRM(**{"mo_id": 1, "tprm_id": 3, "value": "31.1"}))
    session.add(PRM(**{"mo_id": 1, "tprm_id": 4, "value": "51.1"}))
    session.commit()

    tmo_id = session.exec(select(TMO.id).where(TMO.name == "tmo2")).first()

    post_url = URL + f"{tmo_id}"

    file_data = [
        [
            "pov",
            "geometry",
            "Test str TPRM",
            "active",
            "latitude_tprm",
            "longitude_tprm",
        ],
        ['{"test": 1}', None, "1", True, 43.234234, 62.23423],
        ['{"test": 2}', None, "3", True, 41.567568, 67.131231],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "1",
            MO.active.is_(True),
            MO.latitude == 43.234234,
            MO.longitude == 62.23423,
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.geometry is None

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "3",
            MO.active.is_(True),
            MO.latitude == 41.567568,
            MO.longitude == 67.131231,
        )
    ).scalar()
    assert created_mo.pov == {"test": 2}
    assert created_mo.geometry is None

    pprint(session.execute(select(PRM)).scalars().all())
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.value == "1", PRM.mo_id == updated_mo.id
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == tprm.id, PRM.value == "3", PRM.mo_id == created_mo.id
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.mo_id == updated_mo.id,
            PRM.value == "43.234234",
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 3,
            PRM.mo_id == created_mo.id,
            PRM.value == "41.567568",
        )
    ).scalar()

    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.mo_id == updated_mo.id,
            PRM.value == "62.23423",
        )
    ).scalar()
    assert session.execute(
        select(PRM).where(
            PRM.tprm_id == 4,
            PRM.mo_id == created_mo.id,
            PRM.value == "67.131231",
        )
    ).scalar()


def test_successful_post_with_check_true_with_data_for_new_mo(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for not existing MO - returns info how many MO will be created"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.json()["will_be_created_mo"] == 1


def test_successful_post_with_check_true_with_data_for_new_mo_and_it_prm(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for not existing MO and data for MO PRMs - returns info how many MO will be created
    and how many PRMs will be created"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", "2", False],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.json()["will_be_created_parameter_values"] == 2


def test_successful_post_with_check_true_with_data_for_existing_mo_data_does_not_changed(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for existing MO but this data equals to data in the database -
    returns response with 0 for all counters:
    {'will_be_updated_mo': 0,
    'will_be_created_mo': 0,
    'will_be_updated_parameter_values': 0,
    'will_be_created_parameter_values': 0,
    'will_be_deleted_parameter_values': 0}"""
    expected = {
        "will_be_updated_mo": 0,
        "will_be_created_mo": 0,
        "will_be_updated_parameter_values": 0,
        "will_be_created_parameter_values": 0,
        "will_be_deleted_parameter_values": 0,
    }

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="2")
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", "2", False],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )

    assert res.json() == expected


def test_successful_post_with_check_true_with_new_data_for_existing_mo(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for existing MO and new data not equals to the database data -
    returns response with amount for all counters"""

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"

    tprm_3 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_3.name = "Test tprm name 3"
    tprm_3.tmo_id = tmo.id
    tprm_3.required = False
    tprm_3.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.add(tprm_3)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 12},
        name="tprm_data1",
        geometry={"test": 21},
        active=True,
        status=1,
        tmo_id=tmo.id,
    )
    # 3 changes in attrs

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="3")
    # 1 change in tprm
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, tprm_3.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", "2", "4", False],
    ]
    # 1 create tprm
    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )

    expected = {
        "will_be_updated_mo": 1,
        "will_be_created_mo": 0,
        "will_be_updated_parameter_values": 4,
        "will_be_created_parameter_values": 1,
        "will_be_deleted_parameter_values": 0,
    }
    assert res.json() == expected


def test_successful_post_with_check_true_with_new_data_for_existing_mo_attr_data_empty(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for attr is empty - shows how many parameters will be updated"""

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="2")
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, "active"],
        ['{"test": 1}', "", "tprm_data1", "2", "False"],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    expected = {
        "will_be_updated_mo": 1,
        "will_be_created_mo": 0,
        "will_be_updated_parameter_values": 0,
        "will_be_created_parameter_values": 0,
        "will_be_deleted_parameter_values": 1,
    }
    assert res.json() == expected


def test_successful_post_with_check_true_with_new_data_for_existing_mo_prm_data_empty(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - shows how many parameters will be deleted"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="2")
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", "", "False"],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    expected = {
        "will_be_updated_mo": 1,
        "will_be_created_mo": 0,
        "will_be_updated_parameter_values": 0,
        "will_be_created_parameter_values": 0,
        "will_be_deleted_parameter_values": 1,
    }
    assert res.json() == expected


#  STAR OF CHECK FALSE
def test_successful_post_with_check_false_with_new_data_for_existing_mo_attr_data_empty(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for attr is empty - changes MO attr data to default"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=True,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="2")
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name],
        ['{"test": 1}', "", "tprm_data1", "2"],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201

    stmt = select(MO).where(MO.name == "tprm_data1")
    updated_mo_from_db = session.exec(stmt).first()
    print()
    pprint(updated_mo_from_db)
    assert updated_mo_from_db.name == "tprm_data1"
    assert updated_mo_from_db.pov == {"test": 1}
    assert updated_mo_from_db.geometry is None
    assert updated_mo_from_db.status == "1"
    assert updated_mo_from_db.version == 2


def test_successful_post_with_check_false_with_new_data_for_existing_mo_prm_data_empty(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - deletes this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="2")
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name],
        [
            '{"test": 1}',
            '{"test": 2}',
            "tprm_data1",
            "",
        ],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201
    stmt = select(PRM).where(PRM.id == prm_2.id)
    deleted_prm = session.exec(stmt).first()
    assert not deleted_prm


def test_enum_tprm(session: Session, client: TestClient):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - deletes this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "enum"
    tprm_2.constraint = '[123, 321, "string_value"]'
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_2.id]
    session.add(tmo)
    session.commit()
    session.refresh(tmo)

    file_data = [
        [tprm_1.name, tprm_2.name],
        [
            "some_string",
            321,
        ],
    ]

    file = generate_csv_in_memory(file_data)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 201
    created_str_param = session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "some_string")
    ).first()
    created_enum_param = session.execute(
        select(PRM).where(PRM.id == 2, PRM.value == "321")
    ).first()
    assert created_str_param
    assert created_enum_param

    created_mo = session.execute(
        select(MO).where(MO.id == 1, MO.name == "321")
    ).first()
    assert created_mo


def test_enum_tprm_not_valid_constraint(session: Session, client: TestClient):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - deletes this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "enum"
    tprm_2.constraint = '[123, "string_value"]'
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_2.id]
    session.add(tmo)
    session.commit()
    session.refresh(tmo)

    file_data = [
        [tprm_1.name, tprm_2.name],
        [
            "some_string",
            321,
        ],
    ]

    file = generate_csv_in_memory(file_data)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column Test tprm name 2."
        " Error statuses by index:{0: 'not_valid_value_by_constraint'}"
    }


def test_enum_tprm_multiple(session: Session, client: TestClient):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - deletes this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.multiple = True
    tprm_2.val_type = "enum"
    tprm_2.constraint = '[123, 321, "string_value"]'
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    session.add(tmo)
    session.commit()
    session.refresh(tmo)

    file_data = [
        [tprm_1.name, tprm_2.name],
        [
            "some_string",
            ["string_value", "123"],
        ],
    ]

    file = generate_csv_in_memory(file_data)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 201
    created_str_param = session.execute(
        select(PRM).where(PRM.id == 1, PRM.value == "some_string")
    ).first()
    created_enum_param = session.execute(
        select(PRM).where(
            PRM.id == 2,
            PRM.value == pickle.dumps(["string_value", "123"]).hex(),
        )
    ).first()
    assert created_str_param
    assert created_enum_param


def test_enum_multiple_tprm_not_valid_constraint(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO and new data not equals to the database data -
    and new data for MO PRMs is empty - deletes this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.multiple = True
    tprm_2.val_type = "enum"
    tprm_2.constraint = '[321, "string_value"]'
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    session.add(tmo)
    session.commit()
    session.refresh(tmo)

    file_data = [
        [tprm_1.name, tprm_2.name],
        [
            "some_string",
            ["string_value", "123"],
        ],
    ]

    file = generate_csv_in_memory(file_data)

    post_url = URL + f"{tmo.id}"

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )

    print(res.json())
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column Test tprm name 2."
        " Error statuses by index:{0: 'not_valid_value_by_constraint'}"
    }


def test_successful_post_with_check_false_can_update_prms(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO PRMs and new data not equals to the database data -
    and new data for MO PRMs is not empty - updates this PRMs"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)

    prm_2_value_before = "2"
    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value=prm_2_value_before)
    session.add(prm_1)
    session.add(prm_2)
    session.commit()
    prm_2_value_after = "2342342"

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, "active"],
        [
            '{"test": 1}',
            '{"test": 2}',
            "tprm_data1",
            prm_2_value_after,
            "False",
        ],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    stmt = select(PRM).where(PRM.id == prm_2.id)
    deleted_prm = session.exec(stmt).first()
    assert deleted_prm.value == prm_2_value_after


def test_successful_post_with_check_false_can_create_prms_for_existing_mo(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is False -
    if there is MO data in file for existing MO but for not existing PRMs of this MO -
    creates PRMs for this MO"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"

    tprm_3 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_3.name = "Test tprm name 3"
    tprm_3.tmo_id = tmo.id
    tprm_3.required = False
    tprm_3.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.add(tprm_3)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov={"test": 1},
        name="tprm_data1",
        geometry={"test": 2},
        active=False,
        status=1,
        tmo_id=tmo.id,
    )

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value="3")
    # 1 change in tprm
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, tprm_3.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", "3", "4", False],
    ]
    # 1 create tprm
    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    stmt = select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm_3.id)
    new_prm = session.exec(stmt).first()
    assert new_prm is not None
    assert new_prm.value == "4"


def test_successful_post_with_check_true_does_not_change_data(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True - does not change data"""
    pov_attr_before = {"test": 1}
    prm_2_before = "3"
    prm_2_in_change_file = "2"

    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)

    tprm_2 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_2.name = "Test tprm name 2"
    tprm_2.tmo_id = tmo.id
    tprm_2.required = False
    tprm_2.val_type = "str"

    tprm_3 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_3.name = "Test tprm name 3"
    tprm_3.tmo_id = tmo.id
    tprm_3.required = False
    tprm_3.val_type = "str"
    session.add(tprm_1)
    session.add(tprm_2)
    session.add(tprm_3)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    mo = MO(
        pov=pov_attr_before,
        name="tprm_data1",
        geometry={"test": 21},
        active=True,
        status=1,
        tmo_id=tmo.id,
    )
    # 3 changes in attrs

    session.add(mo)
    session.commit()
    session.refresh(mo)
    print(mo)

    prm_1 = PRM(tprm_id=tprm_1.id, mo_id=mo.id, value="tprm_data1")
    prm_2 = PRM(tprm_id=tprm_2.id, mo_id=mo.id, value=prm_2_before)
    # 1 change in tprm
    session.add(prm_1)
    session.add(prm_2)
    session.commit()

    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, tprm_2.name, tprm_3.name, "active"],
        [
            '{"test": 12}',
            '{"test": 2}',
            "tprm_data1",
            prm_2_in_change_file,
            "4",
            False,
        ],
    ]
    # 1 create tprm
    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201
    session.refresh(mo)
    assert mo.pov == pov_attr_before
    session.refresh(prm_2)
    assert prm_2.value == prm_2_before
    session.refresh(tprm_3)
    assert tprm_3 is not None

    stmt = select(PRM).where(PRM.mo_id == mo.id, PRM.tprm_id == tprm_3.id)
    new_prm = session.exec(stmt).first()
    assert new_prm is None


# def test_test_multiple_mo_link(session: Session, client: TestClient):
#     parent_tmo = session.get(TMO, 1)
#
#     parent_object = MO(name="parameter_object", tmo_id=parent_tmo.id)
#     session.add(parent_object)
#     # parent_object_1 = MO(name='parameter_object_1', tmo_id=parent_tmo.id)
#     session.add(parent_object)
#     session.commit()
#
#     child_tmo = TMO(
#         **{
#             "name": "child_tmo",
#             "version": 1,
#             "p_id": 1,
#             "global_uniqueness": False,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#
#     tprm_mo_link = TPRM(
#         **{
#             "name": "tprm_mo_link",
#             "val_type": "mo_link",
#             "tmo_id": 2,
#             "multiple": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#
#     tprm_prm_link = TPRM(
#         **{
#             "name": "tprm_prm_link",
#             "val_type": "prm_link",
#             "tmo_id": 2,
#             "multiple": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#             "constraint": 2,
#         }
#     )
#
#     tprm_string = TPRM(
#         **{
#             "name": "tprm_string",
#             "val_type": "str",
#             "tmo_id": 2,
#             "required": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#     child_tmo.primary = [4]
#
#     parent_object = MO(name="2", tmo_id=parent_tmo.id)
#
#     mo_link_parameter = PRM(value=pickle.dumps([2]).hex(), mo_id=2, tprm_id=2)
#     session.add_all(
#         [
#             child_tmo,
#             tprm_mo_link,
#             tprm_prm_link,
#             parent_object,
#             mo_link_parameter,
#             tprm_string,
#         ]
#     )
#     session.commit()
#     post_url = URL + f"{2}"
#
#     file_data = [["tprm_string", "tprm_prm_link"], ["object_name", [2]]]
#     # 1 create tprm
#     file = generate_csv_in_memory(file_data)
#
#     res = client.post(
#         post_url,
#         data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
#         files={"file": file},
#     )
#     print(res.json())
#     assert res.status_code == 201
#
#     created_mo = session.execute(
#         select(MO).where(MO.name == "object_name")
#     ).scalar()
#     assert created_mo
#     print()
#     pprint(session.execute(select(PRM)).scalars().all())
#     assert session.execute(
#         select(PRM).where(
#             PRM.mo_id == created_mo.id,
#             PRM.value == pickle.dumps([1]).hex(),
#             PRM.tprm_id == 3,
#         )
#     ).scalar()


# def test_test_multiple_not_exists_mo_link(session: Session, client: TestClient):
#     parent_tmo = session.get(TMO, 1)
#
#     parent_object = MO(name="parameter_object", tmo_id=parent_tmo.id)
#     session.add(parent_object)
#     # parent_object_1 = MO(name='parameter_object_1', tmo_id=parent_tmo.id)
#     session.add(parent_object)
#     session.commit()
#
#     child_tmo = TMO(
#         **{
#             "name": "child_tmo",
#             "version": 1,
#             "p_id": 1,
#             "global_uniqueness": False,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#
#     tprm_mo_link = TPRM(
#         **{
#             "name": "tprm_mo_link",
#             "val_type": "mo_link",
#             "tmo_id": 2,
#             "multiple": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#
#     tprm_prm_link = TPRM(
#         **{
#             "name": "tprm_prm_link",
#             "val_type": "prm_link",
#             "tmo_id": 2,
#             "multiple": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#             "constraint": 2,
#         }
#     )
#
#     tprm_string = TPRM(
#         **{
#             "name": "tprm_string",
#             "val_type": "str",
#             "tmo_id": 2,
#             "required": True,
#             "created_by": "Test admin",
#             "modified_by": "Test admin",
#         }
#     )
#     child_tmo.primary = [4]
#
#     parent_object = MO(name="2", tmo_id=parent_tmo.id)
#
#     session.add_all(
#         [child_tmo, tprm_mo_link, tprm_prm_link, parent_object, tprm_string]
#     )
#     session.commit()
#     post_url = URL + f"{2}"
#
#     file_data = [["tprm_string", "tprm_prm_link"], ["object_name", [1]]]
#     # 1 create tprm
#     file = generate_csv_in_memory(file_data)
#
#     res = client.post(
#         post_url,
#         data={"filename": "sdf", "check": False, "type": "multipart/form-data"},
#         files={"file": file},
#     )
#
#     assert res.status_code == 422
#     assert res.json() == {
#         "detail": "There are error data in column tprm_prm_link. Error statuses by index:{0: 'not_exists_objects'}"
#     }


def test_batch_import_with_inherit_location_geometry_line(
    session: Session, client: TestClient
):
    point_a_lat_val = 25
    point_a_lon_val = 52
    point_b_lat_val = point_a_lat_val + 10
    point_b_lon_val = point_a_lon_val + 10
    tmo_for_line_parent_name = "TMO LINE PARENT"
    tmo_for_line_child_name = "TMO LINE CHILD"
    tmo_for_line_geometry_type = "line"

    tmo_for_point_name = "TMO POINT"

    tprm_latitude_name = "TPRM LAT"
    tprm_longitude_name = "TPRM LON"
    tprm_latitude_type = tprm_longitude_type = "float"

    geometry_1 = {
        "path": [
            [point_a_lon_val, point_a_lat_val],
            [88.8, 88.8],
            [point_b_lon_val, point_b_lat_val],
        ],
        "path_length": 111.111,
    }
    geometry_2 = {
        "path": [
            [point_b_lon_val, point_b_lat_val],
            [77.7, 77.7],
            [point_a_lon_val, point_a_lat_val],
        ],
        "path_length": 111.111,
    }

    tmo_line_parent = TMO(
        **{
            "name": tmo_for_line_parent_name,
            "geometry_type": tmo_for_line_geometry_type,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    # Default tmo geometry type is Point
    tmo_point = TMO(
        **{
            "name": tmo_for_point_name,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )

    session.add(tmo_line_parent)
    session.add(tmo_point)
    session.commit()
    session.refresh(tmo_line_parent)
    session.refresh(tmo_point)
    tmo_child = TMO(
        **{
            "name": tmo_for_line_child_name,
            "inherit_location": True,
            "geometry_type": tmo_for_line_geometry_type,
            "p_id": tmo_line_parent.id,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tprm_lat = TPRM(
        **{
            "name": tprm_latitude_name,
            "val_type": tprm_latitude_type,
            "returnable": True,
            "tmo_id": tmo_point.id,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )
    tprm_lon = TPRM(
        **{
            "name": tprm_longitude_name,
            "val_type": tprm_longitude_type,
            "returnable": True,
            "tmo_id": tmo_point.id,
            "created_by": "Test creator",
            "modified_by": "Test modifier",
        }
    )

    session.add(tmo_child)
    session.add(tprm_lat)
    session.add(tprm_lon)
    session.commit()
    session.refresh(tmo_child)
    session.refresh(tprm_lat)
    session.refresh(tprm_lon)
    session.refresh(tmo_line_parent)
    session.refresh(tmo_point)

    data = {
        "version": tmo_point.version,
        "latitude": tprm_lat.id,
        "longitude": tprm_lon.id,
    }
    res = client.patch(
        f"/api/inventory/v1/object_type/{tmo_point.id}", json=data
    )
    print(res.json())
    assert res.status_code == 200
    tmo_point: TMO = TMO(**res.json())

    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": point_a_lat_val, "tprm_id": tprm_lat.id},
            {"value": point_a_lon_val, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    print(res.json())
    assert res.status_code == 200
    point_a: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_point.id,
        "params": [
            {"value": point_b_lat_val, "tprm_id": tprm_lat.id},
            {"value": point_b_lon_val, "tprm_id": tprm_lon.id},
        ],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    point_b: MO = MO(**res.json())
    data = {
        "tmo_id": tmo_line_parent.id,
        "point_a_id": point_a.id,
        "point_b_id": point_b.id,
        "geometry": geometry_1,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    parent_mo_1: MO = MO(**res.json())
    assert parent_mo_1.geometry == geometry_1

    data = {
        "tmo_id": tmo_line_parent.id,
        "point_a_id": point_b.id,
        "point_b_id": point_a.id,
        "geometry": geometry_2,
        "params": [],
    }
    res = client.post("api/inventory/v1/object_with_parameters/", json=data)
    assert res.status_code == 200
    parent_mo_2: MO = MO(**res.json())
    assert parent_mo_2.geometry == geometry_2

    file_data = [["p_id"], [parent_mo_1.id], [parent_mo_2.id]]
    file = generate_csv_in_memory(file_data)

    post_url = URL + f"{tmo_child.id}"
    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.status_code == 201


@pytest.mark.skip(reason="Too big test")
def test_global_test_for_create(session: Session, client: TestClient):
    parent_tmo = TMO(
        **{
            "name": "Parent TMO",
            "version": 1,
            "created_by": "Test admin",
            "modified_by": "Test admin",
        }
    )
    session.add(parent_tmo)

    session.commit()

    parent_tmo = session.execute(
        select(TMO).where(TMO.name == "Parent TMO")
    ).scalar()
    main_tmo = TMO(
        **{
            "name": "Main TMO",
            "version": 1,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "global_uniqueness": False,
            "p_id": parent_tmo.id,
        }
    )
    session.add(main_tmo)
    session.commit()

    PARENT_TPRM_STR_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_parent_str",
            "val_type": "str",
            "required": False,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    PARENT_TPRM_MULTIPLE_STR_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_parent_str_mult",
            "val_type": "str",
            "required": False,
            "multiple": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    mo_0 = MO(name="parent_object_0", tmo_id=parent_tmo.id, active=True)
    mo_1 = MO(name="parent_object_1", tmo_id=parent_tmo.id, active=True)
    mo_2 = MO(name="parent_object_2", tmo_id=parent_tmo.id, active=True)
    mo_3 = MO(name="parent_object_3", tmo_id=parent_tmo.id, active=True)
    session.add_all(
        [
            mo_0,
            mo_1,
            mo_2,
            mo_3,
            PARENT_TPRM_STR_DEFAULT_DATA,
            PARENT_TPRM_MULTIPLE_STR_DEFAULT_DATA,
        ]
    )
    session.commit()

    parent_tprm_str = session.execute(
        select(TPRM).where(TPRM.name == "Test_parent_str")
    ).scalar()
    parent_tprm_multiple_str = session.execute(
        select(TPRM).where(TPRM.name == "Test_parent_str_mult")
    ).scalar()

    mo_0 = session.execute(
        select(MO).where(MO.name == "parent_object_0")
    ).scalar()  # 8
    mo_1 = session.execute(
        select(MO).where(MO.name == "parent_object_1")
    ).scalar()  # 9
    mo_2 = session.execute(
        select(MO).where(MO.name == "parent_object_2")
    ).scalar()  # 10
    mo_3 = session.execute(
        select(MO).where(MO.name == "parent_object_3")
    ).scalar()  # 11

    prm_0 = PRM(value="some_value_1", tprm_id=parent_tprm_str.id, mo_id=mo_0.id)
    prm_1 = PRM(value="some_value_2", tprm_id=parent_tprm_str.id, mo_id=mo_1.id)
    prm_2 = PRM(
        value=pickle.dumps(["some_value_3", "some_value_4"]).hex(),
        tprm_id=parent_tprm_multiple_str.id,
        mo_id=mo_2.id,
    )
    prm_3 = PRM(
        value=pickle.dumps(["some_value_5", "some_value_6"]).hex(),
        tprm_id=parent_tprm_multiple_str.id,
        mo_id=mo_3.id,
    )
    session.add_all([prm_0, prm_1, prm_2, prm_3])
    session.commit()

    prm_0 = session.execute(
        select(PRM).where(
            PRM.tprm_id == parent_tprm_str.id, PRM.mo_id == mo_0.id
        )
    ).scalar()  # 1
    prm_1 = session.execute(
        select(PRM).where(
            PRM.tprm_id == parent_tprm_str.id, PRM.mo_id == mo_1.id
        )
    ).scalar()  # 2
    prm_2 = session.execute(
        select(PRM).where(
            PRM.tprm_id == parent_tprm_multiple_str.id, PRM.mo_id == mo_2.id
        )
    ).scalar()  # 3
    prm_3 = session.execute(
        select(PRM).where(
            PRM.tprm_id == parent_tprm_multiple_str.id, PRM.mo_id == mo_3.id
        )
    ).scalar()  # 4

    main_tmo = session.execute(
        select(TMO).where(TMO.name == "Main TMO")
    ).scalar()

    TPRM_STR_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_str",
            "val_type": "str",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": "^(?=.*[A-Z])[A-Za-z0-9]+$",
        }
    )

    TPRM_INT_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_int",
            "val_type": "int",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    TPRM_INT_MULTIPLE_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_int_mult",
            "val_type": "int",
            "multiple": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": "1:50000",
        }
    )

    TPRM_LAT_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_latitude",
            "val_type": "float",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": "1:50000",
        }
    )

    TPRM_LONG_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_longitude",
            "val_type": "float",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": "1:50000",
        }
    )

    TPRM_STATUS_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_status",
            "val_type": "str",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    TPRM_PRIMARY_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_primary",
            "val_type": "str",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": "^(?=.*[A-Z])[A-Za-z0-9]+$",
        }
    )

    TPRM_MO_LINK_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_primary_mo_link",
            "val_type": "mo_link",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    TPRM_PRM_LINK_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_prm_link",
            "val_type": "prm_link",
            "required": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": str(parent_tprm_str.id),
        }
    )

    TPRM_MO_LINK_MULTIPLE_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_mo_link_mult",
            "val_type": "mo_link",
            "required": True,
            "multiple": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
        }
    )

    TPRM_PRM_LINK_MULTIPLE_DEFAULT_DATA = TPRM(
        **{
            "name": "Test_prm_link_mult",
            "val_type": "prm_link",
            "required": True,
            "multiple": True,
            "created_by": "Test admin",
            "modified_by": "Test admin",
            "tmo_id": main_tmo.id,
            "constraint": str(parent_tprm_str.id),
        }
    )

    session.add_all(
        [
            TPRM_LAT_DEFAULT_DATA,
            TPRM_LONG_DEFAULT_DATA,
            TPRM_STATUS_DEFAULT_DATA,
            TPRM_PRIMARY_DEFAULT_DATA,
            TPRM_STR_DEFAULT_DATA,
            TPRM_INT_DEFAULT_DATA,
            TPRM_MO_LINK_DEFAULT_DATA,
            TPRM_PRM_LINK_DEFAULT_DATA,
            TPRM_INT_MULTIPLE_DEFAULT_DATA,
            TPRM_MO_LINK_MULTIPLE_DEFAULT_DATA,
            TPRM_PRM_LINK_MULTIPLE_DEFAULT_DATA,
        ]
    )

    session.commit()

    lat_tprm_id = (
        session.execute(select(TPRM).where(TPRM.name == "Test_latitude"))
        .scalar()
        .id
    )

    long_tprm_id = (
        session.execute(select(TPRM).where(TPRM.name == "Test_longitude"))
        .scalar()
        .id
    )

    status_tprm_id = (
        session.execute(select(TPRM).where(TPRM.name == "Test_status"))
        .scalar()
        .id
    )

    primary_tprm_id = (
        session.execute(
            select(TPRM.id).where(
                TPRM.name.in_(
                    [
                        "Test_str",
                        "Test_int",
                        "Test_primary",
                        "Test_primary_mo_link",
                    ]
                )
            )
        )
        .scalars()
        .all()
    )
    print(primary_tprm_id)
    main_tmo.latitude = lat_tprm_id
    main_tmo.longitude = long_tprm_id
    main_tmo.status = status_tprm_id
    main_tmo.primary = primary_tprm_id
    session.add(main_tmo)
    session.commit()
    session.flush()

    main_tmo = session.execute(
        select(TMO).where(TMO.name == "Main TMO")
    ).scalar()

    already_exists_object = MO(
        name="parent_object_0-already_exists_object",
        tmo_id=main_tmo.id,
        active=True,
    )
    session.add(already_exists_object)
    session.commit()

    file_data_with_parents = [
        [
            "Test_str",
            "Test_int",
            "Test_int_mult",
            "Test_latitude",
            "Test_longitude",
            "Test_status",
            "Test_primary",
            "Test_primary_mo_link",
            "Test_prm_link",
            "Test_prm_link_mult",
            "Test_mo_link_mult",
            "parent_name",
            "pov",
            "geometry",
        ],
    ]

    row_1 = [
        "PrimaryValue1",
        1,
        [5, 6],
        42.123123,
        73.456456,
        "SomeValue1",
        "SomeValue1",
        "parent_object_0",
        1,
        [2],
        ["parent_object_0", "parent_object_1"],
        "parent_object_0",
        '{"1":2}',
        '{"4":3}',
    ]

    file_data_with_parents.append(row_1)

    row_2 = [
        "PrimaryValue2",
        2,
        [5, 6],
        43.123123,
        72.456456,
        "SomeValue2",
        "SomeValue2",
        "parent_object_1",
        2,
        [1, 2],
        ["parent_object_2", "parent_object_3"],
        "parent_object_1",
        '{"1":2}',
        '{"4":3}',
    ]

    file_data_with_parents.append(row_2)

    start = datetime.now()
    print("start", start)
    file = generate_csv_in_memory(file_data_with_parents)
    res = client.post(
        URL + f"{main_tmo.id}",
        data={"filename": file.name, "type": "multipart/form-data"},
        files={"file": file},
    )
    print("RESULT", datetime.now() - start)
    print(res.json())
    assert res.status_code == 201
    assert res.json() == {
        "detail": "File is valid. Objects will be created soon",
        "status": "ok",
    }

    pprint(session.execute(select(MO)).scalars().all())
    first_obj = session.execute(
        select(MO).where(
            MO.name
            == "parent_object_0-SomeValue1-PrimaryValue1-1-parent_object_0",
            MO.p_id == 1,
            MO.latitude == 42.123123,
            MO.longitude == 73.456456,
            MO.status == "SomeValue1",
        )
    ).scalar()
    assert first_obj
    assert first_obj.pov == {"1": 2}
    assert first_obj.geometry == {"4": 3}

    second_obj = session.execute(
        select(MO).where(
            MO.name
            == "parent_object_1-SomeValue2-PrimaryValue2-2-parent_object_1",
            MO.p_id == 2,
            MO.latitude == 43.123123,
            MO.longitude == 72.456456,
            MO.status == "SomeValue2",
        )
    ).scalar()
    assert second_obj
    assert second_obj.pov == {"1": 2}
    assert second_obj.geometry == {"4": 3}

    print()
    print()
    print()
    pprint(session.execute(select(PRM)).scalars().all())
    all_exists_params = session.execute(select(PRM)).scalars().all()

    expected_params = [
        PRM(
            tprm_id=2,
            value="some_value_1",
            version=1,
            mo_id=1,
            backward_link=None,
            id=1,
        ),
        PRM(
            tprm_id=2,
            value="some_value_2",
            version=1,
            mo_id=2,
            backward_link=None,
            id=2,
        ),
        PRM(
            tprm_id=3,
            value="80049523000000000000005d94288c0c736f6d655f76616c75655f33948c0c736f6d655f76616c75655f3494652e",
            version=1,
            mo_id=3,
            backward_link=None,
            id=3,
        ),
        PRM(
            tprm_id=3,
            value="80049523000000000000005d94288c0c736f6d655f76616c75655f35948c0c736f6d655f76616c75655f3694652e",
            version=1,
            mo_id=4,
            backward_link=None,
            id=4,
        ),
        PRM(
            tprm_id=8,
            value="PrimaryValue1",
            version=1,
            mo_id=6,
            backward_link=None,
            id=5,
        ),
        PRM(tprm_id=9, value="1", version=1, mo_id=6, backward_link=None, id=6),
        PRM(
            tprm_id=12,
            value="80049509000000000000005d94284b054b06652e",
            version=1,
            mo_id=6,
            backward_link=None,
            id=7,
        ),
        PRM(
            tprm_id=4,
            value="42.123123",
            version=1,
            mo_id=6,
            backward_link=None,
            id=8,
        ),
        PRM(
            tprm_id=5,
            value="73.456456",
            version=1,
            mo_id=6,
            backward_link=None,
            id=9,
        ),
        PRM(
            tprm_id=6,
            value="SomeValue1",
            version=1,
            mo_id=6,
            backward_link=None,
            id=10,
        ),
        PRM(
            tprm_id=7,
            value="SomeValue1",
            version=1,
            mo_id=6,
            backward_link=None,
            id=11,
        ),
        PRM(
            tprm_id=10, value="1", version=1, mo_id=6, backward_link=None, id=12
        ),
        PRM(
            tprm_id=11, value="1", version=1, mo_id=6, backward_link=None, id=13
        ),
        PRM(
            tprm_id=14,
            value="80049506000000000000005d944b02612e",
            version=1,
            mo_id=6,
            backward_link=None,
            id=14,
        ),
        PRM(
            tprm_id=13,
            value="80049509000000000000005d94284b014b02652e",
            version=1,
            mo_id=6,
            backward_link=None,
            id=15,
        ),
        PRM(
            tprm_id=8,
            value="PrimaryValue2",
            version=1,
            mo_id=7,
            backward_link=None,
            id=16,
        ),
        PRM(
            tprm_id=9, value="2", version=1, mo_id=7, backward_link=None, id=17
        ),
        PRM(
            tprm_id=12,
            value="80049509000000000000005d94284b054b06652e",
            version=1,
            mo_id=7,
            backward_link=None,
            id=18,
        ),
        PRM(
            tprm_id=4,
            value="43.123123",
            version=1,
            mo_id=7,
            backward_link=None,
            id=19,
        ),
        PRM(
            tprm_id=5,
            value="72.456456",
            version=1,
            mo_id=7,
            backward_link=None,
            id=20,
        ),
        PRM(
            tprm_id=6,
            value="SomeValue2",
            version=1,
            mo_id=7,
            backward_link=None,
            id=21,
        ),
        PRM(
            tprm_id=7,
            value="SomeValue2",
            version=1,
            mo_id=7,
            backward_link=None,
            id=22,
        ),
        PRM(
            tprm_id=10, value="2", version=1, mo_id=7, backward_link=None, id=23
        ),
        PRM(
            tprm_id=11, value="2", version=1, mo_id=7, backward_link=None, id=24
        ),
        PRM(
            tprm_id=14,
            value="80049509000000000000005d94284b014b02652e",
            version=1,
            mo_id=7,
            backward_link=None,
            id=25,
        ),
        PRM(
            tprm_id=13,
            value="80049509000000000000005d94284b034b04652e",
            version=1,
            mo_id=7,
            backward_link=None,
            id=26,
        ),
    ]
    assert all_exists_params == expected_params


def test_force_create_objects(session: Session, client: TestClient):
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.status = 3
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "str"

    status_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    status_tprm.name = "status_tprm"
    status_tprm.tmo_id = tmo.id
    status_tprm.val_type = "str"
    session.add(tprm)
    session.add(status_tprm)
    prm = PRM(tprm_id=2, mo_id=1, value="tprm_data1", version=1)
    session.add(prm)
    session.add(MO(**MO_DEFAULT_DATA_FOR_TMO2))

    session.commit()
    session.refresh(tprm)
    session.refresh(status_tprm)

    updated_mo: MO = session.execute(
        select(MO).where(MO.name == "tprm_data1")
    ).scalar()
    assert updated_mo.active is True
    assert updated_mo.status is None
    assert updated_mo.geometry is None
    assert updated_mo.pov is None

    # FORCE REQUEST
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, 1],
        ['{"test": 3}', '{"test": 5}', "new_object", True, 2],
        ['{"test": 3}', "invalid_value", "new_object", True, 2],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "force": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201
    print()

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "tprm_data1",
            MO.active.is_(False),
            MO.status == "1",
            MO.version == 2,
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.geometry == {"test": 2}
    assert len(session.execute(select(MO)).scalars().all()) == 2

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "new_object", MO.active.is_(True), MO.status == "2"
        )
    ).scalar()
    assert created_mo.pov == {"test": 3}
    assert created_mo.geometry == {"test": 5}
    assert len(session.execute(select(PRM)).scalars().all()) == 4

    # NOT FORCE REQUEST
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, 1],
        ['{"test": 3}', '{"test": 5}', "new_object", True, 2],
        ['{"test": 3}', "invalid_value", "new_object", True, 2],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column geometry. "
        "Error statuses by index:{2: 'not_valid_attribute_value_type'}"
    }


def test_force_update_objects(session: Session, client: TestClient):
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.primary = [2]
    tmo.status = 3
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()
    tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm.tmo_id = tmo.id
    tprm.required = True
    tprm.val_type = "str"

    status_tprm = TPRM(**TPRM_STR_DEFAULT_DATA)
    status_tprm.name = "status_tprm"
    status_tprm.tmo_id = tmo.id
    status_tprm.val_type = "str"
    session.add(tprm)
    session.add(status_tprm)
    prm = PRM(tprm_id=2, mo_id=1, value="tprm_data1", version=1)
    session.add(prm)
    session.add(MO(**MO_DEFAULT_DATA_FOR_TMO2))

    session.commit()
    session.refresh(tprm)
    session.refresh(status_tprm)

    updated_mo: MO = session.execute(
        select(MO).where(MO.name == "tprm_data1")
    ).scalar()
    assert updated_mo.active is True
    assert updated_mo.status is None
    assert updated_mo.geometry is None
    assert updated_mo.pov is None

    # FORCE REQUEST
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, 1],
        ['{"test": 3}', '{"test": 5}', "new_object", True, 2],
        ['{"test": 3}', "invalid_value", "new_object", True, 2],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "force": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 201
    print()

    updated_mo: MO = session.execute(
        select(MO).where(
            MO.name == "tprm_data1",
            MO.active.is_(False),
            MO.status == "1",
            MO.version == 2,
        )
    ).scalar()
    assert updated_mo.pov == {"test": 1}
    assert updated_mo.geometry == {"test": 2}
    assert len(session.execute(select(MO)).scalars().all()) == 2

    created_mo: MO = session.execute(
        select(MO).where(
            MO.name == "new_object", MO.active.is_(True), MO.status == "2"
        )
    ).scalar()
    assert created_mo.pov == {"test": 3}
    assert created_mo.geometry == {"test": 5}
    assert len(session.execute(select(PRM)).scalars().all()) == 4

    # NOT FORCE REQUEST
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", "Test str TPRM", "active", "status_tprm"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False, 1],
        ['{"test": 3}', '{"test": 5}', "new_object", True, 2],
        ['{"test": 3}', "invalid_value", "new_object", True, 2],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "type": "multipart/form-data"},
        files={"file": file},
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": "There are error data in column geometry. "
        "Error statuses by index:{2: 'not_valid_attribute_value_type'}"
    }


def test_successful_post_with_check_true_with_data_for_new_mo_with_force(
    session: Session, client: TestClient
):
    """TEST With successful POST request if 'check' is True -
    if there is MO data in file for not existing MO - returns info how many MO will be created"""
    tmo = TMO(**TMO_DEFAULT_DATA)
    tmo.name = "tmo2"
    tmo.global_uniqueness = False
    session.add(tmo)
    session.flush()

    tprm_1 = TPRM(**TPRM_STR_DEFAULT_DATA)
    tprm_1.tmo_id = tmo.id
    tprm_1.required = True
    tprm_1.val_type = "str"
    session.add(tprm_1)
    session.flush()

    tmo.primary = [tprm_1.id]

    session.commit()
    session.refresh(tmo)

    # WITHOUT FORCE
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={"filename": "sdf", "check": True, "type": "multipart/form-data"},
        files={"file": file},
    )
    print(res.json())
    assert res.json() == {
        "will_be_updated_mo": 0,
        "will_be_created_mo": 1,
        "will_be_created_parameter_values": 1,
        "will_be_updated_parameter_values": 0,
        "will_be_deleted_parameter_values": 0,
    }

    # WITH FORCE
    post_url = URL + f"{tmo.id}"

    file_data = [
        ["pov", "geometry", tprm_1.name, "active"],
        ['{"test": 1}', '{"test": 2}', "tprm_data1", False],
    ]

    file = generate_csv_in_memory(file_data)

    res = client.post(
        post_url,
        data={
            "filename": "sdf",
            "check": True,
            "force": True,
            "type": "multipart/form-data",
        },
        files={"file": file},
    )
    print(res.json())
    assert res.json() == {
        "will_be_updated_mo": 0,
        "will_be_created_mo": 1,
        "will_be_created_parameter_values": 1,
        "will_be_updated_parameter_values": 0,
        "will_be_deleted_parameter_values": 0,
    }
