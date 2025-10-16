# from sqlalchemy import select
# from sqlalchemy.orm import Session
# from starlette.testclient import TestClient
#
# from models import PRM, MO
#
#
# def test_update_parameters(
#     session: Session, client: TestClient, object_type_url: str, object_url: str
# ):
#     tmo_id = 100500
#     tprm_id = 1
#
#     # find first prm
#     stmt = select(PRM).where(PRM.tprm_id == tprm_id).limit(1)
#     prm = session.execute(stmt).scalar_one()
#
#     # set label in tmo
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": [tprm_id], "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # find mo label
#     label_stmt = select(MO.label).where(MO.id == prm.mo_id)
#     label_before = session.execute(label_stmt).scalar_one()
#
#     # update
#     data = [{"tprm_id": prm.tprm_id, "value": "New value", "version": 1}]
#     url = f"{object_url}{prm.mo_id}/parameters/"
#     res = client.patch(url, json=data)
#     assert res.status_code == 200
#
#     # find mo label
#     label_after = session.execute(label_stmt).scalar_one()
#     assert label_before != label_after
#     assert label_after == data[0]["value"]
#
#
# def test_update_parameter(
#     session: Session, client: TestClient, object_type_url: str, object_url: str
# ):
#     tmo_id = 100500
#     tprm_id = 1
#
#     # find first prm
#     stmt = select(PRM).where(PRM.tprm_id == tprm_id).limit(1)
#     prm = session.execute(stmt).scalar_one()
#
#     # set label in tmo
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": [tprm_id], "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # find mo label
#     label_stmt = select(MO.label).where(MO.id == prm.mo_id)
#     label_before = session.execute(label_stmt).scalar_one()
#
#     # update
#     data = {"value": "New value", "version": 1}
#     url = f"{object_url}{prm.mo_id}/param_types/{prm.tprm_id}/parameter"
#
#     res = client.patch(url, json=data)
#     assert res.status_code == 200
#
#     # find mo label
#     label_after = session.execute(label_stmt).scalar_one()
#     assert label_before != label_after
#     assert label_after == data["value"]
#
#
# def test_multiple_update_parameters(
#     session: Session, client: TestClient, object_type_url: str, client_url: str
# ):
#     tmo_id = 100500
#     tprm_id = 1
#
#     # find first prm
#     stmt = select(PRM).where(PRM.tprm_id == tprm_id).limit(1)
#     prm = session.execute(stmt).scalar_one()
#
#     # set label in tmo
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": [tprm_id], "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # find mo label
#     label_stmt = select(MO.label).where(MO.id == prm.mo_id)
#     label_before = session.execute(label_stmt).scalar_one()
#
#     # update
#     data = [
#         {
#             "object_id": prm.mo_id,
#             "new_values": [{"tprm_id": tprm_id, "new_value": "New Value"}],
#         }
#     ]
#     url = f"{client_url}multiple_parameter_update"
#
#     res = client.patch(url, json=data)
#     assert res.status_code == 200
#
#     # find mo label
#     label_after = session.execute(label_stmt).scalar_one()
#     assert label_before != label_after
