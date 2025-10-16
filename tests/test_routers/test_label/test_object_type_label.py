# import pytest
# from sqlalchemy import select
# from sqlmodel import Session
# from starlette.testclient import TestClient
#
# from models import MO
#
#
# @pytest.mark.parametrize(
#     "label",
#     [
#         [1, 2, 3, 4, 5],
#         [1, 3, 4, 5],
#         [1, 6],
#         [6],
#     ],
# )
# def test_set_wrong_label_tprms(
#     session: Session, client: TestClient, object_type_url, label: list[int]
# ):
#     tmo_id = 100500
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": label, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 422
#
#
# @pytest.mark.parametrize(
#     "label, db_labels",
#     [
#         (
#             [1, 4, 5],
#             [
#                 (100502, None),
#                 (100500, "PRM1 100500-4100500-MO 2"),
#                 (100501, "PRM1 100501-4100501-MO 3"),
#             ],
#         ),
#         (
#             [1, 4],
#             [
#                 (100502, None),
#                 (100500, "PRM1 100500-4100500"),
#                 (100501, "PRM1 100501-4100501"),
#             ],
#         ),
#         (
#             [1],
#             [(100502, None), (100500, "PRM1 100500"), (100501, "PRM1 100501")],
#         ),
#     ],
# )
# def test_set_label_tprms(
#     session: Session,
#     client: TestClient,
#     object_type_url,
#     label: list[int],
#     db_labels,
# ):
#     tmo_id = 100500
#     other_tmo_id = 100501
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": label, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     response_label = response.json().get("label", [])
#     assert response_label == label
#
#     db_labels_response = session.execute(
#         select(MO.id, MO.label).where(MO.tmo_id.in_([tmo_id, other_tmo_id]))
#     ).all()
#     assert db_labels_response == db_labels
#
#
# def test_update_label_tprms(
#     session: Session, client: TestClient, object_type_url
# ):
#     tmo_id = 100500
#     label_tprms_before = [1, 5]
#     label_tprms_after = [1]
#     stmt = select(MO.id, MO.label).where(MO.tmo_id.in_([tmo_id]))
#     url = f"{object_type_url}{tmo_id}/"
#
#     data = {"label": label_tprms_before, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#     db_labels_before = session.execute(stmt).all()
#
#     data = {"label": label_tprms_after, "version": 2}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#     db_labels_after = session.execute(stmt).all()
#
#     assert db_labels_before != db_labels_after
