# from time import sleep
#
# from sqlalchemy import select
# from sqlalchemy.orm import Session
# from starlette.testclient import TestClient
#
# from models import MO, PRM
# from test_routers.test_batch_import import generate_csv_in_memory
#
#
# def test_batch_import(
#     batch_url: str, client: TestClient, session: Session, object_type_url: str
# ):
#     # input data
#     tmo_id = 100500
#
#     # set label
#     url = f"{object_type_url}{tmo_id}/"
#     label_tprms = [1, 5]
#     data = {"label": label_tprms, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # import file
#     data = [
#         [1, 2, 3, 4, 5],
#         ["PRM 1 Batch", ["sec"], "three", 1, "MO 1"],
#     ]
#     file = generate_csv_in_memory(default_data=data)
#     url = f"{batch_url}object_and_param_values/{tmo_id}"
#     res = client.post(
#         url=url,
#         data={"filename": file.name, "type": "multipart/form-data"},
#         files={"file": file},
#     )
#     print(res.json())
#     assert res.status_code == 201
#     sleep(0.1)
#
#     find_first_prm_stmt = (
#         select(PRM.mo_id)
#         .where(PRM.tprm_id == data[0][0], PRM.value == data[1][0])
#         .limit(1)
#     )
#     label_stmt = select(MO.label).where(MO.id.in_(find_first_prm_stmt)).limit(1)
#     label = session.execute(label_stmt).scalar_one_or_none()
#     assert label == "PRM 1 Batch-MO 1"
#
#
# def test_batch_import_ignore_label(
#     batch_url: str, client: TestClient, session: Session, object_type_url: str
# ):
#     # input data
#     tmo_id = 100500
#
#     # set label
#     url = f"{object_type_url}{tmo_id}/"
#     label_tprms = [1, 5]
#     data = {"label": label_tprms, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # import file
#     data = [
#         [1, 2, 3, 4, 5, "label"],
#         ["PRM 1 Batch", ["PRM 2", "Batch"], "PRM 3 Batch", 4, "MO 1", ""],
#     ]
#     file = generate_csv_in_memory(default_data=data)
#     url = f"{batch_url}object_and_param_values/{tmo_id}"
#     res = client.post(
#         url=url,
#         data={"filename": file.name, "type": "multipart/form-data"},
#         files={"file": file},
#     )
#     print(res.json())
#     assert res.status_code == 201
#     sleep(0.1)
#
#     find_first_prm_stmt = (
#         select(PRM.mo_id)
#         .where(PRM.tprm_id == data[0][0], PRM.value == data[1][0])
#         .limit(1)
#     )
#     label_stmt = select(MO.label).where(MO.id.in_(find_first_prm_stmt)).limit(1)
#     label = session.execute(label_stmt).scalar_one_or_none()
#     assert label == "PRM 1 Batch-MO 1"
