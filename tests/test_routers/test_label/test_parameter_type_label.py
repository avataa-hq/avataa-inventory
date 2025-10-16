# from sqlmodel import Session
# from starlette.testclient import TestClient
#
#
# def test_delete_label_parameter_type(
#     session: Session,
#     client: TestClient,
#     parameter_type_url: str,
#     object_type_url: str,
# ):
#     tmo_id = 100500
#     tprm_id = 1
#
#     # set label in tmo
#     url = f"{object_type_url}{tmo_id}/"
#     data = {"label": [tprm_id, 5], "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # delete label tprm
#     url = f"{parameter_type_url}{tprm_id}"
#     response = client.delete(url)
#     assert response.status_code == 409
#
#
# def test_update_label_parameter_type(
#     session: Session,
#     client: TestClient,
#     parameter_type_url: str,
#     object_type_url: str,
# ):
#     tmo_id = 100500
#     tprm_id = 1
#
#     # set label in tmo
#     url = f"{object_type_url}{tmo_id}/"
#
#     data = {"label": [tprm_id, 5], "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 200
#
#     # update label tprm
#     url = f"{parameter_type_url}{tprm_id}"
#     data = {"required": False, "version": 1}
#     response = client.patch(url=url, json=data)
#     assert response.status_code == 409
