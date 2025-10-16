# from fastapi.testclient import TestClient
#
#
# def test_read_objects_positive(app: TestClient):
#     url = '/api/inventory/v1/objects/'
#     query_params = {
#         'object_type_id': 3,
#         'order_by_tprms_id': [5],
#         'order_by_asc': [False]
#     }
#
#     expected = [5, 4]
#     response = app.get(url, params=query_params)
#     assert response.status_code == 200
#     response = [r['id'] for r in response.json()]
#     assert len(response) == len(expected)
#     assert all(r == e for r, e in zip(response, expected))
#
#     query_params['order_by_asc'] = [True]
#     response = app.get(url, params=query_params)
#     assert response.status_code == 200
#     response = [r['id'] for r in response.json()]
#     assert len(response) == len(expected)
#     assert all(r != e for r, e in zip(response, expected))
#
#
# def test_read_objects_negative(app: TestClient):
#     url = '/api/inventory/v1/objects/'
#     query_params = {
#         'object_type_id': 3,
#         'order_by_tprms_id': [1],
#         'order_by_asc': [False]
#     }
#
#     expected = [4, 5]
#     response = app.get(url, params=query_params)
#     assert response.status_code == 200
#     response = [r['id'] for r in response.json()]
#     assert len(response) == len(expected)
#     assert all(r == e for r, e in zip(response, expected))
