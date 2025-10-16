# import pickle
#
# import pytest
# from sqlalchemy.event import listen
# from sqlmodel import Session
#
# from models import TMO, TPRM, MO, PRM
# from services.listener_service.processor import ListenerService
#
#
# @pytest.fixture(scope="session")
# def client_url():
#     return "/api/inventory/v1/"
#
#
# @pytest.fixture(scope="session")
# def object_type_url(client_url):
#     return f"{client_url}object_type/"
#
#
# @pytest.fixture(scope="session")
# def parameter_type_url(client_url):
#     return f"{client_url}param_type/"
#
#
# @pytest.fixture(scope="session")
# def object_url(client_url):
#     return f"{client_url}object/"
#
#
# @pytest.fixture(scope="session")
# def batch_url(client_url):
#     return f"{client_url}batch/"
#
#
# @pytest.fixture(scope="function", autouse=True)
# def fill_data(mocker, session, engine):
#     mocker.patch(
#         "services.event_service.processor.get_not_auth_session",
#         new=lambda: iter([Session(engine)]),
#     )
#     mocker.patch(
#         "services.kafka_service.producer.protobuf_producer.kafka_config.KAFKA_TURN_ON",
#         new=False,
#     )
#
#     fill_tmo_data(session=session)
#     session.commit()
#
#     fill_tprm_data(session=session)
#     session.commit()
#
#     fill_mo(session=session)
#     session.commit()
#
#     fill_prm(session=session)
#
#     session.commit()
#     yield session
#
#
# @pytest.fixture(scope="function")
# def kafka_mock(mocker):
#     mock = mocker.patch(
#         "services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka",
#         return_value=True,
#     )
#     return mock
#
#
# @pytest.fixture(scope="function")
# def kafka_partial_mock(mocker):
#     mock = mocker.patch(
#         "services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitionsitions",
#         return_value=True,
#     )
#     return mock
#
#
# @pytest.fixture(scope="function")
# def listened_session(fill_data: Session):
#     session = fill_data
#     listen(session, "after_flush", ListenerService.receive_after_flush)
#     listen(session, "after_commit", ListenerService.receive_after_commit)
#     return session
#
#
# def fill_tmo_data(session: Session):
#     tmos = (
#         TMO(
#             name="TMO 1",
#             id=100500,
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#         TMO(
#             name="TMO 2",
#             id=100501,
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#     )
#     session.add_all(tmos)
#     session.flush()
#
#
# def fill_tprm_data(session: Session):
#     tprms = (
#         TPRM(
#             name="TPRM 1",
#             val_type="str",
#             multiple=False,
#             required=True,
#             tmo_id=100500,
#             field_value="0",
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#         TPRM(
#             name="TPRM 2",
#             val_type="str",
#             multiple=True,
#             required=True,
#             tmo_id=100500,
#             field_value="80049517000000000000005d948c10d09fd0b0d181d185d0b0d0bbd0bad0b094612e",
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#         TPRM(
#             name="TPRM 3",
#             val_type="str",
#             multiple=False,
#             required=False,
#             tmo_id=100500,
#             field_value=None,
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#         TPRM(
#             name="TPRM 4",
#             val_type="int",
#             multiple=False,
#             required=True,
#             tmo_id=100500,
#             field_value="0",
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#         TPRM(
#             name="TPRM 5",
#             val_type="mo_link",
#             multiple=False,
#             required=True,
#             tmo_id=100500,
#             field_value="1",
#             created_by="TestUser",
#             modified_by="TestUser",
#         ),
#     )
#     session.add_all(tprms)
#     session.flush()
#
#
# def fill_mo(session: Session):
#     mos = (
#         MO(name="MO 1", tmo_id=100500, id=100500),
#         MO(name="MO 2", tmo_id=100500, id=100501),
#         MO(name="MO 3", tmo_id=100501, id=100502),
#     )
#     session.add_all(mos)
#     session.flush()
#
#
# def fill_prm(session: Session):
#     prms = []
#     for mo_id in range(100500, 100503):
#         mo_prms = (
#             PRM(tprm_id=1, mo_id=mo_id, value=f"PRM1 {mo_id}"),
#             PRM(
#                 tprm_id=2,
#                 mo_id=mo_id,
#                 value=pickle.dumps(["PRM2", "{str(mo_id)}"]).hex(),
#             ),
#             PRM(tprm_id=3, mo_id=mo_id, value=f"PRM3 {mo_id}"),
#             PRM(tprm_id=4, mo_id=mo_id, value=int(f"4{mo_id}")),
#             PRM(tprm_id=5, mo_id=mo_id, value=mo_id + 1),
#         )
#         prms.extend(mo_prms)
#     session.add_all(prms)
#     session.flush()
#
#
# @pytest.fixture(scope="function")
# def mo_data():
#     return {
#         "name": "Test MO instance",
#         "pov": {1: "test", "text": 1},
#         "geometry": {2: "test2", "text2": 2},
#         "label": "Test label",
#         "active": True,
#         "latitude": 0,
#         "longitude": 0,
#         "version": 1,
#         "tmo_id": None,
#         "p_id": None,
#         "model": "testurl",
#     }
