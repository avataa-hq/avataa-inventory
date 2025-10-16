# """Tests grpc GetObjWithParams"""
# import pickle
#
# import grpc
# import pytest
# from google.protobuf import json_format
# from sqlmodel import select
#
# from services.grpc_service.grpc_server import Informer
# from services.grpc_service.proto_files.inventory_data.files.inventory_data_pb2 import RequestForObjInfoByTMO
#
# from models import PRM, TMO, MO, TPRM
#
# TEST_MULTIPLE_VALUE = ['test', 'test']
# NOT_EXISTING_TPRM_ID = 8000
# NAME_OF_TMO_THAT_HAS_PARENT = 'TMO WITH PARENT'
# NAME_OF_MO_THAT_HAS_CHILDREN = 'MO WITH CHILDREN'
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     # test tmo and mo
#     tmo = TMO(name='TEST TMO', created_by='Admin', modified_by='Admin')
#     session.add(tmo)
#     session.flush()
#     tmo_2 = TMO(name=NAME_OF_TMO_THAT_HAS_PARENT, created_by='Admin', modified_by='Admin',
#                 p_id=tmo.id)
#     session.add(tmo_2)
#     session.flush()
#
#     mo_1 = MO(name='TEST MO1', tmo_id=tmo.id, active=True)
#     session.add(mo_1)
#     mo_2 = MO(name=NAME_OF_MO_THAT_HAS_CHILDREN, tmo_id=tmo.id, active=True)
#     session.add(mo_2)
#     mo_3 = MO(name='TEST MO3', tmo_id=tmo.id, active=False)
#     session.add(mo_3)
#     session.flush()
#     mo_4 = MO(name='TEST MO4', tmo_id=tmo_2.id, active=True, p_id=mo_2.id)
#     session.add(mo_4)
#     mo_5 = MO(name='TEST MO4', tmo_id=tmo_2.id, active=True)
#     session.add(mo_5)
#
#     tprm_1 = TPRM(tmo_id=tmo.id, val_type='str', name='Test TPRM', created_by='Admin', modified_by='Admin')
#     session.add(tprm_1)
#     tprm_2 = TPRM(tmo_id=tmo.id, val_type='str', name='Test TPR2', created_by='Admin', modified_by='Admin')
#     session.add(tprm_2)
#     tprm_3 = TPRM(tmo_id=tmo.id, val_type='str', name='Test TPR3', created_by='Admin', modified_by='Admin',
#                   multiple=True)
#     session.add(tprm_3)
#     session.flush()
#
#     prm_mo_1 = PRM(tprm_id=tprm_1.id, mo_id=mo_1.id, value='value mo 1')
#     prm_mo_2 = PRM(tprm_id=tprm_2.id, mo_id=mo_1.id, value='value mo 1 tprm 2')
#     prm_mo_3 = PRM(tprm_id=tprm_1.id, mo_id=mo_2.id, value='value mo 2')
#     prm_mo_4 = PRM(tprm_id=tprm_1.id, mo_id=mo_3.id, value='value mo 2')
#     session.add(prm_mo_1)
#     session.add(prm_mo_2)
#     session.add(prm_mo_3)
#     session.add(prm_mo_4)
#
#     prm_mo_5 = PRM(tprm_id=tprm_1.id, mo_id=mo_2, value=pickle.dumps(TEST_MULTIPLE_VALUE).hex())
#     session.add(prm_mo_5)
#
#     session.commit()
#
#     yield session
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_with_empty_object_type_id(session, mocker, engine):
#     """TEST GRPC GetObjWithParams returns response with mo_id equal to 0 and
#     p_id equla to 0 and tprm_values equal to empty dict
#     if there are empty object_type_id value in request"""
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO()
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#     counter = 0
#
#     async for item in response:
#         counter += 1
#         assert item.mo_id == 0
#         assert item.tprm_values == dict()
#         assert item.p_id == 0
#
#     assert counter == 1
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_with_empty_tprm_ids(session, mocker, engine):
#     """TEST GRPC GetObjWithParams returns response with empty dict of tprm_values for each mo if
#     there are empty tprm_ids value in request"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     service = Informer()
#     print(tmo)
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id)
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#
#     async for i in response:
#         message_as_dict = json_format.MessageToDict(
#             i,
#             including_default_value_fields=True,
#             preserving_proto_field_name=True
#         )
#
#         for k, v in message_as_dict['tprm_values']:
#             assert v == dict()
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_successful_returns_several_msgs(session, mocker, engine):
#     """TEST GRPC GetObjWithParams returns stream of msgs"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id)
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#     counter = 0
#     async for i in response:
#         counter += 1
#
#     assert counter > 1
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_returns_only_for_mo_active(session, mocker, engine):
#     """TEST GRPC GetObjWithParams returns response with mo_id with params only for MO with active equal True"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     stm = select(MO).where(MO.active == False, MO.tmo_id == tmo.id)  # noqa
#     mo_with_active_false = session.execute(stm).scalar()
#
#     assert mo_with_active_false is not None
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id)
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#     async for i in response:
#         assert i.mo_id != mo_with_active_false.id
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_with_specified_tprms(session, mocker, engine):
#     """TEST GRPC GetObjWithParams returns response with mo_id with params only for MO with active equal True"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     stm = select(TPRM).where(TPRM.tmo_id == tmo.id)
#     tprm = session.execute(stm).scalar()
#
#     assert tprm is not None
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id, tprm_ids=[tprm.id])
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#     async for i in response:
#         message_as_dict = json_format.MessageToDict(
#             i,
#             including_default_value_fields=True,
#             preserving_proto_field_name=True
#         )
#
#         assert len(message_as_dict['tprm_values']) <= 1
#         if len(message_as_dict['tprm_values']) == 1:
#             assert str(tprm.id) in message_as_dict['tprm_values'].keys()
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_response_with_tprm_multiple_values_type(session, mocker, engine):
#     """TEST GRPC GetObjWithParams if in request data included tprm id of TPRM with parameter multiple equal to True,
#      response data of object which has value of such TPRM will have decoded value in string representation"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     stm = select(TPRM).where(TPRM.tmo_id == tmo.id, TPRM.multiple == True)  # noqa
#     tprm = session.execute(stm).scalar()
#
#     assert tprm is not None
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id, tprm_ids=[tprm.id])
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#     async for i in response:
#         message_as_dict = json_format.MessageToDict(
#             i,
#             including_default_value_fields=True,
#             preserving_proto_field_name=True
#         )
#
#         assert len(message_as_dict['tprm_values']) <= 1
#         if len(message_as_dict['tprm_values']) == 1:
#             assert isinstance(message_as_dict['tprm_values'][str(tprm.id)], str)
#             assert message_as_dict['tprm_values'][str(tprm.id)] == str(TEST_MULTIPLE_VALUE)
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_with_not_existing_tprm_id(session, mocker, engine):
#     """TEST GRPC GetObjWithParams if in request data included not existing tprm id, returns
#     response which include mo_id with empty dict as value of tprm_values"""
#     stm = select(TMO)
#     tmo = session.execute(stm).scalar()
#
#     stm = select(TPRM).where(TPRM.tmo_id == NOT_EXISTING_TPRM_ID)
#     tprm = session.execute(stm).scalar()
#
#     stm = select(MO).where(MO.tmo_id == tmo.id, MO.active == True)  # noqa
#     all_mo = session.execute(stm).scalars().all()
#
#     assert tprm is None
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=tmo.id, tprm_ids=[NOT_EXISTING_TPRM_ID])
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#
#     counter = 0
#     async for i in response:
#         counter += 1
#
#         message_as_dict = json_format.MessageToDict(
#             i,
#             including_default_value_fields=True,
#             preserving_proto_field_name=True
#         )
#         assert len(message_as_dict['tprm_values']) == 0
#
#     assert counter == len(all_mo)
#
#
# @pytest.mark.asyncio
# async def test_grpc_getobjwithparams_with_mo_p_id_in_request(session, mocker, engine):
#     """TEST GRPC GetObjWithParams if in request data included mo_p_id, returns
#     response which include data for MO where MO.p_id == mo_p_id"""
#     stm = select(TMO).where(TMO.name == NAME_OF_TMO_THAT_HAS_PARENT)
#     tmo = session.execute(stm).scalar()
#
#     assert tmo is not None
#
#     stm = select(MO).where(MO.active == True, MO.name == NAME_OF_MO_THAT_HAS_CHILDREN)  # noqa
#     mo_with_children = session.execute(stm).scalar()
#
#     assert mo_with_children is not None
#
#     stm = select(MO).where(MO.active == True, MO.p_id == mo_with_children.id)  # noqa
#     mo_with_parent = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = RequestForObjInfoByTMO(object_type_id=mo_with_parent.tmo_id, mo_p_id=mo_with_children.id)
#     mocker.patch('services.groc_service.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = service.GetObjWithParams(info_request, mock_context)
#
#     counter = 0
#     async for i in response:
#         counter += 1
#
#         assert i.mo_id == mo_with_parent.id
#
#     assert counter == 1
