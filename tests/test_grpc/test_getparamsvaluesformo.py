# """Tests grpc GetParamsValuesForMO"""
# import pickle
# from datetime import datetime
#
# import grpc
# import pytest
# from google.protobuf import json_format
# from sqlmodel import select
#
# from models import PRM, TMO, MO, TPRM
# from services.grpc_service.grpc_server import Informer
# from services.grpc_service.proto_files.inventory_data.files.inventory_data_pb2 import InfoRequest
# from services.grpc_service.proto_files.inventory_data.utils import VAL_TYPE_CONVERTER
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     # test tmo and mo
#     tmo = TMO(name='TEST TMO', created_by='Admin', modified_by='Admin')
#     mo = MO(name='TEST MO')
#
#     session.add(tmo)
#     session.add(mo)
#     session.flush()
#
#     # for each val_type creates test tprm
#     tprm_str = TPRM(tmo_id=tmo.id, name='STR', val_type='str', created_by='Admin', modified_by='Admin')
#     tprm_tprm_date = TPRM(tmo_id=tmo.id, name='DATE', val_type='date', created_by='Admin', modified_by='Admin')
#     tprm_datetime = TPRM(tmo_id=tmo.id, name='DATETIME', val_type='datetime', created_by='Admin',
#                          modified_by='Admin')
#     tprm_float = TPRM(tmo_id=tmo.id, name='FLOAT', val_type='float', created_by='Admin', modified_by='Admin')
#     tprm_int = TPRM(tmo_id=tmo.id, name='INT', val_type='int', created_by='Admin', modified_by='Admin')
#     tprm_mo_link = TPRM(tmo_id=tmo.id, name='MO_LINK', val_type='mo_link', created_by='Admin', modified_by='Admin')
#     tprm_prm_link = TPRM(tmo_id=tmo.id, name='PRM_LINK', val_type='prm_link', created_by='Admin',
#                          modified_by='Admin')
#     tprm_user_link = TPRM(tmo_id=tmo.id, name='USER_LINK', val_type='user_link', created_by='Admin',
#                           modified_by='Admin')
#     tprm_formula = TPRM(tmo_id=tmo.id, name='FORMULA', val_type='formula', created_by='Admin', modified_by='Admin')
#     tprm_bool = TPRM(tmo_id=tmo.id, name='BOOL', val_type='bool', created_by='Admin', modified_by='Admin')
#     tprm_int2 = TPRM(tmo_id=tmo.id, name='INT222', val_type='int', created_by='Admin', modified_by='Admin',
#                      multiple=True)
#
#     session.add(tprm_str)
#     session.add(tprm_tprm_date)
#     session.add(tprm_datetime)
#     session.add(tprm_float)
#     session.add(tprm_mo_link)
#     session.add(tprm_prm_link)
#     session.add(tprm_user_link)
#     session.add(tprm_int)
#     session.add(tprm_formula)
#     session.add(tprm_bool)
#     session.add(tprm_int2)
#     session.flush()
#
#     # for each tprm creates prm for test mo
#     prm = PRM(tprm_id=tprm_str.id, mo_id=mo.id, value='string value')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_tprm_date.id, mo_id=mo.id, value='25.08.1990')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_datetime.id, mo_id=mo.id, value=str(datetime.now()))
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_float.id, mo_id=mo.id, value='25.5')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_mo_link.id, mo_id=mo.id, value='tprm_mo_link value')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_prm_link.id, mo_id=mo.id, value='tprm_prm_link value')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_user_link.id, mo_id=mo.id, value='tprm_user_link value')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_int.id, mo_id=mo.id, value='150')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_formula.id, mo_id=mo.id, value='150+5255x')
#     session.add(prm)
#     prm = PRM(tprm_id=tprm_bool.id, mo_id=mo.id, value='False')
#     session.add(prm)
#     s = pickle.dumps([1, 2, 3, 446, 7954, 556333]).hex()
#     prm = PRM(tprm_id=tprm_int2.id, mo_id=mo.id, value=s)
#     session.add(prm)
#     session.commit()
#
#     yield session
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_str(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = str"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'str')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['str']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_date(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = date"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'date')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['date']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_datetime(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = datetime"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'datetime')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['datetime']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_float(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = float"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'float')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['float']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_int(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = int"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'int', TPRM.multiple == False)  # noqa
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['int']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_mo_link(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = mo_link"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'mo_link')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['mo_link']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_prm_link(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = prm_link"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'prm_link')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['prm_link']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_user_link(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = user_link"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'user_link')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['user_link']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_formula(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = formula"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'formula')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['formula']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_bool(session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = bool"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'bool')
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], VAL_TYPE_CONVERTER['bool']['python_deserializer'])
#
#
# @pytest.mark.asyncio
# async def test_grpc_getparamsvaluesformo_success_prm_with_tprm_val_type_int_and_tprm_multiple_true(
#         session, mocker, engine):
#     """TEST GRPC GetParamsValuesForMO successful for deserialization and sending prm with tprm.val_type = int and
#     tprm.multiple == True"""
#     stm = select(PRM).join(TPRM).where(TPRM.val_type == 'int', TPRM.multiple == True)  # noqa
#     prm = session.execute(stm).scalar()
#
#     service = Informer()
#     info_request = InfoRequest(mo_id=prm.mo_id, tprm_ids=[prm.tprm_id])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetParamsValuesForMO(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     new_d = dict()
#     for k, v in message_as_dict['mo_info'].items():
#         new_d[int(k)] = [x['value'] for x in v['mo_tprm_value']]
#
#     new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}
#
#     assert isinstance(new_d[prm.tprm_id], list)
#     for item in new_d[prm.tprm_id]:
#         assert isinstance(item, VAL_TYPE_CONVERTER['int']['python_deserializer'])
