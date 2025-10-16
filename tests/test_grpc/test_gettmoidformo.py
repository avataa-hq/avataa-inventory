# """Tests grpc GetTMOidForMo"""
#
# import grpc
# import pytest
# from google.protobuf import json_format
# from sqlmodel import select
#
# from models import TMO, MO, TPRM, PRM
# from services.grpc_service.grpc_server import Informer
# from services.grpc_service.proto_files.inventory_data.files.inventory_data_pb2 import RequestSeverityMoId, IntValue
#
# NOT_EXISTING_MO_ID = 5255
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     # test tmo and mo
#     tmo = TMO(name='TEST TMO', created_by='Admin', modified_by='Admin')
#     session.add(tmo)
#     session.flush()
#
#     mo = MO(name='TEST MO', tmo_id=tmo.id)
#     mo2 = MO(name='TEST MO2', tmo_id=tmo.id)
#     session.add(mo)
#     session.add(mo2)
#     session.flush()
#     session.commit()
#
#     yield session
#
#
# @pytest.mark.asyncio
# async def test_grpc_gettmoidformo_success(session, mocker, engine):
#     """TEST GRPC GetTMOidForMo successful if exist mo with mo.id equal to value in request"""
#     stm = select(MO)
#     mo = session.execute(stm).scalars().all()[-1]
#     assert mo.id != mo.tmo_id
#
#     service = Informer()
#     info_request = IntValue(value=mo.id)
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetTMOidForMo(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     assert message_as_dict['tmo_id'] == mo.tmo_id
#
#
# @pytest.mark.asyncio
# async def test_grpc_gettmoidformo_success_if_mo_doesnt_exist(session, mocker, engine):
#     """TEST GRPC GetTMOidForMo returns 0 if mo with mo.id equal to value in request doesn`t exist """
#     stm = select(MO).where(MO.id == NOT_EXISTING_MO_ID)
#     mo = session.execute(stm).scalar()
#     assert mo is None
#
#     service = Informer()
#     info_request = IntValue(value=NOT_EXISTING_MO_ID)
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetTMOidForMo(info_request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#
#     assert message_as_dict['tmo_id'] == 0
#
#
# @pytest.mark.asyncio
# async def test_grpc_gettmoidformo_success_if_mo_doesnt_exist1(session, mocker, engine):
#     """TEST GRPC GetTMOidForMo returns 0 if mo with mo.id equal to value in request doesn`t exist """
#     TMO_DEFAULT_DATA = {'name': 'Test TMO',
#                         'version': 1,
#                         'created_by': 'Test admin',
#                         'modified_by': 'Test admin'}
#
#     session.add(TMO(**TMO_DEFAULT_DATA))
#
#     stm = select(TMO.id).where(TMO.name == 'Test TMO')
#     tmo = session.execute(stm).scalar()
#     MO_DEFAULT_DATA = {'name': 'Test MO', 'tmo_id': tmo}
#     MO_DEFAULT_DATA_1 = {'name': 'Test MO 1', 'tmo_id': tmo}
#
#     TPRM_DEFAULT_DATA = {'name': 'severity',
#                          'val_type': 'int',
#                          'required': False,
#                          'created_by': 'Test admin',
#                          'modified_by': 'Test admin',
#                          'tmo_id': tmo
#                          }
#
#     session.add(MO(**MO_DEFAULT_DATA))
#     session.add(MO(**MO_DEFAULT_DATA_1))
#     session.add(TPRM(**TPRM_DEFAULT_DATA))
#     session.commit()
#     stm = select(TPRM.id).where(TPRM.name == 'severity')
#     tprm = session.execute(stm).scalar()
#     stm = select(MO.id).where(MO.name == 'Test MO')
#     mo = session.execute(stm).scalar()
#     stm = select(MO.id).where(MO.name == 'Test MO 1')
#     mo_1 = session.execute(stm).scalar()
#
#     PRM_DEFAULT_DATA = {'mo_id': mo,
#                         'tprm_id': tprm,
#                         'value': int(5)
#                         }
#     PRM_DEFAULT_DATA_1 = {'mo_id': mo_1,
#                           'tprm_id': tprm,
#                           'value': int(10)
#                           }
#
#     session.add(PRM(**PRM_DEFAULT_DATA))
#     session.add(PRM(**PRM_DEFAULT_DATA_1))
#     session.commit()
#
#     service = Informer()
#     request = RequestSeverityMoId(tmo_id=tmo, mo_ids=[3, 4])
#     mocker.patch('grpc_config.grpc_server.engine', new=engine)
#     mock_context = mocker.create_autospec(spec=grpc.aio.ServicerContext)
#     response = await service.GetMOSeverityMaxValue(request, mock_context)
#
#     message_as_dict = json_format.MessageToDict(
#         response,
#         including_default_value_fields=True,
#         preserving_proto_field_name=True
#     )
#     assert message_as_dict['max_severity'] == 5
