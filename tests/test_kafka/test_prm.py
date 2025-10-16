# """Tests PRM is available to send into kafka"""
# from sqlalchemy.event import listen
# import pytest
# from sqlmodel import Session, select
#
# from models import PRM, TMO, MO, TPRM
# from services.grpc_service.proto_files.inventory_instances.files import inventory_instances_pb2
# from services.kafka_service.listeners import receive_after_flush, receive_after_commit
# from services.kafka_service.utils import ObjEventStatus
#
#
# TMO_DEFAULT_DATA = {'name': 'new_name',
#                     'created_by': 'Test user',
#                     'modified_by': 'Test user'}
#
# MO_DEFAULT_DATA = {'pov': {1: 'test', 'text': 1},
#                    'geometry': {2: 'test2', 'text2': 2},
#                    'active': True,
#                    'latitude': 0,
#                    'longitude': 0,
#                    'version': 1,
#                    'tmo_id': None,
#                    'p_id': None,
#                    'model': 'testurl'
#                    }
#
# TPRM_DEFAULT_DATA = {'name': 'Test TPRM',
#                      'val_type': 'str',
#                      'tmo_id': None,
#                      'created_by': 'Test user',
#                      'modified_by': 'Test user'}
#
# DEFAULT_DATA = {'value': 'some val',
#                 'tprm_id': 1,
#                 'mo_id': 1,
#                 }
#
# INST_BEFORE_TESTS = {
#     'value': 'some val2',
#     'tprm_id': 2,
#     'mo_id': 2,
# }
#
# INST_BEFORE_TESTS_2 = {
#     'value': 'some val3',
#     'tprm_id': 2,
#     'mo_id': 1,
# }
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     new_tmo = TMO(**TMO_DEFAULT_DATA)
#     session.add(new_tmo)
#
#     session.flush()
#     MO_DEFAULT_DATA['tmo_id'] = new_tmo.id
#
#     new_mo_1 = MO(**MO_DEFAULT_DATA)
#     new_mo_1.name = 'Mo 1'
#     new_mo_2 = MO(**MO_DEFAULT_DATA)
#     new_mo_2.name = 'Mo 2'
#
#     session.add(new_mo_1)
#     session.add(new_mo_2)
#     session.flush()
#
#     TPRM_DEFAULT_DATA['tmo_id'] = new_tmo.id
#     mew_tprm_1 = TPRM(**TPRM_DEFAULT_DATA)
#     session.add(mew_tprm_1)
#
#     mew_tprm_2 = TPRM(**TPRM_DEFAULT_DATA)
#     mew_tprm_2.name = 'Test TPRM 2'
#     session.add(mew_tprm_2)
#     session.flush()
#
#     DEFAULT_DATA['mo_id'] = new_mo_1.id
#     DEFAULT_DATA['tprm_id'] = mew_tprm_2.id
#     INST_BEFORE_TESTS_2['mo_id'] = new_mo_1.id
#     INST_BEFORE_TESTS_2['tprm_id'] = mew_tprm_1.id
#     INST_BEFORE_TESTS['mo_id'] = new_mo_2.id
#     INST_BEFORE_TESTS['tprm_id'] = mew_tprm_1.id
#
#     prm_1 = PRM(**INST_BEFORE_TESTS_2)
#     prm_2 = PRM(**INST_BEFORE_TESTS)
#     session.add(prm_1)
#     session.add(prm_2)
#     session.commit()
#
#     yield session
#
#
# @pytest.fixture(scope='function', autouse=True)
# def kafka_partition():
#     from config import kafka_config
#     if not hasattr(kafka_config, 'KAFKA_PRODUCER_PART_TOPIC_PARTITIONS'):
#         setattr(kafka_config, 'KAFKA_PRODUCER_PART_TOPIC_PARTITIONS', 10)
#
#
# def test_prm_has_to_proto_method():
#     """TEST The PRM class instance has a 'to_proto' method  """
#     ins = PRM()
#     res = getattr(ins, 'to_proto', False)
#     assert res is not False
#
#
# def test_prm_to_proto_method_is_successful():
#     """TEST The PRM class instance has can use to_proto without errors  """
#     ins = PRM(**DEFAULT_DATA)
#     ins.to_proto()
#
#
# def test_message_formed_on_create_prm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful creation of the PRM, a message to kafka is generated
#     successfully."""
#
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#     ins = PRM(**DEFAULT_DATA)
#     session.add(ins)
#     session.flush()
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.PRM(**ins.to_proto())
#     proto_msg = inventory_instances_pb2.ListPRM(objects=[proto_unit])
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'PRM', ObjEventStatus.CREATED)
#
#
# def test_message_formed_on_delete_prm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful delete of the PRM, a message to kafka is generated
#         successfully."""
#
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(PRM).where(PRM.value == INST_BEFORE_TESTS['value'])
#     inst_from_db = session.execute(statement).scalar()
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.PRM(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListPRM(objects=[proto_unit])
#
#     session.delete(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'PRM', ObjEventStatus.DELETED)
#
#
# def test_message_formed_on_update_prm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful update of the PRM, a message to kafka is generated
#     successfully."""
#
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(PRM).where(PRM.value == INST_BEFORE_TESTS['value'])
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.value = 'Changed value'
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.PRM(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListPRM(objects=[proto_unit])
#
#     session.add(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'PRM', ObjEventStatus.UPDATED)
#
#
# def test_right_order_of_prm_events_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several events with PRM instances: create, delete, update, than
#     msg must be sent in order: create - first, update - second, delete - third!."""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     # update
#     statement = select(PRM).where(PRM.value == INST_BEFORE_TESTS['value'])
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.value = 'Updated value'
#     session.add(inst_from_db)
#     session.flush()
#
#     # delete
#     statement = select(PRM).where(PRM.value == INST_BEFORE_TESTS_2['value'])
#     inst_from_db = session.execute(statement).scalar()
#     session.delete(inst_from_db)
#
#     # create
#     ins = PRM(**DEFAULT_DATA)
#     session.add(ins)
#
#     session.commit()
#
#     expected = [('PRM', ObjEventStatus.CREATED), ('PRM', ObjEventStatus.UPDATED),
#                 ('PRM', ObjEventStatus.DELETED)]
#     res = [(x.args[1], x.args[2]) for x in spy.mock_calls]
#
#     assert res == expected
#
#
# def test_several_eq_events_from_one_session_in_one_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several identical events with PRM instances: all events
#     must be sent in one msg"""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(PRM)
#     inst_from_db = session.execute(statement).scalars().all()
#
#     # Expectation
#     proto_mo = [inventory_instances_pb2.PRM(**item.to_proto()) for item in inst_from_db]
#     proto_msg = inventory_instances_pb2.ListPRM(objects=proto_mo)
#
#     for x in inst_from_db:
#         session.delete(x)
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'PRM', ObjEventStatus.DELETED)
