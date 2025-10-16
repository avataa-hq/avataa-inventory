# """Tests MO is available to send into kafka"""
# from collections import defaultdict
# from sqlalchemy.event import listen
#
# import pytest
# from sqlmodel import Session, select
#
# from models import MO, TMO
# from services.grpc_service.proto_files.inventory_instances.files import inventory_instances_pb2
# from services.kafka_service.listeners import receive_after_flush, receive_after_commit
# from services.kafka_service.utils import ObjEventStatus
#
# DEFAULT_MO_DATA = {'name': 'Test MO instance',
#                    'pov': {1: 'test', 'text': 1},
#                    'geometry': {2: 'test2', 'text2': 2},
#                    'active': True,
#                    'latitude': 0,
#                    'longitude': 0,
#                    'version': 1,
#                    'tmo_id': None,
#                    'p_id': None,
#                    'model': 'testurl'
#                    }
# NAME_OF_MO_THAT_CREATED_BEFORE_TESTS = 'Default MO instance'
# NAME_OF_MO_THAT_CREATED_BEFORE_TESTS_2 = 'Test batch operations'
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     new_tmo = TMO(name='new_name', created_by='Test user', modified_by='Test user')
#     session.add(new_tmo)
#     session.flush()
#
#     DEFAULT_MO_DATA['tmo_id'] = new_tmo.id
#
#     default_mo_in_db = dict()
#     default_mo_in_db.update(DEFAULT_MO_DATA)
#     default_mo_in_db['name'] = NAME_OF_MO_THAT_CREATED_BEFORE_TESTS
#     mo_ints = MO(**default_mo_in_db)
#     session.add(mo_ints)
#     default_mo_in_db['name'] = NAME_OF_MO_THAT_CREATED_BEFORE_TESTS_2
#     mo_ints = MO(**default_mo_in_db)
#     session.add(mo_ints)
#     session.commit()
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
# def test_mo_has_to_proto_method():
#     """TEST The MO class instance has a 'to_proto' method  """
#     ins = MO()
#     res = getattr(ins, 'to_proto', False)
#     assert res is not False
#
#
# def test_mo_to_proto_method_is_successful():
#     """TEST The MO class instance has can use to_proto without errors  """
#     ins = MO(**DEFAULT_MO_DATA)
#     ins.to_proto()
#
#
# def test_message_formed_on_create_mo(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful creation of the MO, a message to kafka is generated
#     successfully."""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     spy2 = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#     ins = MO(**DEFAULT_MO_DATA)
#     session.add(ins)
#     session.flush()
#     # Expectation
#     proto_mo = inventory_instances_pb2.MO(**ins.to_proto())
#     proto_msg = inventory_instances_pb2.ListMO(objects=[proto_mo])
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'MO', ObjEventStatus.CREATED)
#     spy2_msg = defaultdict(list)
#     spy2_msg[(3,)] = [proto_msg]
#     spy2.assert_called_once_with(msg=spy2_msg, obj_class_name='MO', event=ObjEventStatus.CREATED)
#
#
# def test_message_formed_on_delete_mo(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful delete of the MO, a message to kafka is generated
#         successfully."""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     spy2 = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(MO).where(MO.name == NAME_OF_MO_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#
#     # Expectation
#     proto_mo = inventory_instances_pb2.MO(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListMO(objects=[proto_mo])
#
#     session.delete(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'MO', ObjEventStatus.DELETED)
#
#
# def test_message_formed_on_update_mo(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful update of the MO, a message to kafka is generated
#     successfully."""
#
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(MO).where(MO.name == NAME_OF_MO_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.name = 'Change name'
#
#     # Expectation
#     proto_mo = inventory_instances_pb2.MO(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListMO(objects=[proto_mo])
#
#     session.add(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'MO', ObjEventStatus.UPDATED)
#
#
# def test_right_order_of_events_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several events with MO instances: create, delete, update, than
#     msg must be sent in order: create - first, update - second, delete - third!."""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     # update
#     statement = select(MO).where(MO.name == NAME_OF_MO_THAT_CREATED_BEFORE_TESTS_2)
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.name = 'Updated name'
#     session.add(inst_from_db)
#     session.flush()
#
#     # delete
#     statement = select(MO).where(MO.name == NAME_OF_MO_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#     session.delete(inst_from_db)
#
#     # create
#     ins = MO(**DEFAULT_MO_DATA)
#     session.add(ins)
#
#     session.commit()
#
#     expected = [('MO', ObjEventStatus.CREATED), ('MO', ObjEventStatus.UPDATED),
#                 ('MO', ObjEventStatus.DELETED)]
#     res = [(x.args[1], x.args[2]) for x in spy.mock_calls]
#
#     assert res == expected
#
#
# def test_several_eq_events_from_one_session_in_one_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several identical events with MO instances: all events
#     must be sent in one msg"""
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(MO)
#     inst_from_db = session.execute(statement).scalars().all()
#
#     # Expectation
#     proto_mo = [inventory_instances_pb2.MO(**item.to_proto()) for item in inst_from_db]
#     proto_msg = inventory_instances_pb2.ListMO(objects=proto_mo)
#
#     for x in inst_from_db:
#         session.delete(x)
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'MO', ObjEventStatus.DELETED)
