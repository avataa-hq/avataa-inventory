# """Tests TPRM is available to send into kafka"""
# from sqlalchemy.event import listen
# import pytest
# from sqlmodel import Session, select
#
# from models import TPRM, TMO
# from services.grpc_service.proto_files.inventory_instances.files import inventory_instances_pb2
# from services.kafka_service.listeners import receive_after_flush, receive_after_commit
# from services.kafka_service.utils import ObjEventStatus
# from datetime import datetime
#
#
# TMO_DEFAULT_DATA = {'name': 'new_name',
#                     'created_by': 'Test user',
#                     'modified_by': 'Test user'}
#
#
# DEFAULT_DATA = {'name': 'Test TPRM instance',
#                 'description': 'description',
#                 'val_type': 'str',
#                 'multiple': True,
#                 'required': True,
#                 'returnable': True,
#                 'constraint': 'some',
#                 'prm_link_filter': 'some',
#                 'group': 'some',
#                 'tmo_id': None,
#                 'created_by': 'Admin',
#                 'modified_by': 'Admin',
#                 'creation_date': datetime.now(),
#                 'modification_date': datetime.now()
#                 }
#
# NAME_OF_INST_THAT_CREATED_BEFORE_TESTS = 'Default PRM instance'
# NAME_OF_INST_THAT_CREATED_BEFORE_TESTS_2 = 'Default PRM instance 2'
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#     new_tmo = TMO(**TMO_DEFAULT_DATA)
#     session.add(new_tmo)
#     session.flush()
#
#     DEFAULT_DATA['tmo_id'] = new_tmo.id
#
#     default_inst_in_db = dict()
#     default_inst_in_db.update(DEFAULT_DATA)
#     default_inst_in_db['name'] = NAME_OF_INST_THAT_CREATED_BEFORE_TESTS
#     inst = TPRM(**default_inst_in_db)
#     session.add(inst)
#     default_inst_in_db['name'] = NAME_OF_INST_THAT_CREATED_BEFORE_TESTS_2
#     inst = TPRM(**default_inst_in_db)
#     session.add(inst)
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
# def test_tprm_has_to_proto_method():
#     """TEST The TPRM class instance has a 'to_proto' method  """
#     ins = TPRM()
#     res = getattr(ins, 'to_proto', False)
#     assert res is not False
#
#
# def test_tprm_to_proto_method_is_successful():
#     """TEST The TPRM class instance has can use to_proto without errors  """
#     ins = TPRM(**DEFAULT_DATA)
#     ins.to_proto()
#
#
# def test_message_formed_on_create_tprm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful creation of the TPRM, a message to kafka is generated
#     successfully."""
#
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#     ins = TPRM(**DEFAULT_DATA)
#     session.add(ins)
#     session.flush()
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.TPRM(**ins.to_proto())
#     proto_msg = inventory_instances_pb2.ListTPRM(objects=[proto_unit])
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'TPRM', ObjEventStatus.CREATED)
#
#
# def test_message_formed_on_delete_tprm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful delete of the TPRM, a message to kafka is generated
#         successfully."""
#
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(TPRM).where(TPRM.name == NAME_OF_INST_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.TPRM(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListTPRM(objects=[proto_unit])
#
#     session.delete(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'TPRM', ObjEventStatus.DELETED)
#
#
# def test_message_formed_on_update_tprm(session: Session, mocker, kafka_partition):
#     """TEST In case of a successful update of the TPRM, a message to kafka is generated
#     successfully."""
#
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(TPRM).where(TPRM.name == NAME_OF_INST_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.name = 'Changed name'
#
#     # Expectation
#     proto_unit = inventory_instances_pb2.TPRM(**inst_from_db.to_proto())
#     proto_msg = inventory_instances_pb2.ListTPRM(objects=[proto_unit])
#
#     session.add(inst_from_db)
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'TPRM', ObjEventStatus.UPDATED)
#
#
# def test_right_order_of_events_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several events with TPRM instances: create, delete, update, than
#     msg must be sent in order: create - first, update - second, delete - third!."""
#
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     # update
#     statement = select(TPRM).where(TPRM.name == NAME_OF_INST_THAT_CREATED_BEFORE_TESTS)
#     inst_from_db = session.execute(statement).scalar()
#     inst_from_db.name = 'Updated name'
#     session.add(inst_from_db)
#     session.flush()
#
#     # delete
#     statement = select(TPRM).where(TPRM.name == NAME_OF_INST_THAT_CREATED_BEFORE_TESTS_2)
#     inst_from_db = session.execute(statement).scalar()
#     session.delete(inst_from_db)
#
#     # create
#     ins = TPRM(**DEFAULT_DATA)
#     session.add(ins)
#
#     session.commit()
#
#     expected = [('TPRM', ObjEventStatus.CREATED), ('TPRM', ObjEventStatus.UPDATED),
#                 ('TPRM', ObjEventStatus.DELETED)]
#     res = [(x.args[1], x.args[2]) for x in spy.mock_calls]
#
#     assert res == expected
#
#
# def test_several_eq_events_from_one_session_in_one_msg(session: Session, mocker, kafka_partition):
#     """TEST if one session has several identical events with TPRM instances: all events
#     must be sent in one msg"""
#
#     mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka_with_partitions',
#     return_value=True)
#     spy = mocker.patch('services.kafka_service.protobuf_producer.SendMessageToKafka._send_to_kafka',
#     return_value=True)
#     listen(session, "after_flush", receive_after_flush)
#     listen(session, "after_commit", receive_after_commit)
#
#     statement = select(TPRM)
#     inst_from_db = session.execute(statement).scalars().all()
#
#     # Expectation
#     proto_mo = [inventory_instances_pb2.TPRM(**item.to_proto()) for item in inst_from_db]
#     proto_msg = inventory_instances_pb2.ListTPRM(objects=proto_mo)
#
#     for x in inst_from_db:
#         session.delete(x)
#
#     session.commit()
#
#     spy.assert_called_once_with(proto_msg, 'TPRM', ObjEventStatus.DELETED)
