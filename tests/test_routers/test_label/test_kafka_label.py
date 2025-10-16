#
# def test_message_formed_on_create_with_label(kafka_mock, kafka_partial_mock, listened_session: Session,
# mo_data: dict):
#     ins = MO(**mo_data)
#     listened_session.add(ins)
#     listened_session.flush()
#     # Expectation
#     proto_mo = inventory_instances_pb2.MO(**ins.to_proto())  # noqa
#     assert hasattr(proto_mo, 'label')
#     assert proto_mo.label
#     proto_msg = inventory_instances_pb2.ListMO(objects=[proto_mo])  # noqa
#
#     listened_session.commit()
#
#     kafka_mock.assert_called_once_with(proto_msg, 'MO', ObjEventStatus.CREATED)
#
#     kafka_partial_msg = defaultdict(list)
#     kafka_partial_msg[(1,)] = [proto_msg]
#     kafka_partial_mock.assert_called_once_with(
#         msg=kafka_partial_msg, obj_class_name='MO', event=ObjEventStatus.CREATED
#     )
