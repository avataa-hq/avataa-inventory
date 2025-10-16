# from collections import namedtuple
#
# from routers.object_router.utils import concat_order_by
#
#
# def test_concat_order_by_none(session):
#     order_by_tprms_id = None
#     order_by_asc = None
#
#     result = concat_order_by(session=session, order_by_tprms_id=order_by_tprms_id, order_by_asc=order_by_asc)
#     assert result is None
#
#
# def test_concat_order_by_diff(session):
#     order_value = namedtuple('OrderValue', 'type ascending')
#
#     order_by_tprms_id = [1, 2, 3]
#     order_by_asc = [False]
#     expected = {
#         1: order_value(type='str', ascending=False),
#         2: order_value(type='prm_link', ascending=False),
#         3: order_value(type='str', ascending=False),
#                 }
#
#     result = concat_order_by(session=session, order_by_tprms_id=order_by_tprms_id, order_by_asc=order_by_asc)
#     assert result == expected
#
#
# def test_concat_order_by_diff_err(session):
#     order_by_tprms_id = [1, 2, 3]
#     order_by_asc = [False, True]
#
#     result = concat_order_by(session=session, order_by_tprms_id=order_by_tprms_id, order_by_asc=order_by_asc)
#     assert result is None
#
#
# def test_concat_order_by_diff_err2(session):
#     order_by_tprms_id = [1, 2]
#     order_by_asc = [False, True, False]
#
#     result = concat_order_by(session=session, order_by_tprms_id=order_by_tprms_id, order_by_asc=order_by_asc)
#     assert result is None
#
#
# def test_concat_order_by_diff_positive(session):
#     order_value = namedtuple('OrderValue', 'type ascending')
#
#     order_by_tprms_id = [1, 2, 3, 4, 5, 100500]
#     order_by_asc = [False, True, False, False, True, False]
#     expected = {
#         1: order_value(type='str', ascending=False),
#         2: order_value(type='prm_link', ascending=True),
#         3: order_value(type='str', ascending=False),
#         4: order_value(type='mo_link', ascending=False),
#         5: order_value(type='str', ascending=True)
#     }
#
#     result = concat_order_by(session=session, order_by_tprms_id=order_by_tprms_id, order_by_asc=order_by_asc)
#     assert result == expected
