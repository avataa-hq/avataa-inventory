# import pytest
# from sqlalchemy import select
# from sqlalchemy.orm import Session
#
# from models import TPRM, PRM
# from val_types.constants import ErrorHandlingType
#
# from routers.parameter_router.schemas import PRMUpdateByMO
# from val_types.two_way_mo_link_val_type.prm.update import check_update_two_way_prms, update_two_way_mo_link_prms
#
#
# def test_2way_prms__update_validation_same_param(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100501,
#                 tprm_id=34000,
#             ),
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 2
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_not_exist_mo(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100499: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_not_exist_prm(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000_100500,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     print(errors)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_same_value(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100502,
#                 tprm_id=34000,
#             )
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_empty_value(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value='',
#                 tprm_id=34000,
#             )
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_str_value(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value='test',
#                 tprm_id=34000,
#             )
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_multi_reference(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100500,
#                 tprm_id=34000,
#             )
#         ],
#         100501: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100500,
#                 tprm_id=34000,
#             )
#         ],
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 2
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_multi_reference_2(tprms_dict: dict[int, TPRM], session: Session):
#     new_prms = [
#         PRM(tprm_id=34000, mo_id=100501, value='100503', id=34002, backward_link=34003),
#         PRM(tprm_id=34001, mo_id=100503, value='100501', id=34003, backward_link=34002),
#     ]
#     session.add_all(new_prms)
#     session.commit()
#
#     data = {
#         100501: [
#             PRM(tprm_id=34000, value='100502', id=34002),
#         ],
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_version(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=2,
#                 id=34000,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ]
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_validation_both_params(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRM(
#                 version=1,
#                 id=34000,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ],
#         100502: [
#             PRM(
#                 version=1,
#                 id=34001,
#                 value=100501,
#                 tprm_id=34001,
#             ),
#         ],
#     }
#     errors, filtered_data = check_update_two_way_prms(session=session, update_prms=data, tprms_by_tprm_id=tprms_dict)
#     assert len(errors) == 2
#     assert len(filtered_data) == 0
#
#
# def test_2way_prms__update_raise_error(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRMUpdateByMO(
#                 version=1,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ],
#         100502: [
#             PRMUpdateByMO(
#                 version=1,
#                 value=100501,
#                 tprm_id=34001,
#             ),
#         ],
#     }
#     tprms = list(tprms_dict.values())
#     with pytest.raises(ValueError):
#         update_two_way_mo_link_prms(session=session, update_prms=data, tprms=tprms,
#                                     in_case_of_error=ErrorHandlingType.RAISE_ERROR)
#
#
# def test_2way_prms__update_only_check(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRMUpdateByMO(
#                 version=1,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ],
#     }
#     tprms = list(tprms_dict.values())
#     stmt = select(PRM)
#     prms_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in prms_before]
#     errors, filtered_data = update_two_way_mo_link_prms(session=session, update_prms=data, tprms=tprms,
#                                                         in_case_of_error=ErrorHandlingType.ONLY_CHECKING)
#     prms_after = session.execute(stmt).scalars().all()
#
#     assert len(errors) == 0
#     assert len(filtered_data) == 1
#     assert prms_before == prms_after
#
#
# def test_2way_prms__update_process_cleared(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRMUpdateByMO(
#                 version=1,
#                 value=100503,
#                 tprm_id=34000,
#             ),
#         ],
#     }
#     tprms = list(tprms_dict.values())
#     stmt = select(PRM)
#     prms_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in prms_before]
#     errors, filtered_data = update_two_way_mo_link_prms(session=session, update_prms=data, tprms=tprms,
#                                                         in_case_of_error=ErrorHandlingType.PROCESS_CLEARED)
#     prms_after = session.execute(stmt).scalars().all()
#
#     assert len(errors) == 0
#     assert len(filtered_data) == 2
#     assert len(set(map(str, prms_before)).difference(set(map(str, prms_after)))) == 2
