# import pytest
# from sqlalchemy import select
# from sqlalchemy.orm import Session
#
# from models import TPRM, PRM
# from routers.parameter_router.schemas import PRMCreateByMO
# from val_types.constants import ErrorHandlingType
# from val_types.two_way_mo_link_val_type.prm.create import check_create_two_way_prms, create_two_way_mo_link_prms
#
#
# def test_2_way_mo_link_create_validation_exists_same_type(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100502: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_same_type_same_type(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100502: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=34001,
#             ),
#             PRMCreateByMO(
#                 value=100500,
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 5
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_no_exist_mo(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100489: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=34000,
#             )
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 2
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_no_tprm(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100500: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=33999,
#             )
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_tprm_id_mismatch(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=34000,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 2
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_empty_backward_link(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value=100501,
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     tprms_dict[34001].backward_link = None
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_not_integer(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value=100501 / 2,
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_str_value(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100501/2',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_not_exist_value(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100500100500',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_backward_tprm_mismatch(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100504',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_backward_multiple_link(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100500',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 1
#     assert len(cleared_prms) == 0
#
#
# def test_2_way_mo_link_create_validation_positive(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100501',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     errors, cleared_prms = check_create_two_way_prms(session=session, new_prms=data, tprms=tprms_dict)
#     assert len(errors) == 0
#     assert len(cleared_prms) == 1
#
#
# def test_2_way_mo_link_create_prms_raise_error(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100503: [
#             PRMCreateByMO(
#                 value='100500',
#                 tprm_id=34001,
#             ),
#         ]
#     }
#     tprms = list(tprms_dict.values())
#     stmt = select(PRM)
#     prms_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in prms_before]
#     with pytest.raises(ValueError):
#         create_two_way_mo_link_prms(
#             session=session, new_parameters=data, parameter_types=tprms,
#             in_case_of_error=ErrorHandlingType.RAISE_ERROR
#         )
#     prms_after = session.execute(stmt).scalars().all()
#     assert prms_before == prms_after
#
#
# def test_2_way_mo_link_create_prm_create_cleared(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100504: [
#             PRMCreateByMO(
#                 value='100500',
#                 tprm_id=34001,
#             ),
#         ],
#         100503: [
#             PRMCreateByMO(
#                 value='100501',
#                 tprm_id=34001,
#             ),
#         ],
#     }
#     tprms = list(tprms_dict.values())
#     stmt = select(PRM)
#     prms_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in prms_before]
#     errors, created_prms = create_two_way_mo_link_prms(
#         session=session, new_parameters=data, parameter_types=tprms,
#         in_case_of_error=ErrorHandlingType.PROCESS_CLEARED
#     )
#     prms_after = session.execute(stmt).scalars().all()
#
#     assert len(errors) == 2
#     assert len(created_prms) == 2
#     for created_prm in created_prms:
#         prms_after.remove(created_prm)
#     assert prms_before == prms_after
#
#
# def test_2_way_mo_link_create_prm_only_check(tprms_dict: dict[int, TPRM], session: Session):
#     data = {
#         100504: [
#             PRMCreateByMO(
#                 value='100500',
#                 tprm_id=34001,
#             ),
#         ],
#         100503: [
#             PRMCreateByMO(
#                 value='100501',
#                 tprm_id=34001,
#             ),
#         ],
#     }
#     tprms = list(tprms_dict.values())
#     stmt = select(PRM)
#     prms_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in prms_before]
#     errors, cleared_prms = create_two_way_mo_link_prms(
#         session=session, new_parameters=data, parameter_types=tprms, in_case_of_error=ErrorHandlingType.ONLY_CHECKING
#     )
#     prms_after = session.execute(stmt).scalars().all()
#
#     assert len(errors) == 2
#     assert len(cleared_prms) == 1
#     assert prms_before == prms_after
