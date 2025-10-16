# import pytest
# from sqlalchemy import select
# from sqlalchemy.orm import Session
#
# from models import TPRM
# from val_types.constants import two_way_mo_link_val_type_name, ErrorHandlingType
#
# from routers.parameter_type_router.schemas import TPRMUpdate
# from val_types.two_way_mo_link_val_type.tprm.update import check_update_two_way_tprms, update_two_way_mo_link_tprms
#
#
# def test_2way_mo_link_update_validation_not_existing_tmo(session: Session):
#     data = {
#         100500100500: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=False,
#             constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_existing_name(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='TPRM 1',
#             # required=False,
#             # field_value=False,
#             # returnable=False,
#             # constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_empty_name(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='',
#             # required=False,
#             # field_value=False,
#             # returnable=False,
#             # constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_reserved_name(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='geometry',
#             # required=False,
#             # field_value=False,
#             # returnable=False,
#             # constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_required(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             required=True,
#             # field_value=False,
#             # returnable=False,
#             # constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_field_value(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             field_value='123',
#             # returnable=False,
#             # constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_updat_validatione_empty_constraint(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             # field_value='123',
#             # returnable=False,
#             constraint=None,
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_float_constraint(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             # field_value='123',
#             #
#             # returnable=False,
#             #
#             constraint='434343.4343',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_no_existing_constraint(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             # field_value='123',
#             #
#             # returnable=False,
#             #
#             constraint='100499',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 2
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_same_tmo_constraint(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             # field_value='123',
#             #
#             # returnable=False,
#             #
#             constraint='100500',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_version(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             # name='Test',
#             # required=True,
#             # field_value='123',
#             #
#             # returnable=False,
#             #
#             # constraint='100500',
#             version=2,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_nothing_to_change(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_without_force(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_update_validation_positive(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#             force=True,
#         )
#     }
#     errors, filtered_data = check_update_two_way_tprms(session=session, update_tprms=data)
#     assert len(errors) == 0
#     assert len(filtered_data) == 1
#
#
# def test_2way_mo_link_update_updated_twice(session: Session):
#     data = {
#         34001: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#         ),
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#             force=True,
#         ),
#     }
#     errors, checked_tprms = check_update_two_way_tprms(session=session, update_tprms=data)
#
#     assert len(errors) == 3
#     assert len(checked_tprms) == 0
#
#
# def test_2way_mo_link_update_raise_error(session: Session):
#     data = {
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#         )
#     }
#     stmt = select(TPRM).where(TPRM.id == 34000)
#
#     tprm_before = session.execute(stmt).scalar_one_or_none()
#     with pytest.raises(ValueError):
#         update_two_way_mo_link_tprms(session=session, update_tprms=data)
#     tprm_after = session.execute(stmt).scalar_one_or_none()
#     assert tprm_before == tprm_after
#
#
# def test_2way_mo_link_update_only_check(session: Session):
#     data = {
#         34001: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#         ),
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#             force=True,
#         ),
#         34003: TPRMUpdate(
#             constraint='100502',
#             version=1,
#             force=True,
#         ),
#     }
#     stmt = select(TPRM).where(TPRM.id.in_((34000, 34001, 34002, 34003)))
#
#     tprm_before = session.execute(stmt).scalars().all()
#     errors, checked_tprms = update_two_way_mo_link_tprms(
#         session=session, update_tprms=data, in_case_of_error=ErrorHandlingType.ONLY_CHECKING
#     )
#     tprm_after = session.execute(stmt).scalars().all()
#     assert tprm_before == tprm_after
#     assert len(errors) == 3
#     assert len(checked_tprms) == 1
#
#
# def test_2way_mo_link_update_process_cleared(session: Session):
#     data = {
#         34001: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#         ),
#         34000: TPRMUpdate(
#             name='Test',
#             required=False,
#             field_value=False,
#             returnable=True,
#             constraint='100502',
#             version=1,
#             force=True,
#         ),
#         34003: TPRMUpdate(
#             constraint='100502',
#             version=1,
#             force=True,
#         ),
#     }
#     stmt = select(TPRM).where(
#         TPRM.tmo_id.in_((100500, 100501, 100502, 100503)), TPRM.val_type == two_way_mo_link_val_type_name
#     )
#
#     tprm_before = session.execute(stmt).scalars().all()
#     [session.expunge(i) for i in tprm_before]
#     errors, checked_tprms = update_two_way_mo_link_tprms(
#         session=session, update_tprms=data, in_case_of_error=ErrorHandlingType.PROCESS_CLEARED
#     )
#     tprm_after = session.execute(stmt).scalars().all()
#     assert tprm_after != tprm_before
#     assert len(tprm_after) == len(tprm_before)
#     assert len(errors) == 3
#     assert len(checked_tprms) == 2
