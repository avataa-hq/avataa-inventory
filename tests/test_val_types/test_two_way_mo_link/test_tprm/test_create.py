# import pytest
# from sqlalchemy import select, func
# from sqlalchemy.orm import Session
#
# from models import TPRM
# from routers.parameter_type_router.schemas import TPRMCreate
# from val_types.constants import two_way_mo_link_val_type_name, ErrorHandlingType
# from val_types.two_way_mo_link_val_type.tprm.create import check_new_two_way_tprms, create_two_way_mo_link_tprms
#
#
# def test_2way_mo_link_validation_not_existing_tmo(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#
#             constraint='100500',
#             tmo_id=100499,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_not_2way_type(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type='str',
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_multiple(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=True,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_required(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_field_value(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#             field_value='100500',
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_prm_filter(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#             prm_link_filter='100500',
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_empty_constraint(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint=None,
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_blank_constraint(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_not_digit_constraint(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='12.5',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_same_tmo_constraint(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100500',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 3
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_reserved_name(session: Session):
#     data = [
#         TPRMCreate(
#             name='geometry',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_not_existed_constraint(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100500100500',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_combo_one_negative(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100500100500',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 2
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_positive(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 0
#     assert len(filtered_data) == 1
#
#
# def test_2way_mo_link_validation_not_unique_name(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#     ]
#     tprm = TPRM.from_orm(data[0], update={'created_by': '', 'modified_by': ''})
#     session.add(tprm)
#     session.commit()
#
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_multi_same_name(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#         TPRMCreate(
#             name='Test',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100500',
#             tmo_id=100501,
#         ),
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 4
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_multi_positive_negative(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test 1',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#         TPRMCreate(
#             name='Test 2',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         )
#     ]
#     errors, filtered_data = check_new_two_way_tprms(session=session, new_tprms=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 1
#
#
# def test_2way_mo_link_raise_error(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test 1',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#         TPRMCreate(
#             name='Test 2',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         )
#     ]
#     stmt = select(func.count(TPRM.id))
#
#     cnt_before = session.execute(stmt).scalar_one_or_none()
#     with pytest.raises(ValueError):
#         create_two_way_mo_link_tprms(session=session, new_tprms=data)
#     cnt_after = session.execute(stmt).scalar_one_or_none()
#     assert cnt_after == cnt_before
#
#
# def test_2way_mo_link_create_cleared(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test 1',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#         TPRMCreate(
#             name='Test 2',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         )
#     ]
#     stmt = select(func.count(TPRM.id))
#
#     cnt_before = session.execute(stmt).scalar_one_or_none()
#     errors, created_tprms = create_two_way_mo_link_tprms(session=session, new_tprms=data,
#                                                          in_case_of_error=ErrorHandlingType.PROCESS_CLEARED)
#     cnt_after = session.execute(stmt).scalar_one_or_none()
#     assert cnt_after == cnt_before + 2
#     assert len(errors) == 1
#
#     for created_tprm in created_tprms:
#         assert created_tprm.backward_link
#
#
# def test_2way_mo_link_create_only_check(session: Session):
#     data = [
#         TPRMCreate(
#             name='Test 1',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=True,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         ),
#         TPRMCreate(
#             name='Test 2',
#             val_type=two_way_mo_link_val_type_name,
#             multiple=False,
#             required=False,
#             returnable=False,
#             constraint='100501',
#             tmo_id=100500,
#         )
#     ]
#     stmt = select(func.count(TPRM.id))
#
#     cnt_before = session.execute(stmt).scalar_one_or_none()
#     errors, passed_tprms = create_two_way_mo_link_tprms(session=session, new_tprms=data,
#                                                         in_case_of_error=ErrorHandlingType.ONLY_CHECKING)
#     cnt_after = session.execute(stmt).scalar_one_or_none()
#     assert cnt_after == cnt_before
#     assert len(errors) == 1
#     assert len(passed_tprms) == 1
