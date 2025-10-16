# import pytest
# from sqlalchemy import select
# from sqlalchemy.orm import Session
#
# from models import TPRM, PRM
# from val_types.constants import ErrorHandlingType, two_way_mo_link_val_type_name
# from val_types.two_way_mo_link_val_type.prm.delete import check_delete_two_way_prms, delete_two_way_mo_link_prms
#
#
# def test_2way_mo_link_validation_delete_not_exist(session: Session):
#     data = [100500]
#     errors, filtered_data = check_delete_two_way_prms(session=session, prm_ids=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_delete_wrong_type(session: Session):
#     stmt = select(TPRM.id).where(TPRM.val_type != two_way_mo_link_val_type_name).limit(1)
#     stmt = select(PRM.id).where(PRM.tprm_id.in_(stmt)).limit(1)
#     data = session.execute(stmt).scalars().all()
#     errors, filtered_data = check_delete_two_way_prms(session=session, prm_ids=data)
#     assert len(errors) == 1
#     assert len(filtered_data) == 0
#
#
# def test_2way_mo_link_validation_delete_positive(session: Session):
#     data = [34000]
#     errors, filtered_data = check_delete_two_way_prms(session=session, prm_ids=data)
#     assert len(errors) == 0
#     assert len(filtered_data) == 2
#
#
# def test_2way_mo_link_raise_error(session: Session):
#     data = [100500]
#     with pytest.raises(ValueError):
#         delete_two_way_mo_link_prms(session=session, prm_ids=data)
#
#
# def test_2way_mo_link_validate_only(session: Session):
#     data = [100500, 34000]
#     errors, filtered_data = delete_two_way_mo_link_prms(
#         session=session, prm_ids=data, in_case_of_error=ErrorHandlingType.ONLY_CHECKING
#     )
#     assert len(errors) == 1
#     assert len(filtered_data) == 2
#
#     ids = [i.id for i in filtered_data]
#     stmt = select(PRM).where(PRM.id.in_(ids))
#     data = session.execute(stmt).scalars().all()
#     assert len(data) == 2
#
#
# def test_2way_mo_link_process_cleared(session: Session):
#     data = [100500, 34000]
#     errors, filtered_data = delete_two_way_mo_link_prms(
#         session=session, prm_ids=data, in_case_of_error=ErrorHandlingType.PROCESS_CLEARED
#     )
#     assert len(errors) == 1
#     assert len(filtered_data) == 2
#
#     ids = [i.id for i in filtered_data]
#     stmt = select(PRM).where(PRM.id.in_(ids))
#     data = session.execute(stmt).scalars().all()
#     assert not data
