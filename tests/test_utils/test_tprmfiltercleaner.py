# """TESTs for TPRMFilterCliner"""
# import pytest
# from sqlalchemy import or_, cast, Numeric
# from sqlmodel import Session, select
# from starlette.datastructures import ImmutableMultiDict, QueryParams
#
# from models import TMO, TPRM, MO, PRM
# from routers.object_router.utils import TPRMFilterCleaner
#
# TMO_DEFAULT_DATA = {'name': 'Test TMO',
#                     'version': 1,
#                     'created_by': 'Test admin',
#                     'modified_by': 'Test admin',
#                     'status': 'active'}
#
#
# TPRM_STR = {'name': 'STR',
#             'val_type': 'str',
#             'required': False,
#             'created_by': 'Test admin',
#             'modified_by': 'Test admin'}
#
#
# TPRM_DATE = {'name': 'DATE',
#              'val_type': 'date',
#              'required': False,
#              'created_by': 'Test admin',
#              'modified_by': 'Test admin'}
#
#
# TPRM_DATETIME = {'name': 'DATETIME',
#                  'val_type': 'datetime',
#                  'required': False,
#                  'created_by': 'Test admin',
#                  'modified_by': 'Test admin'}
#
#
# TPRM_FLOAT = {'name': 'FLOAT',
#               'val_type': 'float',
#               'required': False,
#               'created_by': 'Test admin',
#               'modified_by': 'Test admin'}
#
#
# TPRM_INT = {'name': 'INT',
#             'val_type': 'int',
#             'required': False,
#             'created_by': 'Test admin',
#             'modified_by': 'Test admin'}
#
#
# TPRM_BOOL = {'name': 'BOOL',
#              'val_type': 'bool',
#              'required': False,
#              'created_by': 'Test admin',
#              'modified_by': 'Test admin'}
#
#
# TPRM_MO_LINK = {'name': 'MO_LINK',
#                 'val_type': 'mo_link',
#                 'required': False,
#                 'created_by': 'Test admin',
#                 'modified_by': 'Test admin'}
#
#
# TPRM_PRM_LINK = {'name': 'PRM_LINK',
#                  'val_type': 'prm_link',
#                  'required': False,
#                  'created_by': 'Test admin',
#                  'modified_by': 'Test admin'}
#
#
# TPRM_USER_LINK = {'name': 'USER_LINK',
#                   'val_type': 'user_link',
#                   'required': False,
#                   'created_by': 'Test admin',
#                   'modified_by': 'Test admin'}
#
#
# TPRM_FORMULA = {'name': 'Formula',
#                 'val_type': 'formula',
#                 'required': False,
#                 'created_by': 'Test admin',
#                 'modified_by': 'Test admin'}
#
#
# @pytest.fixture(scope='function', autouse=True)
# def session_fixture(session):
#
#     tmo = TMO(**TMO_DEFAULT_DATA)
#     session.add(tmo)
#     session.flush()
#
#     tprm_str = TPRM(**TPRM_STR)
#     tprm_str.tmo_id = tmo.id
#     session.add(tprm_str)
#
#     tprm_date = TPRM(**TPRM_DATE)
#     tprm_date.tmo_id = tmo.id
#     session.add(tprm_date)
#
#     tprm_datetime = TPRM(**TPRM_DATETIME)
#     tprm_datetime.tmo_id = tmo.id
#     session.add(tprm_datetime)
#
#     tprm_float = TPRM(**TPRM_FLOAT)
#     tprm_float.tmo_id = tmo.id
#     session.add(tprm_float)
#
#     tprm_int = TPRM(**TPRM_INT)
#     tprm_int.tmo_id = tmo.id
#     session.add(tprm_int)
#
#     tprm_bool = TPRM(**TPRM_BOOL)
#     tprm_bool.tmo_id = tmo.id
#     session.add(tprm_bool)
#
#     session.flush()
#     # str start
#     mo_1 = MO(name='MO 1', tmo_id=tmo.id, active=True)
#     mo_2 = MO(name='MO 2', tmo_id=tmo.id, active=True)
#     mo_3 = MO(name='MO 3', tmo_id=tmo.id, active=True)
#     mo_4 = MO(name='MO 4', tmo_id=tmo.id, active=True)
#     session.add(mo_1)
#     session.add(mo_2)
#     session.add(mo_3)
#     session.add(mo_4)
#     session.flush()
#
#     prm_str_1 = PRM(mo_id=mo_1.id, tprm_id=tprm_str.id, value='mo 1 value')
#     prm_str_2 = PRM(mo_id=mo_2.id, tprm_id=tprm_str.id, value='mo 2 value')
#     prm_str_3 = PRM(mo_id=mo_3.id, tprm_id=tprm_str.id, value='mo 3 value')
#
#     session.add(prm_str_1)
#     session.add(prm_str_2)
#     session.add(prm_str_3)
#     # str end
#     # int start
#     mo_1 = MO(name='MO 1', tmo_id=tmo.id, active=True)
#     mo_2 = MO(name='MO 2', tmo_id=tmo.id, active=True)
#     mo_3 = MO(name='MO 3', tmo_id=tmo.id, active=True)
#     mo_4 = MO(name='MO 4', tmo_id=tmo.id, active=True)
#     session.add(mo_1)
#     session.add(mo_2)
#     session.add(mo_3)
#     session.add(mo_4)
#     session.flush()
#
#     prm_str_1 = PRM(mo_id=mo_1.id, tprm_id=tprm_int.id, value=1)
#     prm_str_2 = PRM(mo_id=mo_2.id, tprm_id=tprm_int.id, value=2)
#     prm_str_3 = PRM(mo_id=mo_3.id, tprm_id=tprm_int.id, value=3)
#
#     session.add(prm_str_1)
#     session.add(prm_str_2)
#     session.add(prm_str_3)
#     # int end
#     # float start
#     mo_1 = MO(name='MO 1', tmo_id=tmo.id, active=True)
#     mo_2 = MO(name='MO 2', tmo_id=tmo.id, active=True)
#     mo_3 = MO(name='MO 3', tmo_id=tmo.id, active=True)
#     mo_4 = MO(name='MO 4', tmo_id=tmo.id, active=True)
#     session.add(mo_1)
#     session.add(mo_2)
#     session.add(mo_3)
#     session.add(mo_4)
#     session.flush()
#
#     prm_str_1 = PRM(mo_id=mo_1.id, tprm_id=tprm_float.id, value=2.1)
#     prm_str_2 = PRM(mo_id=mo_2.id, tprm_id=tprm_float.id, value=2.2)
#     prm_str_3 = PRM(mo_id=mo_3.id, tprm_id=tprm_float.id, value=2.3)
#
#     session.add(prm_str_1)
#     session.add(prm_str_2)
#     session.add(prm_str_3)
#     # float end
#     # bool start
#     mo_1 = MO(name='MO 1', tmo_id=tmo.id, active=True)
#     mo_2 = MO(name='MO 2', tmo_id=tmo.id, active=True)
#     mo_3 = MO(name='MO 3', tmo_id=tmo.id, active=True)
#     mo_4 = MO(name='MO 4', tmo_id=tmo.id, active=True)
#     session.add(mo_1)
#     session.add(mo_2)
#     session.add(mo_3)
#     session.add(mo_4)
#     session.flush()
#
#     prm_str_1 = PRM(mo_id=mo_1.id, tprm_id=tprm_bool.id, value='True')
#     prm_str_2 = PRM(mo_id=mo_2.id, tprm_id=tprm_bool.id, value='False')
#     prm_str_3 = PRM(mo_id=mo_3.id, tprm_id=tprm_bool.id, value='True')
#
#     session.add(prm_str_1)
#     session.add(prm_str_2)
#     session.add(prm_str_3)
#     # date end
#
#     session.commit()
#     yield session
#
#
# def test_str_filter_condition_contains_successful(session: Session):
#     """Condition 'contains' successful for TPRM.val_type = str"""
#     contains_condition = '1'
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|contains', contains_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value.contains(contains_condition)).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_filter_condition_equals_successful(session: Session):
#     """Condition 'equals' successful for TPRM.val_type = str"""
#     equals_condition = 'mo 1 value'
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|equals', equals_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value == equals_condition).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_filter_condition_starts_with_successful(session: Session):
#     """Condition 'starts_with' successful for TPRM.val_type = str"""
#     startswith_condition = 'mo'
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|starts_with', startswith_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value.startswith(startswith_condition)).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_filter_condition_ends_with_successful(session: Session):
#     """Condition 'ends_with' successful for TPRM.val_type = str"""
#     ends_with_condition = 'value'
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|ends_with', ends_with_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value.endswith(ends_with_condition)).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_filter_condition_is_empty_successful(session: Session):
#     """Condition 'is_empty' successful for TPRM.val_type = str"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(MO.id)
#     all_mo_ids = session.exec(stmt).all()
#
#     stmt = select(PRM.mo_id).where(PRM.mo_id.in_(all_mo_ids), PRM.tprm_id == tprm.id)
#     mo_ids_with_values = session.exec(stmt).all()
#     empty_ids = set(all_mo_ids).difference(mo_ids_with_values)
#
#     assert empty_ids == set(result_by_filter_cleaner)
#
#
# def test_str_filter_condition_is_not_empty_successful(session: Session):
#     """Condition 'is_not_empty' successful for TPRM.val_type = str"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_not_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_filter_condition_is_any_of_successful(session: Session):
#     """Condition 'is_any_of' successful for TPRM.val_type = str"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     stmt = select(PRM.value).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     assert len(res_from_db) > 1
#
#     any_of_condition = ';'.join(res_from_db[:2])
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, PRM.value.in_(res_from_db[:2])).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_any_of', any_of_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_str_several_same_filter_conditions_successful(session: Session):
#     """TPRMFilterCleaner successfully support for equivalent multiple filters with different values"""
#     contains_cond_1 = '1'
#     contains_cond_2 = '2'
#
#     stmt = select(TPRM).where(TPRM.val_type == 'str')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|contains', contains_cond_1),
#                                (f'tprm_id{tprm.id}|contains', contains_cond_2),
#                                ('filter_logical_operator', 'or')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#     result = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(or_(PRM.value.contains(contains_cond_2),
#                                        PRM.value.contains(contains_cond_1))
#                                    ).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#
#     assert res_from_db == result
#
#
# def test_int_filter_condition_equals_successful(session: Session):
#     """Condition 'equals' successful for TPRM.val_type = int"""
#     equals = 1
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|equals', equals)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value == str(equals)).where(PRM.tprm_id == tprm.id)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_int_filter_condition_is_empty_successful(session: Session):
#     """Condition 'is_empty' successful for TPRM.val_type = int"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(MO.id)
#     all_mo_ids = session.exec(stmt).all()
#
#     stmt = select(PRM.mo_id).where(PRM.mo_id.in_(all_mo_ids), PRM.tprm_id == tprm.id)
#     mo_ids_with_values = session.exec(stmt).all()
#     empty_ids = set(all_mo_ids).difference(mo_ids_with_values)
#
#     assert empty_ids == set(result_by_filter_cleaner)
#
#
# def test_int_filter_condition_is_not_empty_successful(session: Session):
#     """Condition 'is_not_empty' successful for TPRM.val_type = int"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_not_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_int_filter_condition_is_any_of_successful(session: Session):
#     """Condition 'is_any_of' successful for TPRM.val_type = int"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     stmt = select(PRM.value).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     assert len(res_from_db) > 1
#
#     any_of_condition = ';'.join(res_from_db[:2])
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, PRM.value.in_(res_from_db[:2])).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_any_of', any_of_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_int_filter_condition_more_successful(session: Session):
#     """Condition 'more' successful for TPRM.val_type = int"""
#
#     more = 2
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|more', more)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#     stmt = select(PRM.mo_id).where(
#         PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) > more
#     )
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
#
# def test_int_filter_condition_more_or_eq_successful(session: Session):
#     """Condition 'more_or_eq' successful for TPRM.val_type = int"""
#
#     more_or_eq = 2
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|more_or_eq', more_or_eq)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) >= more_or_eq)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_int_filter_condition_less_successful(session: Session):
#     """Condition 'less' successful for TPRM.val_type = int"""
#
#     less = 2
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|less', less)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) < less)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_int_filter_condition_less_or_eq_successful(session: Session):
#     """Condition 'less_or_eq' successful for TPRM.val_type = int"""
#
#     less = 2
#     stmt = select(TPRM).where(TPRM.val_type == 'int')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|less_or_eq', less)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) <= less)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_equals_successful(session: Session):
#     """Condition 'equals' successful for TPRM.val_type = float"""
#     equals = 2.2
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|equals', equals)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) == equals)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_is_empty_successful(session: Session):
#     """Condition 'is_empty' successful for TPRM.val_type = float"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(MO.id)
#     all_mo_ids = session.exec(stmt).all()
#
#     stmt = select(PRM.mo_id).where(PRM.mo_id.in_(all_mo_ids), PRM.tprm_id == tprm.id)
#     mo_ids_with_values = session.exec(stmt).all()
#     empty_ids = set(all_mo_ids).difference(mo_ids_with_values)
#
#     assert empty_ids == set(result_by_filter_cleaner)
#
#
# def test_float_filter_condition_is_not_empty_successful(session: Session):
#     """Condition 'is_not_empty' successful for TPRM.val_type = float"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_not_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_is_any_of_successful(session: Session):
#     """Condition 'is_any_of' successful for TPRM.val_type = float"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     stmt = select(PRM.value).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     assert len(res_from_db) > 1
#
#     any_of_condition = ';'.join(res_from_db[:2])
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, PRM.value.in_(res_from_db[:2])).distinct()
#     res_from_db = session.exec(stmt).all()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_any_of', any_of_condition)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_more_successful(session: Session):
#     """Condition 'more' successful for TPRM.val_type = float"""
#
#     more = 2.2
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|more', more)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) > more)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_more_or_eq_successful(session: Session):
#     """Condition 'more_or_eq' successful for TPRM.val_type = float"""
#
#     more_or_eq = 2.2
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|more_or_eq', more_or_eq)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) >= more_or_eq)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_less_successful(session: Session):
#     """Condition 'less' successful for TPRM.val_type = float"""
#
#     less = 2.2
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|less', less)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) < less)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_float_filter_condition_less_or_eq_successful(session: Session):
#     """Condition 'less_or_eq' successful for TPRM.val_type = float"""
#
#     less = 2.2
#     stmt = select(TPRM).where(TPRM.val_type == 'float')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|less_or_eq', less)])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id, cast(PRM.value, Numeric()) <= less)
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_bool_filter_condition_is_true_successful(session: Session):
#     """Condition 'is_true' successful for TPRM.val_type = bool"""
#     stmt = select(TPRM).where(TPRM.val_type == 'bool')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_true', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value == 'True').where(PRM.tprm_id == tprm.id) # noqa
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_bool_filter_condition_is_false_successful(session: Session):
#     """Condition 'is_false' successful for TPRM.val_type = bool"""
#     stmt = select(TPRM).where(TPRM.val_type == 'bool')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_false', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.value == 'False').where(PRM.tprm_id == tprm.id) # noqa
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
#
#
# def test_bool_filter_condition_is_empty_successful(session: Session):
#     """Condition 'is_empty' successful for TPRM.val_type = bool"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'bool')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(MO.id)
#     all_mo_ids = session.exec(stmt).all()
#
#     stmt = select(PRM.mo_id).where(PRM.mo_id.in_(all_mo_ids), PRM.tprm_id == tprm.id)
#     mo_ids_with_values = session.exec(stmt).all()
#     empty_ids = set(all_mo_ids).difference(mo_ids_with_values)
#
#     assert empty_ids == set(result_by_filter_cleaner)
#
#
# def test_bool_filter_condition_is_not_empty_successful(session: Session):
#     """Condition 'is_not_empty' successful for TPRM.val_type = bool"""
#
#     stmt = select(TPRM).where(TPRM.val_type == 'bool')
#     tprm = session.exec(stmt).first()
#
#     data = ImmutableMultiDict([(f'tprm_id{tprm.id}|is_not_empty', '')])
#
#     query_params = QueryParams(data)
#     filter_cleaner = TPRMFilterCleaner(session, query_params)
#
#     result_by_filter_cleaner = filter_cleaner.get_mo_ids_which_match_clean_filter_conditions()
#
#     stmt = select(PRM.mo_id).where(PRM.tprm_id == tprm.id).distinct()
#     res_from_db = session.exec(stmt).all()
#     assert len(res_from_db) > 0
#     assert res_from_db == result_by_filter_cleaner
