# from fastapi.testclient import TestClient
# from sqlalchemy import select, and_
# from sqlmodel import Session
#
# from functions.functions_utils.utils import calculate_by_formula_batch
# from models import TMO, TPRM, MO, PRM
#
#
# def test_batch_utils_for_complex_inner_formula(session: Session, client: TestClient):
#     """Check inner utils for batch import and calculate formula with inner max"""
#     tmo_formula_name = 'TMO_FORMULA'
#
#     tprm_for_formula_name = 'TPRM FORMULA'
#     tprm_for_formula_type = 'formula'
#     formula_increment =  1
#
#     tprm_for_int_first_name = 'TPRM INT 1'
#     tprm_for_int_first_type = 'int'
#     tprm_for_int_second_name = 'TPRM INT 2'
#     tprm_for_int_second_type = 'int'
#
#     prm_value_int_first = 20
#     prm_value_int_second = 10
#
#     TMO_FORMULA = {'name': tmo_formula_name,
#                    'created_by': 'Test creator',
#                    'modified_by': 'Test modifier'}
#     tmo_formula = TMO(**TMO_FORMULA)
#     session.add(tmo_formula)
#     session.commit()
#     session.refresh(tmo_formula)
#
#     TPRM_FOR_INT_FIRST = {'name': tprm_for_int_first_name,
#                           'val_type': tprm_for_int_first_type,
#                           'returnable': True,
#                           'tmo_id': tmo_formula.id,
#                           'created_by': 'Test creator',
#                           'modified_by': 'Test modifier'}
#     tprm_for_int_first = TPRM(**TPRM_FOR_INT_FIRST)
#     session.add(tprm_for_int_first)
#     TPRM_FOR_INT_SECOND = {'name': tprm_for_int_second_name,
#                            'val_type': tprm_for_int_second_type,
#                            'returnable': True,
#                            'tmo_id': tmo_formula.id,
#                            'created_by': 'Test creator',
#                            'modified_by': 'Test modifier'}
#     tprm_for_int_second = TPRM(**TPRM_FOR_INT_SECOND)
#     session.add(tprm_for_int_second)
#     session.commit()
#     session.refresh(tprm_for_int_first)
#     session.refresh(tprm_for_int_second)
#     session.refresh(tmo_formula)
#     formula_constraint = (f"if INNER_MAX['{tprm_for_int_first_name}'] > INNER_MAX['{tprm_for_int_second_name}'] "
#                           f"then INNER_MAX['{tprm_for_int_first_name}'] + {formula_increment}; "
#                           f"else INNER_MAX['{tprm_for_int_second_name}'] + {formula_increment}")
#     # Create TPRM
#     data = {'name': tprm_for_formula_name,
#             'val_type': tprm_for_formula_type,
#             'constraint': formula_constraint,
#             'returnable': True,
#             'tmo_id': tmo_formula.id}
#     res = client.post(f"/api/inventory/v1/param_type", json=data)
#     print(res.json())
#     assert res.status_code == 200
#     formula_tprm: TPRM = TPRM(**res.json())
#     current_tprm_version = res.json().get("version")
#     # Create couple MO
#     data = {'tmo_id': tmo_formula.id,
#             'params': [{'value': prm_value_int_first,
#                         'tprm_id': tprm_for_int_first.id},
#                        {'value': prm_value_int_second,
#                         'tprm_id': tprm_for_int_second.id}]}
#     res = client.post(f"/api/inventory/v1/object_with_parameters/", json=data)
#     assert res.status_code == 200
#     mo_1: MO = MO(**res.json())
#     stmt = select(PRM).where(and_(PRM.tprm_id == formula_tprm.id, PRM.mo_id == mo_1.id))
#     prm_for_mo_1: PRM = session.execute(stmt).scalar()
#     assert prm_for_mo_1.value == str(max(prm_value_int_first, prm_value_int_second) + 1)
#     # Add data from batch
#     prm_data = {}
#     result = calculate_by_formula_batch(session=session, formula_tprm=formula_tprm, prm_data=prm_data)
#     assert result == max(prm_value_int_first, prm_value_int_second) + formula_increment
