# import pytest
# from fastapi import HTTPException
#
# from routers.object_type_router.router import read_object_types, create_object_type, update_object_type, \
#     delete_object_type
# from routers.object_type_router.schemas import TMOUpdate, TMOCreate
#
#
# @pytest.mark.asyncio
# async def test_read_object_types_other(session, jwt_other):
#     session.info['action'] = 'read'
#     session.info['jwt'] = jwt_other
#     result = await read_object_types(session=session, object_types_ids=None)
#     assert len(result) == 1
#
#
# @pytest.mark.asyncio
# async def test_read_object_types_reader(session, jwt_reader):
#     session.info['action'] = 'read'
#     session.info['jwt'] = jwt_reader
#     result = await read_object_types(session=session, object_types_ids=None)
#     assert len(result) == 3
#
#
# @pytest.mark.asyncio
# async def test_read_object_types_admin(session, jwt_admin):
#     session.info['action'] = 'read'
#     session.info['jwt'] = jwt_admin
#     result = await read_object_types(session=session, object_types_ids=None)
#     assert len(result) == 3
#
#
# @pytest.mark.asyncio
# async def test_create_object_type_negative(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'create'
#     item = TMOCreate(name='LVL3', p_id=None, icon=None, description=None, virtual=False, global_uniqueness=True)
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         await create_object_type(item, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         await create_object_type(item, session=session)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     with pytest.raises(HTTPException):
#         await create_object_type(item, session=session)
#
#
# @pytest.mark.asyncio
# async def test_create_object_type_positive(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'create'
#     item = TMOCreate(name='TMO', p_id=None, icon=None, description=None, virtual=False, global_uniqueness=True)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     await create_object_type(item, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         await create_object_type(item, session=session)
#
#
# @pytest.mark.asyncio
# async def test_create_object_type_positive2(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'create'
#     item = TMOCreate(name='TMO', p_id=None, icon=None, description=None, virtual=False, global_uniqueness=True)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     await create_object_type(item, session=session)
#     with pytest.raises(HTTPException):
#         await create_object_type(item, session=session)
#
#
# @pytest.mark.asyncio
# async def test_update_object_type_negative(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'update'
#     item = TMOUpdate(version=1, description='New description')
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         await update_object_type(id=2, object_type=item, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         await update_object_type(id=1, object_type=item, session=session)
#
#
# @pytest.mark.asyncio
# async def test_update_object_type_positive(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'update'
#     item = TMOUpdate(version=2, description='New description')
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     await update_object_type(id=2, object_type=item, session=session)
#     with pytest.raises(HTTPException):
#         item.description = 'Other description'
#         await update_object_type(id=2, object_type=item, session=session)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     item = TMOUpdate(version=3, description='New description')
#     await update_object_type(id=2, object_type=item, session=session)
#     with pytest.raises(HTTPException):
#         item.description = 'Other description'
#         await update_object_type(id=2, object_type=item, session=session)
#
#
# @pytest.mark.asyncio
# async def test_delete_object_type_negative(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'delete'
#
#     # other
#     session.info['jwt'] = jwt_other
#     with pytest.raises(HTTPException):
#         await delete_object_type(id=1, delete_childs=True, session=session)
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     with pytest.raises(HTTPException):
#         await delete_object_type(id=1, delete_childs=True, session=session)
#
#
# @pytest.mark.asyncio
# async def test_delete_object_type_positive(session, jwt_other, jwt_reader, jwt_admin):
#     session.info['action'] = 'delete'
#
#     # reader
#     session.info['jwt'] = jwt_reader
#     await delete_object_type(id=2, delete_childs=True, session=session)
#     with pytest.raises(HTTPException):
#         await delete_object_type(id=2, delete_childs=True, session=session)
#
#     # admin
#     session.info['jwt'] = jwt_admin
#     await delete_object_type(id=1, delete_childs=True, session=session)
#     with pytest.raises(HTTPException):
#         await delete_object_type(id=1, delete_childs=True, session=session)
