from copy import deepcopy
from typing import List

from sqlalchemy.orm import Session, selectinload
from sqlmodel import select

from functions.db_functions.db_delete import (
    delete_mo_links_by_tmo_id,
    delete_prm_links_by_tmo_id,
    delete_point_links_by_tmo_id,
)
from models import TMO
from routers.object_type_router.exceptions import (
    ObjectTypeAlreadyExists,
)
from routers.object_type_router.schemas import (
    GetObjectTypesRequest,
    TMOResponseWithParameters,
    TMOCreate,
    TMOUpdateRequest,
    TMOUpdate,
    GetObjectTypeChildRequest,
    TMOResponse,
    GetChildrenOfObjectTypeWithDataRequest,
    DeleteObjectTypeRequest,
    SearchObjectTypeRequest,
    GetObjectTypeBreadcrumbsRequest,
)
from routers.object_type_router.utils import ObjectTypeDBGetter
from routers.parameter_type_router.schemas import TPRMResponse


class GetObjectTypes(ObjectTypeDBGetter):
    def __init__(self, session: Session, request: GetObjectTypesRequest):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def _response_with_parameter_types(self, where_conditions: list):
        query = (
            select(TMO)
            .options(selectinload(TMO.children, TMO.tprms))
            .where(*where_conditions)
            .order_by(TMO.name)
        )
        object_type_instances = self._session.exec(query).all()

        return [
            TMOResponseWithParameters(
                **object_type_instance.dict(),
                child_count=len(object_type_instance.children),
                tprms=object_type_instance.tprms,
            )
            for object_type_instance in object_type_instances
        ]

    def _response_with_just_object_types(self, where_conditions: list):
        query = (
            select(TMO)
            .options(selectinload(TMO.children))
            .where(*where_conditions)
            .order_by(TMO.name)
        )
        object_type_instances = self._session.exec(query).all()
        return [
            TMOResponseWithParameters(
                **o.dict(), child_count=len(o.children), tprms=list()
            )
            for o in object_type_instances
        ]

    def execute(self):
        where_conditions = []
        if self._request.object_types_ids:
            where_conditions = [TMO.id.in_(self._request.object_types_ids)]

        match self._request.with_parameter_types:
            case True:
                return self._response_with_parameter_types(
                    where_conditions=where_conditions
                )

            case _:
                return self._response_with_just_object_types(
                    where_conditions=where_conditions
                )


class CreateObjectType(ObjectTypeDBGetter):
    def __init__(self, session: Session, request: TMOCreate):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        self._session.info["disable_security"] = True
        object_type_instance = self._get_object_type_instance_by_name(
            object_type_name=self._request.name, raise_error=False
        )

        if object_type_instance:
            raise ObjectTypeAlreadyExists(
                status_code=409, detail="This object type already exists."
            )

        new_object_type_instance = self._create_object_type(
            object_type_to_create=self._request
        )
        self._session.add(new_object_type_instance)
        self._session.commit()
        self._session.refresh(new_object_type_instance)

        return new_object_type_instance


class UpdateObjectType(ObjectTypeDBGetter):
    def __init__(self, session: Session, request: TMOUpdateRequest):
        super().__init__(session=session)
        self._session = session
        self._request = request

    @staticmethod
    def _get_data_needs_to_be_update(
        object_type_instance: TMO, object_type_to_update: TMOUpdate
    ):
        temp_object_type_instance: dict = deepcopy(object_type_instance.dict())
        temp_object_type_to_update: dict = deepcopy(
            object_type_to_update.dict(exclude_unset=True)
        )

        del temp_object_type_instance["version"]
        del temp_object_type_to_update["version"]

        data_to_update = {}
        for key, value in temp_object_type_to_update.items():
            old_attribute_value = temp_object_type_instance[key]
            if old_attribute_value == temp_object_type_to_update[key]:
                continue

            data_to_update[key] = value

        data_to_update["version"] = object_type_to_update.version
        return data_to_update

    def execute(self):
        object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=self._request.object_type_id
        )
        data_for_update = self._get_data_needs_to_be_update(
            object_type_instance=object_type_instance,
            object_type_to_update=TMOUpdate(
                **self._request.dict(exclude_unset=True)
            ),
        )
        object_type_instance = self._update_object_type(
            object_type_instance=object_type_instance,
            object_type_to_update=data_for_update,
        )

        self._session.add(object_type_instance)
        self._session.commit()
        self._session.refresh(object_type_instance)

        return object_type_instance


class GetObjectTypeChild(ObjectTypeDBGetter):
    def __init__(self, request: GetObjectTypeChildRequest, session: Session):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self) -> List[TMOResponse]:
        query = select(TMO).where(TMO.p_id == None)  # noqa

        if self._request.parent_id != 0:
            self._get_object_type_instance_by_id(
                object_type_id=self._request.parent_id
            )
            query = (
                select(TMO)
                .where(TMO.p_id == self._request.parent_id)
                .order_by(TMO.name)
            )

        query = query.order_by(TMO.name)

        child_object_types = self._session.execute(query).scalars().all()
        return [
            TMOResponse(**child_tmo.dict(), child_count=len(child_tmo.children))
            for child_tmo in child_object_types
        ]


class GetChildrenOfObjectTypeWithData(ObjectTypeDBGetter):
    """
    Returns the parent TMO and all direct and non-direct child TMOs.
    with_params flag gives ability to get related TPRMs
    """

    def __init__(
        self, request: GetChildrenOfObjectTypeWithDataRequest, session: Session
    ):
        super().__init__(session=session)
        self._request = request
        self._response = []

    def execute(self):
        object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=self._request.object_type_id
        )
        pre_result = [object_type_instance]

        order = [[self._request.object_type_id]]
        for object_type_ids in order:
            stmt = select(TMO).where(TMO.p_id.in_(object_type_ids))
            if self._request.with_params:
                stmt.options(selectinload(TMO.tprms))

            children = self._session.exec(stmt).all()

            if children:
                order.append([item.id for item in children])
                pre_result.extend(children)

        if self._request.with_params:
            for tmo in pre_result:
                data = tmo.dict()
                data["tprms"] = [
                    TPRMResponse.from_orm(tprm) for tprm in tmo.tprms
                ]
                self._response.append(data)

        else:
            self._response = pre_result

        return self._response


class DeleteObjectType(ObjectTypeDBGetter):
    def __init__(self, session: Session, request: DeleteObjectTypeRequest):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        object_type = self._get_object_type_instance_by_id(
            object_type_id=self._request.object_type_id
        )

        if self._request.delete_children:
            for child in object_type.children:
                delete_mo_links_by_tmo_id(
                    session=self._session, object_type_id=child.id
                )
                delete_prm_links_by_tmo_id(
                    session=self._session, object_type_id=child.id
                )
                delete_point_links_by_tmo_id(
                    session=self._session, object_type_id=child.id
                )
                self._session.delete(child)

        delete_mo_links_by_tmo_id(
            session=self._session,
            object_type_id=self._request.object_type_id,
        )
        delete_prm_links_by_tmo_id(
            session=self._session,
            object_type_id=self._request.object_type_id,
        )
        delete_point_links_by_tmo_id(
            session=self._session,
            object_type_id=self._request.object_type_id,
        )
        self._session.delete(object_type)
        self._session.commit()
        return {"ok": True}


class SearchObjectTypes:
    def __init__(self, session: Session, request: SearchObjectTypeRequest):
        self._session = session
        self._request = request

    def execute(self):
        query = select(TMO).where(
            TMO.name.ilike("%" + self._request.object_type_name + "%")
        )
        object_type_instances = self._session.exec(query).all()

        return [
            TMOResponse(
                **object_type.dict(), child_count=len(object_type.children)
            )
            for object_type in object_type_instances
        ]


class GetObjectTypeBreadcrumbs(ObjectTypeDBGetter):
    """
    Returns breadcrumbs for particular object type id
    """

    def __init__(
        self, session: Session, request: GetObjectTypeBreadcrumbsRequest
    ):
        super().__init__(session=session)
        self._session = session
        self._request = request

    def execute(self):
        object_type_instance = self._get_object_type_instance_by_id(
            object_type_id=self._request.object_type_id
        )
        breadcrumbs = self.get_breadcrumbs_for_object_type(
            object_type_instance=object_type_instance
        )
        return [
            TMOResponse(**o.dict(), child_count=len(o.children))
            for o in breadcrumbs
        ]
