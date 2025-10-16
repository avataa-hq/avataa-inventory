import json
from fastapi import HTTPException

import grpc
from sqlmodel import Session

from database import engine
from routers.object_router.schemas import MOCreateWithParams
from routers.parameter_router.schemas import PRMCreateByMO
from services.grpc_service.proto_files.task_service.files.tasks_inventory_pb2 import (
    CreateObjectWithParamsRequest,
    CreateObjectWithParamsResponse,
)
from services.grpc_service.proto_files.task_service.files.tasks_inventory_pb2_grpc import (
    TasksInventoryServicer,
)
from services.tasks_inventory_service.controller import TasksInventoryController


class TasksInventoryManager(TasksInventoryServicer):
    def CreateObjectWithParams(
        self,
        request: CreateObjectWithParamsRequest,
        context: grpc.ServicerContext,
    ) -> CreateObjectWithParamsResponse:
        with Session(engine) as session:
            controller = TasksInventoryController(session=session)
            params = [
                PRMCreateByMO(tprm_id=param.tprm_id, value=param.value)
                for param in request.params
            ]
            mo = MOCreateWithParams(
                tmo_id=request.tmo_id,
                p_id=request.p_id if request.p_id else None,
                point_a_id=request.point_a_id if request.point_a_id else None,
                point_b_id=request.point_b_id if request.point_b_id else None,
                pov=json.loads(request.pov) if request.pov else None,
                geometry=json.loads(request.geometry)
                if request.geometry
                else None,
                params=params,
            )

            try:
                controller.create_object_with_params(obj=mo)
            except HTTPException as exc:
                if exc.status_code in [400, 422]:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                elif exc.status_code == 409:
                    context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(exc.detail)

            return CreateObjectWithParamsResponse()
