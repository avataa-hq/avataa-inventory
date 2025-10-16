import math
import pickle

from grpc import ServicerContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from services.dataview_manager.controller import GrpcController

from database import engine
from services.grpc_service.proto_files.dataview.files.dataview_to_inventory_pb2 import (
    GetMOByTMOIdForViewRequest,
    GetMOByTMOIdForViewResponse,
)

from models import TPRM
from services.grpc_service.proto_files.dataview.files.dataview_to_inventory_pb2_grpc import (
    DataviewToInventoryServicer,
)


class DataviewToInventoryManager(DataviewToInventoryServicer):
    def GetMOByTMOIdForView(
        self,
        request: GetMOByTMOIdForViewRequest,
        context: ServicerContext,
    ) -> GetMOByTMOIdForViewResponse:
        grpc_message_max_size = 100 * 1024 * 1024
        with Session(engine) as session:
            # get link tprms before loading objects
            query = select(TPRM.id).where(
                TPRM.tmo_id == request.tmo_id, TPRM.val_type == "mo_link"
            )
            mo_link_tprms = session.execute(query).scalars().all()

            query = select(TPRM.id).where(
                TPRM.tmo_id == request.tmo_id, TPRM.val_type == "prm_link"
            )
            prm_link_tprms = session.execute(query).scalars().all()

            objects = GrpcController.get_objects(
                session=session, tmo_id=request.tmo_id
            )
            objects_with_params = GrpcController.get_parameters(
                session=session, objects=objects
            )
            result = GrpcController.replace_links(
                session=session,
                objects=objects_with_params,
                mo_links=mo_link_tprms,
                prm_links=prm_link_tprms,
            )

            for chunk in result:
                chunk = [pickle.dumps(item).hex() for item in chunk.values()]
                msg = GetMOByTMOIdForViewResponse(mos_with_params=chunk)
                msg_size = msg.ByteSize()

                # create stack
                steps = math.ceil(msg_size / grpc_message_max_size)
                per_step = int(len(chunk) / steps)
                for step in range(steps):
                    start = step * per_step
                    msg_data = chunk[start : start + per_step]
                    yield GetMOByTMOIdForViewResponse(mos_with_params=msg_data)
