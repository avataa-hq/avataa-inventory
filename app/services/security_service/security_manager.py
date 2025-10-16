import grpc
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session

from database import engine
from services.grpc_service.proto_files.security.files import (
    security_manager_pb2,
)
from services.grpc_service.proto_files.security.files.security_manager_pb2_grpc import (
    SecurityManagerInformerServicer,
)
from services.security_service.data.permissions.inventory import MOPermission


class SecurityManagerInformer(SecurityManagerInformerServicer):
    def __init__(self, engine: Engine):
        super().__init__()
        self.session_builder = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )

    async def GetMOPermissions(
        self,
        request: security_manager_pb2.RequestPermissionsForMO,
        context: grpc.aio.ServicerContext,
    ):
        with Session(engine) as session:
            mo_permission = (
                session.execute(select(MOPermission)).scalars().all()
            )

            for index in range(0, len(mo_permission), 20_000):
                chunk_result = []
                chunk_permissions = mo_permission[index : index + 20_000]
                for permission in chunk_permissions:
                    permission = {
                        "read": permission.read,
                        "update": permission.update,
                        "create": permission.create,
                        "delete": permission.delete,
                        "admin": permission.admin,
                        "parent_id": permission.parent_id,
                        "root_permission_id": permission.root_permission_id,
                        "permission_name": permission.permission_name,
                        "permission": permission.permission,
                    }

                    chunk_result.append(
                        security_manager_pb2.MOPermission(**permission)
                    )
                yield security_manager_pb2.MOPermissions(
                    mo_permissions=chunk_result
                )
