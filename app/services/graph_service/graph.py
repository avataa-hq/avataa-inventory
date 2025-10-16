import json
import pickle
import sys
import traceback
from collections import defaultdict
from typing import AsyncIterable, Iterator

from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp  # noqa
from grpc import StatusCode
from grpc.aio import ServicerContext

from services.grpc_service.proto_files.graph.files.graph_pb2_grpc import (
    GraphInformerServicer,
)
from sqlalchemy import null, select, true
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased, sessionmaker

from models import TMO, MO, TPRM, PRM
from services.grpc_service.proto_files.graph.files.graph_pb2 import (
    TreeNode,
    OutTprms,
    InTmoIds,
    TPRM as TprmProto,
    InMOsByTMOid,
    OutMOsStream,
    MO as MoProto,
    PRM as PrmProto,
    InTmoByMoId,
    OutTmoId,
    InMOsByMoIds,
    OutMOsByMoIds,
    InPRMsByPRMIds,
    OutPRMsByPRMIds,
    OutTmoIds,
    InTprmId,
    InTprmIds,
    InTmoId,
    OutGetTMOTree,
)


class GraphInformer(GraphInformerServicer):
    def __init__(self, engine: Engine):
        super().__init__()
        self.session_builder = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )

    def _get_session(self) -> Iterator[Session]:
        with self.session_builder() as session:
            yield session

    def _get_point_tmo_const(self, tmo_id: int, session: Session):
        stmt_sub = (
            select(MO.point_a_id)
            .filter(
                MO.active == true(),
                MO.tmo_id == tmo_id,
                MO.point_a_id != null(),
            )
            .union_all(
                select(MO.point_b_id).filter(
                    MO.active == true(),
                    MO.tmo_id == tmo_id,
                    MO.point_b_id != null(),
                )
            )
        )
        stmt = select(MO.tmo_id).filter(MO.id.in_(stmt_sub)).distinct()
        results = session.execute(stmt).scalars().all()
        return results

    def _tree_recursive(
        self,
        data: dict[int | None, list[TMO]],
        tmo_p_id: int | None,
        session: Session,
    ) -> list[TreeNode] | None:
        parent_nodes = data.get(tmo_p_id, None)
        if not parent_nodes:
            return

        results = []
        for parent_node in parent_nodes:  # type: TMO
            creation_date = Timestamp()
            creation_date.FromDatetime(parent_node.creation_date)
            modification_date = Timestamp()
            modification_date.FromDatetime(parent_node.modification_date)
            result = TreeNode(
                name=parent_node.name,
                p_id=parent_node.p_id,
                icon=parent_node.icon,
                description=parent_node.description,
                virtual=parent_node.virtual,
                global_uniqueness=parent_node.global_uniqueness,
                lifecycle_process_definition=parent_node.lifecycle_process_definition,
                geometry_type=parent_node.geometry_type,
                materialize=parent_node.materialize,
                points_constraint_by_tmo=(
                    parent_node.points_constraint_by_tmo
                    if parent_node.points_constraint_by_tmo
                    else self._get_point_tmo_const(
                        tmo_id=parent_node.id, session=session
                    )
                ),
                child=self._tree_recursive(
                    data=data, tmo_p_id=parent_node.id, session=session
                ),
                id=parent_node.id,
                minimize=parent_node.minimize,
                created_by=parent_node.created_by,
                modified_by=parent_node.modified_by,
                latitude=parent_node.latitude,
                longitude=parent_node.longitude,
                creation_date=creation_date,
                modification_date=modification_date,
                primary=parent_node.primary,
                severity_id=parent_node.severity_id,
                status=parent_node.status,
                version=parent_node.version,
                line_type=parent_node.line_type,
                label=parent_node.label,
            )
            results.append(result)
        return results

    def create_tree(
        self, data: list[TMO], tmo_id: int | None, session: Session
    ) -> OutGetTMOTree:
        data_dict: dict[int | None, list[TMO]] = defaultdict(list)
        tmo_p_id = None
        for d in data:
            data_dict[d.p_id].append(d)
            if tmo_id is not None and d.id == tmo_id:
                tmo_p_id = d.p_id

        tree_nodes = self._tree_recursive(
            data=data_dict, tmo_p_id=tmo_p_id, session=session
        )
        return OutGetTMOTree(nodes=tree_nodes)

    async def GetTMOTree(
        self, request: InTmoId, context: ServicerContext
    ) -> OutGetTMOTree:
        if request.tmo_id:
            hierarchy = (
                select(TMO)
                .where(TMO.id == request.tmo_id)
                .cte(name="hierarchy", recursive=True)
            )
        else:
            hierarchy = (
                select(TMO)
                .where(TMO.p_id == null())
                .cte(name="hierarchy", recursive=True)
            )
        parent = aliased(hierarchy, name="p")
        children = aliased(TMO, name="c")
        hierarchy = hierarchy.union_all(
            select(children).filter(children.p_id == parent.c.id)
        )

        stmt = select(TMO).join(hierarchy, TMO.id == hierarchy.c.id)
        with self.session_builder() as session:
            response = list(session.execute(stmt).scalars().all())
            return self.create_tree(
                data=response, tmo_id=request.tmo_id, session=session
            )

    def _create_tprm_constraint(
        self, tprm_id: int, multiple: bool, session: Session
    ) -> str | None:
        stmt = select(PRM.value).filter(PRM.tprm_id == tprm_id).distinct()
        values = session.execute(stmt).scalars().all()
        if multiple:
            values_new = set()
            value_map = map(
                lambda value: pickle.loads(bytes.fromhex(value)), values
            )
            [values_new.update(v) for v in value_map]
            values = list(values_new)
        else:
            values = [int(value) for value in values]

        stmt = select(MO.tmo_id).filter(MO.id.in_(values)).distinct()
        response = session.execute(stmt).scalars().all()
        return json.dumps(response)

    def _prepare_tprm(self, tprm: TPRM, session: Session) -> TprmProto:
        creation_date = Timestamp()
        creation_date.FromDatetime(tprm.creation_date)
        modification_date = Timestamp()
        modification_date.FromDatetime(tprm.modification_date)
        return TprmProto(
            tmo_id=tprm.tmo_id,
            name=tprm.name,
            val_type=tprm.val_type,
            multiple=tprm.multiple,
            required=tprm.required,
            constraint=self._create_tprm_constraint(
                tprm_id=tprm.id, session=session, multiple=tprm.multiple
            )
            if tprm.val_type == "mo_link"
            else tprm.constraint,
            returnable=tprm.returnable,
            description=tprm.description,
            prm_link_filter=tprm.prm_link_filter,
            group=tprm.group,
            id=tprm.id,
            field_value=tprm.field_value,
            created_by=tprm.created_by,
            modified_by=tprm.modified_by,
            creation_date=creation_date,
            modification_date=modification_date,
            version=tprm.version,
        )

    async def GetTPRMsByTMOid(
        self, request: InTmoIds, context: ServicerContext
    ) -> OutTprms:
        try:
            stmt = select(TPRM).filter(TPRM.tmo_id.in_(request.tmo_id))
            with self.session_builder() as session:
                response = session.execute(stmt).scalars().all()
                proto_tprms = [
                    self._prepare_tprm(tprm=i, session=session)
                    for i in response
                ]
                return OutTprms(tprms=proto_tprms)
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            print(e)
            raise e

    async def GetMOsByTMOid(
        self, request: InMOsByTMOid, context: ServicerContext
    ) -> AsyncIterable[OutMOsStream]:
        stmt = select(MO).filter(MO.tmo_id == request.tmo_id)
        if request.mo_filter_by:
            mo_filter_by = json.loads(request.mo_filter_by)
            stmt = stmt.filter_by(**mo_filter_by)

        chunk_size = request.chunk_size or 50
        for session in self._get_session():
            for partition in (
                session.execute(stmt)
                .yield_per(chunk_size)
                .partitions(chunk_size)
            ):
                chunk: dict[int, dict] = dict()
                for p in partition:
                    p = p[0]
                    chunk[p.id] = p.dict(exclude_none=True)
                stmt_prms = select(PRM).filter(
                    PRM.mo_id.in_(list(chunk.keys()))
                )
                if request.prm_filter_by:
                    prm_filter_by = json.loads(request.prm_filter_by)
                    stmt_prms = stmt_prms.filter_by(**prm_filter_by)

                chunk_prms: dict[int, list[dict]] = defaultdict(list)
                for prm in session.execute(stmt_prms).scalars():  # type: PRM
                    prm_proto = prm.dict(exclude_none=True)
                    chunk_prms[prm.mo_id].append(prm_proto)

                prepared_partition = []
                for mo_id, mo in chunk.items():
                    if (
                        mo_id not in chunk_prms
                        and not request.keep_mo_without_prm
                    ):
                        continue
                    mo["params"] = chunk_prms.get(mo_id, [])
                    mo["geometry"] = (
                        json.dumps(mo["geometry"]) if "geometry" in mo else None
                    )
                    mo["pov"] = json.dumps(mo["pov"]) if "pov" in mo else None
                    mo_proto = ParseDict(
                        mo, MoProto(), ignore_unknown_fields=True
                    )
                    prepared_partition.append(mo_proto)
                yield OutMOsStream(mo=prepared_partition)

    async def GetMOsByTMOidPages(
        self, request: InMOsByTMOid, context: ServicerContext
    ) -> OutMOsStream | ServicerContext:
        stmt = select(MO).filter(MO.tmo_id == request.tmo_id).order_by(MO.id)
        if request.mo_filter_by:
            mo_filter_by = json.loads(request.mo_filter_by)
            stmt = stmt.filter_by(**mo_filter_by)

        chunk_size = request.chunk_size or 50
        min_size = 0
        max_size = 10_000
        if min_size >= chunk_size or chunk_size >= max_size:
            context.set_code(StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Chunk size must be between {min_size} and {max_size}. Received {chunk_size}"
            )
            return context

        offset = request.offset or 0
        if 0 > offset:
            context.set_code(StatusCode.INVALID_ARGUMENT)
            context.set_details("Offset must be more than 0")
            return context

        stmt = stmt.offset(offset).limit(chunk_size)

        with self.session_builder() as session:
            chunk: dict[int, dict] = dict()
            for p in session.execute(stmt).scalars():
                chunk[p.id] = p.dict(exclude_none=True)
            stmt_prms = select(PRM).filter(PRM.mo_id.in_(list(chunk.keys())))
            if request.prm_filter_by:
                prm_filter_by = json.loads(request.prm_filter_by)
                stmt_prms = stmt_prms.filter_by(**prm_filter_by)

            chunk_prms: dict[int, list[dict]] = defaultdict(list)
            for prm in session.execute(stmt_prms).scalars():  # type: PRM
                prm_proto = prm.dict(exclude_none=True)
                chunk_prms[prm.mo_id].append(prm_proto)

            prepared_partition = []
            for mo_id, mo in chunk.items():
                if mo_id not in chunk_prms and not request.keep_mo_without_prm:
                    continue
                mo["params"] = chunk_prms.get(mo_id, [])
                mo["geometry"] = (
                    json.dumps(mo["geometry"]) if "geometry" in mo else None
                )
                mo["pov"] = json.dumps(mo["pov"]) if "pov" in mo else None
                mo_proto = ParseDict(mo, MoProto(), ignore_unknown_fields=True)
                prepared_partition.append(mo_proto)
            return OutMOsStream(mo=prepared_partition)

    async def GetTmoByMoId(
        self, request: InTmoByMoId, context: ServicerContext
    ) -> OutTmoId | ServicerContext:
        stmt = select(MO.tmo_id).filter(MO.id == request.mo_id)
        with self.session_builder() as session:
            response = session.execute(stmt).scalar_one_or_none()
            if not response:
                context.set_code(StatusCode.NOT_FOUND)
                context.set_details(f'MO with id "{request.mo_id}" not found')
                return context
            result = OutTmoId(tmo_id=(response or -1))
            return result

    async def GetMOsByMoIds(
        self, request: InMOsByMoIds, context: ServicerContext
    ) -> OutMOsByMoIds | ServicerContext:
        stmt = select(MO).filter(MO.id.in_(request.mo_ids))
        with self.session_builder() as session:
            response = session.execute(stmt).scalars().all()
            if not response:
                context.set_code(StatusCode.NOT_FOUND)
                context.set_details(f'MO with ids "{request.mo_ids}" not found')
                return context
            mos = []
            for mo in response:
                mo_dict = mo.dict(by_alias=True, exclude_none=True)
                mo_dict["geometry"] = (
                    json.dumps(mo_dict["geometry"])
                    if "geometry" in mo_dict
                    else None
                )
                result = ParseDict(
                    mo_dict, MoProto(), ignore_unknown_fields=True
                )
                mos.append(result)
            results = OutMOsByMoIds(mos=mos)
            return results

    async def GetPRMsByPRMIds(
        self, request: InPRMsByPRMIds, context: ServicerContext
    ) -> OutPRMsByPRMIds | ServicerContext:
        stmt = select(PRM).filter(PRM.id.in_(request.prm_ids))
        with self.session_builder() as session:
            response = session.execute(stmt).scalars().all()
            if not response:
                context.set_code(StatusCode.NOT_FOUND)
                context.set_details(
                    f'PRM with ids "{request.prm_ids}" not found'
                )
                return context
            prms = []
            for prm in response:
                result = ParseDict(
                    prm.dict(), PrmProto(), ignore_unknown_fields=True
                )
                prms.append(result)
            results = OutPRMsByPRMIds(prms=prms)
            return results

    async def GetPointTmoConst(
        self, request: InTmoId, context: ServicerContext
    ) -> OutTmoIds | ServicerContext:
        with self.session_builder() as session:
            response = self._get_point_tmo_const(
                tmo_id=request.tmo_id, session=session
            )
            grpc_response = OutTmoIds(tmo_ids=response)
            return grpc_response

    async def GetTprmConst(
        self, request: InTprmId, context: ServicerContext
    ) -> OutTmoIds | ServicerContext:
        stmt = select(TPRM).filter(TPRM.id == request.tprm_id)
        with self.session_builder() as session:
            tprm = session.execute(stmt).scalars().one_or_none()
            if not tprm:
                context.set_code(StatusCode.NOT_FOUND)
                context.set_details(
                    f'TPRM with ids "{request.tprm_id}" not found'
                )
                return context
            if tprm.val_type != "mo_link":
                context.set_code(StatusCode.INVALID_ARGUMENT)
                context.set_details(
                    f'TPRM with ids "{request.tprm_id}" is not mo_link'
                )
                return context
            response = self._create_tprm_constraint(
                tprm_id=tprm.id, session=session, multiple=tprm.multiple
            )
            grpc_response = OutTmoIds(tmo_ids=response)
            return grpc_response

    async def GetTprmByTprmIds(
        self, request: InTprmIds, context: ServicerContext
    ) -> OutTprms:
        stmt = select(TPRM).filter(TPRM.id.in_(request.tprm_ids))
        with self.session_builder() as session:
            tprms = session.execute(stmt).scalars().all()
            results = [
                self._prepare_tprm(tprm=i, session=session) for i in tprms
            ]
            return OutTprms(tprms=results)
