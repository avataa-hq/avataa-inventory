import asyncio
import json
import logging
import pickle
import traceback
from sys import stderr
from typing import AsyncGenerator

import grpc
import math
from google.protobuf import json_format, any_pb2
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlmodel import Session, select

from database import engine
from functions.db_functions.db_read import get_objects_with_parameters
from functions.functions_dicts import value_convertation_by_val_type
from functions.functions_utils.utils import decode_multiple_value
from models import PRM, TPRM, MO, TMO
from routers.object_router.utils import TPRMFilterCleaner
from services.airflow_service.servicer import AirflowManager
from services.dataview_manager.servicer import DataviewToInventoryManager
from services.event_service.grpc_servicer import EventManagerManager
from services.graph_service.graph import GraphInformer
from services.grpc_service.grpc_utils import (
    check_tmo_has_sevrirty,
    get_smallest_severity_value,
    get_severity_names_with_mos_quantity,
    GetAllMOWithParamsByTMOId,
    GetAllMOAttrsByTMOIdWithSpecialParameters,
)
from services.grpc_service.proto_files.airflow.files import (
    airflow_manager_pb2_grpc,
)
from services.grpc_service.proto_files.dataview.files import (
    dataview_to_inventory_pb2_grpc,
)
from services.grpc_service.proto_files.event_manager_methods.files import (
    event_manager_pb2_grpc,
)
from services.grpc_service.proto_files.graph.files import graph_pb2_grpc
from services.grpc_service.proto_files.inventory_data.files import (
    inventory_data_pb2_grpc,
    inventory_data_pb2,
)
from services.grpc_service.proto_files.inventory_data.handlers import (
    FilteredObjWithParamsHandler,
)
from services.grpc_service.proto_files.inventory_data.utils import (
    VAL_TYPE_CONVERTER,
)
from services.grpc_service.proto_files.security.files import (
    security_manager_pb2_grpc,
)
from services.grpc_service.proto_files.task_service.files import (
    tasks_inventory_pb2_grpc,
)
from services.grpc_service.proto_files.transfer.files import transfer_pb2_grpc
from services.grpc_service.proto_files.zeebe.files import (
    zeebe_to_inventory_pb2_grpc,
)
from services.security_service.implementation.disabled import DisabledSecurity
from services.security_service.security_data_models import UserData
from services.security_service.security_factory import (
    security as security_instance,
)
from services.security_service.security_manager import SecurityManagerInformer
from services.security_service.transfer_security import transfer_inventory
from services.tasks_inventory_service.servicer import TasksInventoryManager
from services.zeebe_service.zeebe_client import ZeebeInformer

if not isinstance(security_instance, DisabledSecurity):
    from services.security_service.data import listener  # noqa


class Informer(inventory_data_pb2_grpc.InformerServicer):
    max_chunk_size = 1_000_000

    async def GetParamsValuesForMO(
        self,
        request: inventory_data_pb2.InfoRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.InfoReply:
        """Returns tmo_id and p_id for requested mo_id"""
        request_as_dict = json_format.MessageToDict(
            request,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )

        with Session(engine) as session:
            stm = (
                select(PRM.tprm_id, PRM.value, TPRM.val_type, TPRM.multiple)
                .join(TPRM)
                .where(
                    PRM.mo_id == request_as_dict["mo_id"],
                    PRM.tprm_id.in_(request_as_dict["tprm_ids"]),
                )
            )

            res = session.exec(stm).all()

            # create message
            msg_data = dict()
            for item in res:
                converter = VAL_TYPE_CONVERTER[item[2]]["proto_serializer"]
                deserializer = VAL_TYPE_CONVERTER[item[2]][
                    "python_deserializer"
                ]

                # if multiply
                if item[3]:
                    packed_value_list = []
                    value_list = decode_multiple_value(item[1])
                    for value in value_list:
                        value = converter(value=deserializer(value))
                        packed_value = any_pb2.Any()
                        packed_value.Pack(value)
                        packed_value_list.append(packed_value)
                    msg_val = inventory_data_pb2.ValueOfDict(
                        mo_tprm_value=packed_value_list
                    )

                else:
                    value = converter(value=deserializer(item[1]))
                    packed_value = any_pb2.Any()
                    packed_value.Pack(value)
                    msg_val = inventory_data_pb2.ValueOfDict(
                        mo_tprm_value=[packed_value]
                    )

                msg_data[item[0]] = msg_val

        return inventory_data_pb2.InfoReply(mo_info=msg_data)

    async def GetTMOidForMo(
        self,
        request: inventory_data_pb2.IntValue,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.MOInfo:
        """Returns tmo_id and p_id for requested mo_id"""

        res = {"tmo_id": 0, "p_id": 0}

        if request.value != 0:
            with Session(engine) as session:
                stm = select(MO).where(MO.id == request.value)
                mo = session.exec(stm).first()

                if mo is not None:
                    res["tmo_id"] = mo.tmo_id if mo.tmo_id is not None else 0
                    res["p_id"] = mo.p_id if mo.p_id is not None else 0

        return inventory_data_pb2.MOInfo(**res)

    async def GetObjWithParams(
        self,
        request: inventory_data_pb2.RequestForObjInfoByTMO,
        context: grpc.aio.ServicerContext,
    ):
        """Returns stream of ResponseWithObjInfoByTMO. Response includes all TPRM values for MO
        if there are empty tprm_ids in request otherwise response includes only TPRM values which was
        specified in request tprm_ids"""
        tprms = request.tprm_ids
        object_type_id = request.object_type_id
        mo_p_id = request.mo_p_id

        if object_type_id == 0:
            yield inventory_data_pb2.ResponseWithObjInfoByTMO()

        mo_where_condition = []
        if mo_p_id != 0:
            mo_where_condition.append(MO.p_id == mo_p_id)

        with Session(engine) as session:
            stmt = select(
                TPRM.id.label("id"), TPRM.multiple.label("multiple")
            ).where(TPRM.id.in_(tprms), TPRM.tmo_id == object_type_id)
            tprms_cache = session.exec(stmt).all()

            tprms_cache = {str(item.id): item.multiple for item in tprms_cache}

            stm = select(
                MO.id.label("mo_id"), MO.p_id.label("parent_id")
            ).where(
                MO.tmo_id == object_type_id,
                MO.active == True,  # noqa
                *mo_where_condition,
            )

            for tprm_id in tprms:
                label_name = str(tprm_id)
                select_stmt = select(
                    PRM.mo_id, PRM.value.label(label_name)
                ).where(PRM.tprm_id == tprm_id)
                aliased_table = aliased(select_stmt.subquery())
                stm = stm.outerjoin(
                    aliased_table, MO.id == aliased_table.c.mo_id
                ).add_columns(getattr(aliased_table.c, label_name, None))

            all_mo_ids = session.exec(stm).all()

            for item in all_mo_ids:
                response_dict = dict()
                response_dict["mo_id"] = item.mo_id
                if item.parent_id:
                    response_dict["p_id"] = item.parent_id

                tprm_values = {}
                for tprm_column_name in tprms_cache:
                    tprm_value = getattr(item, tprm_column_name, None)
                    if tprm_value:
                        if tprms_cache[tprm_column_name]:
                            tprm_value = str(decode_multiple_value(tprm_value))
                        tprm_values[int(tprm_column_name)] = tprm_value

                response_dict["tprm_values"] = tprm_values

                yield inventory_data_pb2.ResponseWithObjInfoByTMO(
                    **response_dict
                )

    async def GetMOQuantityBySeverity(
        self,
        request: inventory_data_pb2.RequestSeverityValues,
        context: grpc.aio.ServicerContext,
    ):
        severities = pickle.loads(bytes.fromhex(request.dict_severities))

        # get the smallest gradation, to automatically add tmo without severity
        smallest_severity_name, smallest_severity_value = (
            get_smallest_severity_value(severities)
        )

        tmo_with_mo_ids = pickle.loads(
            bytes.fromhex(request.dict_tmo_with_mo_ids)
        )
        mos_with_severities = {}
        result = {key: 0 for key in severities}

        with Session(engine) as session:
            if tmo_with_mo_ids:
                for tmo_id, mo_ids in tmo_with_mo_ids.items():
                    tprm_severity = check_tmo_has_sevrirty(
                        tmo_id=tmo_id, session=session
                    )
                    if not tprm_severity:
                        if smallest_severity_value == 0:
                            result[smallest_severity_name] = len(mo_ids)
                    else:
                        mos_with_severities[tprm_severity] = mo_ids

            if mos_with_severities:
                mos, tprms = [], []

                for key, value in mos_with_severities.items():
                    tprms.append(key)
                    mos.extend(value)

                stmt = select(PRM.value).where(
                    PRM.mo_id.in_(mos), PRM.tprm_id.in_(tprms)
                )
                severity_values = session.exec(stmt).all()
                response = get_severity_names_with_mos_quantity(
                    severity_values=severity_values,
                    severities=severities,
                    result=result,
                )
                pickle_response = pickle.dumps(response).hex()

        return inventory_data_pb2.ResponseMOQuantityBySeverity(
            dict_mo_info=pickle_response
        )

    async def GetMOSeverityMaxValue(
        self,
        request: inventory_data_pb2.RequestSeverityMoId,
        context: grpc.aio.ServicerContext,
    ):
        with Session(engine) as session:
            stmt = select(TPRM.id).where(
                TPRM.tmo_id == request.tmo_id,
                TPRM.name.ilike("%" + "severity" + "%"),
            )
            tprm_id = session.exec(stmt).first()

            if tprm_id and request.mo_ids:
                stmt = select(func.max(PRM.value)).where(
                    PRM.tprm_id == tprm_id, PRM.mo_id.in_(request.mo_ids)
                )
                max_severity = session.exec(stmt).first()
                if max_severity:
                    return inventory_data_pb2.ResponseSeverityMoId(
                        max_severity=int(max_severity)
                    )

            return inventory_data_pb2.ResponseSeverityMoId(max_severity=0)

    async def GetFilteredObjWithParams(
        self,
        request: inventory_data_pb2.RequestForFilteredObjInfoByTMO,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseMOdata:
        """Returns ResponseMOdata."""
        handler = FilteredObjWithParamsHandler()
        resp_data: list[str] = await handler.process(request=request)
        return inventory_data_pb2.ResponseMOdata(
            objects_with_parameters=resp_data
        )

    async def GetFilteredObjWithParamsStream(
        self,
        request: inventory_data_pb2.RequestForFilteredObjInfoByTMO,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator[inventory_data_pb2.ResponseMOdata, None]:
        """Returns stream of ResponseMOdata."""
        handler = FilteredObjWithParamsHandler()
        try:
            async for response in handler.get_stream_response_chunked(
                request=request
            ):
                yield response
        except Exception as ex:
            await context.abort(
                grpc.StatusCode.INTERNAL, f"Error in process: {str(ex)}"
            )

    async def GetFilteredObjSpecial(
        self,
        request: inventory_data_pb2.RequestForFilteredObjSpecial,
        context: grpc.aio.ServicerContext,
    ):
        """Returns ResponseMOdataOrMOIds."""
        CHUNK_SIZE = 1000

        tprm_cleaner_data = dict()
        if request.object_type_id:
            tprm_cleaner_data["object_type_id"] = request.object_type_id

        if request.query_params:
            tprm_cleaner_data["query_params"] = pickle.loads(
                bytes.fromhex(request.query_params)
            )

        additional_filter_data = {}
        if request.order_by:
            additional_filter_data["order_by"] = pickle.loads(
                bytes.fromhex(request.order_by)
            )

        if request.mo_ids:
            additional_filter_data["obj_ids"] = request.mo_ids

        if request.p_ids:
            additional_filter_data["p_id"] = request.p_ids

        with Session(engine) as session:
            if request.decoded_jwt:
                session.info["jwt"] = UserData.from_jwt(
                    pickle.loads(bytes.fromhex(request.decoded_jwt))
                )
                session.info["action"] = "read"

            mos_ids = []
            pickle_mo_dataset = []
            tprm_cleaner = TPRMFilterCleaner(
                session=session, **tprm_cleaner_data
            )

            if any(
                [
                    tprm_cleaner.check_filter_data_in_query_params(),
                    additional_filter_data,
                ]
            ):
                mos_ids = (
                    tprm_cleaner.get_mo_ids_which_match_clean_filter_conditions(
                        active=True, **additional_filter_data
                    )
                )
            else:
                if request.object_type_id:
                    stmt = select(MO.id).where(
                        MO.tmo_id == request.object_type_id
                    )
                    mos_ids = session.execute(stmt).scalars().all()

            if not mos_ids:
                yield inventory_data_pb2.ResponseMOdataSpecial(
                    mo_ids=[], pickle_mo_dataset=[]
                )
                return

            if request.only_ids:
                for part in range(math.ceil(len(mos_ids) / CHUNK_SIZE)):
                    inner_offset = part * CHUNK_SIZE
                    inner_limit = inner_offset + CHUNK_SIZE
                    response = inventory_data_pb2.ResponseMOdataSpecial(
                        mo_ids=mos_ids[inner_offset:inner_limit],
                        pickle_mo_dataset=[],
                    )
                    print(
                        f"filtered-object-special: message-size: {response.ByteSize()}"
                    )
                    yield response
                return

            mo_fields = {MO.id, MO.p_id}
            mo_fields_names = {"id", "p_id"}
            if request.mo_attrs:
                mo_fields_add = {
                    attr
                    for attr_name in request.mo_attrs
                    if (attr := getattr(MO, attr_name, None))
                }
                mo_fields_names_add = {
                    attr_name
                    for attr_name in request.mo_attrs
                    if getattr(MO, attr_name, None)
                }
                mo_fields.update(mo_fields_add)
                mo_fields_names.update(mo_fields_names_add)

            if request.tprm_ids:
                stmt = select(TPRM).where(TPRM.id.in_(request.tprm_ids))
                tprms_cache = session.exec(stmt).all()
                tprms_cache = {
                    str(item.id): {
                        "multiple": item.multiple,
                        "val_type": item.val_type,
                    }
                    for item in tprms_cache
                }
                stm = select(*mo_fields).where(MO.id.in_(mos_ids))

                for tprm_id in request.tprm_ids:
                    label_name = str(tprm_id)
                    select_stmt = select(
                        PRM.mo_id, PRM.value.label(label_name)
                    ).where(PRM.tprm_id == tprm_id, PRM.mo_id.in_(mos_ids))
                    aliased_table = aliased(select_stmt.subquery())
                    stm = stm.join(
                        aliased_table,
                        MO.id == aliased_table.c.mo_id,
                        isouter=True,
                    ).add_columns(getattr(aliased_table.c, label_name, None))

                all_mo_ids = session.exec(stm).all()

                def get_tprm_value(item, tprm_column_name, tprm_data):
                    tprm_value = getattr(item, tprm_column_name, None)
                    if tprm_value:
                        if tprm_data["multiple"]:
                            return decode_multiple_value(tprm_value)
                        else:
                            convert_func = value_convertation_by_val_type.get(
                                tprm_data["val_type"]
                            )
                            if convert_func:
                                return convert_func(tprm_value)
                            else:
                                return tprm_value
                    return tprm_value

                def combine_data(item):
                    res = {
                        int(column): get_tprm_value(item, column, tprm_data)
                        for column, tprm_data in tprms_cache.items()
                    }

                    res.update(
                        {
                            column_name: getattr(item, column_name)
                            for column_name in mo_fields_names
                        }
                    )
                    return pickle.dumps(res).hex()

                pickle_mo_dataset = [combine_data(item) for item in all_mo_ids]

            else:
                stmt = select(MO).where(MO.id.in_(mos_ids))
                mos = session.execute(stmt).scalars().all()

                if mos:
                    pickle_mo_dataset = [
                        pickle.dumps(
                            {
                                column_name: attr_data
                                for column_name in mo_fields_names
                                if (
                                    attr_data := getattr(
                                        item, column_name, None
                                    )
                                )
                            }
                        ).hex()
                        for item in mos
                    ]

            for part in range(math.ceil(len(mos_ids) / CHUNK_SIZE)):
                inner_offset = part * CHUNK_SIZE
                inner_limit = inner_offset + CHUNK_SIZE
                response = inventory_data_pb2.ResponseMOdataSpecial(
                    mo_ids=mos_ids[inner_offset:inner_limit],
                    pickle_mo_dataset=pickle_mo_dataset[
                        inner_offset:inner_limit
                    ],
                )
                print(
                    f"filtered-object-special: message-size: {response.ByteSize()}"
                )
                yield response

    async def GetTMOlifecycle(
        self,
        request: inventory_data_pb2.RequestTMOlifecycleByTMOidList,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseTMOlifecycleByTMOidList:
        with Session(engine) as session:
            stm = select(TMO.id).where(
                TMO.id.in_(request.tmo_ids),
                TMO.lifecycle_process_definition is not None,
            )
            all_tmo_ids = session.exec(stm).all()
            return inventory_data_pb2.ResponseTMOlifecycleByTMOidList(
                tmo_ids_with_lifecycle=all_tmo_ids
            )

    async def GetTPRMNames(
        self,
        request: inventory_data_pb2.RequestTPRMIds,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseTPRMNames:
        res = inventory_data_pb2.ResponseTPRMNames()
        if not request.tprm_ids:
            return res

        with Session(engine) as session:
            stm = select(TPRM.id, TPRM.name).where(
                TPRM.id.in_(request.tprm_ids)
            )
            tprm_data = session.exec(stm).all()
            tprm_data = [
                inventory_data_pb2.ResponseTPRMName(
                    tprm_id=item.id, tprm_name=item.name
                )
                for item in tprm_data
            ]
            res = inventory_data_pb2.ResponseTPRMNames(items=tprm_data)

        return res

    async def GetHierarchyLevelChildren(
        self,
        request: inventory_data_pb2.RequestListLevels,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseListNodes:
        results = {}
        with Session(engine) as session:
            for level in request.items:
                parent_mo_ids = []
                start_tmo = level.level_tmo_id
                path_of_children_tmos = level.path_of_children_tmos

                order = list()
                order.extend(path_of_children_tmos)

                level_data = level.level_data
                [parent_mo_ids.extend(node.mo_ids) for node in level_data]

                results.update({node.node_id: list() for node in level_data})
                level_parent_cache = {}
                [
                    level_parent_cache.update(
                        dict().fromkeys(node.mo_ids, node.node_id)
                    )
                    for node in level_data
                ]

                def add_child_mo_id_to_main_node(mo_id, parent_id):
                    main_node_id = level_parent_cache.get(parent_id)
                    level_parent_cache[mo_id] = main_node_id
                    results.setdefault(main_node_id, list()).append(mo_id)

                for step in order:
                    where_condition = [MO.p_id.in_(parent_mo_ids)]
                    if step == start_tmo:
                        where_condition = [MO.id.in_(parent_mo_ids)]
                    stmt = select(MO.id, MO.p_id).where(
                        MO.tmo_id == step, *where_condition
                    )
                    res = session.execute(stmt)
                    res = res.all()

                    if not res:
                        break

                    parent_mo_ids = res
                    if step in level.collect_data_for_tmos:
                        if step == start_tmo:
                            [
                                add_child_mo_id_to_main_node(item.id, item.id)
                                for item in res
                            ]
                        else:
                            [
                                add_child_mo_id_to_main_node(item.id, item.p_id)
                                for item in res
                            ]

        return inventory_data_pb2.ResponseListNodes(
            items=[
                inventory_data_pb2.ResponseNode(node_id=k, children_mo_ids=v)
                for k, v in results.items()
            ]
        )

    def GetMODetailsWithTPRMNames(
        self,
        request: inventory_data_pb2.RequestMODetailsWithTPRMNames,
        context: grpc.aio.ServicerContext,
    ):
        result = ["tmo_id"]
        with Session(engine) as session:
            query = select(TPRM.name).where(TPRM.tmo_id == request.tmo_id)
            params_names = session.execute(query)
            params_names = params_names.scalars().all()
            if len(params_names) == 0:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("TMO with given id not found!")
                return context
            result.extend(params_names)

        return inventory_data_pb2.ResponseMODetailsWithTPRMNames(column=result)

    async def GetColumnsForMaterializedView(
        self,
        request: inventory_data_pb2.RequestTMOAttrsAndTypes,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseTMOAttrsAndTypes:
        """Returns dict of mo attrs, tprms ids as keys and their types as values for particular tmo_id"""

        res = [
            inventory_data_pb2.TMOAttrAndType(name="parent_name", type="str")
        ]

        if request.tmo_id != 0:
            with Session(engine) as session:
                stmt = select(TMO).where(TMO.id == request.tmo_id)
                tmo = session.exec(stmt).first()

                if not tmo:
                    return inventory_data_pb2.ResponseTMOAttrsAndTypes()

                for column in MO.__table__.columns:
                    attr = inventory_data_pb2.TMOAttrAndType(
                        name=column.name, type=str(column.type)
                    )
                    res.append(attr)
                stm = (
                    select(TPRM)
                    .where(TPRM.tmo_id == request.tmo_id)
                    .order_by(TPRM.id)
                )
                tprms = session.exec(stm).all()

                for tprm in tprms:
                    attr = inventory_data_pb2.TMOAttrAndType(
                        name=str(tprm.id),
                        type=tprm.val_type,
                        multiply=tprm.multiple,
                    )
                    res.append(attr)

        return inventory_data_pb2.ResponseTMOAttrsAndTypes(attrs=res)

    async def GetTMOInfoByTMOId(
        self,
        request: inventory_data_pb2.TMOInfoRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.TMOInfoResponse:
        try:
            with Session(engine) as session:
                stm = select(TMO).where(TMO.id.in_(request.tmo_id))
                tmo_info = session.exec(stm).all()
                result = {}

                if tmo_info:
                    for tmo in tmo_info:
                        result[tmo.id] = json.loads(tmo.json())
            result = pickle.dumps(result).hex()
            return inventory_data_pb2.TMOInfoResponse(tmo_info=result)
        except Exception:
            print(traceback.format_exc(), file=stderr)

    async def GetAllTMO(
        self,
        request: inventory_data_pb2.GetAllTMORequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.GetAllTMOResponse:
        with Session(engine) as session:
            stm = select(TMO)
            all_tmos = session.exec(stm).all()
            result = [
                pickle.dumps(json.loads(x.json())).hex() for x in all_tmos
            ]
            return inventory_data_pb2.GetAllTMOResponse(tmo_info=result)

    def GetTPRMNameToTypeMapper(
        self,
        request: inventory_data_pb2.RequestTPRMNameToType,
        context: grpc.ServicerContext,
    ):
        with Session(engine) as session:
            query = select(TPRM.name, TPRM.val_type).where(
                TPRM.tmo_id == request.tmo_id
            )
            if len(request.columns) > 0:
                query = query.where(TPRM.name.in_(request.columns))
            response = session.execute(query)
            mapper = {res[0]: res[1] for res in response.fetchall()}
            return inventory_data_pb2.ResponseTPRMNameToType(mapper=mapper)

    async def GetTMOInfoByMOId(
        self,
        request: inventory_data_pb2.MOInfoRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseListInt:
        with Session(engine) as session:
            stm = select(MO).where(MO.id.in_(request.mo_ids))
            mos = session.exec(stm).all()
            result = set([mo.tmo_id for mo in mos])

        return inventory_data_pb2.ResponseListInt(values=result)

    def GetObjWithParamsLimited(
        self,
        request: inventory_data_pb2.RequestObjWithParamsLimited,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseObjWithParamsLimited:
        if request.tprm_names:
            request.tprm_names.extend(["tmo_id"])
        with Session(engine) as session:
            result = get_objects_with_parameters(
                session=session,
                limit=request.limit,
                offset=request.offset if request.offset else None,
                object_type_id=request.tmo_id,
                returnable=False,
                active=True,
            )
            try:
                ids = set()
                for param in result[0]["params"]:
                    ids.add(param.tprm_id)
                response = session.execute(
                    select(TPRM.id, TPRM.name).where(TPRM.id.in_(ids))
                )
                tprm_id_to_names = {
                    res[0]: res[1] for res in response.fetchall()
                }
                if not request.tprm_names:
                    request.tprm_names.extend(tprm_id_to_names.values())
                    request.tprm_names.extend(["tmo_id"])

            except IndexError:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Objects not found!")
                return context

            for res in result:
                params = res.pop("params")

                for param in params:
                    res[tprm_id_to_names[param.tprm_id]] = param.value

                res = {k: v for k, v in res.items() if k in request.tprm_names}
                yield inventory_data_pb2.ResponseObjWithParamsLimited(
                    data=pickle.dumps(res).hex()
                )

    async def GetTPRMData(
        self,
        request: inventory_data_pb2.RequestTPRMData,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseTPRMData:
        """Returns all tprm data"""
        with Session(engine) as session:
            stm = select(TPRM).where(TPRM.id.in_(request.tprm_ids))
            tprms = session.exec(stm).all()
            result = []
            for tprm in tprms:
                data = dict(tprm)
                if data.get("_sa_instance_state"):
                    del data["_sa_instance_state"]

                result.append(pickle.dumps(data).hex())

        return inventory_data_pb2.ResponseTPRMData(tprms_data=result)

    async def DeleteMOsByIds(
        self,
        request: inventory_data_pb2.DeleteMOIdsRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.DeleteMOIdsResponse:
        """Delete mos by id"""
        with Session(engine) as session:
            stm = select(MO).where(MO.id.in_(request.mo_id))
            mos = session.exec(stm).all()

            count = 0
            if mos:
                for mo in mos:
                    count += 1
                    session.delete(mo)
            session.commit()

        return inventory_data_pb2.DeleteMOIdsResponse(deleted_quantity=count)

    async def GetAllMOWithParamsByTMOId(
        self,
        request: inventory_data_pb2.GetAllMOWithParamsByTMOIdRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.GetAllMOWithParamsByTMOIdResponse:
        """Returns GetAllMOWithParamsByTMOIdResponse."""
        grpc_message_max_size = 4100000
        if not request.tmo_id:
            msg = inventory_data_pb2.GetAllMOWithParamsByTMOIdResponse(
                mos_with_params=[]
            )
            yield msg
            return

        with Session(engine) as session:
            process_of_getting_data = GetAllMOWithParamsByTMOId(
                session=session, tmo_id=request.tmo_id
            )
            for data in process_of_getting_data.get_result_generator(
                replace_links=request.replace_links
            ):
                msg = inventory_data_pb2.GetAllMOWithParamsByTMOIdResponse(
                    mos_with_params=data
                )
                msg_size = msg.ByteSize()

                # create stack
                steps = math.ceil(msg_size / grpc_message_max_size)
                len_of_objects = len(data)
                per_step = int(len_of_objects / steps)
                stack = [
                    data[start : start + per_step]
                    for start in range(0, len_of_objects, per_step)
                ]

                while True:
                    try:
                        msg_items = stack.pop()
                    except IndexError:
                        break
                    msg = inventory_data_pb2.GetAllMOWithParamsByTMOIdResponse(
                        mos_with_params=msg_items
                    )
                    msg_size = msg.ByteSize()
                    if msg_size > grpc_message_max_size:
                        half = len(msg_items) // 2
                        stack.extend([msg_items[:half], msg_items[half:]])
                        continue
                    else:
                        yield msg

    async def GetMODataByIds(
        self,
        request: inventory_data_pb2.GetMODataByIdsRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.GetMODataByIdsResponse:
        """Returns GetMONamesByIdsResponse."""
        grpc_message_max_size = 4100000
        query_max_params = 30000
        if not request.mo_ids:
            msg = inventory_data_pb2.GetMODataByIdsResponse()
            yield msg
            return

        with Session(engine) as session:
            list_of_mo_names_and_ids = []
            len_of_mo_ids = len(request.mo_ids)
            if len_of_mo_ids > query_max_params:
                steps = math.ceil(len_of_mo_ids / query_max_params)

                for step in range(steps):
                    start = step * query_max_params
                    end = start + query_max_params
                    stmt = select(MO.id, MO.name, MO.tmo_id).where(
                        MO.id.in_(request.mo_ids[start:end])
                    )
                    mos_data = session.execute(stmt).mappings().all()
                    list_of_mo_names_and_ids.extend(mos_data)
            else:
                stmt = select(MO.id, MO.name, MO.tmo_id).where(
                    MO.id.in_(request.mo_ids)
                )
                mos_data = session.execute(stmt).mappings().all()
                list_of_mo_names_and_ids.extend(mos_data)

            inner_msg = [
                inventory_data_pb2.MOData(**mo_data)
                for mo_data in list_of_mo_names_and_ids
            ]

            msg = inventory_data_pb2.GetMODataByIdsResponse(
                list_of_mo=inner_msg
            )

            msg_size = msg.ByteSize()
            if msg_size > grpc_message_max_size:
                msg_order = list()
                one_msg = inventory_data_pb2.GetMODataByIdsResponse()
                for item in inner_msg:
                    one_msg.list_of_mo.append(item)

                    if one_msg.ByteSize() > grpc_message_max_size:
                        last_one = one_msg.list_of_mo.pop()
                        msg_order.append(one_msg)
                        one_msg = inventory_data_pb2.GetMODataByIdsResponse()
                        one_msg.list_of_mo.append(last_one)

                if len(one_msg.list_of_mo) > 0:
                    msg_order.append(one_msg)

                for msg in msg_order:
                    yield msg
            else:
                yield msg

    async def GetPRMsByPRMIds(
        self,
        request: inventory_data_pb2.GetPRMsByPRMIdsRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.GetPRMsByPRMIdsResponse:
        """Returns GetPRMsByPRMIdsResponse."""
        grpc_message_max_size = 4100000
        query_max_params = 30000
        if not request.prm_ids:
            msg = inventory_data_pb2.GetPRMsByPRMIdsResponse()
            yield msg
            return

        with Session(engine) as session:
            list_of_prms_data = []
            len_of_mo_ids = len(request.prm_ids)
            if len_of_mo_ids > query_max_params:
                steps = math.ceil(len_of_mo_ids / query_max_params)

                for step in range(steps):
                    start = step * query_max_params
                    end = start + query_max_params
                    stmt = select(
                        PRM.id, PRM.tprm_id, PRM.version, PRM.value
                    ).where(PRM.id.in_(request.prm_ids[start:end]))
                    prms_from_db = session.execute(stmt).mappings().all()
                    list_of_prms_data.extend(prms_from_db)
            else:
                stmt = select(
                    PRM.id, PRM.tprm_id, PRM.version, PRM.value
                ).where(PRM.id.in_(request.prm_ids))
                prms_from_db = session.execute(stmt).mappings().all()
                list_of_prms_data.extend(prms_from_db)

            inner_msg = [
                inventory_data_pb2.PRMMsgValueAsString(**prm_data)
                for prm_data in list_of_prms_data
            ]

            msg = inventory_data_pb2.GetPRMsByPRMIdsResponse(
                list_of_prm=inner_msg
            )

            msg_size = msg.ByteSize()
            if msg_size > grpc_message_max_size:
                msg_order = list()
                one_msg = inventory_data_pb2.GetPRMsByPRMIdsResponse()
                for item in inner_msg:
                    one_msg.list_of_prm.append(item)

                    if one_msg.ByteSize() > grpc_message_max_size:
                        last_one = one_msg.list_of_prm.pop()
                        msg_order.append(one_msg)
                        one_msg = inventory_data_pb2.GetPRMsByPRMIdsResponse()
                        one_msg.list_of_prm.append(last_one)

                if len(one_msg.list_of_prm) > 0:
                    msg_order.append(one_msg)

                for msg in msg_order:
                    yield msg
            else:
                yield msg

    async def GetTPRMAllData(
        self,
        request: inventory_data_pb2.RequestGetTPRMAlldata,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseGetTPRMAlldata:
        """Returns TPRM data by list of tprm_ids, if list is empty - returns data for all TPRMs"""
        yield_per = 10000
        grpc_message_max_size = 4100000

        with Session(engine) as session:
            if request.tprm_ids:
                stmt = (
                    select(TPRM)
                    .where(TPRM.id.in_(request.tprm_ids))
                    .execution_options(yield_per=yield_per)
                )
            else:
                stmt = select(TPRM).execution_options(yield_per=yield_per)

            for partition in (
                session.execute(stmt).scalars().partitions(size=yield_per)
            ):
                tprms_data = [
                    pickle.dumps(data.dict()).hex() for data in partition
                ]
                msg = inventory_data_pb2.ResponseGetTPRMAlldata(
                    tprms_data=tprms_data
                )

                msg_size = msg.ByteSize()
                if msg_size > grpc_message_max_size:
                    msg_order = list()
                    one_msg = inventory_data_pb2.ResponseGetTPRMAlldata()
                    for item in tprms_data:
                        one_msg.tprms_data.append(item)

                        if one_msg.ByteSize() > grpc_message_max_size:
                            last_one = one_msg.tprms_data.pop()
                            msg_order.append(one_msg)
                            one_msg = (
                                inventory_data_pb2.ResponseGetTPRMAlldata()
                            )
                            one_msg.tprms_data.append(last_one)

                    if len(one_msg.tprms_data) > 0:
                        msg_order.append(one_msg)

                    for msg in msg_order:
                        yield msg
                else:
                    yield msg

    async def GetAllTPRMSByTMOId(
        self,
        request: inventory_data_pb2.RequestGetAllTPRMSByTMOId,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseGetAllTPRMSByTMOId:
        """Returns TPRM data by tmo_id"""
        yield_per = 10000
        grpc_message_max_size = 4100000

        with Session(engine) as session:
            stmt = (
                select(TPRM)
                .where(TPRM.tmo_id == request.tmo_id)
                .execution_options(yield_per=yield_per)
            )

            for partition in (
                session.execute(stmt).scalars().partitions(size=yield_per)
            ):
                tprms_data = [
                    pickle.dumps(data.dict()).hex() for data in partition
                ]
                msg = inventory_data_pb2.ResponseGetAllTPRMSByTMOId(
                    tprms_data=tprms_data
                )

                msg_size = msg.ByteSize()
                if msg_size > grpc_message_max_size:
                    msg_order = list()
                    one_msg = inventory_data_pb2.ResponseGetAllTPRMSByTMOId()
                    for item in tprms_data:
                        one_msg.tprms_data.append(item)

                        if one_msg.ByteSize() > grpc_message_max_size:
                            last_one = one_msg.tprms_data.pop()
                            msg_order.append(one_msg)
                            one_msg = (
                                inventory_data_pb2.ResponseGetAllTPRMSByTMOId()
                            )
                            one_msg.tprms_data.append(last_one)

                    if len(one_msg.tprms_data) > 0:
                        msg_order.append(one_msg)

                    for msg in msg_order:
                        yield msg
                else:
                    yield msg

    async def GetAllRawPRMDataByTPRMId(
        self,
        request: inventory_data_pb2.RequestGetAllRawPRMDataByTPRMId,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMId:
        """Returns PRMs data by tmo_id"""
        if not request.tprm_id:
            yield inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMId()
            return

        prm_yield_per = 40000
        grpc_message_max_size = 4100000
        with Session(engine) as session:
            # get tmo tprms
            stmt = (
                select(PRM)
                .where(PRM.tprm_id == request.tprm_id)
                .execution_options(yield_per=prm_yield_per)
            )

            for prm_partition in (
                session.execute(stmt).scalars().partitions(size=prm_yield_per)
            ):
                msg_data = [
                    inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMIdInnerMsg(
                        id=prm_data.id,
                        version=prm_data.version,
                        tprm_id=prm_data.tprm_id,
                        mo_id=prm_data.mo_id,
                        value=prm_data.value,
                    )
                    for prm_data in prm_partition
                ]

                msg = inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMId(
                    prms=msg_data
                )
                msg_size = msg.ByteSize()
                if msg_size > grpc_message_max_size:
                    msg_order = list()
                    one_msg = (
                        inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMId()
                    )
                    for item in msg_data:
                        one_msg.prms.append(item)
                        if one_msg.ByteSize() > grpc_message_max_size:
                            last_one = one_msg.prms.pop()
                            msg_order.append(one_msg)
                            one_msg = inventory_data_pb2.ResponseGetAllRawPRMDataByTPRMId()
                            one_msg.prms.append(last_one)

                    if len(one_msg.prms) > 0:
                        msg_order.append(one_msg)

                    for msg in msg_order:
                        yield msg
                else:
                    yield msg

    def GetFilteredObjSpecialExperimental(self, request, context):
        """Returns ResponseMOdataOrMOIds."""
        CHUNK_SIZE = 100

        tprm_cleaner_data = dict()
        if request.object_type_id:
            print("set tmo_id for cleaner")
            tprm_cleaner_data["object_type_id"] = request.object_type_id

        if request.query_params:
            print("set query_params for cleaner")
            tprm_cleaner_data["query_params"] = pickle.loads(
                bytes.fromhex(request.query_params)
            )

        additional_filter_data = {}
        if request.order_by:
            print("set sort for cleaner")
            additional_filter_data["order_by"] = pickle.loads(
                bytes.fromhex(request.order_by)
            )

        if request.mo_ids:
            print("set mo_ids for cleaner")
            additional_filter_data["obj_ids"] = request.mo_ids

        if request.p_ids:
            print("set p_id for cleaner")
            additional_filter_data["p_id"] = request.p_ids

        with Session(engine) as session:
            if request.decoded_jwt:
                print("set user_data")
                session.info["jwt"] = UserData.from_jwt(
                    pickle.loads(bytes.fromhex(request.decoded_jwt))
                )
                session.info["action"] = "read"

            mos_ids = []
            pickle_mo_dataset = []
            print("init cleaner")
            tprm_cleaner = TPRMFilterCleaner(
                session=session, **tprm_cleaner_data
            )

            if any(
                [
                    tprm_cleaner.check_filter_data_in_query_params(),
                    additional_filter_data,
                ]
            ):
                print("filter (query filter) data provided")
                mos_ids = (
                    tprm_cleaner.get_mo_ids_which_match_clean_filter_conditions(
                        active=True, **additional_filter_data
                    )
                )
            else:
                print("filter (query filter) NOT data provided")
                if request.object_type_id:
                    print("tmo_id provided")
                    # stmt = select(MO.id).where(MO.tmo_id == request.object_type_id)
                    # mos_ids = session.execute(stmt).scalars().all()
                    stmt = (
                        select(MO.id)
                        .where(MO.tmo_id == request.object_type_id)
                        .execution_options(yield_per=CHUNK_SIZE)
                    )
                    for part in (
                        session.execute(stmt).scalars().partitions(CHUNK_SIZE)
                    ):
                        mos_ids.extend(part)

            if not mos_ids:
                print("mo_ids not provided")
                yield inventory_data_pb2.ResponseMOdataSpecial(
                    mo_ids=[], pickle_mo_dataset=[]
                )
                return

            if request.only_ids:
                print("only ids")
                # for i in range(len(mos_ids)):
                #     response = inventory_data_pb2.ResponseMOdataSpecial(mo_ids=[mos_ids[i]],
                #                                                  pickle_mo_dataset=[])
                for part in range(math.ceil(len(mos_ids) / CHUNK_SIZE)):
                    inner_offset = part * CHUNK_SIZE
                    inner_limit = inner_offset + CHUNK_SIZE
                    response = inventory_data_pb2.ResponseMOdataSpecial(
                        mo_ids=mos_ids[inner_offset:inner_limit],
                        pickle_mo_dataset=[],
                    )
                    print(
                        f"filtered-object-special: message-size: {response.ByteSize()}"
                    )
                    yield response
                return

            mo_fields = {MO.id, MO.p_id}
            mo_fields_names = {"id", "p_id"}
            if request.mo_attrs:
                mo_fields_add = {
                    attr
                    for attr_name in request.mo_attrs
                    if (attr := getattr(MO, attr_name, None))
                }
                mo_fields_names_add = {
                    attr_name
                    for attr_name in request.mo_attrs
                    if getattr(MO, attr_name, None)
                }
                mo_fields.update(mo_fields_add)
                mo_fields_names.update(mo_fields_names_add)

            if request.tprm_ids:
                stmt = select(TPRM).where(TPRM.id.in_(request.tprm_ids))
                tprms_cache = session.exec(stmt).all()
                tprms_cache = {
                    str(item.id): {
                        "multiple": item.multiple,
                        "val_type": item.val_type,
                    }
                    for item in tprms_cache
                }
                stm = select(*mo_fields).where(MO.id.in_(mos_ids))

                for tprm_id in request.tprm_ids:
                    label_name = str(tprm_id)
                    select_stmt = select(
                        PRM.mo_id, PRM.value.label(label_name)
                    ).where(PRM.tprm_id == tprm_id, PRM.mo_id.in_(mos_ids))
                    aliased_table = aliased(select_stmt.subquery())
                    # stm = stm.join(aliased_table, MO.id == aliased_table.c.mo_id, isouter=True).add_columns(
                    #     getattr(aliased_table.c, label_name, None)
                    # )
                    stm = (
                        stm.join(
                            aliased_table,
                            MO.id == aliased_table.c.mo_id,
                            isouter=True,
                        )
                        .add_columns(getattr(aliased_table.c, label_name, None))
                        .execution_options(yield_per=CHUNK_SIZE)
                    )
                # all_mo_ids = session.exec(stm).all()
                all_mo_ids = []
                for part in session.execute(stm).partitions(CHUNK_SIZE):
                    all_mo_ids.extend(part)

                def get_tprm_value(item, tprm_column_name, tprm_data):
                    tprm_value = getattr(item, tprm_column_name, None)
                    if tprm_value:
                        if tprm_data["multiple"]:
                            return decode_multiple_value(tprm_value)
                        else:
                            convert_func = value_convertation_by_val_type.get(
                                tprm_data["val_type"]
                            )
                            if convert_func:
                                return convert_func(tprm_value)
                            else:
                                return tprm_value
                    return tprm_value

                def combine_data(item):
                    res = {
                        int(column): get_tprm_value(item, column, tprm_data)
                        for column, tprm_data in tprms_cache.items()
                    }

                    res.update(
                        {
                            column_name: getattr(item, column_name)
                            for column_name in mo_fields_names
                        }
                    )
                    return pickle.dumps(res).hex()

                pickle_mo_dataset = [combine_data(item) for item in all_mo_ids]

            else:
                # stmt = select(MO).where(MO.id.in_(mos_ids))
                # mos = session.execute(stmt).scalars().all()
                mos = []
                stmt = (
                    select(MO)
                    .where(MO.id.in_(mos_ids))
                    .execution_options(yield_per=CHUNK_SIZE)
                )
                for part in (
                    session.execute(stmt).scalars().partitions(CHUNK_SIZE)
                ):
                    mos.extend(part)

                if mos:
                    pickle_mo_dataset = [
                        pickle.dumps(
                            {
                                column_name: attr_data
                                for column_name in mo_fields_names
                                if (
                                    attr_data := getattr(
                                        item, column_name, None
                                    )
                                )
                            }
                        ).hex()
                        for item in mos
                    ]

            print(f"sending {len(mos_ids)} objects")
            # for i in range(len(mos_ids)):
            # response = inventory_data_pb2.ResponseMOdataSpecial(mo_ids=[mos_ids[i]],
            #                                              pickle_mo_dataset=[pickle_mo_dataset[i]])
            for part in range(math.ceil(len(mos_ids) / CHUNK_SIZE)):
                inner_offset = part * CHUNK_SIZE
                inner_limit = inner_offset + CHUNK_SIZE
                response = inventory_data_pb2.ResponseMOdataSpecial(
                    mo_ids=mos_ids[inner_offset:inner_limit],
                    pickle_mo_dataset=pickle_mo_dataset[
                        inner_offset:inner_limit
                    ],
                )
                print(
                    f"filtered-object-special: message-size: {response.ByteSize()}"
                )
                yield response

    async def GetAllMOByTMOIdWithSpecialParameters(
        self,
        request: inventory_data_pb2.MOWithSpecialParametersRequest,
        context: grpc.aio.ServicerContext,
    ) -> inventory_data_pb2.MOWithSpecialParametersResponse:
        """Returns MOWithSpecialParametersResponse."""
        grpc_message_max_size = 4100000
        if not request.tmo_id:
            msg = inventory_data_pb2.MOWithSpecialParametersResponse(
                mos_with_params=[]
            )
            yield msg
            return

        with Session(engine) as session:
            init_data = {"session": session, "tmo_id": request.tmo_id}
            if request.tprm_ids:
                init_data["tprm_ids"] = request.tprm_ids
            process_of_getting_data = GetAllMOAttrsByTMOIdWithSpecialParameters(
                **init_data
            )
            for data in process_of_getting_data.get_result_generator():
                msg = inventory_data_pb2.MOWithSpecialParametersResponse(
                    mos_with_params=data
                )
                msg_size = msg.ByteSize()
                if msg_size > grpc_message_max_size:
                    msg_order = list()
                    one_msg = (
                        inventory_data_pb2.MOWithSpecialParametersResponse()
                    )
                    for item in data:
                        one_msg.mos_with_params.append(item)

                        if one_msg.ByteSize() > grpc_message_max_size:
                            last_one = one_msg.mos_with_params.pop()
                            msg_order.append(one_msg)
                            one_msg = inventory_data_pb2.MOWithSpecialParametersResponse()
                            one_msg.mos_with_params.append(last_one)

                    if len(one_msg.mos_with_params) > 0:
                        msg_order.append(one_msg)
                    for msg in msg_order:
                        yield msg
                else:
                    yield msg

    def GetMOsNamesByIds(
        self,
        request: inventory_data_pb2.RequestGetMOsNamesByIds,
        context: grpc.ServicerContext,
    ) -> inventory_data_pb2.ResponseGetMOsNamesByIds:
        with Session(engine) as session:
            query = select(MO.id, MO.name).where(MO.id.in_(request.mo_ids))
            response = session.execute(query)
            result = {res[0]: res[1] for res in response.fetchall()}

        return inventory_data_pb2.ResponseGetMOsNamesByIds(mo_names=result)


async def start_grpc_serve() -> None:
    # Keepalive options for server https://github.com/grpc/grpc/blob/master/examples/python/keep_alive/greeter_server.py
    server_options = [
        ("grpc.keepalive_time_ms", 20_000),
        ("grpc.keepalive_timeout_ms", 15_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 5_000),
        ("grpc.max_connection_idle_ms", 600_000),
        ("grpc.max_connection_age_ms", 3_600_000 * 24),
        ("grpc.max_connection_age_grace_ms", 5_000),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.keepalive_permit_without_calls", 1),
    ]
    port = 50051
    listen_addr = "[::]:" + str(port)

    server = grpc.aio.server(options=server_options)
    transfer_pb2_grpc.add_TransferServicer_to_server(
        transfer_inventory.Transfer(), server
    )
    inventory_data_pb2_grpc.add_InformerServicer_to_server(Informer(), server)
    airflow_manager_pb2_grpc.add_AirflowManagerServicer_to_server(
        AirflowManager(), server
    )
    dataview_to_inventory_pb2_grpc.add_DataviewToInventoryServicer_to_server(
        DataviewToInventoryManager(), server
    )
    zeebe_to_inventory_pb2_grpc.add_ZeebeInformerServicer_to_server(
        ZeebeInformer(engine=engine), server
    )
    graph_pb2_grpc.add_GraphInformerServicer_to_server(
        GraphInformer(engine=engine), server
    )
    security_manager_pb2_grpc.add_SecurityManagerInformerServicer_to_server(
        SecurityManagerInformer(engine=engine), server
    )
    tasks_inventory_pb2_grpc.add_TasksInventoryServicer_to_server(
        TasksInventoryManager(), server
    )
    event_manager_pb2_grpc.add_EventManagerInformerServicer_to_server(
        EventManagerManager(), server
    )

    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start_grpc_serve())
