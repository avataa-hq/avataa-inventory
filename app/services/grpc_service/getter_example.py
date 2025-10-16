import asyncio
import logging
import pickle
import time
from typing import List

import grpc
from google.protobuf import json_format
from grpc.aio import Channel
from starlette.datastructures import QueryParams

from services.grpc_service.proto_files.graph.files import (
    graph_pb2,
    graph_pb2_grpc,
)
from services.grpc_service.proto_files.graph.files.graph_pb2 import InMOsByTMOid
from services.grpc_service.proto_files.graph.files.graph_pb2_grpc import (
    GraphInformerStub,
)

from services.grpc_service.proto_files.zeebe.files import (
    zeebe_to_inventory_pb2_grpc as inv_zeebe_client_pb2_grpc,
)

from services.grpc_service.proto_files.zeebe.files.zeebe_to_inventory_pb2 import (
    InReadObjectTypes,
    OutTMOArray,
    InReadObjectTypeParamTypes,
    OutTPRMArray,
    TMO,
    InUpdateObjectType,
    TMOUpdate,
    InReadObjects,
    InReadChildObjectTypes,
    InGetTMOIdsByMoIds,
)

from services.grpc_service.proto_files.inventory_data.files import (
    inventory_data_pb2_grpc as mo_info_pb2_grpc,
    inventory_data_pb2 as mo_info_pb2,
)
from services.grpc_service.proto_files.security.files import (
    security_manager_pb2_grpc,
    security_manager_pb2,
)


async def run_get_info() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.InfoRequest(mo_id=1, tprm_ids=[1, 2])
        response = await stub.GetParamsValuesForMO(msg)

        message_as_dict = json_format.MessageToDict(
            response,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )

        new_d = dict()
        for k, v in message_as_dict["mo_info"].items():
            new_d[int(k)] = [x["value"] for x in v["mo_tprm_value"]]
        new_d = {k: v[0] if len(v) == 1 else v for k, v in new_d.items()}

        return new_d


async def run_get_tmo_id_for_mo() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.IntValue(value=1)
        response = await stub.GetTMOidForMo(msg)

        message_as_dict = json_format.MessageToDict(
            response,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )

        return message_as_dict


async def run_get_obj_with_params() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestForObjInfoByTMO(
            object_type_id=1, tprm_ids=[1, 2]
        )
        response_async_generator = stub.GetObjWithParams(msg)

        async for item in response_async_generator:
            message_as_dict = json_format.MessageToDict(
                item,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
            )
            logging.warning(message_as_dict)
        return response_async_generator


async def run_get_filtered_obj_with_params():
    async with grpc.aio.insecure_channel(
        "localhost:50051",
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ],
    ) as channel:
        some = QueryParams({"tprm_id122864|contains": "1"})
        some = pickle.dumps(some).hex()
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestForFilteredObjInfoByTMO(
            object_type_id=41557, query_params=some
        )
        response = await stub.GetFilteredObjWithParams(msg)

        print(
            [
                pickle.loads(bytes.fromhex(item))
                for item in response.objects_with_parameters
            ]
        )


async def run_get_filtered_obj_with_params_stream():
    async with grpc.aio.insecure_channel(
        "localhost:50051",
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ],
    ) as channel:
        some = QueryParams({"tprm_id122864|contains": "1"})
        some = pickle.dumps(some).hex()
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestForFilteredObjInfoByTMO(
            object_type_id=41557, query_params=some
        )
        response = stub.GetFilteredObjWithParamsStream(msg)

        async for chunk in response:
            print(
                [
                    pickle.loads(bytes.fromhex(item))
                    for item in chunk.objects_with_parameters
                ]
            )


async def run_get_filtered_obj_without_params():
    async with grpc.aio.insecure_channel(
        "localhost:50051",
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ],
    ) as channel:
        some = QueryParams({"tprm_id106732|more_or_eq": "3"})
        some = pickle.dumps(some).hex()
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestForFilteredObjSpecial(object_type_id=1441)
        response = stub.GetFilteredObjSpecial(msg)
        async for item in response:
            print(
                [pickle.loads(bytes.fromhex(x)) for x in item.pickle_mo_dataset]
            )

            # print(item.mo_ids)

        return response


async def run_get_filtered_obj_by_tmo_id(channel):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.RequestForFilteredObjSpecial(
        object_type_id=4,
        only_ids=True,
        tprm_ids=[],
        mo_attrs=["pov", "geometry", "tmo_id", "p_id", "point_a_id"],
    )
    grpc_response = stub.GetFilteredObjSpecial(msg)

    response = {"mo_ids": [], "pickle_mo_dataset": []}
    async for grpc_chunk in grpc_response:
        response["mo_ids"].extend(grpc_chunk.mo_ids)
        response["pickle_mo_dataset"].extend(grpc_chunk.pickle_mo_dataset)

    res = [
        pickle.loads(bytes.fromhex(item))
        for item in response["pickle_mo_dataset"]
    ]
    return res


async def run_get_tmo_info_by_tmo_id():
    async with grpc.aio.insecure_channel(
        "localhost:50051",
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ],
    ) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.TMOInfoRequest(tmo_id=[41557, 4])
        response = await stub.GetTMOInfoByTMOId(msg)
        print(pickle.loads(bytes.fromhex(response.tmo_info)))


async def run_get_mo_quantity_by_severity():
    async with grpc.aio.insecure_channel(
        "localhost:50051",
        options=[
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ],
    ) as channel:
        severities = {
            "Indeterminate": {"max": 0},
            "Minor": {"min": 10, "max": 70},
            "Alarm": {
                "min": 70,
            },
        }
        tmo_with_mo_ids = {
            41557: [
                2383065,
                2383066,
                2383067,
                2383068,
                2383069,
                2383070,
                2383071,
                2383072,
            ]
        }
        severities = pickle.dumps(severities).hex()
        tmo_with_mo_ids = pickle.dumps(tmo_with_mo_ids).hex()
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestSeverityValues(
            dict_severities=severities, dict_tmo_with_mo_ids=tmo_with_mo_ids
        )
        response = await stub.GetMOQuantityBySeverity(msg)

        print(pickle.loads(bytes.fromhex(response.dict_mo_info)))


async def get_tmo_info_by_mo_id():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.MOInfoRequest(mo_ids=[*range(100)])
        resp = await stub.GetTMOInfoByMOId(msg)
        print(resp.values)


async def get_tprm_names_by_ids():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestTPRMIds(tprm_ids=[1, 2])
        resp = await stub.GetTPRMNames(msg)
        for item in resp.items:
            print(item)


async def get_columns_for_materialized_views():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestTMOAttrsAndTypes(tmo_id=1441)
        resp = await stub.GetColumnsForMaterializedView(msg)
        for item in resp.attrs:
            print(item)


async def get_tprms_data_by_tprms_ids():
    """getter for GetTPRMData"""
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestTPRMData(tprm_ids=[1, 2, 3, 4, 5, 6, 1000])
        resp = await stub.GetTPRMData(msg)
        for item in resp.tprms_data:
            item_dict = pickle.loads(bytes.fromhex(item))
            print(type(item_dict))


async def delete_mos_by_id():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.DeleteMOIdsRequest(mo_id=[1, 9])
        resp = await stub.DeleteMOsByIds(msg)
        print(resp.deleted_quantity)


async def read_object_types(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)
    tmo_ids = [41557, 1441]
    msg = InReadObjectTypes(id=tmo_ids)
    resp: OutTMOArray = await stub.ReadObjectTypes(msg)
    for tmo in resp.tmo:
        print(tmo)


async def read_child_object_types(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)
    parent_id = 41989
    msg = InReadChildObjectTypes(parent_id=parent_id)
    resp: OutTMOArray = await stub.ReadChildObjectTypes(msg)
    for tmo in resp.tmo:
        print(tmo)


async def read_object_type_param_types(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)
    tmo_id = 41557
    group = None
    tprm_ids = None
    msg = InReadObjectTypeParamTypes(id=tmo_id, group=group, tprm_ids=tprm_ids)
    resp: OutTPRMArray = await stub.ReadObjectTypeParamTypes(msg)
    for params in resp.array:
        print(params)


async def update_object_type(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)
    tmo_id = 14221
    object_type = TMOUpdate(version=2, description="test")
    reset_parameters = ["description"]
    msg = InUpdateObjectType(
        id=tmo_id, object_type=object_type, reset_parameters=reset_parameters
    )
    resp: TMO = await stub.UpdateObjectType(msg)
    print(resp)


async def read_objects(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)

    msg = InReadObjects(object_type_id=41557, limit=50, offset=0)
    resp = await stub.ReadObjects(msg)
    print(resp)


async def get_tmo_ids_by_mo_ids(channel: Channel):
    stub = inv_zeebe_client_pb2_grpc.ZeebeInformerStub(channel=channel)

    mo_ids = [1, 2, 3, 4, 5]
    msg = InGetTMOIdsByMoIds(mo_ids=mo_ids)
    resp = await stub.GetTMOIdsByMoIds(msg)
    print(resp)


async def get_mos_by_tmo_id(channel: Channel):
    stub = GraphInformerStub(channel=channel)
    msg = InMOsByTMOid(tmo_id=2021, chunk_size=50)
    async for resp in stub.GetMOsByTMOid(msg):
        print(len(resp.mo))


async def get_all_tmos(channel: Channel):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetAllTMORequest()
    resp = await stub.GetAllTMO(msg)
    print([pickle.loads(bytes.fromhex(item)) for item in resp.tmo_info])


async def get_all_mo_with_params_by_tmo_id(channel: Channel, tmo_id: int):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetAllMOWithParamsByTMOIdRequest(tmo_id=tmo_id)
    grpc_response = stub.GetAllMOWithParamsByTMOId(msg)
    start = time.time()
    count = 0
    async for grpc_chunk in grpc_response:
        len_s = len(grpc_chunk.mos_with_params)
        count += len_s
        print(len_s)
        # print([pickle.loads(bytes.fromhex(item)) for item in grpc_chunk.mos_with_params])
    print(f"end {time.time() - start}")
    print(count)


async def get_mo_data_by_mo_ids(
    channel: Channel, mo_ids: List[int] = [2, 8747324]
):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetMODataByIdsRequest(mo_ids=mo_ids)
    grpc_response = stub.GetMODataByIds(msg)
    async for grpc_chunk in grpc_response:
        print(grpc_chunk)


async def get_mo_prm_data_by_prm_ids(
    channel: Channel, prm_ids: List[int] = [7211224]
):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetPRMsByPRMIdsRequest(prm_ids=prm_ids)
    grpc_response = stub.GetPRMsByPRMIds(msg)
    async for grpc_chunk in grpc_response:
        print(grpc_chunk)


async def get_tprm_data_by_tprm_ids(
    channel: Channel, tprm_ids: List[int] = list()
):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.RequestGetTPRMAlldata(tprm_ids=tprm_ids)
    grpc_response = stub.GetTPRMAllData(msg)
    async for grpc_chunk in grpc_response:
        print(grpc_chunk)


async def get_tprm_data_by_tmo_id(channel: Channel, tmo_id: int):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.RequestGetAllTPRMSByTMOId(tmo_id=tmo_id)
    grpc_response = stub.GetAllTPRMSByTMOId(msg)
    async for grpc_chunk in grpc_response:
        print(grpc_chunk)


async def get_prm_data_by_tmo_id(channel: Channel, tprm_id: int):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.RequestGetAllRawPRMDataByTPRMId(tprm_id=tprm_id)
    grpc_response = stub.GetAllRawPRMDataByTPRMId(msg)
    async for grpc_chunk in grpc_response:
        chunk = [
            {
                "id": item.id,
                "version": item.version,
                "tprm_id": item.tprm_id,
                "mo_id": item.mo_id,
                "value": item.value,
            }
            for item in grpc_chunk.prms
        ]
        print(chunk)


async def graph_get_tprms_by_tmo_id(channel: Channel, tmo_id: list[int]):
    stub = graph_pb2_grpc.GraphInformerStub(channel=channel)
    msg = graph_pb2.InTmoIds(tmo_id=tmo_id)
    grpc_response = await stub.GetTPRMsByTMOid(msg)
    print(grpc_response)


async def graph_get_mos_by_mo_ids(channel: Channel, mo_ids: list[int]):
    stub = graph_pb2_grpc.GraphInformerStub(channel=channel)
    msg = graph_pb2.InMOsByMoIds(mo_ids=mo_ids)
    grpc_response = await stub.GetMOsByMoIds(msg)
    print(grpc_response)


async def graph_get_prms_by_prm_ids(channel: Channel, prm_ids: list[int]):
    stub = graph_pb2_grpc.GraphInformerStub(channel=channel)
    msg = graph_pb2.InPRMsByPRMIds(prm_ids=prm_ids)
    grpc_response = await stub.GetPRMsByPRMIds(msg)
    print(grpc_response)


async def get_all_mo_with_special_params_by_tmo_id(
    channel: Channel, tmo_id: int, tprm_ids: List[int]
):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.MOWithSpecialParametersRequest(
        tmo_id=tmo_id, tprm_ids=tprm_ids
    )
    grpc_response = stub.GetAllMOByTMOIdWithSpecialParameters(msg)
    async for grpc_chunk in grpc_response:
        print(
            [
                pickle.loads(bytes.fromhex(item))
                for item in grpc_chunk.mos_with_params
            ]
        )


async def get_available_object_to_read(channel: Channel):
    stub = security_manager_pb2_grpc.SecurityManagerInformerStub(channel)
    msg = security_manager_pb2.RequestPermissionsForMO(get_permissions=True)
    grpc_response = stub.GetMOPermissions(msg)
    temp = []
    async for grpc_chunk in grpc_response:
        temp.append(grpc_chunk)
    for i in temp:
        for j in i.mo_permissions:
            print(j.read)


async def main():
    # await run_get_filtered_obj_without_params()
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        # await read_object_types(channel=channel)
        # await read_object_type_param_types(channel=channel)
        # await update_object_type(channel=channel)
        # await read_objects(channel=channel)
        # await run_get_filtered_obj_by_tmo_id(channel=channel)
        # await get_prm_data_by_tmo_id(channel, 124970)
        # await get_all_tmos(channel=channel)
        # await graph_get_tprms_by_tmo_id(tmo_id=[42752], channel=channel)
        # await get_mos_by_tmo_id(channel=channel)
        # await graph_get_mos_by_mo_ids(mo_ids=[1, 2, 3, 4, 5], channel=channel)
        # await graph_get_prms_by_prm_ids(prm_ids=[3, 4, 5, 6, 7], channel=channel)
        # await get_all_mo_with_params_by_tmo_id(channel=channel, tmo_id=41555)
        # await get_permissions_by_tmo_id(channel=channel, tmo_ids=[44487, 44485, 43633])
        # await get_permissions_by_mo_id(channel=channel, mo_ids=[11378057, 11238161])
        # await run_get_filtered_obj_with_params()
        # await run_get_filtered_obj_with_params_stream()
        await get_available_object_to_read(channel=channel)


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(main())
