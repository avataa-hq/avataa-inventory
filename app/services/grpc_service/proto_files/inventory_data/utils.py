from services.grpc_service.proto_files.inventory_data.files import (
    inventory_data_pb2,
)

VAL_TYPE_CONVERTER = {
    "str": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "date": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "datetime": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "float": {
        "proto_serializer": inventory_data_pb2.FloatValue,
        "python_deserializer": float,
    },
    "int": {
        "proto_serializer": inventory_data_pb2.IntValue,
        "python_deserializer": int,
    },
    "mo_link": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "prm_link": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "user_link": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "formula": {
        "proto_serializer": inventory_data_pb2.StringValue,
        "python_deserializer": str,
    },
    "bool": {
        "proto_serializer": inventory_data_pb2.BoolValue,
        "python_deserializer": bool,
    },
}
