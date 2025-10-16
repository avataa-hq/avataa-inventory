import copy
import json
from collections import defaultdict
from typing import Any, Callable, Union

from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from resistant_kafka_avataa import DataSend
from resistant_kafka_avataa.producer import ProducerInitializer

from common.common_constant import MODEL_EQ_MESSAGE
from config import kafka_config
from config.kafka_config import KAFKA_EVENTS_PRODUCER_TOPIC
from services.listener_service.constants import AdditionalData


class SendMessageToKafka:
    def __init__(
        self,
        key_class_name: str,
        key_event: str,
        data_to_send: Union[list, set],
        additional_data: AdditionalData,
        producer_manager: ProducerInitializer,
        producer_manager_with_partitions: ProducerInitializer | None = None,
    ):
        self._key_class_name: str = key_class_name
        self._key_event: str = key_event
        self._data_to_send: Union[list, set] = copy.deepcopy(data_to_send)
        self._additional_data: AdditionalData = additional_data

        self._producer_manager = producer_manager
        self._producer_manager_with_partitions = (
            producer_manager_with_partitions
        )

        # Create partition message as dict {(kafka_partition_ids,): [MO_id1, MO_id11, MO_id21, MO_id31]} for MO, PRM
        self._res_proto_for_part: defaultdict[Any, list] = defaultdict(list)

        # Message with ListMO {(kafka_partition_ids,): [ListMO]} for MO, PRM
        self._res_message_for_part: defaultdict[Any, list] = defaultdict(list)

        self.ENTITY_FOR_PART: dict = {"MO": "id", "PRM": "mo_id"}
        self.KEY_FOR_MESSAGE: str = (
            str(self._key_class_name) + ":" + self._key_event
        )
        if kafka_config.KAFKA_TURN_ON:
            if kafka_config.KAFKA_WITH_SCHEMA_REGISTRY:
                conf = {"use.deprecated.format": False}
                # Create Schema Registry client
                self.schema_registry_client = SchemaRegistryClient(
                    {
                        "url": kafka_config.KAFKA_SCHEMA_REGISTRY_URL,
                        "timeout": 5,
                    }
                )
                # Create Schema Registry Serializers
                self.protobuf_serializer_list_tmo = ProtobufSerializer(
                    msg_type=MODEL_EQ_MESSAGE.get("TMO").proto_list_template,
                    schema_registry_client=self.schema_registry_client,
                    conf=conf,
                )
                self.protobuf_serializer_list_tprm = ProtobufSerializer(
                    msg_type=MODEL_EQ_MESSAGE.get("TPRM").proto_list_template,
                    schema_registry_client=self.schema_registry_client,
                    conf=conf,
                )
                self.protobuf_serializer_list_mo = ProtobufSerializer(
                    msg_type=MODEL_EQ_MESSAGE.get("MO").proto_list_template,
                    schema_registry_client=self.schema_registry_client,
                    conf=conf,
                )
                self.protobuf_serializer_list_prm = ProtobufSerializer(
                    msg_type=MODEL_EQ_MESSAGE.get("PRM").proto_list_template,
                    schema_registry_client=self.schema_registry_client,
                    conf=conf,
                )

    def _split_data_by_partitions_for_special_classes(self):
        """
        Generate list of entities for kafka {kafka_partition_id: [MO_id1, MO_id11, MO_id21, MO_id31]}
        """
        for unit in self._data_to_send:
            self._res_proto_for_part[
                (
                    unit[self.ENTITY_FOR_PART[self._key_class_name]]
                    % kafka_config.KAFKA_PRODUCER_PART_TOPIC_PARTITIONS,
                )
            ].append(self._proto_unit_serializer(**unit))

        for partition, data_for_partition in self._res_proto_for_part.items():  # type: tuple, list
            self._res_message_for_part[partition].append(
                self._proto_list_serializer(objects=data_for_partition)
            )

    def _split_data_by_partitions(self):
        """
        If TMO, TPRM {(1,2,3,...)): [ListTMO]}
        """

        res_proto_for_part = {
            tuple(range(kafka_config.KAFKA_PRODUCER_PART_TOPIC_PARTITIONS)): [
                self._proto_unit_serializer(**unit)
                for unit in self._data_to_send
            ]
        }

        # {(1,2,3,...)): [ListTMO1, ListTMO2]}
        for partition, data_for_partition in res_proto_for_part.items():
            self._res_message_for_part[partition].append(
                self._proto_list_serializer(objects=data_for_partition)
            )

        return self._res_message_for_part

    def _send_messages_chunked(self, proto_units: list) -> None:
        stack = [proto_units]

        def get_size_in_bytes(units):
            # We should respect headers and key sizes
            headers_size = 0
            for k, v in self._additional_data.__dict__.items():
                key_len = len(k.encode("utf-8"))
                val_len = len(v) if v is not None else 0
                headers_size += key_len + val_len
            return (
                int(units.ByteSize()) + len(self.KEY_FOR_MESSAGE) + headers_size
            )

        def split_units(units):
            mid = len(units) // 2
            return units[:mid], units[mid:]

        while stack:
            current_units = stack.pop()

            units_in_list_format = self._proto_list_serializer(
                objects=current_units
            )

            size_in_bytes = get_size_in_bytes(units=units_in_list_format)
            if (
                size_in_bytes < kafka_config.KAFKA_DEFAULT_MSG_MAX_SIZE
                or len(current_units) == 1
            ):
                self._send_to_kafka(message_to_send=units_in_list_format)

            else:
                first_half, second_half = split_units(units=current_units)
                stack.append(second_half)
                stack.append(first_half)

    @staticmethod
    def __chunk_objects_by_kafka_limit(
        proto_objects: list,
        serialize_fn: Callable,
        max_bytes: int,
        overhead_bytes: int = 0,
    ) -> list:
        result = []
        stack = [proto_objects]

        def get_size(units: list) -> int:
            return overhead_bytes + serialize_fn(objects=units).ByteSize()

        while stack:
            current = stack.pop()
            if get_size(current) < max_bytes or len(current) == 1:
                result.append(current)
            else:
                mid = len(current) // 2
                stack.append(current[:mid])
                stack.append(current[mid:])

        return result

    # def _send_message_with_partitions_old(self):
    #     result_message = {}
    #     for part_number, proto_data in self._res_message_for_part.items():
    #         if len(proto_data[0].objects) > kafka_config.KAFKA_MSG_MAX_MSG_LEN:
    #             parts = math.ceil(
    #                 len(proto_data[0].objects)
    #                 / kafka_config.KAFKA_MSG_MAX_MSG_LEN
    #             )
    #             result_message[part_number] = []
    #             for step in range(parts):
    #                 start = step * kafka_config.KAFKA_MSG_MAX_MSG_LEN
    #                 end = start + kafka_config.KAFKA_MSG_MAX_MSG_LEN
    #                 result_message[part_number].append(
    #                     self._proto_list_serializer(
    #                         objects=proto_data[0].objects[start:end]
    #                     )
    #                 )
    #     if result_message:
    #         self._res_message_for_part = result_message
    #
    #     self._send_to_kafka_with_partitions(
    #         message_to_send=self._res_message_for_part
    #     )

    def _calculate_header_overhead(self) -> int:
        total = len(self.KEY_FOR_MESSAGE)
        for k, v in self._additional_data.__dict__.items():
            total += len(k.encode("utf-8")) + (len(v) if v is not None else 0)
        return total

    def _send_message_with_partitions(self):
        result_message = {}

        for part_number, proto_data in self._res_message_for_part.items():
            proto_objects = proto_data[0].objects

            chunks = self.__chunk_objects_by_kafka_limit(
                proto_objects=proto_objects,
                serialize_fn=self._proto_list_serializer,
                max_bytes=kafka_config.KAFKA_DEFAULT_MSG_MAX_SIZE,
                overhead_bytes=self._calculate_header_overhead(),
            )

            result_message[part_number] = [
                self._proto_list_serializer(objects=chunk) for chunk in chunks
            ]

        self._res_message_for_part = result_message
        self._send_to_kafka_with_partitions(
            message_to_send=self._res_message_for_part
        )

    def _prepare_message_for_kafka(self):
        class_serializers = MODEL_EQ_MESSAGE.get(self._key_class_name)

        if class_serializers:
            self._proto_list_serializer = class_serializers.proto_list_template
            self._proto_unit_serializer = class_serializers.proto_unit_template

            try:
                self._proto_units = [
                    self._proto_unit_serializer(**unit)
                    for unit in self._data_to_send
                ]
                self._proto_message_list = self._proto_list_serializer(
                    objects=self._proto_units
                )

                if self._key_class_name in self.ENTITY_FOR_PART.keys():
                    self._split_data_by_partitions_for_special_classes()

                else:
                    self._split_data_by_partitions()

            except ValueError:
                print("Invalid input, discarding record...")
            except TypeError as ex:
                print("Serialize TypeError:", ex)
                print(f"{self._data_to_send=}")

            else:
                self._send_messages_chunked(proto_units=self._proto_units)
                self._send_message_with_partitions()

    def _prepared_message_to_kafka(self, message_to_send, topic: str) -> bytes:
        if kafka_config.KAFKA_WITH_SCHEMA_REGISTRY:
            match self._key_class_name:
                case "TMO":
                    prepared_message_value = self.protobuf_serializer_list_tmo(
                        message_to_send,
                        SerializationContext(topic, MessageField.VALUE),
                    )
                case "TPRM":
                    prepared_message_value = self.protobuf_serializer_list_tprm(
                        message_to_send,
                        SerializationContext(topic, MessageField.VALUE),
                    )
                case "MO":
                    prepared_message_value = self.protobuf_serializer_list_mo(
                        message_to_send,
                        SerializationContext(topic, MessageField.VALUE),
                    )
                case "PRM":
                    prepared_message_value = self.protobuf_serializer_list_prm(
                        message_to_send,
                        SerializationContext(topic, MessageField.VALUE),
                    )
                case _:
                    raise ValueError(f"Incorrect Key: {self._key_class_name}")
        else:
            if (
                self._producer_manager._producer_name
                == KAFKA_EVENTS_PRODUCER_TOPIC
            ):
                prepared_message_value = json.dumps(message_to_send)

            else:
                prepared_message_value = message_to_send.SerializeToString()

        return prepared_message_value

    def _send_to_kafka(self, message_to_send):
        self._producer_manager.send_message(
            partition_number=0,
            data_to_send=DataSend(
                key=self.KEY_FOR_MESSAGE,
                value=self._prepared_message_to_kafka(
                    message_to_send, kafka_config.KAFKA_PRODUCER_TOPIC
                ),
                headers=[
                    (key, value)
                    for key, value in self._additional_data.__dict__.items()
                ],
            ),
        )

    def _send_to_kafka_with_partitions(self, message_to_send: dict):
        if self._producer_manager_with_partitions:
            try:
                for partitions, part_msg in message_to_send.items():
                    for part_number in partitions:
                        for chunk in part_msg:
                            self._producer_manager_with_partitions.send_message(
                                partition_number=part_number,
                                data_to_send=DataSend(
                                    key=self.KEY_FOR_MESSAGE,
                                    value=self._prepared_message_to_kafka(
                                        chunk,
                                        kafka_config.KAFKA_PRODUCER_PART_TOPIC_NAME,
                                    ),
                                    headers=[
                                        (key, value)
                                        for key, value in self._additional_data.__dict__.items()
                                    ],
                                ),
                            )

            except KafkaException as ex:
                print(ex.args[0].str())
                return

            except Exception as ex:
                print(f"send to kafka with partition raise error: {ex}")
                return

    def _format_object_type_message(self):
        if self._key_class_name == "TMO":
            for object_type_data in self._data_to_send:
                if object_type_data.get("geometry_type"):
                    object_type_data["geometry_type"] = object_type_data[
                        "geometry_type"
                    ].split(".")[-1]

    def send_message(self):
        if kafka_config.KAFKA_TURN_ON:
            self._format_object_type_message()
            self._prepare_message_for_kafka()

    def send_any_message(self):
        if kafka_config.KAFKA_TURN_ON:
            self._format_object_type_message()

            self._send_to_kafka(message_to_send=self._data_to_send)
