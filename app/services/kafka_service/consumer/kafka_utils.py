from collections import defaultdict

from confluent_kafka import ConsumerGroupState, ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient

from services.kafka_service.producer.utils import producer_config


async def get_topic_info() -> dict:
    admin = AdminClient(producer_config())
    result = {}
    try:
        metadata_topics = admin.list_topics().topics
        for k, v in metadata_topics.items():
            if k.startswith("__"):
                continue
            result[k] = v.partitions
    except Exception:
        raise
    return result


async def get_consumer_groups(states: list[str]) -> dict:
    admin = AdminClient(producer_config())
    if not states:
        # UNKOWN - deprecated in confluent_kafka 2.3.0, replace to UNKNOWN
        states = ["STABLE", "UNKOWN"]
    kafka_states = {ConsumerGroupState[state] for state in states}
    future = admin.list_consumer_groups(request_timeout=10, states=kafka_states)
    result = {}
    try:
        list_consumer_groups_result = future.result()
        for valid in list_consumer_groups_result.valid:  # type: ConsumerGroupListing
            result["valid"] = {
                "group_id": valid.group_id,
                "is simple consumer group": valid.is_simple_consumer_group,
                "state": valid.state,
            }
        for error in list_consumer_groups_result.errors:
            print("    error: {}".format(error))
    except Exception:
        raise
    return result


async def get_consumer_group_offset() -> dict:
    admin = AdminClient(producer_config())
    result = {}
    list_cons_groups = admin.list_consumer_groups()
    list_consumer_groups_result = list_cons_groups.result()
    client_ids = admin.describe_consumer_groups(
        group_ids=[
            group.group_id for group in list_consumer_groups_result.valid
        ]
    )
    topic_partitions = {}
    for client_id, future in client_ids.items():
        topic_partitions[client_id] = (
            future.result().members[0].assignment.topic_partitions
        )
    groups = [
        ConsumerGroupTopicPartitions(gr.group_id, topic_partitions[gr.group_id])
        for gr in list_consumer_groups_result.valid
    ]
    offsets = admin.list_consumer_group_offsets(groups)
    for group_id, future in offsets.items():
        response_offset_info = future.result()
        result[group_id] = defaultdict(list)
        for topic_partition in response_offset_info.topic_partitions:
            result[group_id][topic_partition.topic].append(
                {
                    "topic partition": topic_partition.partition,
                    "offset": topic_partition.offset,
                }
            )
    return result
