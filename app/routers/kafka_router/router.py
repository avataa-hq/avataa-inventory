from fastapi import APIRouter

from services.kafka_service.consumer.kafka_utils import (
    get_topic_info,
    get_consumer_groups,
    get_consumer_group_offset,
)

router = APIRouter(tags=["Kafka"])


@router.get("/topic_info")
async def get_kafka_topic():
    result = await get_topic_info()
    return {"kafka_topic": result}


@router.get("/consumer_group_info")
async def get_list_consumer_groups():
    result = await get_consumer_groups([])
    return {"kafka consumer group": result}


@router.get("/consumer_group_offset")
async def get_list_consumer_group_offset():
    result = await get_consumer_group_offset()
    return {"kafka consumer group offset": result}
