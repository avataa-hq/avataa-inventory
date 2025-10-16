import functools

from config import kafka_config
from services.kafka_service.kafka_connection_utils import (
    get_token_for_kafka_by_keycloak,
)


def producer_config():
    if not kafka_config.KAFKA_SECURED:
        return kafka_config.KAFKA_PRODUCER_CONNECT_CONFIG

    config_dict = dict()
    config_dict.update(kafka_config.KAFKA_PRODUCER_CONNECT_CONFIG)
    config_dict["oauth_cb"] = functools.partial(get_token_for_kafka_by_keycloak)
    return config_dict
