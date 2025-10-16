import os

AVAILABLE_TRUE_VALUES = ["TRUE", "Y", "YES", "1"]
# https://github.com/confluentinc/librdkafka/issues/3125
KAFKA_DEFAULT_MSG_MAX_SIZE = 1_000_000

KAFKA_TURN_ON = (
    str(os.environ.get("KAFKA_TURN_ON", False)).upper() in AVAILABLE_TRUE_VALUES
)
KAFKA_SECURED = (
    str(os.environ.get("KAFKA_SECURED", False)).upper() in AVAILABLE_TRUE_VALUES
)


KAFKA_URL = os.environ.get("KAFKA_URL", "kafka:9092")

KAFKA_WITH_SCHEMA_REGISTRY = (
    str(os.environ.get("KAFKA_WITH_SCHEMA_REGISTRY", False)).upper()
    in AVAILABLE_TRUE_VALUES
)

KAFKA_SCHEMA_REGISTRY_URL = os.environ.get("KAFKA_SCHEMA_REGISTRY_URL", None)

KAFKA_SECURITY_TOPIC = os.environ.get(
    "KAFKA_SECURITY_TOPIC", "inventory.security"
)

KAFKA_MSG_MAX_SIZE = os.environ.get("KAFKA_MSG_MAX_SIZE", False)

# Default value for Kafka
# https://github.com/confluentinc/librdkafka/issues/3125
KAFKA_MSG_MAX_MSG_LEN = os.environ.get("KAFKA_MSG_MAX_MSG_LEN", "1000000")

KAFKA_MSG_MAX_MSG_LEN = int(KAFKA_MSG_MAX_MSG_LEN)

KAFKA_PRODUCER_TOPIC = os.environ.get(
    "KAFKA_PRODUCER_TOPIC", "inventory.changes"
)
KAFKA_EVENTS_PRODUCER_TOPIC = os.environ.get(
    "KAFKA_EVENTS_PRODUCER_TOPIC", "event_migrate"
)

KAFKA_KEYCLOAK_CLIENT_ID = os.environ.get("KAFKA_KEYCLOAK_CLIENT_ID", "kafka")

KAFKA_KEYCLOAK_CLIENT_SECRET = os.environ.get(
    "KAFKA_KEYCLOAK_CLIENT_SECRET", ""
)
KAFKA_KEYCLOAK_TOKEN_URL = os.environ.get(
    "KAFKA_KEYCLOAK_TOKEN_URL",
    "http://keycloak:8080/realms/avataa/protocol/openid-connect/token",
)

KAFKA_KEYCLOAK_SCOPES = os.environ.get("KAFKA_KEYCLOAK_SCOPES", "profile")

KAFKA_DOCUMENTS_CHANGES_TOPIC = "documents.changes"

KAFKA_SUBSCRIBE_TOPICS = os.environ.get(
    "KAFKA_SUBSCRIBE_TOPICS", "documents.changes"
)

KAFKA_CONSUMER_GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "Inventory")

KAFKA_CONSUMER_OFFSET = os.environ.get("KAFKA_CONSUMER_OFFSET", "latest")

KAFKA_PRODUCER_CONNECT_CONFIG = {"bootstrap.servers": KAFKA_URL}

KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISMS = "OAUTHBEARER"

KAFKA_PRODUCER_PART_TOPIC_NAME = os.environ.get(
    "KAFKA_PRODUCER_PARTITION_TOPIC_NAME", "inventory.changes.part"
)

KAFKA_CONSUMER_CONNECT_CONFIG = {
    "bootstrap.servers": KAFKA_URL,
    "group.id": KAFKA_CONSUMER_GROUP_ID,
    "auto.offset.reset": KAFKA_CONSUMER_OFFSET,
    "enable.auto.commit": False,
}

if KAFKA_SECURED:
    SECURED_SETTINGS = {
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISMS,
    }
    KAFKA_PRODUCER_CONNECT_CONFIG.update(SECURED_SETTINGS)
    KAFKA_CONSUMER_CONNECT_CONFIG.update(SECURED_SETTINGS)

KAFKA_PRODUCER_PART_TOPIC_PARTITIONS = int(
    os.environ.get("KAFKA_PRODUCER_PARTITION_TOPIC_PARTITIONS", 10)
)
