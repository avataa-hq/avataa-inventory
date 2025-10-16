import time

import requests
from requests.exceptions import MissingSchema
from resistant_kafka_avataa.common_exceptions import TokenIsNotValid

from config import kafka_config


def get_token_for_kafka_by_keycloak(conf):
    payload = {
        "grant_type": "client_credentials",
        "scope": str(kafka_config.KAFKA_KEYCLOAK_SCOPES),
    }

    attempt = 5
    while attempt > 0:
        try:
            response = requests.post(
                kafka_config.KAFKA_KEYCLOAK_TOKEN_URL,
                timeout=30,
                auth=(
                    kafka_config.KAFKA_KEYCLOAK_CLIENT_ID,
                    kafka_config.KAFKA_KEYCLOAK_CLIENT_SECRET,
                ),
                data=payload,
            )
        except ConnectionError as ex:
            print(
                f"get_token_for_kafka_by_keycloak: Failed to connect to token service - {ex}"
            )
            time.sleep(1)
            attempt -= 1
        except MissingSchema as ex:
            print(
                f"get_token_for_kafka_by_keycloak: Invalid URL for token service - {ex}"
            )
            time.sleep(1)
            attempt -= 1
        except Exception as ex:
            print(
                f"get_token_for_kafka_by_keycloak: Unexpected error - {type(ex).__name__}: {ex}"
            )
            time.sleep(1)
            attempt -= 1
        else:
            if response.status_code == 200:
                token = response.json()
                return token["access_token"], time.time() + float(
                    token["expires_in"]
                )
            else:
                print(
                    f"get_token_for_kafka_by_keycloak: Token service returned non-200 status - {response.status_code}"
                )
                time.sleep(1)
                attempt -= 1

    raise TokenIsNotValid(
        "Failed to obtain valid token after multiple attempts"
    )
