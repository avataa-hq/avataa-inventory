# Inventory

## Environment variables

```toml
ASYNC_DB_TYPE=postgresql+asyncpg
CELERY_BROKER_URL=redis://celery-redis:6379/0
CELERY_RESULT_BACKEND=redis://celery-redis:6379/0
DB_HOST=<pgbouncer/postgres_host>
DB_NAME=<pgbouncer/postgres_inventory_db_name>
DB_PASS=<pgbouncer/postgres_inventory_password>
DB_PORT=<pgbouncer/postgres_port>
DB_TYPE=postgresql
DB_USER=<pgbouncer/postgres_inventory_user>
DOCS_CUSTOM_ENABLED=<True/False>
DOCS_REDOC_JS_URL=<redoc_js_url>
DOCS_SWAGGER_CSS_URL=<swagger_css_url>
DOCS_SWAGGER_JS_URL=<swagger_js_url>
DOCUMENTS_GRPC_HOST=<documents_host>
DOCUMENTS_GRPC_PORT=<documents_grpc_port>
EVENT_MANAGER_GRPC_HOST=<event_manager_grpc_host>
EVENT_MANAGER_GRPC_PORT=<event_manager_grpc_port>
KAFKA_CONSUMER_GROUP_ID=Inventory
KAFKA_CONSUMER_OFFSET=latest
KAFKA_KEYCLOAK_CLIENT_ID=<kafka_client>
KAFKA_KEYCLOAK_CLIENT_SECRET=<kafka_client_secret>
KAFKA_KEYCLOAK_SCOPES=profile
KAFKA_KEYCLOAK_TOKEN_URL=<keycloak_protocol>://<keycloak_host>:<keycloak_port>/realms/avataa/protocol/openid-connect/token
KAFKA_PRODUCER_PARTITION_TOPIC_NAME=inventory.changes.part
KAFKA_PRODUCER_PARTITION_TOPIC_PARTITIONS=<kafka_producer_partition_topic_partitions_number>
KAFKA_PRODUCER_TOPIC=inventory.changes
KAFKA_SECURED=<True/False>
KAFKA_SUBSCRIBE_TOPICS=documents.changes
KAFKA_TURN_ON=<True/False>
KAFKA_URL=<kafka_host>:<kafka_port>
KEYCLOAK_HOST=<keycloak_host>
KEYCLOAK_PORT=<keycloak_port>
KEYCLOAK_PROTOCOL=<keycloak_protocol>
KEYCLOAK_REALM=avataa
KEYCLOAK_REDIRECT_HOST=<keycloak_external_host>
KEYCLOAK_REDIRECT_PORT=<keycloak_external_port>
KEYCLOAK_REDIRECT_PROTOCOL=<keycloak_external_protocol>
MINIO_BUCKET=<minio_inventory_bucket>
MINIO_PASSWORD=<minio_inventory_password>
MINIO_SECURE=<True/False>
MINIO_URL=<minio_api_host>
MINIO_USER=<minio_inventory_user>
OPA_HOST=<opa_host>
OPA_POLICY=main
OPA_PORT=<opa_port>
OPA_PROTOCOL=<opa_protocol>
SECURITY_MIDDLEWARE_HOST=security-middleware
SECURITY_MIDDLEWARE_PORT=8000
SECURITY_MIDDLEWARE_PROTOCOL=http
SECURITY_TYPE=<security_type>
UVICORN_WORKERS=<uvicorn_workers_number>
ZEEBE_GRPC_HOST=<zeebe_client_host>
ZEEBE_GRPC_PORT=<zeebe_client_grpc_port>
ZEEBE_HOST=<zeebe_host>
ZEEBE_PORT=<zeebe_grpc_port>
```

## Explanation

### Database

`DB_TYPE` Type of database 
(default: _postgresql_)  
`DB_USER` Pre-created user in the database with rights to edit the database 
(default: _inventory_admin_)  
`DB_PASS` Database user password
(default: _inventory_pass_)  
`DB_HOST` Database host
(default: _localhost_)  
`DB_PORT`  Database port 
(default: _5432_)  
`DB_NAME`  Name of the previously created database
(default: _inventory_)  

### Kafka

`KAFKA_URL` kafka address
(default: _127.0.0.1:9092_)  
`KAFKA_TOPIC` messaging thread
(default: _New_Topic_)  

### Keycloak

`KEYCLOAK_PROTOCOL` Protocol for internal communication of microservice with Keycloak
(default: _http_)  
`KEYCLOAK_HOST` Host for internal communication of microservice with Keycloak
(default: _keycloak_)  
`KEYCLOAK_PORT` Port for internal communication of microservice with Keycloak
(default: _8080_)  
`KEYCLOAK_REDIRECT_PROTOCOL` Protocol that is used to redirect the user for authorization in Keycloak
(default: _as in key `KEYCLOAK_PROTOCOL`_)  
`KEYCLOAK_REDIRECT_HOST` Host that is used to redirect the user for authorization in Keycloak
(default: _as in key `KEYCLOAK_HOST`_)  
`KEYCLOAK_REDIRECT_PORT` Port that is used to redirect the user for authorization in Keycloak
(default: _as in key `KEYCLOAK_PORT`_)  
`KEYCLOAK_REALM`  Realm for the current microservice
(default: _master_)  
`KEYCLOAK_CLIENT_ID` Client ID for the current microservice 
`KEYCLOAK_CLIENT_SECRET` Client secret for the current microservice
(default: _EMPTY_)  

### OPA

`OPA_PROTOCOL` = Protocol for internal communication of microservice with OPA
(default: _http_)  
`OPA_HOST` = Host for internal communication of microservice with OPA
(default: _opa_)  
`OPA_PORT` = Port for internal communication of microservice with OPA
(default: _8181_)  
`OPA_POLICY` = The name of the policy for checking access rights
(default: _main_)  

### Other

`DEBUG` Debug mode
(default: _False_)  
`TEST_DOCKER_DB_HOST` Variable for test environment if external dockers 
`SECURITY_TYPE` microservice security type (default: _DISABLE_):
- `DISABLE` protection disabled
- `KEYCLOAK` protection is organized on the verification of the token by the microservice
- `OPA-JWT-RAW` Requests from the user are first redirected to the OPA, which checks both the token and the access level
- `OPA-JWT-PARSED` Protection is organized on the fact that the token is checked by the microservice. We send the already decoded token data and the necessary data for verification to the OPA

### Compose

- `REGISTRY_URL` - Docker regitry URL, e.g. `harbor.avataa.dev`
- `PLATFORM_PROJECT_NAME` - Docker regitry project Docker image can be downloaded from, e.g. `avataa`

## Requirements

### Install

```sh
uv sync
```

## Running

### Run

```sh
cd app
uvicorn main:app --reload
```

### Successful Logs

```sh
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [28720]
INFO:     Started server process [28722]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

