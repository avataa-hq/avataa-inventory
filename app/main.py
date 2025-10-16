from threading import Thread

from starlette.middleware.cors import CORSMiddleware

from common.common_utils import clear_event_cache_daily
from config import app_config
from config.app_config import APP_PREFIX, APP_VERSION, APP_TITLE
from init_app import create_app
from routers.background_tasks_router.router import (
    router as background_task_router,
)
from routers.batch_router.router import router as batch
from routers.history_router.router import router as history
from routers.kafka_router.router import router as kafka
from routers.migration_router.router import router as migration
from routers.object_router.router import router as objects
from routers.object_type_router.router import router as object_types
from routers.parameter_router.router import router as parameters
from routers.parameter_type_router.router import router as param_types
from routers.session_registry_router.router import router as session_registry
from routers.synhronizer_router.router import router as synchronizer
from services.kafka_service.init_kafka_consumer import init_kafka_connection
from services.listener_service.service import init_listener
from services.minio_service.minio_client import init_minio_client
from services.security_service.routers.utils.adder import add_security_routers

app = create_app(root_path=APP_PREFIX)

if app_config.DEBUG:
    app.debug = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

app_v1 = create_app(root_path=f"{APP_PREFIX}/v{APP_VERSION}", title=APP_TITLE)
app.mount("/v1", app_v1)

app_v1.include_router(object_types)
app_v1.include_router(param_types)
app_v1.include_router(objects)
app_v1.include_router(parameters)
app_v1.include_router(history)
app_v1.include_router(batch)
app_v1.include_router(session_registry)
app_v1.include_router(synchronizer)
app_v1.include_router(kafka)
app_v1.include_router(background_task_router)
app_v1.include_router(migration)

add_security_routers(app_v1, prefix="")


@app.on_event("startup")
async def on_startup():
    init_minio_client()
    Thread(target=clear_event_cache_daily, daemon=True).start()


init_kafka_connection()
init_listener()
