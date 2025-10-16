from fastapi import FastAPI

from config import security_config


def add_security_routers(app: FastAPI, prefix: str):
    if security_config.SECURITY_TYPE == "DISABLE":
        return

    from routers.security_router.router import security_router

    app.include_router(security_router, prefix=prefix)
