import json
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urljoin

import requests
from fastapi import HTTPException
from fastapi.requests import Request

from services.security_service.security_data_models import UserData


class OPA(ABC):
    def __init__(
        self,
        opa_url: str,
        policy_path: str,
    ):
        self._opa_url = opa_url
        self._policy_path = policy_path
        self._url = urljoin(self._opa_url, self._policy_path)

    @abstractmethod
    async def __call__(self, request: Request) -> UserData:
        pass

    def _check_opa(self, request: Request, data: Optional[dict] = None) -> dict:
        method = request.scope.get("method")
        server_host, server_port = request.scope.get("server", (None, None))
        root_path = list(
            filter(None, request.scope.get("root_path").split("/"))
        )
        path = list(filter(None, request.scope.get("path").split("/")))
        full_data = {
            "method": method,
            "server_host": server_host,
            "server_port": server_port,
            "root_path": root_path,
            "path": path,
        }
        if data:
            full_data.update(data)
        data_json = json.dumps({"input": full_data})
        response = requests.post(
            self._url, headers=request.headers, data=data_json, timeout=30
        )
        if response.status_code > 300:
            raise HTTPException(
                status_code=403, detail="Check authorization server"
            )
        response = response.json()
        if not response.get("result", {"allow": False}).get("allow", False):
            raise HTTPException(status_code=403, detail="Not allowed")
        return response["result"]
