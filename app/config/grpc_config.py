import os

DOCUMENTS_GRPC_HOST = os.environ.get("DOCUMENTS_GRPC_HOST", "documents")
DOCUMENTS_GRPC_PORT = os.environ.get("DOCUMENTS_GRPC_PORT", "50051")

ZEEBE_GRPC_HOST = os.environ.get("ZEEBE_GRPC_HOST", "zeebe-client")
ZEEBE_GRPC_PORT = os.environ.get("ZEEBE_GRPC_PORT", "50051")

EVENT_MANAGER_GRPC_HOST = os.environ.get(
    "EVENT_MANAGER_GRPC_HOST", "event-manager"
)
EVENT_MANAGER_GRPC_PORT = os.environ.get("EVENT_MANAGER_GRPC_PORT", "50051")

# Zeebe configuration
ZEEBE_HOST = os.environ.get("ZEEBE_HOST", "zeebe")
ZEEBE_PORT = os.environ.get("ZEEBE_PORT", "26500")

# Uvicorn configuration
UVICORN_WORKERS = os.environ.get("UVICORN_WORKERS", "")
