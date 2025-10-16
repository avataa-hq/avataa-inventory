# MINIO
import os

MINIO_URL = os.environ.get("MINIO_URL")
MINIO_USER = os.environ.get("MINIO_USER", "inventory")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "static")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "False").upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)
