from minio import Minio

from config.minio_config import (
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
    MINIO_SECURE,
    MINIO_BUCKET,
)

if MINIO_URL:
    minio_client = Minio(
        MINIO_URL, MINIO_USER, MINIO_PASSWORD, secure=MINIO_SECURE
    )
else:
    minio_client = False


def init_minio_client():
    if minio_client and not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)

        return minio_client

    return
