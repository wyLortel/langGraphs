"""MinIO(S3 대체) 접근. minio 라이브러리는 동기식이라 asyncio.to_thread로 감싼다."""
import asyncio
import io
from datetime import timedelta

from minio import Minio
from minio.error import S3Error

from . import config

_client = Minio(config.S3_ENDPOINT, access_key=config.S3_ACCESS_KEY,
                secret_key=config.S3_SECRET_KEY, secure=False)
# WHY 별도 클라이언트: presigned URL의 서명에는 host가 들어간다.
# 컨테이너 안 주소(minio:9000)로 서명하면 브라우저(localhost:9000)에서 쓸 수 없다.
# region을 지정하면 presign 시 네트워크 호출 없이 계산만 한다.
_public = Minio(config.S3_PUBLIC_ENDPOINT, access_key=config.S3_ACCESS_KEY,
                secret_key=config.S3_SECRET_KEY, secure=False, region="us-east-1")


def _ensure_bucket():
    if not _client.bucket_exists(config.S3_BUCKET):
        try:
            _client.make_bucket(config.S3_BUCKET)
        except S3Error as e:
            if e.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise


async def ensure_bucket():
    await asyncio.to_thread(_ensure_bucket)


async def put_text(key: str, text: str, content_type="text/plain; charset=utf-8"):
    data = text.encode("utf-8")
    await asyncio.to_thread(_client.put_object, config.S3_BUCKET, key, io.BytesIO(data), len(data),
                            content_type=content_type)
    return len(data)


async def get_text(key: str) -> str:
    def _get():
        r = _client.get_object(config.S3_BUCKET, key)
        try:
            return r.read().decode("utf-8")
        finally:
            r.close()
            r.release_conn()
    return await asyncio.to_thread(_get)


async def exists(key: str) -> bool:
    def _stat():
        try:
            _client.stat_object(config.S3_BUCKET, key)
            return True
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchObject", "NotFound"):
                return False
            raise
    return await asyncio.to_thread(_stat)


async def list_objects(prefix: str = ""):
    def _ls():
        return [{"key": o.object_name, "size": o.size,
                 "modified": o.last_modified.isoformat() if o.last_modified else None}
                for o in _client.list_objects(config.S3_BUCKET, prefix=prefix, recursive=True)]
    return await asyncio.to_thread(_ls)


async def delete_all():
    def _rm():
        for o in _client.list_objects(config.S3_BUCKET, recursive=True):
            _client.remove_object(config.S3_BUCKET, o.object_name)
    await asyncio.to_thread(_rm)


def presign(key: str) -> str:
    return _public.presigned_get_object(config.S3_BUCKET, key,
                                        expires=timedelta(seconds=config.PRESIGN_EXPIRES_SEC))
