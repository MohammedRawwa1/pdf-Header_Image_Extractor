import datetime
import logging
import os
import uuid

logger = logging.getLogger(__name__)

try:
    import config
except Exception:
    config = None


def _build_s3_client(cfg=None):
    """Create a boto3 S3 client from app config, or return None.

    Mirrors the client construction in ``utils/storage.py`` so background jobs
    see the same endpoint/region/addressing behaviour as the rest of the app:
    a missing region or a virtual-hosted-style request against an
    S3-compatible endpoint makes S3 answer ``301 PermanentRedirect``, which
    botocore resolves by calling ``head_bucket`` — and that call redirects
    again, looping until ``RecursionError``.  Getting the endpoint and region
    right (plus path-style addressing where configured) avoids the redirect
    entirely.
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        logger.exception("boto3 not available; cannot create S3 client")
        return None

    cfg = cfg if cfg is not None else config

    client_kwargs = {}
    region = (
        getattr(cfg, "S3_REGION", None)
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
    )
    if region:
        client_kwargs["region_name"] = region
    endpoint = str(getattr(cfg, "S3_ENDPOINT", "") or "").strip()
    if endpoint:
        if not endpoint.startswith(("http://", "https://")):
            scheme = (
                "https"
                if getattr(cfg, "S3_USE_SSL", True) is not False
                else "http"
            )
            endpoint = f"{scheme}://{endpoint}"
        client_kwargs["endpoint_url"] = endpoint.rstrip("/")
    access_key = getattr(cfg, "AWS_ACCESS_KEY_ID", None)
    secret_key = getattr(cfg, "AWS_SECRET_ACCESS_KEY", None)
    if access_key or secret_key:
        client_kwargs["aws_access_key_id"] = access_key or None
        client_kwargs["aws_secret_access_key"] = secret_key or None
    session_token = os.getenv("AWS_SESSION_TOKEN")
    if session_token:
        client_kwargs["aws_session_token"] = session_token

    try:
        sig = getattr(cfg, "S3_SIGNATURE_VERSION", None) or "s3v4"
        force_path = str(os.getenv("S3_FORCE_PATH_STYLE", "")).lower() in (
            "1",
            "true",
            "yes",
        )
        cfg_kwargs = {
            "signature_version": sig,
            "retries": {"max_attempts": 3, "mode": "standard"},
        }
        if force_path:
            cfg_kwargs["s3"] = {"addressing_style": "path"}
        return boto3.client(
            "s3", config=BotoConfig(**cfg_kwargs), **client_kwargs
        )
    except Exception:
        logger.exception("Failed to create S3 client")
        return None


def upload_file_and_get_presigned_url(
    file_path: str, object_name: str | None = None
) -> str | None:
    """Upload `file_path` to configured S3 bucket and return a presigned GET URL.

    Uses `boto3` and configuration from `config.py`.
    Returns the presigned URL string on success, or `None` on failure.
    """
    if not getattr(config, "S3_BUCKET", None):
        logger.error("S3 bucket not configured")
        return None

    s3 = _build_s3_client()
    if s3 is None:
        return None

    bucket = config.S3_BUCKET
    if not object_name:
        object_name = os.path.basename(file_path)
    # create a unique key to avoid collisions
    key = f"pdf-bot/{uuid.uuid4().hex}/{object_name}"

    try:
        # include original filename and size as user metadata
        extra_args = {}
        try:
            orig_size = str(os.path.getsize(file_path))
        except Exception:
            orig_size = "0"
        metadata = {
            "orig_filename": os.path.basename(object_name)
            if object_name
            else os.path.basename(file_path),
            "orig_size": orig_size,
        }
        extra_args["Metadata"] = metadata
        s3.upload_file(file_path, bucket, key, ExtraArgs=extra_args)
    except Exception:
        logger.exception("S3 upload failed for %s", file_path)
        return None

    try:
        expiry = int(getattr(config, "S3_PRESIGNED_EXPIRY", 3600))
    except Exception:
        expiry = 3600

    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expiry,
        )
        return url
    except Exception:
        logger.exception("Generating presigned URL failed for key %s", key)
        return None


def purge_objects_older_than(
    ttl_seconds: int, prefix: str = "pdf-bot/"
) -> int:
    """Delete objects under `prefix` older than `ttl_seconds`.

    Returns the number of deleted objects.
    """
    if not getattr(config, "S3_BUCKET", None):
        logger.error("S3 bucket not configured for purge")
        return 0

    s3 = _build_s3_client()
    if s3 is None:
        return 0

    bucket = config.S3_BUCKET
    deleted_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    now = datetime.datetime.now(datetime.UTC)
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=datetime.UTC)
                age = now - last_modified
                if age.total_seconds() > ttl_seconds:
                    try:
                        s3.delete_object(Bucket=bucket, Key=key)
                        deleted_count += 1
                        logger.info(
                            "Deleted S3 object %s (age=%.0fs)",
                            key,
                            age.total_seconds(),
                        )
                    except Exception:
                        logger.exception("Failed deleting S3 object %s", key)
        return deleted_count
    except RecursionError:
        # botocore follows a 301/PermanentRedirect by calling head_bucket, and
        # with a wrong endpoint/region that call redirects too, recursing until
        # the interpreter gives up.  Report it once instead of dumping a
        # multi-thousand-line traceback every cleanup cycle.
        logger.error(
            "Failed listing S3 objects for purge: botocore entered a "
            "bucket-region redirect loop (bucket=%s endpoint=%s region=%s). "
            "Check S3_BUCKET/S3_REGION against S3_ENDPOINT and set "
            "S3_FORCE_PATH_STYLE=1 for S3-compatible endpoints that require "
            "path-style addressing.",
            bucket,
            getattr(config, "S3_ENDPOINT", "") or "default",
            getattr(config, "S3_REGION", "") or "default",
        )
        return deleted_count
    except Exception:
        logger.exception("Failed listing S3 objects for purge")
        return deleted_count


def get_storage_backend_sync():
    """Return a synchronous storage backend object with `upload_file(file_path, key)`.

    Returns None when S3 is not configured or boto3 is unavailable.
    """
    try:
        # reload config inside function to allow dynamic env changes
        import config as _config
    except Exception:
        _config = config

    bucket = getattr(_config, "S3_BUCKET", None)
    if not bucket:
        logger.error("S3 bucket not configured; storage backend not available")
        return None

    s3 = _build_s3_client(_config)
    if s3 is None:
        return None

    class _S3BackendSync:
        def __init__(self, s3_client, bucket_name):
            self.s3 = s3_client
            self.bucket = bucket_name

        def upload_file(
            self, file_path: str, key: str, ExtraArgs: dict | None = None
        ):
            extra = ExtraArgs or {}
            try:
                orig_size = str(os.path.getsize(file_path))
            except Exception:
                orig_size = "0"
            # ensure metadata keys exist
            md = extra.get("Metadata", {})
            md.setdefault(
                "orig_filename",
                os.path.basename(key) if key else os.path.basename(file_path),
            )
            md.setdefault("orig_size", orig_size)
            extra["Metadata"] = md
            self.s3.upload_file(file_path, self.bucket, key, ExtraArgs=extra)

        def copy_key(self, src_key: str, dest_key: str):
            copy_source = {"Bucket": self.bucket, "Key": src_key}
            self.s3.copy_object(
                Bucket=self.bucket, CopySource=copy_source, Key=dest_key
            )

        def _upload_bytes_sync(self, data: bytes, dest_key: str) -> str:
            # put_object takes Metadata directly; there is no ExtraArgs
            # parameter, and passing one raised ParamValidationError.
            self.s3.put_object(
                Bucket=self.bucket,
                Key=dest_key,
                Body=data,
                Metadata={"orig_size": str(len(data))},
            )
            return dest_key

    return _S3BackendSync(s3, bucket)


async def get_storage_backend():
    """Async wrapper around `get_storage_backend_sync` that provides async `upload_file`/`copy_key`.

    Returns None when S3 is not configured.
    """
    backend = get_storage_backend_sync()
    if backend is None:
        return None

    class _AsyncBackend:
        def __init__(self, sync_backend):
            self._b = sync_backend

        async def upload_file(
            self, file_path: str, key: str, ExtraArgs: dict | None = None
        ):
            import asyncio

            return await asyncio.to_thread(
                self._b.upload_file, file_path, key, ExtraArgs or None
            )

        async def copy_key(self, src_key: str, dest_key: str):
            import asyncio

            return await asyncio.to_thread(self._b.copy_key, src_key, dest_key)

        async def upload_bytes(self, data: bytes, dest_key: str) -> str:
            import asyncio

            return await asyncio.to_thread(
                self._b._upload_bytes_sync, data, dest_key
            )

    return _AsyncBackend(backend)
