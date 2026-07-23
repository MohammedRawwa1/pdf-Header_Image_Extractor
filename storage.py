import logging
import os
import uuid

logger = logging.getLogger(__name__)

try:
    import config
except Exception:
    config = None


def upload_file_and_get_presigned_url(file_path: str, object_name: str | None = None) -> str | None:
    """Upload `file_path` to configured S3 bucket and return a presigned GET URL.

    Uses `boto3` and configuration from `config.py`.
    Returns the presigned URL string on success, or `None` on failure.
    """
    try:
        import boto3
    except Exception:
        logger.exception("boto3 not available; cannot upload to S3")
        return None

    try:
        import config
    except Exception:
        logger.exception("Failed to import config for S3 upload")
        return None

    bucket = getattr(config, 'S3_BUCKET', None)
    if not bucket:
        logger.error("S3 bucket not configured")
        return None

    # Build client kwargs
    client_kwargs = {}
    if getattr(config, 'S3_REGION', None):
        client_kwargs['region_name'] = config.S3_REGION
    if getattr(config, 'S3_ENDPOINT', None):
        client_kwargs['endpoint_url'] = config.S3_ENDPOINT
    # Allow explicit credentials if provided, otherwise boto3 will use env/instance role
    if getattr(config, 'AWS_ACCESS_KEY_ID', None) or getattr(config, 'AWS_SECRET_ACCESS_KEY', None):
        client_kwargs['aws_access_key_id'] = config.AWS_ACCESS_KEY_ID or None
        client_kwargs['aws_secret_access_key'] = config.AWS_SECRET_ACCESS_KEY or None

    try:
        from botocore.config import Config as BotoConfig
        sig = getattr(config, 'S3_SIGNATURE_VERSION', 's3v4')
        boto_cfg = BotoConfig(signature_version=sig)
        s3 = boto3.client('s3', config=boto_cfg, **client_kwargs)
    except Exception:
        logger.exception("Failed to create S3 client")
        return None

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
            'orig_filename': os.path.basename(object_name) if object_name else os.path.basename(file_path),
            'orig_size': orig_size,
        }
        extra_args['Metadata'] = metadata
        s3.upload_file(file_path, bucket, key, ExtraArgs=extra_args)
    except Exception:
        logger.exception("S3 upload failed for %s", file_path)
        return None

    try:
        expiry = int(getattr(config, 'S3_PRESIGNED_EXPIRY', 3600))
    except Exception:
        expiry = 3600

    try:
        url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expiry)
        return url
    except Exception:
        logger.exception("Generating presigned URL failed for key %s", key)
        return None


def purge_objects_older_than(ttl_seconds: int, prefix: str = 'pdf-bot/') -> int:
    """Delete objects under `prefix` older than `ttl_seconds`.

    Returns the number of deleted objects.
    """
    try:
        import boto3
    except Exception:
        logger.exception("boto3 not available; cannot purge S3 objects")
        return 0

    try:
        import config
    except Exception:
        logger.exception("Failed to import config for S3 purge")
        return 0

    bucket = getattr(config, 'S3_BUCKET', None)
    if not bucket:
        logger.error("S3 bucket not configured for purge")
        return 0

    client_kwargs = {}
    if getattr(config, 'S3_REGION', None):
        client_kwargs['region_name'] = config.S3_REGION
    if getattr(config, 'S3_ENDPOINT', None):
        client_kwargs['endpoint_url'] = config.S3_ENDPOINT
    if getattr(config, 'AWS_ACCESS_KEY_ID', None) or getattr(config, 'AWS_SECRET_ACCESS_KEY', None):
        client_kwargs['aws_access_key_id'] = config.AWS_ACCESS_KEY_ID or None
        client_kwargs['aws_secret_access_key'] = config.AWS_SECRET_ACCESS_KEY or None

    try:
        from botocore.config import Config as BotoConfig
        sig = getattr(config, 'S3_SIGNATURE_VERSION', 's3v4')
        boto_cfg = BotoConfig(signature_version=sig)
        s3 = boto3.client('s3', config=boto_cfg, **client_kwargs)
    except Exception:
        logger.exception("Failed to create S3 client for purge")
        return 0

    deleted_count = 0
    paginator = s3.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                last_modified = obj['LastModified']
                # compare with now
                import datetime
                age = datetime.datetime.utcnow() - last_modified.replace(tzinfo=None)
                if age.total_seconds() > ttl_seconds:
                    try:
                        s3.delete_object(Bucket=bucket, Key=key)
                        deleted_count += 1
                        logger.info("Deleted S3 object %s (age=%.0fs)", key, age.total_seconds())
                    except Exception:
                        logger.exception("Failed deleting S3 object %s", key)
        return deleted_count
    except Exception:
        logger.exception("Failed listing S3 objects for purge")
        return deleted_count


def get_storage_backend_sync():
    """Return a synchronous storage backend object with `upload_file(file_path, key)`.

    Returns None when S3 is not configured or boto3 is unavailable.
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        logger.exception("boto3 not available; storage backend sync not available")
        return None

    try:
        # reload config inside function to allow dynamic env changes
        import config as _config
    except Exception:
        _config = config

    bucket = getattr(_config, 'S3_BUCKET', None)
    if not bucket:
        logger.error("S3 bucket not configured; storage backend not available")
        return None

    client_kwargs = {}
    if getattr(_config, 'S3_REGION', None):
        client_kwargs['region_name'] = _config.S3_REGION
    if getattr(_config, 'S3_ENDPOINT', None):
        client_kwargs['endpoint_url'] = _config.S3_ENDPOINT
    if getattr(_config, 'AWS_ACCESS_KEY_ID', None) or getattr(_config, 'AWS_SECRET_ACCESS_KEY', None):
        client_kwargs['aws_access_key_id'] = _config.AWS_ACCESS_KEY_ID or None
        client_kwargs['aws_secret_access_key'] = _config.AWS_SECRET_ACCESS_KEY or None

    try:
        sig = getattr(_config, 'S3_SIGNATURE_VERSION', 's3v4')
        boto_cfg = BotoConfig(signature_version=sig)
        s3 = boto3.client('s3', config=boto_cfg, **client_kwargs)
    except Exception:
        logger.exception("Failed to create S3 client for backend")
        return None

    class _S3BackendSync:
        def __init__(self, s3_client, bucket_name):
            self.s3 = s3_client
            self.bucket = bucket_name

        def upload_file(self, file_path: str, key: str, ExtraArgs: dict | None = None):
            extra = ExtraArgs or {}
            try:
                orig_size = str(os.path.getsize(file_path))
            except Exception:
                orig_size = "0"
            # ensure metadata keys exist
            md = extra.get('Metadata', {})
            md.setdefault('orig_filename', os.path.basename(key) if key else os.path.basename(file_path))
            md.setdefault('orig_size', orig_size)
            extra['Metadata'] = md
            self.s3.upload_file(file_path, self.bucket, key, ExtraArgs=extra)

        def copy_key(self, src_key: str, dest_key: str):
            copy_source = {'Bucket': self.bucket, 'Key': src_key}
            self.s3.copy_object(Bucket=self.bucket, CopySource=copy_source, Key=dest_key)

        def _upload_bytes_sync(self, data: bytes, dest_key: str) -> str:
            extra = {'Metadata': {'orig_size': str(len(data))}}
            self.s3.put_object(Bucket=self.bucket, Key=dest_key, Body=data, ExtraArgs=extra)
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

        async def upload_file(self, file_path: str, key: str, ExtraArgs: dict | None = None):
            import asyncio

            return await asyncio.to_thread(self._b.upload_file, file_path, key, ExtraArgs or None)

        async def copy_key(self, src_key: str, dest_key: str):
            import asyncio

            return await asyncio.to_thread(self._b.copy_key, src_key, dest_key)

        async def upload_bytes(self, data: bytes, dest_key: str) -> str:
            import asyncio

            return await asyncio.to_thread(self._b._upload_bytes_sync, data, dest_key)

    return _AsyncBackend(backend)
