"""Asynchronous storage backend abstraction.

Provides a small async-friendly wrapper for local filesystem storage and
S3/S3-compatible (e.g. Cloudflare R2) using `aioboto3`.

Usage example:
    from utils.storage import get_storage_backend

    storage = await get_storage_backend()
    await storage.upload_file("/tmp/video.mp4", "uploads/job123/video.mp4")
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Any

try:
    import aioboto3
except Exception:  # pragma: no cover - aioboto3 may be optional
    aioboto3 = None

try:
    import boto3
except Exception:
    boto3 = None

try:
    from botocore.config import Config as BotoConfig
except Exception:
    BotoConfig = None

import config

logger = logging.getLogger(__name__)


class AsyncStorageBackend(ABC):
    @abstractmethod
    async def upload_file(self, src_path: str, dest_key: str) -> str:
        """Upload a local file at `src_path` to storage and return the storage key or path."""

    @abstractmethod
    async def download_file(self, key: str, dest_path: str) -> bool:
        """Download a storage object `key` to local `dest_path`. Return True on success."""

    @abstractmethod
    async def generate_presigned_post(
        self, key: str, expires: int | None = None
    ) -> dict[str, Any]:
        """Return a dict with presigned POST upload info (url/fields) or raise when unsupported."""

    @abstractmethod
    async def generate_presigned_get(
        self, key: str, expires: int | None = None
    ) -> str:
        """Return a presigned GET URL for `key` or raise when unsupported."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete object at `key` from storage. Return True if deleted or not found."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if object `key` exists in storage, False otherwise."""


class LocalStorageBackend(AsyncStorageBackend):
    def __init__(self, base_path: str | None = None):
        self.base = base_path or config.STORAGE_PATH

    def _abs_path(self, key: str) -> str:
        return os.path.join(self.base, key)

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        dest = self._abs_path(dest_key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src_path, dest)
        return dest

    async def download_file(self, key: str, dest_path: str) -> bool:
        src = self._abs_path(key)
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src, dest_path)
        return True

    async def generate_presigned_post(
        self, key: str, expires: int | None = None
    ) -> dict[str, Any]:
        raise NotImplementedError(
            "Presigned uploads are not supported for local backend"
        )

    async def generate_presigned_get(
        self, key: str, expires: int | None = None
    ) -> str:
        # Provide a file:// URL for convenience (may not be usable remotely)
        return "file://" + os.path.abspath(self._abs_path(key))

    async def delete(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            if os.path.exists(p):
                await asyncio.to_thread(os.remove, p)
                return True
            return True
        except Exception:
            return False

    async def exists(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            return os.path.exists(p)
        except Exception:
            return False


class S3AsyncBackend(AsyncStorageBackend):
    def __init__(
        self,
        bucket: str | None = None,
        endpoint_url: str | None = None,
        region: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        use_ssl: bool = True,
    ):
        # Support both async aioboto3 (preferred) and sync boto3 (fallback).
        self._use_aioboto3 = aioboto3 is not None

        self.bucket = bucket or config.S3_BUCKET
        self.endpoint_url = endpoint_url or (config.S3_ENDPOINT or None)
        self.region = region or (config.S3_REGION or None)
        self.aws_access_key_id = (
            aws_access_key_id or config.AWS_ACCESS_KEY_ID or None
        )
        self.aws_secret_access_key = (
            aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY or None
        )
        # Support temporary session tokens (AWS STS / assumed-role / R2 variants)
        self.aws_session_token = (
            aws_session_token or os.getenv("AWS_SESSION_TOKEN") or None
        )
        self.use_ssl = use_ssl

        # async session only when aioboto3 is available
        self._session = aioboto3.Session() if self._use_aioboto3 else None

        # optional botocore config (used for both aioboto3 and boto3 clients)
        self._boto_config = None
        if BotoConfig is not None:
            try:
                # Allow forcing path-style addressing for S3-compatible endpoints
                force_path = str(
                    os.getenv("S3_FORCE_PATH_STYLE", "")
                ).lower() in ("1", "true", "yes")
                if force_path:
                    try:
                        self._boto_config = BotoConfig(
                            signature_version="s3v4",
                            s3={"addressing_style": "path"},
                        )
                    except Exception:
                        self._boto_config = BotoConfig(
                            signature_version="s3v4"
                        )
                else:
                    self._boto_config = BotoConfig(signature_version="s3v4")
            except Exception:
                self._boto_config = None

    def _client_kwargs(self) -> dict[str, Any]:
        kw = {}
        if self.region:
            kw["region_name"] = self.region
        if self.endpoint_url:
            ep = str(self.endpoint_url).strip()
            if (
                ep
                and not ep.startswith("http://")
                and not ep.startswith("https://")
            ):
                scheme = "https" if self.use_ssl else "http"
                ep = f"{scheme}://{ep}"
            ep = ep.rstrip("/")
            kw["endpoint_url"] = ep
        if self.aws_access_key_id:
            kw["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kw["aws_secret_access_key"] = self.aws_secret_access_key
        if self._boto_config is not None:
            kw["config"] = self._boto_config
        if self.aws_session_token:
            kw["aws_session_token"] = self.aws_session_token
        return kw

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        if not src_path:
            raise ValueError(f"Invalid src_path: {src_path}")
        src_path = os.path.abspath(src_path)
        if not os.path.exists(src_path):
            raise ValueError(f"Invalid src_path: {src_path}")
        if not dest_key:
            raise ValueError("dest_key must not be empty")
        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(
                f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}"
            )

        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        for attempt in range(1, retries + 1):
            try:
                masked_key = None
                if self.aws_access_key_id:
                    ak = str(self.aws_access_key_id)
                    masked_key = f"{ak[:4]}...{ak[-4:]}" if len(ak) > 8 else ak
                else:
                    masked_key = "(env)"

                logger.info(
                    "Uploading file \u2192 bucket=%s key=%s (attempt %s/%s) [ak=%s endpoint=%s]",
                    self.bucket,
                    dest_key,
                    attempt,
                    retries,
                    masked_key,
                    (self.endpoint_url or "default"),
                )

                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.upload_file(
                            src_path, self.bucket, dest_key
                        )
                    return dest_key

                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required when aioboto3 is not installed"
                    )

                def _sync_upload():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.upload_file(src_path, self.bucket, dest_key)

                await asyncio.to_thread(_sync_upload)
                return dest_key

            except Exception as e:
                logger.warning(
                    "S3 upload failed (attempt %s/%s): %s",
                    attempt,
                    retries,
                    e,
                )
                if attempt == retries:
                    logger.exception(
                        "S3 upload failed permanently for key=%s", dest_key
                    )
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())  # nosec B311

    async def upload_file_streaming(self, src_path: str, dest_key: str) -> str:
        src_path = os.path.abspath(src_path)
        if not os.path.exists(src_path):
            raise ValueError(f"File not found: {src_path}")
        if self._use_aioboto3:
            async with self._session.client(
                "s3", **self._client_kwargs()
            ) as client:
                with open(src_path, "rb") as f:
                    await client.put_object(
                        Bucket=self.bucket, Key=dest_key, Body=f
                    )
            return dest_key
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required when aioboto3 is not installed"
            )

        def _sync():
            client = boto3.client("s3", **self._client_kwargs())
            with open(src_path, "rb") as f:
                client.put_object(Bucket=self.bucket, Key=dest_key, Body=f)

        await asyncio.to_thread(_sync)
        return dest_key

    async def upload_bytes(self, data: bytes, dest_key: str) -> str:
        """Upload bytes directly to S3 without writing to local disk first."""
        if not dest_key:
            raise ValueError("dest_key must not be empty")
        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(
                f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}"
            )

        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        for attempt in range(1, retries + 1):
            try:
                logger.info(
                    "Uploading bytes \u2192 bucket=%s key=%s (attempt %s/%s) size=%d",
                    self.bucket,
                    dest_key,
                    attempt,
                    retries,
                    len(data),
                )
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.put_object(
                            Bucket=self.bucket, Key=dest_key, Body=data
                        )
                    return dest_key
                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required when aioboto3 is not installed"
                    )

                def _sync():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.put_object(
                        Bucket=self.bucket, Key=dest_key, Body=data
                    )

                await asyncio.to_thread(_sync)
                return dest_key
            except Exception as e:
                logger.warning(
                    "S3 bytes upload failed (attempt %s/%s): %s",
                    attempt,
                    retries,
                    e,
                )
                if attempt == retries:
                    logger.exception(
                        "S3 bytes upload failed permanently for key=%s",
                        dest_key,
                    )
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())  # nosec B311

    async def download_file(self, key: str, dest_path: str) -> bool:
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.download_file(self.bucket, key, dest_path)
                    return True
                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required for S3 operations when aioboto3 is not installed"
                    )

                def _sync_download():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.download_file(self.bucket, key, dest_path)

                await asyncio.to_thread(_sync_download)
                return True
            except Exception as e:
                logger.warning(
                    "S3 download attempt %s/%s failed for key %s: %s",
                    attempt,
                    retries,
                    key,
                    e,
                )
                if attempt == retries:
                    logger.exception(
                        "S3 download failed after %s attempts for key %s",
                        retries,
                        key,
                    )
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())  # nosec B311

    async def generate_presigned_post(
        self, key: str, expires: int | None = None
    ) -> dict[str, Any]:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client(
                "s3", **self._client_kwargs()
            ) as client:
                post = client.generate_presigned_post(
                    Bucket=self.bucket, Key=key, ExpiresIn=expires
                )
                get_url = client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=expires * 24,
                )
            return {
                "url": post["url"],
                "fields": post["fields"],
                "key": key,
                "get_url": get_url,
            }
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required for S3 operations when aioboto3 is not installed"
            )

        def _sync_post():
            client = boto3.client("s3", **self._client_kwargs())
            post = client.generate_presigned_post(
                Bucket=self.bucket, Key=key, ExpiresIn=expires
            )
            get_url = client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires * 24,
            )
            return {
                "url": post["url"],
                "fields": post["fields"],
                "key": key,
                "get_url": get_url,
            }

        return await asyncio.to_thread(_sync_post)

    async def generate_presigned_get(
        self, key: str, expires: int | None = None
    ) -> str:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client(
                "s3", **self._client_kwargs()
            ) as client:
                url = client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=expires,
                )
            return url
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required for S3 operations when aioboto3 is not installed"
            )

        def _sync_get():
            client = boto3.client("s3", **self._client_kwargs())
            return client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires,
            )

        return await asyncio.to_thread(_sync_get)

    async def delete(self, key: str) -> bool:
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.delete_object(Bucket=self.bucket, Key=key)
                    return True
                if boto3 is None:
                    logger.error(
                        "boto3 is required for S3 operations when aioboto3 is not installed"
                    )
                    return False

                def _sync_delete():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.delete_object(Bucket=self.bucket, Key=key)

                await asyncio.to_thread(_sync_delete)
                return True
            except Exception as e:
                logger.warning(
                    "S3 delete attempt %s/%s failed for key %s: %s",
                    attempt,
                    retries,
                    key,
                    e,
                )
                if attempt == retries:
                    logger.exception(
                        "S3 delete failed after %s attempts for key %s",
                        retries,
                        key,
                    )
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())  # nosec B311

    async def exists(self, key: str) -> bool:
        if not key:
            return False
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.head_object(Bucket=self.bucket, Key=key)
                    return True
                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required for S3 operations when aioboto3 is not installed"
                    )

                def _sync_head():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.head_object(Bucket=self.bucket, Key=key)

                await asyncio.to_thread(_sync_head)
                return True
            except Exception as e:
                logger.debug(
                    "S3 head_object attempt %s/%s failed for key %s: %s",
                    attempt,
                    retries,
                    key,
                    e,
                )
                if attempt == retries:
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())  # nosec B311


_STORAGE_SINGLETON: AsyncStorageBackend | None = None


async def get_storage_backend() -> AsyncStorageBackend:
    """Return a shared AsyncStorageBackend instance based on configuration.

    This factory chooses between `local` and `s3`/`r2` backends depending on
    `config.STORAGE_BACKEND`. The result is cached for the lifetime of the
    process.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (
        os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local"
    ).lower()
    if backend in ("s3", "r2"):
        _STORAGE_SINGLETON = S3AsyncBackend(
            bucket=config.S3_BUCKET,
            endpoint_url=(
                os.getenv("S3_ENDPOINT") or config.S3_ENDPOINT or None
            ),
            region=(os.getenv("S3_REGION") or config.S3_REGION or None),
            aws_access_key_id=(
                os.getenv("AWS_ACCESS_KEY_ID")
                or config.AWS_ACCESS_KEY_ID
                or None
            ),
            aws_secret_access_key=(
                os.getenv("AWS_SECRET_ACCESS_KEY")
                or config.AWS_SECRET_ACCESS_KEY
                or None
            ),
            use_ssl=config.S3_USE_SSL,
        )
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(
            base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH)
        )

    return _STORAGE_SINGLETON


def get_storage_backend_sync() -> AsyncStorageBackend:
    """Synchronous convenience wrapper to obtain a backend without awaiting.

    Note: callers should prefer `await get_storage_backend()` where possible.
    This helper will create the same singleton but will raise if S3 backend
    requires `aioboto3` and it's not installed.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (
        os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local"
    ).lower()
    if backend in ("s3", "r2"):
        _STORAGE_SINGLETON = S3AsyncBackend(
            bucket=config.S3_BUCKET,
            endpoint_url=config.S3_ENDPOINT or None,
            region=config.S3_REGION or None,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY or None,
            use_ssl=config.S3_USE_SSL,
        )
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(
            base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH)
        )

    return _STORAGE_SINGLETON
