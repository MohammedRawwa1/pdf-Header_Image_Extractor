from __future__ import annotations

import asyncio
import io
import logging
import os
import shutil
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

try:
    import aioboto3
except Exception:
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

ProgressCallback = Callable[[int, int], None] | None


class _TransferProgress:
    def __init__(self, total: int, callback: ProgressCallback):
        self.total = total or 0
        self.callback = callback
        self.seen = 0

    def __call__(self, bytes_amount: int) -> None:
        try:
            self.seen += int(bytes_amount or 0)
        except Exception:
            return
        if self.total and self.seen > self.total:
            self.seen = self.total
        if self.callback is None:
            return
        try:
            self.callback(self.seen, self.total)
        except Exception:
            pass


class AsyncStorageBackend(ABC):
    @abstractmethod
    async def upload_file(
        self,
        src_path: str,
        dest_key: str,
        progress_callback: ProgressCallback = None,
    ) -> str:
        pass

    @abstractmethod
    async def download_file(
        self,
        key: str,
        dest_path: str,
        progress_callback: ProgressCallback = None,
    ) -> bool:
        pass

    @abstractmethod
    async def generate_presigned_post(
        self, key: str, expires: int | None = None
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    async def generate_presigned_get(
        self, key: str, expires: int | None = None
    ) -> str:
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        pass


class LocalStorageBackend(AsyncStorageBackend):
    def __init__(self, base_path: str | None = None):
        self.base = base_path or config.STORAGE_PATH

    def _abs_path(self, key: str) -> str:
        return os.path.join(self.base, key)

    async def upload_file(
        self,
        src_path: str,
        dest_key: str,
        progress_callback: ProgressCallback = None,
    ) -> str:
        dest = self._abs_path(dest_key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if progress_callback is None:
            await asyncio.to_thread(shutil.copy2, src_path, dest)
            return dest
        try:
            total = os.path.getsize(src_path)
        except Exception:
            total = 0
        _cb = _TransferProgress(total, progress_callback)

        def _copy():
            with open(src_path, "rb") as f_src, open(dest, "wb") as f_dst:
                while True:
                    chunk = f_src.read(1024 * 1024)
                    if not chunk:
                        break
                    f_dst.write(chunk)
                    _cb(len(chunk))

        await asyncio.to_thread(_copy)
        return dest

    async def download_file(
        self,
        key: str,
        dest_path: str,
        progress_callback: ProgressCallback = None,
    ) -> bool:
        src = self._abs_path(key)
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        if progress_callback is None:
            await asyncio.to_thread(shutil.copy2, src, dest_path)
            return True
        try:
            total = os.path.getsize(src)
        except Exception:
            total = 0
        _cb = _TransferProgress(total, progress_callback)

        def _copy():
            with open(src, "rb") as f_src, open(dest_path, "wb") as f_dst:
                while True:
                    chunk = f_src.read(1024 * 1024)
                    if not chunk:
                        break
                    f_dst.write(chunk)
                    _cb(len(chunk))

        await asyncio.to_thread(_copy)
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
        return "file://" + os.path.abspath(self._abs_path(key))

    async def delete(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            if os.path.exists(p):
                await asyncio.to_thread(os.remove, p)
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
        self.aws_session_token = (
            aws_session_token or os.getenv("AWS_SESSION_TOKEN") or None
        )
        self.use_ssl = use_ssl
        self._session = aioboto3.Session() if self._use_aioboto3 else None
        self._boto_config = None
        if BotoConfig is not None:
            try:
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

    async def upload_file(
        self,
        src_path: str,
        dest_key: str,
        progress_callback: ProgressCallback = None,
    ) -> str:
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
        try:
            _total = os.path.getsize(src_path)
        except Exception:
            _total = 0
        _cb = _TransferProgress(_total, progress_callback)
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
                            src_path, self.bucket, dest_key, Callback=_cb
                        )
                    return dest_key
                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required when aioboto3 is not installed"
                    )

                def _sync_upload():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.upload_file(
                        src_path, self.bucket, dest_key, Callback=_cb
                    )

                await asyncio.to_thread(_sync_upload)
                return dest_key
            except Exception as e:
                logger.warning(
                    "S3 upload failed (attempt %s/%s): %s", attempt, retries, e
                )
                if attempt == retries:
                    logger.exception(
                        "S3 upload failed permanently for key=%s", dest_key
                    )
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())

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

    async def upload_bytes(
        self,
        data: bytes,
        dest_key: str,
        progress_callback: ProgressCallback = None,
    ) -> str:
        if not dest_key:
            raise ValueError("dest_key must not be empty")
        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(
                f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}"
            )
        _use_fileobj = progress_callback is not None
        _cb = _TransferProgress(len(data), progress_callback)
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
                        if _use_fileobj:
                            await client.upload_fileobj(
                                io.BytesIO(data),
                                self.bucket,
                                dest_key,
                                Callback=_cb,
                            )
                        else:
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
                    if _use_fileobj:
                        client.upload_fileobj(
                            io.BytesIO(data),
                            self.bucket,
                            dest_key,
                            Callback=_cb,
                        )
                    else:
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
                await asyncio.sleep(backoff + random.random())

    async def download_file(
        self,
        key: str,
        dest_path: str,
        progress_callback: ProgressCallback = None,
    ) -> bool:
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        import random

        _total = 0
        if progress_callback is not None:
            try:
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        _total = int(
                            (
                                await client.head_object(
                                    Bucket=self.bucket, Key=key
                                )
                            )["ContentLength"]
                        )
                elif boto3 is not None:
                    client = boto3.client("s3", **self._client_kwargs())
                    _total = int(
                        client.head_object(Bucket=self.bucket, Key=key)[
                            "ContentLength"
                        ]
                    )
            except Exception:
                _total = 0
        _cb = _TransferProgress(_total, progress_callback)

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client(
                        "s3", **self._client_kwargs()
                    ) as client:
                        await client.download_file(
                            self.bucket, key, dest_path, Callback=_cb
                        )
                    return True
                if boto3 is None:
                    raise RuntimeError(
                        "boto3 is required for S3 operations when aioboto3 is not installed"
                    )

                def _sync_download():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.download_file(
                        self.bucket, key, dest_path, Callback=_cb
                    )

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
                await asyncio.sleep(backoff + random.random())

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
                await asyncio.sleep(backoff + random.random())

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
                await asyncio.sleep(backoff + random.random())


_STORAGE_SINGLETON: AsyncStorageBackend | None = None


async def get_storage_backend() -> AsyncStorageBackend:
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON
    backend_type = getattr(config, "STORAGE_BACKEND", "local")
    if backend_type == "s3" or backend_type == "r2":
        _STORAGE_SINGLETON = S3AsyncBackend()
    else:
        _STORAGE_SINGLETON = LocalStorageBackend()
    return _STORAGE_SINGLETON
