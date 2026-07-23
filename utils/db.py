"""Unified MongoDB connection manager with Laravel-style prepared statement query builder.

Database: pdf_header_bot (configurable via MONGODB_NAME env var)

Collections:
  - job_metadata       : RQ job lifecycle, io:in/io:out, progress, errors
                        TTL: 7 days on created_at
  - user_sessions      : User interaction tracking (user_id, last_action, etc.)
                        TTL: 30 days on last_active
  - forward_batches    : /startbatch forwarded file batches
                        TTL: 24 hours on created_at
  - telethon_forwards  : Telethon userbot forward metadata
                        TTL: 7 days on created_at

Usage (Laravel-style prepared statements):
    from utils.db import query

    # Select with parameter binding
    job = await query(COL_JOBS).where('job_id', '=', job_id).first()
    jobs = await query(COL_JOBS).where('status', '=', 'active').limit(10).get()

    # Insert with field validation
    await query(COL_JOBS).insert({'job_id': '...', 'status': 'done'})

    # Update with parameter binding
    await query(COL_JOBS).where('job_id', '=', job_id).update({'status': 'done'})

    # Delete
    await query(COL_JOBS).where('job_id', '=', job_id).delete()

All operations use field whitelists and parameter binding to prevent NoSQL injection.
"""

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_client = None
_db = None

# ── Configuration ─────────────────────────────────────────────

MONGO_URI_KEYS = ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL")
DEFAULT_DB_NAME = "pdf_header_bot"

# ── Collection names (single source of truth) ─────────────────

COL_JOBS = "job_metadata"
COL_SESSIONS = "user_sessions"
COL_BATCHES = "forward_batches"
COL_TELETHON = "telethon_forwards"

# TTL in seconds
TTL_JOBS = 7 * 24 * 3600  # 7 days
TTL_SESSIONS = 30 * 24 * 3600  # 30 days
TTL_BATCHES = 24 * 3600  # 24 hours
TTL_TELETHON = 7 * 24 * 3600  # 7 days


# ═══════════════════════════════════════════════════════════════════
#  Laravel-style Query Builder with Prepared Statements
# ═══════════════════════════════════════════════════════════════════

# ── Field Whitelists (like Laravel's $fillable) ──────────────
# Each collection has a whitelist of allowed field names.
# Only these fields can be used in where(), select(), or set() calls.
# This prevents injection of arbitrary field names into MongoDB queries.

_ALLOWED_FIELDS: dict[str, set] = {
    COL_JOBS: {
        "job_id",
        "status",
        "created_at",
        "updated_at",
        "io_in",
        "io_out",
        "type",
        "unique_id",
        "error",
        "timestamps",
        "durations",
        "sizes",
        "s3",
        "tg_response",
        "file_id",
        "file_unique_id",
        "filename",
        "mime",
        "chat_id",
        "message_id",
        "forward_info",
        "enqueued_at",
        "bot_id",
    },
    COL_SESSIONS: {
        "user_id",
        "username",
        "first_name",
        "last_name",
        "chat_id",
        "chat_type",
        "last_action",
        "last_seen",
        "last_active",
        "created_at",
        "is_owner",
        "is_admin",
        "telethon_session",
        "pyrogram_session",
        "string_session",
        "logged_out",
        "logged_out_at",
    },
    COL_BATCHES: {
        "chat_id",
        "user_id",
        "items",
        "count",
        "created_at",
    },
    COL_TELETHON: {
        "job_id",
        "chat_id",
        "message_id",
        "file_id",
        "file_unique_id",
        "filename",
        "size",
        "mime",
        "input_key",
        "original_filename",
        "cleanup_input",
        "created_at",
    },
}

# Fields that are always allowed for internal operations (timestamps, metadata)
_INTERNAL_FIELDS = {"_id", "created_at", "updated_at", "last_active"}


def _validate_field(field: str, collection: str) -> str:
    """Validate a field name against the collection's whitelist.

    Like Laravel's query builder which only allows $fillable fields.
    Raises ValueError if the field is not in the whitelist.

    Args:
        field: Field name to validate.
        collection: Collection name to validate against.

    Returns:
        The validated field name.

    Raises:
        ValueError: If the field is not allowed.
    """
    # Internal fields are always allowed
    if field in _INTERNAL_FIELDS:
        return field
    whitelist = _ALLOWED_FIELDS.get(collection, set())
    if field not in whitelist:
        raise ValueError(
            f"NoSQL injection prevention: field '{field}' is not in the "
            f"whitelist for collection '{collection}'. Allowed fields: {sorted(whitelist)}"
        )
    return field


def _validate_value(value: Any) -> Any:
    """Sanitize a value to prevent MongoDB operator injection.

    - Strips $ from strings that look like MongoDB operators
    - Limits string length
    - Recursively sanitizes dicts and lists
    """
    if isinstance(value, str):
        # If a string looks like a MongoDB operator ($ne, $gt, etc.), strip it
        if value.startswith("$") and len(value) > 1 and value[1:].isalnum():
            logger.warning(
                "NoSQL injection prevention: stripped operator-like value '%s'",
                value,
            )
            return "_" + value[1:]
        # Limit string length to 10MB max (MongoDB BSON limit)
        return value[:10_000_000]
    elif isinstance(value, dict):
        return {
            k: _validate_value(v)
            for k, v in value.items()
            if not k.startswith("$")
        }  # Strip $ keys from dicts
    elif isinstance(value, list):
        return [_validate_value(v) for v in value]
    return value


def _sanitize_doc(doc: dict) -> dict:
    """Full document sanitization for insert/update operations.

    Strips $ keys, validates all field names, sanitizes values.
    """
    sanitized = {}
    for key, value in doc.items():
        if key.startswith("$"):
            logger.warning(
                "NoSQL injection prevention: stripped key starting with '$': '%s'",
                key,
            )
            continue
        safe_key = key.replace(".", "_")
        sanitized[safe_key] = _validate_value(value)
    return sanitized


class MongoQueryBuilder:
    """Laravel-style MongoDB query builder with prepared statement parameter binding.

    All queries use parameter binding through where() clauses —
    no raw dicts are ever injected into MongoDB query filters.
    Field names are validated against collection whitelists.

    Usage:
        # Select
        result = await query(COL_JOBS).where('job_id', '=', job_id).first()
        results = await query(COL_JOBS).where('status', '=', 'active').get()

        # Insert (uses field whitelist validation)
        await query(COL_JOBS).insert({'job_id': 'x', 'status': 'done'})

        # Update (uses parameter binding in where, field validation in set)
        await query(COL_JOBS).where('job_id', '=', job_id).update({'status': 'done'})

        # Delete
        await query(COL_JOBS).where('job_id', '=', job_id).delete()

        # Count
        count = await query(COL_JOBS).where('status', '=', 'active').count()
    """

    def __init__(self, collection: str, db_session=None):
        """Initialize the query builder for a specific collection.

        Args:
            collection: Collection name (e.g., COL_JOBS, COL_SESSIONS).
            db_session: Optional pre-resolved database session. If None,
                       one will be resolved on first operation via get_db().
        """
        self._collection = collection
        self._db = db_session
        self._filters: list[
            dict[str, Any]
        ] = []  # where clauses (parameterized)
        self._projection: dict[str, int] | None = None  # select fields
        self._sort_field: str | None = None
        self._sort_order: int = -1
        self._limit_count: int | None = None
        self._skip_count: int | None = None

    # ── Public API: Query building (method chaining) ──────────

    def where(
        self, field: str, operator: str, value: Any
    ) -> "MongoQueryBuilder":
        """Add a WHERE clause to the query (parameterized).

        Like Laravel: ->where('field', '=', value)

        Args:
            field: Field name (validated against whitelist).
            operator: Comparison operator ('=', '!=', '>', '<', '>=', '<=', 'in', 'nin').
            value: Bound parameter value (sanitized to prevent injection).

        Returns:
            The query builder instance for method chaining.
        """
        # Validate field name against whitelist
        _validate_field(field, self._collection)
        # Sanitize value to prevent operator injection
        safe_value = _validate_value(value)

        # Map Laravel-style operators to MongoDB operators
        op_map = {
            "=": "$eq",
            "!=": "$ne",
            ">": "$gt",
            ">=": "$gte",
            "<": "$lt",
            "<=": "$lte",
            "in": "$in",
            "nin": "$nin",
        }
        mongo_op = op_map.get(operator)
        if mongo_op is None:
            raise ValueError(
                f"Unsupported operator '{operator}'. "
                f"Use: =, !=, >, >=, <, <=, in, nin"
            )

        if operator == "=":
            # Simple equality (most common case)
            self._filters.append({field: safe_value})
        elif operator == "in":
            self._filters.append({field: {mongo_op: safe_value}})
        elif operator == "nin":
            self._filters.append({field: {mongo_op: safe_value}})
        else:
            self._filters.append({field: {mongo_op: safe_value}})

        return self

    def where_in(self, field: str, values: list[Any]) -> "MongoQueryBuilder":
        """Add a WHERE IN clause.

        Like Laravel: ->whereIn('field', [1, 2, 3])
        """
        return self.where(field, "in", values)

    def where_nin(self, field: str, values: list[Any]) -> "MongoQueryBuilder":
        """Add a WHERE NOT IN clause."""
        return self.where(field, "nin", values)

    def select(self, *fields: str) -> "MongoQueryBuilder":
        """Specify which fields to return (projection).

        Like Laravel: ->select('field1', 'field2')

        Automatically excludes _id unless explicitly included.
        """
        proj: dict[str, int] = {}
        for f in fields:
            _validate_field(f, self._collection)
            proj[f] = 1
        # Exclude _id by default unless explicitly asked for
        if "_id" not in proj:
            proj["_id"] = 0
        self._projection = proj
        return self

    def order_by(
        self, field: str, direction: str = "desc"
    ) -> "MongoQueryBuilder":
        """Add an ORDER BY clause.

        Like Laravel: ->orderBy('created_at', 'desc')
        """
        _validate_field(field, self._collection)
        self._sort_field = field
        self._sort_order = -1 if direction.lower() == "desc" else 1
        return self

    def limit(self, count: int) -> "MongoQueryBuilder":
        """Add a LIMIT clause.

        Like Laravel: ->limit(10)
        """
        self._limit_count = count
        return self

    def skip(self, count: int) -> "MongoQueryBuilder":
        """Add an OFFSET/SKIP clause.

        Like Laravel: ->skip(10) or ->offset(10)
        """
        self._skip_count = count
        return self

    # ── Terminal methods (execute the query) ──────────────────

    async def _resolve_db(self):
        """Resolve the database session if not already set."""
        if self._db is None:
            self._db = await get_db()
        return self._db is not None

    def _build_filter(self) -> dict[str, Any]:
        """Build the MongoDB filter dict from the parameterized where clauses.

        Each where() call appends a parameterized filter. They are combined
        with implicit $and (MongoDB default).
        """
        if not self._filters:
            return {}
        if len(self._filters) == 1:
            return self._filters[0]
        return {"$and": self._filters}

    def _validate_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Validate and sanitize a document for write operations.

        Like Laravel's $fillable protection — only allowed fields pass through.
        """
        validated = {}
        for key, value in doc.items():
            try:
                _validate_field(key, self._collection)
            except ValueError:
                logger.warning(
                    "MongoQueryBuilder: skipping field '%s' not in whitelist for '%s'",
                    key,
                    self._collection,
                )
                continue
            validated[key] = _validate_value(value)
        return validated

    async def first(self) -> dict[str, Any] | None:
        """Execute the query and return the first matching document.

        Like Laravel: ->first()
        """
        if not await self._resolve_db():
            return None
        try:
            query_filter = self._build_filter()
            cursor = self._db[self._collection].find(
                query_filter,
                self._projection or {"_id": 0},
            )
            if self._sort_field:
                cursor = cursor.sort(self._sort_field, self._sort_order)
            cursor = cursor.limit(1)
            return await cursor.to_list(length=1) or None
        except Exception as e:
            logger.debug("MongoQueryBuilder.first failed: %s", e)
            return None

    async def get(self) -> list[dict[str, Any]]:
        """Execute the query and return all matching documents.

        Like Laravel: ->get()
        """
        if not await self._resolve_db():
            return []
        try:
            query_filter = self._build_filter()
            cursor = self._db[self._collection].find(
                query_filter,
                self._projection or {"_id": 0},
            )
            if self._sort_field:
                cursor = cursor.sort(self._sort_field, self._sort_order)
            if self._limit_count:
                cursor = cursor.limit(self._limit_count)
            if self._skip_count:
                cursor = cursor.skip(self._skip_count)
            return await cursor.to_list(length=self._limit_count or 1000)
        except Exception as e:
            logger.debug("MongoQueryBuilder.get failed: %s", e)
            return []

    async def insert(self, data: dict[str, Any]) -> bool:
        """Insert a new document.

        Like Laravel: ->insert({...})

        Field validation against whitelist prevents injection of
        arbitrary fields or MongoDB operators.
        """
        if not await self._resolve_db():
            return False
        try:
            doc = self._validate_doc(data)
            if "created_at" not in doc:
                doc["created_at"] = time.time()
            result = await self._db[self._collection].insert_one(doc)
            return result.acknowledged
        except Exception as e:
            logger.debug("MongoQueryBuilder.insert failed: %s", e)
            return False

    async def update(self, data: dict[str, Any]) -> bool:
        """Update matching documents.

        Like Laravel: ->where(...)->update({...})

        Only fields in the whitelist are applied. Uses parameter binding
        in the WHERE clause and field validation in the SET data.
        """
        if not await self._resolve_db():
            return False
        try:
            set_data = self._validate_doc(data)
            if not set_data:
                logger.warning(
                    "MongoQueryBuilder.update: no valid fields to update"
                )
                return False
            set_data["updated_at"] = time.time()
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "MongoQueryBuilder.update: no WHERE clause — refusing to update all documents"
                )
                return False
            result = await self._db[self._collection].update_one(
                filter_query,
                {"$set": set_data},
            )
            return result.modified_count > 0 or result.upserted_id is not None
        except Exception as e:
            logger.debug("MongoQueryBuilder.update failed: %s", e)
            return False

    async def upsert(self, data: dict[str, Any]) -> bool:
        """Insert or update a document (upsert).

        Like Laravel's updateOrCreate.

        Uses the WHERE clause as the filter and the data as the $set payload.
        """
        if not await self._resolve_db():
            return False
        try:
            set_data = self._validate_doc(data)
            if not set_data:
                logger.warning(
                    "MongoQueryBuilder.upsert: no valid fields to set"
                )
                return False
            set_data["updated_at"] = time.time()
            if "created_at" not in set_data:
                set_data["created_at"] = time.time()
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "MongoQueryBuilder.upsert: no WHERE clause — refusing to upsert all documents"
                )
                return False
            result = await self._db[self._collection].update_one(
                filter_query,
                {"$set": set_data},
                upsert=True,
            )
            return result.acknowledged
        except Exception as e:
            logger.debug("MongoQueryBuilder.upsert failed: %s", e)
            return False

    async def delete(self) -> bool:
        """Delete matching documents.

        Like Laravel: ->where(...)->delete()

        Requires a WHERE clause to prevent accidental mass deletion.
        """
        if not await self._resolve_db():
            return False
        try:
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "MongoQueryBuilder.delete: no WHERE clause — refusing to delete all documents"
                )
                return False
            result = await self._db[self._collection].delete_one(filter_query)
            return result.deleted_count > 0
        except Exception as e:
            logger.debug("MongoQueryBuilder.delete failed: %s", e)
            return False

    async def delete_many(self) -> int:
        """Delete all matching documents.

        Requires a WHERE clause.
        Returns the number of deleted documents.
        """
        if not await self._resolve_db():
            return 0
        try:
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "MongoQueryBuilder.delete_many: no WHERE clause — refusing"
                )
                return 0
            result = await self._db[self._collection].delete_many(filter_query)
            return result.deleted_count
        except Exception as e:
            logger.debug("MongoQueryBuilder.delete_many failed: %s", e)
            return 0

    async def count(self) -> int:
        """Count matching documents.

        Like Laravel: ->where(...)->count()
        """
        if not await self._resolve_db():
            return 0
        try:
            query_filter = self._build_filter()
            return await self._db[self._collection].count_documents(
                query_filter
            )
        except Exception as e:
            logger.debug("MongoQueryBuilder.count failed: %s", e)
            return 0

    async def exists(self) -> bool:
        """Check if any matching documents exist.

        Like Laravel: ->where(...)->exists()
        """
        result = await self.limit(1).first()
        return result is not None

    async def pluck(self, field: str) -> list[Any]:
        """Retrieve a list of values for a single field.

        Like Laravel: ->pluck('field')
        """
        _validate_field(field, self._collection)
        results = await self.select(field).get()
        return [r[field] for r in results if field in r]

    async def value(self, field: str) -> Any | None:
        """Retrieve a single value from the first matching document.

        Like Laravel: ->value('field')
        """
        _validate_field(field, self._collection)
        result = await self.select(field).first()
        if result:
            return result.get(field)
        return None


def query(collection: str, db_session=None) -> MongoQueryBuilder:
    """Create a new query builder instance for the given collection.

    This is the entry point for all MongoDB queries.

    Like Laravel's DB::table('users')->where(...)->get()

    Args:
        collection: Collection name (e.g., COL_JOBS, COL_SESSIONS).
        db_session: Optional pre-resolved database session.

    Returns:
        A MongoQueryBuilder instance for method chaining.
    """
    return MongoQueryBuilder(collection, db_session=db_session)


# ═══════════════════════════════════════════════════════════════════
#  Sync MongoQueryBuilder (for RQ worker background tasks)
# ═══════════════════════════════════════════════════════════════════


class SyncMongoQueryBuilder:
    """Synchronous version of MongoQueryBuilder for RQ worker background tasks.

    Mirrors the async MongoQueryBuilder API but uses synchronous pymongo methods.
    All the same security features: field whitelists, parameter binding, $ operator prevention.

    Usage:
        from utils.db import sync_query, COL_JOBS

        # Upsert
        ok = sync_query(COL_JOBS, db).where('job_id', '=', uid).upsert(data)

        # Read
        doc = sync_query(COL_JOBS, db).where('job_id', '=', uid).first()
        docs = sync_query(COL_JOBS, db).where('status', '=', 'active').limit(10).get()

        # Update / Delete
        sync_query(COL_JOBS, db).where('job_id', '=', uid).update(data)
        sync_query(COL_JOBS, db).where('job_id', '=', uid).delete()

        # Count
        count = sync_query(COL_JOBS, db).where('status', '=', 'active').count()
    """

    def __init__(self, collection: str, db_session=None):
        self._collection = collection
        self._db = db_session
        self._filters: list[dict[str, Any]] = []
        self._projection: dict[str, int] | None = None
        self._sort_field: str | None = None
        self._sort_order: int = -1
        self._limit_count: int | None = None
        self._skip_count: int | None = None

    # ── Public API: Query building (method chaining) ──────────

    def where(
        self, field: str, operator: str, value: Any
    ) -> "SyncMongoQueryBuilder":
        """Add a WHERE clause with parameter binding."""
        _validate_field(field, self._collection)
        safe_value = _validate_value(value)

        op_map = {
            "=": "$eq",
            "!=": "$ne",
            ">": "$gt",
            ">=": "$gte",
            "<": "$lt",
            "<=": "$lte",
            "in": "$in",
            "nin": "$nin",
        }
        mongo_op = op_map.get(operator)
        if mongo_op is None:
            raise ValueError(
                f"Unsupported operator '{operator}'. "
                f"Use: =, !=, >, >=, <, <=, in, nin"
            )

        if operator == "=":
            self._filters.append({field: safe_value})
        elif operator in ("in", "nin"):
            self._filters.append({field: {mongo_op: safe_value}})
        else:
            self._filters.append({field: {mongo_op: safe_value}})

        return self

    def where_in(
        self, field: str, values: list[Any]
    ) -> "SyncMongoQueryBuilder":
        """Add a WHERE IN clause."""
        return self.where(field, "in", values)

    def where_nin(
        self, field: str, values: list[Any]
    ) -> "SyncMongoQueryBuilder":
        """Add a WHERE NOT IN clause."""
        return self.where(field, "nin", values)

    def select(self, *fields: str) -> "SyncMongoQueryBuilder":
        """Specify which fields to return (projection)."""
        proj: dict[str, int] = {}
        for f in fields:
            _validate_field(f, self._collection)
            proj[f] = 1
        if "_id" not in proj:
            proj["_id"] = 0
        self._projection = proj
        return self

    def order_by(
        self, field: str, direction: str = "desc"
    ) -> "SyncMongoQueryBuilder":
        """Add an ORDER BY clause."""
        _validate_field(field, self._collection)
        self._sort_field = field
        self._sort_order = -1 if direction.lower() == "desc" else 1
        return self

    def limit(self, count: int) -> "SyncMongoQueryBuilder":
        """Add a LIMIT clause."""
        self._limit_count = count
        return self

    def skip(self, count: int) -> "SyncMongoQueryBuilder":
        """Add an OFFSET/SKIP clause."""
        self._skip_count = count
        return self

    # ── Internal helpers ──────────────────────────────────────

    def _resolve_db(self) -> bool:
        """Resolve the database session synchronously."""
        if self._db is None:
            self._db = get_sync_db()
        return self._db is not None

    def _build_filter(self) -> dict[str, Any]:
        """Build the MongoDB filter dict from parameterized where clauses."""
        if not self._filters:
            return {}
        if len(self._filters) == 1:
            return self._filters[0]
        return {"$and": self._filters}

    def _validate_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Validate and sanitize a document for write operations.

        Like Laravel's $fillable protection — only whitelisted fields pass through.
        Also sanitizes values to prevent $ operator injection.
        """
        validated = {}
        for key, value in doc.items():
            try:
                _validate_field(key, self._collection)
            except ValueError:
                logger.warning(
                    "SyncMongoQueryBuilder: skipping field '%s' not in whitelist for '%s'",
                    key,
                    self._collection,
                )
                continue
            validated[key] = _validate_value(value)
        return validated

    # ── Terminal methods (execute the query) ──────────────────

    def first(self) -> dict[str, Any] | None:
        """Return the first matching document."""
        if not self._resolve_db():
            return None
        try:
            query_filter = self._build_filter()
            cursor = self._db[self._collection].find(
                query_filter,
                self._projection or {"_id": 0},
            )
            if self._sort_field:
                cursor = cursor.sort(self._sort_field, self._sort_order)
            cursor = cursor.limit(1)
            docs = list(cursor)
            return docs[0] if docs else None
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.first failed: %s", e)
            return None

    def get(self) -> list[dict[str, Any]]:
        """Return all matching documents."""
        if not self._resolve_db():
            return []
        try:
            query_filter = self._build_filter()
            cursor = self._db[self._collection].find(
                query_filter,
                self._projection or {"_id": 0},
            )
            if self._sort_field:
                cursor = cursor.sort(self._sort_field, self._sort_order)
            if self._limit_count:
                cursor = cursor.limit(self._limit_count)
            if self._skip_count:
                cursor = cursor.skip(self._skip_count)
            return list(cursor)
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.get failed: %s", e)
            return []

    def insert(self, data: dict[str, Any]) -> bool:
        """Insert a new document with field validation."""
        if not self._resolve_db():
            return False
        try:
            doc = self._validate_doc(data)
            if "created_at" not in doc:
                doc["created_at"] = time.time()
            result = self._db[self._collection].insert_one(doc)
            return result.acknowledged
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.insert failed: %s", e)
            return False

    def update(self, data: dict[str, Any]) -> bool:
        """Update matching documents. Requires a WHERE clause."""
        if not self._resolve_db():
            return False
        try:
            set_data = self._validate_doc(data)
            if not set_data:
                logger.warning(
                    "SyncMongoQueryBuilder.update: no valid fields to update"
                )
                return False
            set_data["updated_at"] = time.time()
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "SyncMongoQueryBuilder.update: no WHERE clause — refusing"
                )
                return False
            result = self._db[self._collection].update_one(
                filter_query, {"$set": set_data}
            )
            return result.modified_count > 0 or result.upserted_id is not None
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.update failed: %s", e)
            return False

    def upsert(self, data: dict[str, Any]) -> bool:
        """Insert or update a document (upsert). Requires a WHERE clause."""
        if not self._resolve_db():
            return False
        try:
            set_data = self._validate_doc(data)
            if not set_data:
                logger.warning(
                    "SyncMongoQueryBuilder.upsert: no valid fields to set"
                )
                return False
            set_data["updated_at"] = time.time()
            if "created_at" not in set_data:
                set_data["created_at"] = time.time()
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "SyncMongoQueryBuilder.upsert: no WHERE clause — refusing"
                )
                return False
            result = self._db[self._collection].update_one(
                filter_query,
                {"$set": set_data},
                upsert=True,
            )
            return result.acknowledged
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.upsert failed: %s", e)
            return False

    def delete(self) -> bool:
        """Delete matching documents. Requires a WHERE clause."""
        if not self._resolve_db():
            return False
        try:
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "SyncMongoQueryBuilder.delete: no WHERE clause — refusing"
                )
                return False
            result = self._db[self._collection].delete_one(filter_query)
            return result.deleted_count > 0
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.delete failed: %s", e)
            return False

    def delete_many(self) -> int:
        """Delete all matching documents. Requires a WHERE clause."""
        if not self._resolve_db():
            return 0
        try:
            filter_query = self._build_filter()
            if not filter_query:
                logger.warning(
                    "SyncMongoQueryBuilder.delete_many: no WHERE clause — refusing"
                )
                return 0
            result = self._db[self._collection].delete_many(filter_query)
            return result.deleted_count
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.delete_many failed: %s", e)
            return 0

    def count(self) -> int:
        """Count matching documents."""
        if not self._resolve_db():
            return 0
        try:
            query_filter = self._build_filter()
            return self._db[self._collection].count_documents(query_filter)
        except Exception as e:
            logger.debug("SyncMongoQueryBuilder.count failed: %s", e)
            return 0

    def exists(self) -> bool:
        """Check if any matching documents exist."""
        return self.limit(1).first() is not None

    def pluck(self, field: str) -> list[Any]:
        """Retrieve a list of values for a single field."""
        _validate_field(field, self._collection)
        results = self.select(field).get()
        return [r[field] for r in results if field in r]

    def value(self, field: str) -> Any | None:
        """Retrieve a single value from the first matching document."""
        _validate_field(field, self._collection)
        result = self.select(field).first()
        if result:
            return result.get(field)
        return None


def sync_query(collection: str, db_session=None) -> SyncMongoQueryBuilder:
    """Create a new synchronous query builder instance for the given collection.

    This is the sync entry point for RQ worker background tasks.
    Like the async `query()` function but uses pymongo instead of motor.

    Args:
        collection: Collection name (e.g., COL_JOBS, COL_SESSIONS).
        db_session: Optional pre-resolved pymongo database session.

    Returns:
        A SyncMongoQueryBuilder instance for method chaining.
    """
    return SyncMongoQueryBuilder(collection, db_session=db_session)


# ═══════════════════════════════════════════════════════════════════
#  End of Query Builder — the rest is unchanged connection management
# ═══════════════════════════════════════════════════════════════════

# ── Helpers ───────────────────────────────────────────────────


def get_db_name() -> str:
    """Return the MongoDB database name."""
    return os.environ.get("MONGODB_NAME", DEFAULT_DB_NAME)


def get_mongo_uri() -> str | None:
    """Return the MongoDB connection URI from environment."""
    for key in MONGO_URI_KEYS:
        val = os.environ.get(key, "").strip()
        if val:
            return val
    return None


async def get_db():
    """Return the shared motor database instance (lazy singleton)."""
    global _client, _db
    if _db is not None:
        return _db

    uri = get_mongo_uri()
    if not uri:
        logger.debug("db: no MONGO_URI configured; MongoDB disabled")
        return None

    try:
        from motor.motor_asyncio import AsyncIOMotorClient

        _client = AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        db_name = get_db_name()
        _db = _client[db_name]
        # Verify connectivity
        await _client.admin.command("ping")
        logger.info("db: MongoDB connected (db=%s)", db_name)
        # Create TTL indexes for automatic cleanup
        await _ensure_indexes(_db)
        return _db
    except Exception as e:
        logger.warning("db: MongoDB connection failed: %s", e)
        _client = None
        _db = None
        return None


async def _ensure_indexes(db):
    """Create TTL and unique indexes on all collections."""
    try:
        await db[COL_JOBS].create_index("job_id", unique=True, background=True)
        await db[COL_JOBS].create_index(
            "created_at", expireAfterSeconds=TTL_JOBS, background=True
        )
        await db[COL_SESSIONS].create_index(
            "user_id", unique=True, background=True
        )
        await db[COL_SESSIONS].create_index(
            "last_active", expireAfterSeconds=TTL_SESSIONS, background=True
        )
        await db[COL_BATCHES].create_index(
            "created_at", expireAfterSeconds=TTL_BATCHES, background=True
        )
        await db[COL_TELETHON].create_index(
            "created_at", expireAfterSeconds=TTL_TELETHON, background=True
        )
        logger.debug("db: indexes ensured on all collections")
    except Exception as e:
        logger.debug("db: index creation skipped: %s", e)


# ── Sync MongoDB client (for RQ worker background tasks) ────────

_sync_pymongo_client = None
_sync_pymongo_db = None


def get_sync_db():
    """Return a cached sync pymongo database (lazy singleton).

    Used by background workers (RQ) that need synchronous MongoDB access.
    Shares the same URI and db_name config as the async connection.
    """
    global _sync_pymongo_client, _sync_pymongo_db
    if _sync_pymongo_db is not None:
        return _sync_pymongo_db

    uri = get_mongo_uri()
    if not uri:
        logger.debug("db: no MONGO_URI configured; sync MongoDB disabled")
        return None

    try:
        import pymongo

        _sync_pymongo_client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        db_name = get_db_name()
        _sync_pymongo_db = _sync_pymongo_client[db_name]
        # Verify connectivity with a lightweight ping
        _sync_pymongo_client.admin.command("ping")
        logger.info("db: sync MongoDB connected (db=%s)", db_name)
        return _sync_pymongo_db
    except Exception as e:
        logger.warning("db: sync MongoDB connection failed: %s", e)
        _sync_pymongo_client = None
        _sync_pymongo_db = None
        return None


async def close_db():
    """Close async MongoDB connection."""
    global _client, _db
    if _client is not None:
        try:
            _client.close()
        except Exception:
            pass
    _client = None
    _db = None


def close_sync_db():
    """Close sync MongoDB connection."""
    global _sync_pymongo_client, _sync_pymongo_db
    if _sync_pymongo_client is not None:
        try:
            _sync_pymongo_client.close()
        except Exception:
            pass
    _sync_pymongo_client = None
    _sync_pymongo_db = None


# ═══════════════════════════════════════════════════════════════════
#  Domain-specific functions (refactored to use the query builder)
# ═══════════════════════════════════════════════════════════════════

# ── Job Metadata ──────────────────────────────────────────────
# Stores RQ job lifecycle, io metadata, progress, and errors.
# Keyed by job_id (unique).


async def save_job_metadata(job_id: str, meta: dict[str, Any]) -> bool:
    """Persist job metadata (io:in, io:out, progress) to MongoDB.

    Uses prepared statement parameter binding via the query builder.
    Only whitelisted fields are persisted.
    """
    db = await get_db()
    if not db:
        return False
    try:
        meta["job_id"] = str(job_id)
        return (
            await query(COL_JOBS, db)
            .where("job_id", "=", str(job_id))
            .upsert(meta)
        )
    except Exception as e:
        logger.debug("db: save_job_metadata failed for %s: %s", job_id, e)
        return False


async def get_job_metadata(job_id: str) -> dict[str, Any] | None:
    """Retrieve job metadata from MongoDB.

    Uses prepared statement parameter binding.
    """
    db = await get_db()
    if not db:
        return None
    try:
        return (
            await query(COL_JOBS, db).where("job_id", "=", str(job_id)).first()
        )
    except Exception as e:
        logger.debug("db: get_job_metadata failed for %s: %s", job_id, e)
        return None


async def update_job_metadata(job_id: str, fields: dict[str, Any]) -> bool:
    """Update specific fields in job metadata.

    Uses prepared statement parameter binding.
    Only whitelisted fields are updated.
    """
    db = await get_db()
    if not db:
        return False
    try:
        return (
            await query(COL_JOBS, db)
            .where("job_id", "=", str(job_id))
            .update(fields)
        )
    except Exception as e:
        logger.debug("db: update_job_metadata failed for %s: %s", job_id, e)
        return False


async def list_jobs(
    status: str | None = None, limit: int = 20
) -> list[dict[str, Any]]:
    """List recent jobs, optionally filtered by status.

    Uses prepared statement parameter binding for the status filter.
    Prevents NoSQL injection by validating the status against the whitelist.
    """
    db = await get_db()
    if not db:
        return []
    try:
        q = query(COL_JOBS, db).order_by("created_at", "desc").limit(limit)
        if status is not None:
            q = q.where("status", "=", str(status))
        return await q.get()
    except Exception:
        return []


async def count_jobs() -> int:
    """Count total jobs in MongoDB."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await query(COL_JOBS, db).count()
    except Exception:
        return 0


# ── User Sessions ─────────────────────────────────────────────
# Tracks user interactions: last action, last seen, role flags.
# Keyed by user_id (unique).


async def save_user_session(
    user_id: int, session_data: dict[str, Any]
) -> bool:
    """Persist user session data to MongoDB.

    Uses prepared statement parameter binding.
    Only whitelisted session fields are persisted.
    """
    db = await get_db()
    if not db:
        return False
    try:
        session_data["user_id"] = user_id
        return (
            await query(COL_SESSIONS, db)
            .where("user_id", "=", int(user_id))
            .upsert(session_data)
        )
    except Exception as e:
        logger.debug("db: save_user_session failed for %s: %s", user_id, e)
        return False


async def get_user_session(user_id: int) -> dict[str, Any] | None:
    """Retrieve user session from MongoDB.

    Uses prepared statement parameter binding.
    """
    db = await get_db()
    if not db:
        return None
    try:
        return (
            await query(COL_SESSIONS, db)
            .where("user_id", "=", int(user_id))
            .first()
        )
    except Exception as e:
        logger.debug("db: get_user_session failed for %s: %s", user_id, e)
        return None


async def list_sessions(limit: int = 50) -> list[dict[str, Any]]:
    """List recent user sessions."""
    db = await get_db()
    if not db:
        return []
    try:
        return (
            await query(COL_SESSIONS, db)
            .order_by("last_active", "desc")
            .limit(limit)
            .get()
        )
    except Exception:
        return []


async def count_sessions() -> int:
    """Count total user sessions in MongoDB."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await query(COL_SESSIONS, db).count()
    except Exception:
        return 0


# ── Forward Batches ───────────────────────────────────────────
# Stores /startbatch forwarded file lists.
# Keyed by (chat_id, user_id).


async def save_forward_batch(chat_id: int, user_id: int, items: list) -> bool:
    """Persist forward batch metadata to MongoDB.

    Uses prepared statement parameter binding.
    Only whitelisted fields are persisted.
    Items are sanitized to prevent NoSQL injection.
    """
    db = await get_db()
    if not db:
        return False
    try:
        doc = {
            "chat_id": chat_id,
            "user_id": user_id,
            "items": items,
            "count": len(items),
        }
        return (
            await query(COL_BATCHES, db)
            .where("chat_id", "=", int(chat_id))
            .where("user_id", "=", int(user_id))
            .upsert(doc)
        )
    except Exception as e:
        logger.debug("db: save_forward_batch failed: %s", e)
        return False


async def get_forward_batch(chat_id: int, user_id: int) -> list | None:
    """Retrieve forward batch items from MongoDB.

    Uses prepared statement parameter binding.
    """
    db = await get_db()
    if not db:
        return None
    try:
        result = (
            await query(COL_BATCHES, db)
            .where("chat_id", "=", int(chat_id))
            .where("user_id", "=", int(user_id))
            .select("items")
            .first()
        )
        return result.get("items") if result else None
    except Exception as e:
        logger.debug("db: get_forward_batch failed: %s", e)
        return None


async def delete_forward_batch(chat_id: int, user_id: int) -> bool:
    """Delete forward batch from MongoDB.

    Uses prepared statement parameter binding.
    """
    db = await get_db()
    if not db:
        return False
    try:
        return (
            await query(COL_BATCHES, db)
            .where("chat_id", "=", int(chat_id))
            .where("user_id", "=", int(user_id))
            .delete()
        )
    except Exception as e:
        logger.debug("db: delete_forward_batch failed: %s", e)
        return False


# ── Telethon Forwards ─────────────────────────────────────────
# Stores metadata from Telethon userbot ingestion.
# Keyed by job_id, appended (not upserted).


async def save_telethon_forward(job: dict) -> bool:
    """Save Telethon forward metadata to MongoDB.

    Uses prepared statement insert with field validation.
    Only whitelisted fields are persisted.
    """
    db = await get_db()
    if not db:
        return False
    try:
        result = await query(COL_TELETHON, db).insert(job)
        if result:
            logger.info("db: saved telethon forward %s", job.get("job_id"))
        return result
    except Exception as e:
        logger.debug("db: save_telethon_forward failed: %s", e)
        return False


async def count_telethon_forwards() -> int:
    """Count total Telethon forward records."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await query(COL_TELETHON, db).count()
    except Exception:
        return 0


# ── Diagnostics ───────────────────────────────────────────────


async def db_stats() -> dict[str, Any]:
    """Return a summary of all collection sizes."""
    db = await get_db()
    if not db:
        return {"connected": False, "db_name": get_db_name()}
    try:
        stats = {
            "connected": True,
            "db_name": get_db_name(),
            "collections": {},
        }
        for name in [COL_JOBS, COL_SESSIONS, COL_BATCHES, COL_TELETHON]:
            try:
                count = await query(name, db).count()
                stats["collections"][name] = count
            except Exception:
                stats["collections"][name] = -1
        return stats
    except Exception as e:
        return {"connected": False, "error": str(e)}
