#!/usr/bin/env python3
"""Inspect Redis io:in:{id} and io:out:{id} keys for a job.

Usage:
  python scripts/io_inspect.py <id> [--redis REDIS_URL] [--raw]

The script reads `REDIS_URL` from environment if `--redis` is not provided.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

try:
    import redis
except Exception:
    print("Missing dependency: redis. Install with `pip install redis`.", file=sys.stderr)
    raise


def fetch_and_parse(r: redis.Redis, key: str) -> Any:
    v = r.get(key)
    if v is None:
        return None
    # redis-py with decode_responses=True returns str already
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode('utf-8')
        except Exception:
            # return raw bytes if decode fails
            return v
    try:
        return json.loads(v)
    except Exception:
        return v


def main() -> int:
    p = argparse.ArgumentParser(description="Inspect io:in/io:out Redis keys for a job")
    p.add_argument("id", help="Unique id (file_id or file_unique_id) to inspect")
    p.add_argument("--redis", help="Redis URL (e.g. redis://:pass@host:6379/0). Defaults to $REDIS_URL")
    p.add_argument("--raw", action="store_true", help="Print Python reprs instead of pretty JSON")
    args = p.parse_args()

    redis_url = args.redis or os.getenv("REDIS_URL")
    if not redis_url:
        print("Redis URL not provided. Set REDIS_URL env var or pass --redis", file=sys.stderr)
        return 2

    try:
        r = redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        print(f"Failed to connect to Redis: {e}", file=sys.stderr)
        return 3

    key_in = f"io:in:{args.id}"
    key_out = f"io:out:{args.id}"

    in_val = fetch_and_parse(r, key_in)
    out_val = fetch_and_parse(r, key_out)

    def pretty(obj: Any) -> str:
        try:
            return json.dumps(obj, indent=2, ensure_ascii=False)
        except Exception:
            return repr(obj)

    print(f"Key: {key_in}")
    if in_val is None:
        print("  <missing>")
    else:
        print(pretty(in_val) if not args.raw else repr(in_val))

    print(f"\nKey: {key_out}")
    if out_val is None:
        print("  <missing>")
    else:
        print(pretty(out_val) if not args.raw else repr(out_val))

    # also print TTLs for convenience
    try:
        ttl_in = r.ttl(key_in)
        ttl_out = r.ttl(key_out)
        print(f"\nTTLs (seconds): {key_in}={ttl_in}, {key_out}={ttl_out}")
    except Exception:
        pass

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
