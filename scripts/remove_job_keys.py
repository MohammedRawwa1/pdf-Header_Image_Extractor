#!/usr/bin/env python3
"""Remove Redis keys matching a job id (used to clear stuck RQ intermediate keys).
Usage: python scripts/remove_job_keys.py <jobid>
"""

import os
import sys

try:
    import redis
except Exception:
    print("redis package not installed; install redis (pip install redis)")
    sys.exit(1)


def load_env(path=".env"):
    env = {}
    if os.path.exists(path):
        for raw in open(path).read().splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("\"'")
    return env


env = load_env()
REDIS_URL = os.environ.get("REDIS_URL") or env.get("REDIS_URL")
if not REDIS_URL:
    print("No REDIS_URL found in environment or .env")
    sys.exit(1)

jobid = None
if len(sys.argv) > 1:
    jobid = sys.argv[1]
else:
    print("Usage: python scripts/remove_job_keys.py <jobid>")
    sys.exit(1)

print("Using REDIS_URL:", REDIS_URL)
print("Target job id:", jobid)

r = redis.from_url(REDIS_URL, decode_responses=False)
keys = r.keys("*" + jobid + "*")
print("Matching keys count:", len(keys))
for k in keys:
    try:
        print(" -", k)
    except Exception:
        print(" - (unprintable key)")

if not keys:
    print("No keys found. Exiting.")
    sys.exit(0)

print("\nDeleting matched keys...")
for k in keys:
    try:
        r.delete(k)
        print("Deleted", k)
    except Exception as e:
        print("Failed to delete", k, e)

print("Done")
