#!/usr/bin/env python3
"""Remove a job id from any rq:queue:* list and delete its job key.
Usage: python scripts/dequeue_job.py <jobid>
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

if len(sys.argv) < 2:
    print("Usage: python scripts/dequeue_job.py <jobid>")
    sys.exit(1)

jobid = sys.argv[1]
print("Using REDIS_URL:", REDIS_URL)
print("Target job id:", jobid)

r = redis.from_url(REDIS_URL, decode_responses=False)
# remove from any rq:queue:* lists
queue_keys = r.keys("rq:queue:*")
removed_from = []
for q in queue_keys:
    try:
        # only try list types
        if r.type(q) != b"list" and r.type(q) != "list":
            continue
        before = r.llen(q)
        removed = r.lrem(q, 0, jobid)
        after = r.llen(q)
        if removed:
            removed_from.append((q, removed, before, after))
    except Exception as e:
        print("Failed to inspect/remove from", q, e)

if removed_from:
    for q, removed, before, after in removed_from:
        print(
            f"Removed {removed} occurrence(s) from {q} (len {before} -> {after})"
        )
else:
    print("Job id not found in any rq:queue:* lists")

# delete job hash and related keys if present
job_key = f"rq:job:{jobid}"
if r.exists(job_key):
    r.delete(job_key)
    print("Deleted job hash", job_key)
else:
    print("Job hash not found:", job_key)

# also try to delete intermediate keys and first_seen keys
patterns = [f"*{jobid}*"]
extra_keys = r.keys(patterns[0])
for k in extra_keys:
    # skip queue keys already handled
    if k.startswith(b"rq:queue:"):
        continue
    try:
        r.delete(k)
        print("Deleted extra key", k)
    except Exception as e:
        print("Failed to delete extra key", k, e)

print("Done")
