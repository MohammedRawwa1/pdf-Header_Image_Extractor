#!/usr/bin/env python3
"""Check Redis / RQ connectivity and list queues/workers.

This script will try to use `redis` Python package to inspect RQ keys.
If `redis` is not installed, it falls back to a TCP connectivity check.
"""

import os
import sys
import socket
from pathlib import Path


def load_env(path='.env'):
    env = {}
    p = Path(path)
    if p.exists():
        for raw in p.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


env = load_env()
REDIS_URL = env.get('REDIS_URL') or os.environ.get('REDIS_URL')
if not REDIS_URL:
    print('No REDIS_URL found in .env or environment')
    sys.exit(0)

print('REDIS_URL:', REDIS_URL)

# Quick TCP check
try:
    from urllib.parse import urlparse
    parsed = urlparse(REDIS_URL)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 6379
except Exception:
    host = 'localhost'
    port = 6379

print(f'Checking TCP connect to {host}:{port} ...', end=' ')
try:
    s = socket.create_connection((host, port), timeout=5)
    s.close()
    print('OK')
except Exception as e:
    print('FAILED', e)

try:
    import redis

    r = redis.from_url(REDIS_URL, decode_responses=True)
    print('PING ->', r.ping())
    queue_keys = r.keys('rq:queue:*')
    if not queue_keys:
        print('No RQ queues found (rq:queue:*).')
    else:
        for k in queue_keys:
            try:
                length = r.llen(k)
            except Exception:
                length = 'N/A'
            print(f'Queue {k} length={length}')
    workers = r.smembers('rq:workers')
    print('rq:workers set members count ->', len(workers))
    if workers:
        print('Workers:', workers)
    worker_keys = r.keys('rq:worker:*')
    print('Worker keys:', worker_keys)
    # show sample job ids
    for k in (queue_keys[:1] if queue_keys else []):
        ids = r.lrange(k, 0, -1)
        print(f'Sample jobs in {k} ({len(ids)}):')
        for jid in ids[:10]:
            print(' -', jid)
            try:
                job = r.hgetall(f'rq:job:{jid}')
                print('   job fields:', list(job.keys()))
            except Exception as e:
                print('   failed reading job data:', e)
except ModuleNotFoundError:
    print('redis Python package not installed; cannot inspect queues via redis-py.')
except Exception as e:
    print('Error connecting to Redis via redis-py:', e)
    import traceback

    traceback.print_exc()
