#!/usr/bin/env python3
"""Check Redis / RQ connectivity and list queues/workers.

This script will try to use `redis` Python package to inspect RQ keys.
If `redis` is not installed, it falls back to a TCP connectivity check.
"""

import os
import socket
import sys
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
        # Filter to keys that are actually lists to avoid WRONGTYPE errors
        queue_list_keys = []
        for k in queue_keys:
            try:
                ktype = r.type(k)
            except Exception:
                ktype = None
            # decode bytes if necessary
            if isinstance(ktype, (bytes, bytearray)):
                try:
                    ktype = ktype.decode('utf-8')
                except Exception:
                    ktype = str(ktype)
            if ktype != 'list':
                print(f'Queue {k} type={ktype} (skipping)')
                continue
            try:
                length = r.llen(k)
            except Exception:
                length = 'N/A'
            print(f'Queue {k} length={length}')
            queue_list_keys.append(k)
    workers = r.smembers('rq:workers')
    print('rq:workers set members count ->', len(workers))
    if workers:
        print('Workers:', workers)
    worker_keys = r.keys('rq:worker:*')
    print('Worker keys:', worker_keys)
    # show sample job ids (only from keys that are lists)
    for k in (queue_list_keys[:1] if queue_keys else []):
        ids = r.lrange(k, 0, -1)
        print(f'Sample jobs in {k} ({len(ids)}):')
        for jid in ids[:10]:
            print(' -', jid)
            try:
                # RQ job data can be stored as binary (pickled). Use a binary-safe
                # redis client to fetch raw fields and avoid UTF-8 decode errors.
                r_bin = redis.from_url(REDIS_URL, decode_responses=False)
                job_raw = r_bin.hgetall(f'rq:job:{jid}')
                if not job_raw:
                    print('   job not found or empty')
                else:
                    fields = []
                    for kk, vv in job_raw.items():
                        try:
                            key_str = kk.decode('utf-8') if isinstance(kk, (bytes, bytearray)) else str(kk)
                        except Exception:
                            key_str = repr(kk)
                        if isinstance(vv, (bytes, bytearray)):
                            # Try to decode short values for readability, otherwise show size
                            try:
                                val_str = vv.decode('utf-8')
                                # truncate long values
                                if len(val_str) > 200:
                                    val_str = val_str[:200] + '...'
                                fields.append(f'{key_str}: {val_str}')
                            except Exception:
                                fields.append(f'{key_str}: <binary {len(vv)} bytes>')
                        else:
                            fields.append(f'{key_str}: {vv}')
                    print('   job fields:')
                    for f in fields:
                        print('    -', f)
                # Try to fetch the job using RQ's Job.fetch to decode pickled data
                try:
                    from rq.job import Job

                    # Use a binary-safe redis connection for RQ
                    r_conn = redis.from_url(REDIS_URL)
                    job_obj = Job.fetch(jid, connection=r_conn)
                    print('   RQ Job.fetch:')
                    print('    - func_name:', job_obj.func_name)
                    print('    - args:', job_obj.args)
                    print('    - kwargs:', job_obj.kwargs)
                except Exception as e:
                    print('   RQ Job.fetch failed:', e)
            except Exception as e:
                print('   failed reading job data:', e)
except ModuleNotFoundError:
    print('redis Python package not installed; cannot inspect queues via redis-py.')
except Exception as e:
    print('Error connecting to Redis via redis-py:', e)
    import traceback

    traceback.print_exc()
