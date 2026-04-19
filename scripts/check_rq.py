#!/usr/bin/env python3
import os, sys
from pathlib import Path

def load_env(path='.env'):
    env={}
    p=Path(path)
    if p.exists():
        for raw in p.read_text().splitlines():
            line=raw.strip()
            if not line or line.startswith('#'): continue
            if '=' not in line: continue
            k,v=line.split('=',1)
            env[k.strip()]=v.strip().strip('"').strip("'")
    return env

env = load_env()
REDIS_URL = env.get('REDIS_URL') or os.environ.get('REDIS_URL')
if not REDIS_URL:
    print('No REDIS_URL found in .env or environment')
    sys.exit(0)
print('REDIS_URL:', REDIS_URL)

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
    for k in queue_keys[:1]:
        ids = r.lrange(k, 0, -1)
        print(f'Sample jobs in {k} ({len(ids)}):')
        for jid in ids[:10]:
            print(' -', jid)
            try:
                job = r.hgetall(f'rq:job:{jid}')
                print('   job fields:', list(job.keys()))
            except Exception as e:
                print('   failed reading job data:', e)
except Exception as e:
    print('Error connecting to Redis:', e)
    import traceback; traceback.print_exc()
