#!/usr/bin/env python3
"""Clear stuck jobs from BOTH background pipelines, for consistency.

The project runs two independent background pipes:

  1. RQ worker pipe (``worker.py``)
       - Queue lists: ``rq:queue:*`` (list of job ids)
       - Job hashes:  ``rq:job:<id>``
       - Registries:  ``rq:wip:*`` / ``rq:started:*`` / ``rq:finished:*`` /
                      ``rq:failed:*`` / ``rq:scheduled:*`` / ``rq:deferred:*`` /
                      ``rq:canceled:*``

  2. BigFile pipeline pipe (``pipeline_worker.py``)
       - Queue list:  ``pdf:jobs`` (list of JSON job dicts)
       - Delayed set: ``pdf:delayed`` (zset of JSON job dicts)
       - Job hashes:  ``pdf:job:<id>``
       - Progress:    ``pdf:progress:<id>``
       - S3 inputs:   objects under ``inputs/<job_id>/`` (uploaded by
                      ``utils/bigfile_pipeline.py`` as ``inputs/<job_id>/source``)

Both pipes also share per-job bookkeeping keys (``progress:<id>``,
``io:in:<id>``, ``io:out:<id>``, ``cancel:<id>``).

When clearing jobs, the corresponding S3 input objects are purged too so a
cleared job leaves no orphaned blobs behind (requires S3_BUCKET to be set;
otherwise the S3 purge is skipped with a notice).

Usage:
  python scripts/clear_jobs.py list                  # show both queues
  python scripts/clear_jobs.py clear <job_id>        # remove one job from both pipes (+ S3 inputs)
  python scripts/clear_jobs.py clear --all --yes     # empty BOTH queues entirely (+ all S3 inputs)
"""

import argparse
import json
import os
import sys

try:
    import redis
except Exception:
    print("redis package not installed; install redis (pip install redis)")
    sys.exit(1)

# Keys that belong to the RQ worker pipe.
RQ_QUEUE_PATTERN = "rq:queue:*"
RQ_JOB_PREFIX = "rq:job:"
RQ_REGISTRY_PATTERNS = (
    "rq:wip:*",
    "rq:started:*",
    "rq:finished:*",
    "rq:failed:*",
    "rq:scheduled:*",
    "rq:deferred:*",
    "rq:canceled:*",
)

# Keys that belong to the BigFile pipeline pipe.
PDF_JOB_LIST = "pdf:jobs"
PDF_DELAYED = "pdf:delayed"
PDF_JOB_PREFIX = "pdf:job:"
PDF_PROGRESS_PATTERN = "pdf:progress:*"

# Shared per-job bookkeeping keys (used by /canceljob and tasks.py).
COMMON_JOB_PREFIXES = ("progress:", "io:in:", "io:out:", "cancel:")

# S3 prefix under which BigFilePipeline stores input blobs (see
# utils/bigfile_pipeline.py: ``inputs/<job_id>/source``).
S3_INPUTS_PREFIX = "inputs/"


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


def get_redis():
    env = load_env()
    redis_url = os.environ.get("REDIS_URL") or env.get("REDIS_URL")
    if not redis_url:
        print("No REDIS_URL found in environment or .env")
        sys.exit(1)
    return redis.from_url(redis_url, decode_responses=True), redis_url


def _s3_client():
    """Build a boto3 S3 client from env/.env.

    Mirrors the client construction in ``utils/storage.py`` so the script
    behaves identically to the app (S3_ENDPOINT / R2 path-style / session
    tokens honored). Returns ``(client, bucket)``, or ``(None, reason)``
    where ``reason`` explains why S3 cleanup is unavailable.
    """
    env = load_env()

    def _env(name: str) -> str:
        return (os.environ.get(name) or env.get(name) or "").strip()

    bucket = _env("S3_BUCKET")
    if not bucket:
        return None, "S3_BUCKET is not set"
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        return None, "boto3 is not installed"

    client_kwargs = {}
    if _env("S3_REGION"):
        client_kwargs["region_name"] = _env("S3_REGION")
    if _env("S3_ENDPOINT"):
        client_kwargs["endpoint_url"] = _env("S3_ENDPOINT")
    if _env("AWS_ACCESS_KEY_ID") or _env("AWS_SECRET_ACCESS_KEY"):
        client_kwargs["aws_access_key_id"] = _env(
            "AWS_ACCESS_KEY_ID"
        ) or None
        client_kwargs["aws_secret_access_key"] = _env(
            "AWS_SECRET_ACCESS_KEY"
        ) or None
    if _env("AWS_SESSION_TOKEN"):
        client_kwargs["aws_session_token"] = _env("AWS_SESSION_TOKEN")
    try:
        sig = _env("S3_SIGNATURE_VERSION") or "s3v4"
        force_path = _env("S3_FORCE_PATH_STYLE").lower() in (
            "1",
            "true",
            "yes",
        )
        if force_path:
            boto_cfg = BotoConfig(
                signature_version=sig,
                s3={"addressing_style": "path"},
            )
        else:
            boto_cfg = BotoConfig(signature_version=sig)
        return boto3.client("s3", config=boto_cfg, **client_kwargs), bucket
    except Exception as e:
        return None, f"S3 client creation failed: {e}"


def purge_s3_inputs(prefix: str) -> int:
    """Delete all S3 objects under ``prefix`` (e.g. ``inputs/<job_id>/``).

    Returns the number of objects deleted (0 when S3 is unavailable or the
    prefix has no objects). Never raises — best-effort cleanup.
    """
    client, bucket = _s3_client()
    if client is None:
        # On failure the second tuple element carries the skip reason.
        print(f"S3 input purge skipped: {bucket}")
        return 0
    deleted = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                try:
                    client.delete_object(Bucket=bucket, Key=key)
                    deleted += 1
                    print(f"S3: deleted input object {key}")
                except Exception as e:
                    print(f"S3: failed to delete {key}: {e}")
    except Exception as e:
        print(f"S3: failed listing/purging '{prefix}': {e}")
    return deleted


def _fmt(key) -> str:
    """Decode bytes keys/values for printing."""
    if isinstance(key, (bytes, bytearray)):
        return key.decode("utf-8", "replace")
    return str(key)


def list_jobs(r) -> None:
    """Print a summary of both pipes' queues and their contents."""
    print("=== RQ worker pipe (rq:queue:*) ===")
    queue_keys = sorted(_fmt(k) for k in r.keys(RQ_QUEUE_PATTERN))
    if not queue_keys:
        print("  (no RQ queues)")
    for q in queue_keys:
        try:
            length = r.llen(q)
        except Exception:
            length = "N/A"
        print(f"  {q}  length={length}")
        if isinstance(length, int) and length > 0:
            for jid in r.lrange(q, 0, 19):
                print(f"    - {_fmt(jid)}")

    print("\n=== BigFile pipeline pipe (pdf:jobs / pdf:delayed) ===")
    try:
        pdf_len = r.llen(PDF_JOB_LIST)
    except Exception:
        pdf_len = 0
    print(f"  {PDF_JOB_LIST}  length={pdf_len}")
    if pdf_len > 0:
        for raw in r.lrange(PDF_JOB_LIST, 0, 19):
            try:
                d = json.loads(raw)
                jid = d.get("job_id", "?")
                name = d.get("original_filename", "?")
                print(f"    - {jid}  ({name})")
            except Exception:
                print(f"    - {_fmt(raw)}")
    try:
        delayed_len = r.zcard(PDF_DELAYED)
    except Exception:
        delayed_len = 0
    print(f"  {PDF_DELAYED}  length={delayed_len}")

    n_pdf_hashes = len(list(r.scan_iter(f"{PDF_JOB_PREFIX}*", count=100)))
    n_pdf_progress = len(list(r.scan_iter(PDF_PROGRESS_PATTERN, count=100)))
    print(f"  {PDF_JOB_PREFIX}* hashes: {n_pdf_hashes}")
    print(f"  {PDF_PROGRESS_PATTERN} keys: {n_pdf_progress}")


def _job_in_pdf_queue(raw, job_id: str) -> bool:
    """Return True if a raw pdf:jobs/pdf:delayed entry matches ``job_id``."""
    try:
        d = json.loads(raw)
        return d.get("job_id") == job_id
    except Exception:
        return False


def clear_job(r, job_id: str) -> int:
    """Remove one job from BOTH pipes. Returns number of keys/entries touched."""
    removed = 0

    # ── RQ pipe ──────────────────────────────────────────────
    for q in r.keys(RQ_QUEUE_PATTERN):
        try:
            n = r.lrem(q, 0, job_id)
            if n:
                print(f"Removed {n} occurrence(s) of {job_id} from {_fmt(q)}")
                removed += n
        except Exception as e:
            print(f"Failed to remove from {_fmt(q)}: {e}")
    for pattern in RQ_REGISTRY_PATTERNS:
        for key in r.keys(pattern):
            try:
                if r.zrem(key, job_id):
                    print(f"Removed {job_id} from registry {_fmt(key)}")
                    removed += 1
            except Exception as e:
                print(f"Failed to update registry {_fmt(key)}: {e}")
    job_key = f"{RQ_JOB_PREFIX}{job_id}"
    if r.exists(job_key):
        r.delete(job_key)
        print(f"Deleted job hash {job_key}")
        removed += 1

    # ── BigFile pipeline pipe ────────────────────────────────
    for raw in r.lrange(PDF_JOB_LIST, 0, -1):
        if _job_in_pdf_queue(raw, job_id):
            r.lrem(PDF_JOB_LIST, 0, raw)
            print(f"Removed {job_id} from {PDF_JOB_LIST}")
            removed += 1
    # pdf:delayed entries are JSON too; remove any matching entry
    for raw in r.zrange(PDF_DELAYED, 0, -1):
        if _job_in_pdf_queue(raw, job_id):
            r.zrem(PDF_DELAYED, raw)
            print(f"Removed {job_id} from {PDF_DELAYED}")
            removed += 1
    for key in (f"{PDF_JOB_PREFIX}{job_id}", f"pdf:progress:{job_id}"):
        if r.exists(key):
            r.delete(key)
            print(f"Deleted {key}")
            removed += 1

    # ── Shared bookkeeping keys ──────────────────────────────
    for prefix in COMMON_JOB_PREFIXES:
        key = f"{prefix}{job_id}"
        if r.exists(key):
            r.delete(key)
            print(f"Deleted {key}")
            removed += 1

    # ── S3 input objects (inputs/<job_id>/*) ─────────────────
    # Purge unconditionally by prefix (not gated on the job being found in
    # pdf:jobs): the goal is to also catch orphaned S3 inputs whose Redis
    # trace is already gone. For RQ-only jobs the prefix simply matches
    # nothing and the purge is a no-op.
    removed += purge_s3_inputs(f"{S3_INPUTS_PREFIX}{job_id}/")

    return removed


def clear_all(r, yes: bool) -> int:
    """Empty BOTH queues plus all job-related keys. Returns number of keys removed."""
    if not yes:
        answer = input(
            "This will delete ALL queued/in-flight jobs from BOTH pipes "
            "(rq:queue:*, rq:job:*, pdf:jobs, pdf:delayed, pdf:job:*, "
            "pdf:progress:*, progress:*, io:in:*, io:out:*, cancel:*)\n"
            "AND all S3 input objects under 'inputs/' (if S3 is configured).\n"
            "Type 'yes' to continue: "
        )
        if answer.strip().lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    removed = 0

    # RQ pipe: queue lists + registries + job hashes.
    # (rq:workers / rq:worker:* heartbeats are intentionally left alone.)
    for pattern in (
        RQ_QUEUE_PATTERN,
        *RQ_REGISTRY_PATTERNS,
        f"{RQ_JOB_PREFIX}*",
    ):
        for key in list(r.keys(pattern)):
            try:
                r.delete(key)
                print(f"Deleted {_fmt(key)}")
                removed += 1
            except Exception as e:
                print(f"Failed to delete {_fmt(key)}: {e}")

    # BigFile pipe: queue list + delayed set + hashes + progress keys.
    for key in (PDF_JOB_LIST, PDF_DELAYED):
        if r.exists(key):
            r.delete(key)
            print(f"Deleted {key}")
            removed += 1
    for pattern in (f"{PDF_JOB_PREFIX}*", PDF_PROGRESS_PATTERN):
        for key in list(r.keys(pattern)):
            try:
                r.delete(key)
                print(f"Deleted {_fmt(key)}")
                removed += 1
            except Exception as e:
                print(f"Failed to delete {_fmt(key)}: {e}")

    # Shared bookkeeping keys.
    for prefix in COMMON_JOB_PREFIXES:
        for key in list(r.keys(f"{prefix}*")):
            try:
                r.delete(key)
                print(f"Deleted {_fmt(key)}")
                removed += 1
            except Exception as e:
                print(f"Failed to delete {_fmt(key)}: {e}")

    # ── S3 input objects (inputs/*) ──────────────────────────
    removed += purge_s3_inputs(S3_INPUTS_PREFIX)

    return removed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear stuck jobs from BOTH background pipelines "
        "(RQ worker + BigFile pipeline worker)."
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="Show jobs in both queues")

    clear = sub.add_parser("clear", help="Remove job(s) from both pipes")
    clear.add_argument("job_id", nargs="?", help="Job id to remove from both pipes")
    clear.add_argument(
        "--all", action="store_true", help="Clear BOTH queues entirely"
    )
    clear.add_argument(
        "--yes", action="store_true", help="Skip confirmation for --all"
    )

    args = parser.parse_args()

    r, redis_url = get_redis()
    print("Using REDIS_URL:", redis_url)

    if not args.command or args.command == "list":
        list_jobs(r)
        return

    if args.command == "clear":
        if args.all:
            removed = clear_all(r, args.yes)
            print(f"Done. Removed {removed} keys/entries from both pipes.")
        elif args.job_id:
            removed = clear_job(r, args.job_id)
            if removed == 0:
                print(f"Job {args.job_id} not found in either pipe.")
            else:
                print(f"Done. Removed {removed} key(s)/entry(ies) for {args.job_id}.")
        else:
            parser.error("clear requires a <job_id> or --all")
    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
