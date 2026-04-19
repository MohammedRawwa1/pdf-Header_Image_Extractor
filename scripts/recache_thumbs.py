#!/usr/bin/env python3
"""CLI: recache thumbnails stored in Redis where blob is missing.

Usage:
    python scripts/recache_thumbs.py --limit 100 --dry-run

Optional: pass --notify-chat <chat_id> to receive progress messages via Bot API.
"""
import argparse
import sys

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--limit', type=int, default=None, help='Maximum number of keys to scan')
    p.add_argument('--dry-run', action='store_true', help='Do not write back to cache')
    p.add_argument('--notify-chat', type=int, default=None, help='Admin chat id to receive progress updates')
    args = p.parse_args()

    try:
        import tasks
    except Exception as e:
        print('Failed to import tasks:', e, file=sys.stderr)
        sys.exit(2)

    res = tasks.recache_thumbs_job(admin_chat_id=args.notify_chat, limit=args.limit, dry_run=args.dry_run)
    print('Recache result:', res)
