import json
import os
import time
from pathlib import Path


def load_env_file(path: str = ".env") -> dict:
    env = {}
    p = Path(path)
    if not p.exists():
        return env
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        env[k.strip()] = v
    return env


def main():
    env = load_env_file()
    bot_token = env.get("BOT_TOKEN") or os.getenv("BOT_TOKEN")
    if not bot_token:
        print("BOT_TOKEN not found in .env or environment")
        return
    port = int(env.get("PORT", os.getenv("PORT", "8000")))

    post_url = f"http://127.0.0.1:{port}/webhook/{bot_token}"

    update = {
        "update_id": int(time.time()),
        "message": {
            "message_id": 1,
            "date": int(time.time()),
            "chat": {"id": int(env.get("TEST_CHAT_ID", "123456")), "type": "private"},
            "from": {"id": int(env.get("TEST_USER_ID", "123456")), "is_bot": False, "first_name": "Tester"},
            "text": "test webhook",
        },
    }

    data = json.dumps(update).encode("utf-8")

    try:
        # use stdlib to avoid extra deps
        from urllib.request import (  # nosec B310 - test script hitting localhost only
            Request,
            urlopen,
        )

        req = Request(post_url, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=10) as resp:  # nosec B310 - localhost-only test call  # noqa: S310
            print("Status:", resp.status)
            body = resp.read().decode()
            print("Response body:", body)
    except Exception as e:
        print("Error posting webhook:", e)


if __name__ == "__main__":
    main()
