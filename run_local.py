from pathlib import Path
import os


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
        os.environ[k.strip()] = v
    return env


if __name__ == "__main__":
    # Load environment from .env (if present) without printing secrets
    load_env_file()
    # Prevent the application from trying to set a remote webhook during local tests
    os.environ.pop("WEBHOOK_URL", None)

    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("bot:app", host="127.0.0.1", port=port)
