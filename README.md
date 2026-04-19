# PDF Header / Cover Image Extractor Bot

This repository provides a Telegram bot that extracts a header/cover image (first PDF page) and generates thumbnails for PDFs and images. It runs as a FastAPI webhook service (or polling for local development) and is packaged with Docker for deployment on Render or similar platforms.

**Files added**
- `bot.py` — FastAPI app + Telegram `Dispatcher` and webhook endpoint.
- `tools.py` — utilities for creating thumbnails from PDF/image files (PyMuPDF required).
- `Dockerfile` — Docker image using `PyMuPDF` (no poppler required).
- `render.yaml` — sample Render service manifest (replace placeholders).
- `python-version.txt` — chosen Python version for this project.

**Environment variables**
- `BOT_TOKEN` (required) — Telegram bot token.
- `WEBHOOK_URL` (optional) — public URL (e.g. https://my-app.onrender.com) used to set Telegram webhook on startup.
- `USE_POLLING` (optional) — set to `true` to use long-polling instead of webhooks (useful for local/dev).
- `PORT` (optional) — service port (default 8000).
- `HOST` (optional) — host to bind the server to (default `0.0.0.0`).
- `ADMIN_USERS` (optional) — comma-separated Telegram user IDs allowed to run admin bot commands (e.g. `12345,67890`). If empty, use `ADMIN_SECRET` to secure HTTP admin endpoints.
- `ADMIN_SECRET` (optional) — HTTP admin secret used to protect REST admin endpoints (set a strong random value).
- `LOG_CHANNEL` (optional) — Telegram channel ID to send logs/messages to.
- `SENTRY_DSN` (optional) — Sentry DSN for error reporting.
- `REDIS_URL` (optional) — Redis URL used for background queues (e.g. `redis://redis:6379/0`).
- `MAX_FILE_SIZE` (optional) — maximum allowed upload size in bytes (default `52428800` = 50MB).
- `TMP_DIR` (optional) — directory to use for temporary downloads (defaults to system temp dir).

Local development

1. Install deps in a virtualenv:

```bash
python -m pip install -r requirements.txt
export BOT_TOKEN="<your-bot-token>"
export USE_POLLING=true
python bot.py
```

2. Alternatively run with `uvicorn` directly:

```bash
uvicorn bot:app --host 0.0.0.0 --port 8000
```

Docker (build & run)

```bash
docker build -t pdf-bot:latest .
docker run -e BOT_TOKEN="<your-bot-token>" -e USE_POLLING=true -p 8000:8000 pdf-bot:latest
```

Render deployment (high-level)

1. Push this repo to GitHub.
2. In Render: Create a new service → select **Web Service** → connect your GitHub repo.
3. Choose **Docker** as the environment (Render will use `Dockerfile`).
4. Set environment variables in Render: `BOT_TOKEN` (secret), `WEBHOOK_URL` (your service URL without trailing `/webhook/...`), and optionally `USE_POLLING`.
5. Deploy. The app will set the webhook to `${WEBHOOK_URL}/webhook/${BOT_TOKEN}` on startup when `WEBHOOK_URL` is provided.

Notes

- The project requires `PyMuPDF` (`fitz`) for PDF rendering. The provided `Dockerfile` installs `PyMuPDF` via pip and does not require `poppler`.
- The project requires `PyMuPDF` (`fitz`) for PDF rendering. The provided `Dockerfile` installs `PyMuPDF` via pip and does not require `poppler`.

System dependency: Ghostscript
--------------------------------

The PDF compression helper (`tools.compress_pdf`) calls the `gs` (Ghostscript) binary. Ghostscript is a system package (not a Python package) and must be available in the runtime image. The included `Dockerfile` installs Ghostscript; if you build/run locally or on another host, install Ghostscript for your platform:

- Debian/Ubuntu (including Docker images based on `python:<tag>-slim`):

```bash
apt-get update && apt-get install -y --no-install-recommends ghostscript
```

- Alpine (if you use an Alpine base):

```bash
apk add --no-cache ghostscript
```

No extra Python packages are required for compression beyond the existing `requirements.txt` (it already includes `PyMuPDF` and `Pillow`). If you later add S3 upload fallback, you'll need to add `boto3` to `requirements.txt` and provide AWS credentials in the environment.

Large files and forwarded content

- The bot streams downloads directly to disk (using `aiohttp` + `aiofiles`), so it can handle very large PDFs without loading them fully into memory. There is no hard upper limit in code — limits are determined by available disk space and platform.
- Forwarded messages containing documents are handled automatically (the `Document` field is detected even when forwarded).
- If a forwarded message contains a link to a PDF (or you send a message with a PDF URL), the bot will attempt to download and extract the first page as a thumbnail.

Webhook behavior

- The webhook endpoint schedules update processing in the background so webhook requests return quickly (prevents Telegram webhook timeouts). Heavy work (downloads, rendering) runs asynchronously off the request path.


API Endpoints

- `POST /webhook/{token}` — Telegram webhook endpoint (used by Telegram to POST updates). The token path segment must match `BOT_TOKEN`.
- `GET /health` — simple health check, returns `{ "ok": true }`.
- `GET /status` — returns bot status; include header `X-ADMIN-TOKEN` with `ADMIN_SECRET` to get webhook info.
- `GET /commands` — admin-only (use `X-ADMIN-TOKEN`) — list current bot commands.
- `POST /set_webhook` — admin-only, JSON body `{ "url": "https://example.com" }` — sets webhook to `${url}/webhook/${BOT_TOKEN}`.
- `POST /delete_webhook` — admin-only — deletes webhook.
- `POST /set_commands` — admin-only, JSON body `{ "commands": [{"command":"start","description":"..."}] }` — sets bot command list.

Bot commands (Telegram)

- `/start` — start the bot and receive usage info.
- `/help` — display help and available commands.
- `/status` — returns basic status (admin users see webhook info).
- `/setwebhook <url>` — admin-only, set webhook URL (equivalent to `POST /set_webhook`).
- `/delwebhook` — admin-only, delete webhook (equivalent to `POST /delete_webhook`).
- `/setcommands` — admin-only, set default command list on Telegram.

Helper scripts

- `manage_commands.py` — CLI helper to set the default bot commands. Run with `BOT_TOKEN` in env to push the command list to Telegram.

