# PDF Header / Cover Image Extractor Bot

A Telegram bot that extracts a header/cover image (first PDF page) and generates thumbnails for PDFs and images. Supports large PDFs (>50MB) via userbot fallback and runs as a FastAPI webhook service on Render.

## Features

- **PDF Thumbnail Extraction** — Automatically selects the best page as cover image
- **Automatic Thumb Detection** — Uses visual entropy to pick the most relevant page
- **Big PDF Support** — Handles files >50MB via Telethon/Pyrogram userbot download
- **Progress Tracking** — Real-time download/upload progress bars
- **Keep-Alive Heartbeat** — Prevents Render free-tier spin-down (15min inactivity)
- **Background Worker** — In-process RQ worker for job processing
- **S3/R2 Storage** — Optional cloud storage fallback for large files
- **Owner-Only Security** — `/s` webhook commands protected by `OWNER_ID`

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | Telegram bot token |
| `WEBHOOK_URL` | Public URL for webhook (e.g. `https://your-app.onrender.com`) |

### Optional — Core

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_POLLING` | `false` | Use long-polling instead of webhooks (local dev) |
| `PORT` | `8000` | Service port |
| `HOST` | `0.0.0.0` | Bind host |
| `LOG_LEVEL` | `INFO` | Logging level |
| `TMP_DIR` | (system) | Temp directory for downloads |

### Optional — Security

| Variable | Default | Description |
|----------|---------|-------------|
| `OWNER_ID` | (empty) | Telegram user ID with full control over `/s` commands |
| `ADMIN_USERS` | (empty) | Comma-separated Telegram user IDs for admin access |
| `ADMIN_SECRET` | (empty) | HTTP admin secret for API endpoints |

### Optional — Big PDF Pipeline

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BACKEND` | `local` | Storage backend: `local`, `s3`, or `r2` |
| `STORAGE_PATH` | `./storage` | Local storage root |
| `S3_BUCKET` | (empty) | S3/R2 bucket name |
| `S3_ENDPOINT` | (empty) | S3-compatible endpoint URL |
| `S3_REGION` | (empty) | AWS region |
| `AWS_ACCESS_KEY_ID` | (empty) | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | (empty) | AWS secret key |
| `BOT_API_MAX_MB` | `50` | Telegram Bot API max file size in MB |

### Optional — Userbot (for files >50MB)

| Variable | Default | Description |
|----------|---------|-------------|
| `API_ID` | (empty) | Telegram API ID for userbot |
| `API_HASH` | (empty) | Telegram API hash for userbot |
| `PYROGRAM_SESSION` | (empty) | Pyrogram session string (preferred) |
| `TELETHON_SESSION` | (empty) | Telethon string session |

### Optional — Background Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | (empty) | Redis URL for job queue and caching |
| `RUN_WORKER_IN_PROC` | `false` | Run RQ worker inside web process |

### Optional — Keep-Alive

| Variable | Default | Description |
|----------|---------|-------------|
| `KEEP_ALIVE_URL` | (empty) | URL to ping for keep-alive (defaults to `WEBHOOK_URL`) |
| `KEEP_ALIVE_INTERVAL` | `600` | Seconds between pings (60–840) |
| `KEEP_ALIVE_DISABLED` | `false` | Disable keep-alive heartbeat |

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Show welcome message |
| `/help` | Show help and available commands |
| `/status` | Get bot status |
| `/setwebhook <url>` | **Owner-only** — Set webhook URL |
| `/delwebhook` | **Owner-only** — Delete webhook |
| `/setcommands` | **Owner-only** — Set bot command list |
| `/startbatch` | Start collecting forwarded files |
| `/endbatch` | Process collected batch |
| `/cancelbatch` | Cancel batch collection |

## HTTP API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/health` | GET | None | Health check |
| `/status` | GET | Admin | Bot status |
| `/webhook/{token}` | POST | Telegram | Webhook endpoint |
| `/set_webhook` | POST | Owner + Admin | Set webhook URL |
| `/delete_webhook` | POST | Owner + Admin | Delete webhook |
| `/set_commands` | POST | Owner + Admin | Set bot commands |
| `/commands` | GET | Admin | List bot commands |
| `/admin/recache_thumbs` | POST | Admin | Recache thumbnails |
| `/admin/purge_s3` | POST | Admin | Purge old S3 objects |

### Owner-Only Headers

For `/set_webhook`, `/delete_webhook`, and `/set_commands`, include the header:
```
owner-id: <OWNER_ID>
```

## Architecture

```
┌─────────────────────────────────────────────┐
│  FastAPI (bot.py)                           │
│  ├── Telegram webhook endpoint              │
│  ├── Health / status endpoints              │
│  ├── Keep-alive heartbeat task              │
│  └── Background RQ worker (optional)        │
├─────────────────────────────────────────────┤
│  Telegram Bot API (python-telegram-bot)     │
│  ├── Document handler → thumbnail extract   │
│  ├── Photo handler → thumbnail              │
│  └── URL handler → download & extract       │
├─────────────────────────────────────────────┤
│  PDF Processing (tools.py)                  │
│  ├── PyMuPDF page rendering (300-600 DPI)   │
│  ├── Visual entropy page selection          │
│  └── Ghostscript compression (optional)     │
├─────────────────────────────────────────────┤
│  Big File Pipeline (utils/)                 │
│  ├── userbot_downloader (Telethon/Pyrogram) │
│  ├── userbot_uploader                       │
│  ├── bigfile_pipeline (S3 → Redis → Worker) │
│  └── storage (local / S3 / R2)              │
├─────────────────────────────────────────────┤
│  Background Worker (worker.py + tasks.py)   │
│  ├── RQ job queue (Redis)                   │
│  └── process_document_job                   │
└─────────────────────────────────────────────┘
```

## Local Development

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Set environment
export BOT_TOKEN="your-bot-token"
export USE_POLLING=true

# Run the bot
python bot.py
# or
uvicorn bot:app --host 127.0.0.1 --port 8000
```

## Docker

```bash
docker build -t pdf-bot:latest .
docker run \
  -e BOT_TOKEN="your-bot-token" \
  -e USE_POLLING=true \
  -p 8000:8000 \
  pdf-bot:latest
```

## Render Deployment

1. Push to GitHub
2. Create Web Service → Docker environment
3. Set env vars: `BOT_TOKEN`, `WEBHOOK_URL`, `USE_POLLING=false`
4. Deploy — webhook auto-set on startup

### Render Environment Variables

```yaml
envVars:
  - key: BOT_TOKEN
    sync: false  # secret
  - key: WEBHOOK_URL
    value: "https://your-app.onrender.com"
  - key: OWNER_ID
    sync: false  # your Telegram user ID
  - key: REDIS_URL
    sync: false  # from Render Redis addon
  - key: RUN_WORKER_IN_PROC
    value: "true"
  - key: KEEP_ALIVE_INTERVAL
    value: "600"
```

## Big PDF Pipeline

For files exceeding Telegram's 50MB Bot API limit:

1. Bot detects file size > `BOT_API_MAX_MB`
2. Routes through `BigFilePipeline` in `utils/bigfile_pipeline.py`
3. Downloads via Telethon/Pyrogram userbot
4. Uploads to S3/R2 storage
5. Enqueues Redis job for background processing
6. Worker extracts thumbnail and sends result

### Setup

```bash
# Set userbot credentials
export API_ID="12345"
export API_HASH="your-api-hash"
export PYROGRAM_SESSION="your-session-string"

# Set storage backend
export STORAGE_BACKEND="s3"
export S3_BUCKET="your-bucket"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

## Progress Tracking

The bot shows real-time progress bars for download/upload operations:

```
📊 PDF Processing Progress

📁 File: document.pdf
📏 Size: 12.5 MB / 50.0 MB
📈 Progress: 25.0%
🟩🟩⬜⬜⬜⬜⬜⬜⬜⬜

⏱️ Elapsed: 15s
⏳ Remaining: 45s
📥 Status: Downloading

🆔 Task: a1b2c3d4
```

## File Structure

```
├── bot.py              # FastAPI app + Telegram handlers
├── config.py           # Environment configuration
├── tools.py            # PDF thumbnail extraction (PyMuPDF)
├── tasks.py            # Background job processing (RQ)
├── worker.py           # RQ worker process
├── storage.py          # S3/local storage backend
├── requirements.txt    # Python dependencies
├── Dockerfile          # Docker build
├── render.yaml         # Render deployment config
├── utils/
│   ├── __init__.py
│   ├── telethon_session.py    # Telethon/Pyrogram session mgmt
│   ├── userbot_downloader.py  # Big file download via userbot
│   ├── userbot_uploader.py    # Big file upload via userbot
│   ├── bigfile_pipeline.py    # Orchestrates big file flow
│   ├── cache.py               # Redis async cache
│   ├── job_queue.py           # Redis job queue
│   ├── progress_tracker.py    # Download/upload progress bars
│   ├── webhook_monitor.py     # Webhook health monitoring
│   ├── forward_store.py       # Forward metadata storage
│   └── telethon_mongo.py      # Telethon-MongoDB bridge
├── scripts/
│   └── telethon_ingest.py     # Standalone Telethon ingestion
└── media_conersion_bot/       # Reference implementation (not used)
```

## License

See [LICENSE](LICENSE) for details.
