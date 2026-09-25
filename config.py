"""
worker/config.py
══════════════════════════════════════════════════════════════════════════════
All configuration for one Worker instance, loaded from environment variables.

Each Worker deployment needs a unique BOT_TOKEN and WORKER_ID.
No String Session required — the worker communicates via its bot token only.
The worker bot must be added as a member of WORKER_CONTROL_GROUP_ID.
══════════════════════════════════════════════════════════════════════════════
"""

import os


class Config:
    # ── Telegram ──────────────────────────────────────────────────────────────
    API_ID   = int(os.environ.get("API_ID", "20140875"))
    API_HASH = os.environ.get("API_HASH", "a06fa97d5a853ec2da79015b11335a17")

    # Each worker has its own Bot Token
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # ── Worker identity ───────────────────────────────────────────────────────
    WORKER_ID          = os.environ.get("WORKER_ID", "")
    WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "3"))
    WORKER_VERSION     = os.environ.get("WORKER_VERSION", "1.0.0")

    # ── Channels / Groups ─────────────────────────────────────────────────────
    WORKER_CONTROL_GROUP_ID  = int(os.environ.get("WORKER_CONTROL_GROUP_ID",  "-1004454437880"))
    WORKER_OUTPUT_CHANNEL_ID = int(os.environ.get("WORKER_OUTPUT_CHANNEL_ID", "-1004488266962"))

    # ── Database ──────────────────────────────────────────────────────────────
    MONGO_URI = os.environ.get("MONGO_URI", "")
    DB_NAME   = os.environ.get("DB_NAME", "DistributedRenameBot")

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    HEARTBEAT_INTERVAL   = int(os.environ.get("HEARTBEAT_INTERVAL",   "30"))   # seconds
    WORKER_OFFLINE_TIMEOUT = int(os.environ.get("WORKER_OFFLINE_TIMEOUT", "120"))

    # ── ImgBB (thumbnail) ─────────────────────────────────────────────────────
    IMGBB_API_KEY = os.environ.get("IMGBB_API_KEY", "7c884ffafafa0846a595d70b373be802")

    # ── File size limits ──────────────────────────────────────────────────────
    BOT_MAX_SIZE  = 2000 * 1024 * 1024   # 2 GB standard bot limit
    USER_MAX_SIZE = 4000 * 1024 * 1024   # 4 GB premium / userbot limit

    # ── Health check ─────────────────────────────────────────────────────────
    PORT = int(os.environ.get("PORT", "8015"))

    # ── Graceful shutdown ────────────────────────────────────────────────────
    SHUTDOWN_GRACE_SECONDS = int(os.environ.get("SHUTDOWN_GRACE_SECONDS", "60"))

    BOT_UPTIME = __import__("time").time()
