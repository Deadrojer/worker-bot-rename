"""
helper/utils.py
────────────────
Shared utility functions.
"""

import os
from datetime import datetime
from pytz import timezone
from config import Config


def humanbytes(size: int | float) -> str:
    """Convert bytes to human-readable string."""
    if not size:
        return "0 B"
    for unit in (" B", " KB", " MB", " GB", " TB"):
        if abs(size) < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f} PB"


def TimeFormatter(milliseconds: int) -> str:
    """Convert milliseconds to human-readable duration string."""
    seconds, ms = divmod(int(milliseconds), 1000)
    minutes, s  = divmod(seconds, 60)
    hours, m    = divmod(minutes, 60)
    days, h     = divmod(hours, 24)
    parts = []
    if days:    parts.append(f"{days}d")
    if h:       parts.append(f"{h}h")
    if m:       parts.append(f"{m}m")
    if s:       parts.append(f"{s}s")
    if not parts and ms:
        parts.append(f"{ms}ms")
    return " ".join(parts) or "0s"


def convert(seconds: int) -> str:
    """Convert seconds to HH:MM:SS string."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}:{m:02d}:{s:02d}"


async def send_log(bot, user) -> None:
    """Log a new user to LOG_CHANNEL."""
    if not Config.LOG_CHANNEL:
        return
    ist  = datetime.now(timezone("Asia/Kolkata"))
    try:
        await bot.send_message(
            Config.LOG_CHANNEL,
            f"◈ <b>New User</b>\n\n"
            f"<blockquote>"
            f"Name  →  {user.mention}\n"
            f"ID    →  <code>{user.id}</code>\n"
            f"User  →  @{user.username or 'N/A'}\n"
            f"Date  →  {ist.strftime('%d %B %Y  %I:%M %p')}"
            f"</blockquote>\n"
            f"<a href='tg://openmessage?user_id={user.id}'>Open Chat</a>",
        )
    except Exception:
        pass


def add_prefix_suffix(name: str, prefix: str = "", suffix: str = "") -> str:
    """
    Apply prefix/suffix to a filename while preserving the extension.
    Prefix is prepended to the stem, suffix is appended before the extension.
    """
    prefix = (prefix or "").strip() or None
    suffix = (suffix or "").strip() or None

    if not prefix and not suffix:
        return name

    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext
    else:
        stem, ext = name, ""

    result = stem
    if prefix:
        result = prefix + result
    if suffix:
        result = result + " " + suffix

    return result + ext
